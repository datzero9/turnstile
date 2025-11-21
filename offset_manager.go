package turnstile

import (
	"context"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"
)

type OffsetMessage struct {
	Topic     string
	Partition int
	Offset    int64
}

type CommitFunc func(ctx context.Context, message kafka.Message) error

type FetchOffsetFunc func(partition int) (int64, error)

type OffsetManager struct {
	topic          string
	commitFunc     CommitFunc
	fetchOffsetFunc FetchOffsetFunc
	logger         *zap.SugaredLogger
	minCommitCount int64
	maxInterval    time.Duration
	forceInterval  time.Duration
	maxRetries     int
	retryDelay     time.Duration

	seenPartitions               sync.Map // map[int]bool
	lastCommitOffsetByPartition   sync.Map // map[int]int64
	commitMutexByPartition        sync.Map // map[int]*sync.Mutex
	canCommitOffsetByPartition    sync.Map // map[int]int64
	lastCommitTimeByPartition     sync.Map // map[int]time.Time
	processingMessagesByPartition sync.Map // map[int]*sync.Map
}

type OffsetManagerConfig struct {
	Topic           string
	CommitFunc      CommitFunc
	FetchOffsetFunc FetchOffsetFunc
	Logger          *zap.SugaredLogger
	MinCommitCount  int64
	MaxInterval     time.Duration
	ForceInterval   time.Duration
	MaxRetries      int
	RetryDelay      time.Duration
}

func NewOffsetManager(config OffsetManagerConfig) *OffsetManager {
	return &OffsetManager{
		topic:                         config.Topic,
		commitFunc:                    config.CommitFunc,
		fetchOffsetFunc:               config.FetchOffsetFunc,
		logger:                        config.Logger,
		minCommitCount:                config.MinCommitCount,
		maxInterval:                   config.MaxInterval,
		forceInterval:                 config.ForceInterval,
		maxRetries:                    config.MaxRetries,
		retryDelay:                    config.RetryDelay,
		seenPartitions:                sync.Map{},
		lastCommitOffsetByPartition:   sync.Map{},
		commitMutexByPartition:        sync.Map{},
		processingMessagesByPartition: sync.Map{},
		canCommitOffsetByPartition:    sync.Map{},
		lastCommitTimeByPartition:     sync.Map{},
	}
}

// Track initializes partition offset tracking on first encounter and registers a message for tracking.
func (m *OffsetManager) Track(partition int, offset int64) error {
	if _, seen := m.seenPartitions.LoadOrStore(partition, true); !seen {
		m.logger.Infof("New partition detected: partition=%d", partition)

		committedOffset := int64(-1)
		if m.fetchOffsetFunc != nil {
			fetchedOffset, err := m.fetchOffsetFunc(partition)
			if err != nil {
				m.logger.Warnf("Failed to fetch committed offset for partition %d: %v, using default", partition, err)
			} else {
				committedOffset = fetchedOffset
			}
		}

		m.logger.Infof("Initializing partition %d with committed offset %d", partition, committedOffset)
		if committedOffset > 0 {
			m.InitPartitionOffset(partition, committedOffset)
		} else {
			// No committed offset: initialize watermarks just before the first seen message.
			mutex := m.getOrCreatePartitionMutex(partition)
			mutex.Lock()
			m.lastCommitOffsetByPartition.Store(partition, offset-1)
			m.canCommitOffsetByPartition.Store(partition, offset-1)
			m.lastCommitTimeByPartition.Store(partition, time.Now())
			mutex.Unlock()
		}
	}

	msgMap, _ := m.processingMessagesByPartition.LoadOrStore(partition, &sync.Map{})
	msgMap.(*sync.Map).Store(offset, false) // false = not yet done

	return nil
}

// MarkDone marks a message as completed and attempts to commit.
func (m *OffsetManager) MarkDone(ctx context.Context, partition int, offset int64) error {
	rawMsgMap, ok := m.processingMessagesByPartition.Load(partition)
	if !ok {
		return nil
	}

	msgMap := rawMsgMap.(*sync.Map)
	if _, ok := msgMap.Load(offset); !ok {
		return nil
	}

	msgMap.Store(offset, true) // true = done

	return m.commit(ctx, partition)
}

// InitPartitionOffset initializes the offset tracking for a partition based on the committed offset fetched from Kafka.
func (m *OffsetManager) InitPartitionOffset(partition int, offset int64) {
	mutex := m.getOrCreatePartitionMutex(partition)
	mutex.Lock()
	defer mutex.Unlock()

	if offset > 0 {
		m.lastCommitOffsetByPartition.Store(partition, offset-1)
		m.canCommitOffsetByPartition.Store(partition, offset-1)
	} else {
		m.lastCommitOffsetByPartition.Delete(partition)
		m.canCommitOffsetByPartition.Delete(partition)
	}
}

func (m *OffsetManager) getPartitionMutex(partition int) *sync.Mutex {
	rawMutex, ok := m.commitMutexByPartition.Load(partition)
	if !ok {
		return nil
	}
	return rawMutex.(*sync.Mutex)
}

func (m *OffsetManager) getOrCreatePartitionMutex(partition int) *sync.Mutex {
	rawMutex, _ := m.commitMutexByPartition.LoadOrStore(partition, &sync.Mutex{})
	return rawMutex.(*sync.Mutex)
}

// ForceCommit forces commit of all pending offsets.
func (m *OffsetManager) ForceCommit(ctx context.Context) {
	m.canCommitOffsetByPartition.Range(func(k, _ interface{}) bool {
		partition := k.(int)

		mutex := m.getPartitionMutex(partition)
		if mutex == nil {
			return true
		}
		mutex.Lock()
		defer mutex.Unlock()

		rawCanCommitOffset, _ := m.canCommitOffsetByPartition.Load(partition)
		canCommitOffset := rawCanCommitOffset.(int64)
		rawLastCommitOffset, _ := m.lastCommitOffsetByPartition.Load(partition)
		lastCommitOffset := rawLastCommitOffset.(int64)
		if canCommitOffset > lastCommitOffset {
			msg := OffsetMessage{Topic: m.topic, Partition: partition, Offset: canCommitOffset}
			_ = m.commitLocked(ctx, msg)

			rawProcessingMessages, ok := m.processingMessagesByPartition.Load(partition)
			if ok {
				processingMessages := rawProcessingMessages.(*sync.Map)
				for i := lastCommitOffset + 1; i <= canCommitOffset; i++ {
					processingMessages.Delete(i)
				}
			}
		}

		return true
	})
}

// commit only commits when all messages from last commit to current offset are done.
func (m *OffsetManager) commit(ctx context.Context, partition int) error {
	rawMsgMap, ok := m.processingMessagesByPartition.Load(partition)
	if !ok {
		return nil
	}
	msgMap := rawMsgMap.(*sync.Map)

	mutex := m.getPartitionMutex(partition)
	if mutex == nil {
		return nil
	}
	mutex.Lock()
	defer mutex.Unlock()

	rawCanCommitOffset, ok := m.canCommitOffsetByPartition.Load(partition)
	if !ok {
		return nil
	}
	canCommitOffset := rawCanCommitOffset.(int64)

	// Advance the watermark through all consecutive done offsets.
	// Bounded naturally by in-flight count (MaxInFlight), no artificial limit needed.
	for {
		val, ok := msgMap.Load(canCommitOffset + 1)
		if !ok || !val.(bool) {
			break
		}
		canCommitOffset++
	}

	if canCommitOffset > rawCanCommitOffset.(int64) {
		m.canCommitOffsetByPartition.Store(partition, canCommitOffset)
	}

	rawLastCommitOffset, _ := m.lastCommitOffsetByPartition.Load(partition)
	lastCommitOffset := rawLastCommitOffset.(int64)
	shouldCommit := false
	if canCommitOffset-lastCommitOffset >= m.minCommitCount {
		shouldCommit = true
	} else {
		rawLastCommitTime, ok := m.lastCommitTimeByPartition.Load(partition)
		if ok {
			if time.Since(rawLastCommitTime.(time.Time)) >= m.maxInterval && canCommitOffset > lastCommitOffset {
				shouldCommit = true
			}
		} else if canCommitOffset > lastCommitOffset {
			shouldCommit = true
		}
	}

	if !shouldCommit {
		return nil
	}

	msg := OffsetMessage{Topic: m.topic, Partition: partition, Offset: canCommitOffset}
	err := m.commitLocked(ctx, msg)
	if err == nil {
		for i := lastCommitOffset + 1; i <= canCommitOffset; i++ {
			msgMap.Delete(i)
		}
		// Clean up the partition entry if fully drained (#11).
		isEmpty := true
		msgMap.Range(func(_, _ interface{}) bool {
			isEmpty = false
			return false
		})
		if isEmpty {
			m.processingMessagesByPartition.Delete(partition)
		}
	}

	return err
}

// commitLocked performs the actual commit with retries. Caller must hold the partition mutex.
func (m *OffsetManager) commitLocked(ctx context.Context, msg OffsetMessage) error {
	partition := msg.Partition
	offset := msg.Offset

	rawLastCommitOffset, _ := m.lastCommitOffsetByPartition.Load(partition)
	lastCommitOffset := rawLastCommitOffset.(int64)

	if offset <= lastCommitOffset {
		return nil
	}

	kafkaMsg := kafka.Message{
		Topic:     msg.Topic,
		Partition: msg.Partition,
		Offset:    msg.Offset,
	}
	var err error
	for retry := 0; retry < m.maxRetries; retry++ {
		err = m.commitFunc(context.Background(), kafkaMsg)
		if err == nil {
			m.lastCommitOffsetByPartition.Store(partition, offset)
			m.lastCommitTimeByPartition.Store(partition, time.Now())
			m.logger.Infof("Committed offset %d for %s partition %d", offset, msg.Topic, msg.Partition)
			return nil
		}

		m.logger.Errorf("Failed to commit offset %d for %s partition %d: %v (retry %d/%d)",
			offset, msg.Topic, msg.Partition, err, retry+1, m.maxRetries)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(m.retryDelay):
		}
	}

	return err
}
