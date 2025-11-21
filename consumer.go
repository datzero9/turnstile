package turnstile

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"
)

// Consumer represents a Kafka consumer.
type Consumer struct {
	config        Config
	logger        *zap.SugaredLogger
	reader        *kafka.Reader
	kafkaClient   *kafka.Client
	offsetManager *OffsetManager
	backpressure  *BackpressureController
	keySequencer  *KeySequencer
	ctx           context.Context    // controls the fetch loop and retry delays
	cancel        context.CancelFunc
	procCtx       context.Context    // passed to HandleMessage; outlives ctx during graceful drain
	procCancel    context.CancelFunc
	wg            sync.WaitGroup
	running       bool
	runningMutex  sync.Mutex
}

// NewConsumer creates a new Kafka consumer.
func NewConsumer(config Config) (*Consumer, error) {
	config.applyDefaults()

	if err := config.Validate(); err != nil {
		return nil, err
	}

	zapLogger, err := zap.NewProduction()
	if err != nil {
		return nil, fmt.Errorf("failed to initialize logger: %w", err)
	}
	logger := zapLogger.Sugar()

	ctx, cancel := context.WithCancel(context.Background())
	procCtx, procCancel := context.WithCancel(context.Background())

	c := &Consumer{
		config:     config,
		logger:     logger,
		ctx:        ctx,
		cancel:     cancel,
		procCtx:    procCtx,
		procCancel: procCancel,
	}

	c.backpressure = NewBackpressureController(config.MaxInFlight)

	if !config.UnOrdered {
		c.keySequencer = NewKeySequencerWithConfig(config.MaxQueuedPerKey)
	}

	c.kafkaClient = &kafka.Client{
		Addr: kafka.TCP(config.Brokers...),
		Transport: &kafka.Transport{
			ClientID: config.GroupID,
		},
	}

	c.reader = kafka.NewReader(kafka.ReaderConfig{
		Brokers:     config.Brokers,
		Topic:       config.Topic,
		GroupID:     config.GroupID,
		MinBytes:    config.MinBytes,
		MaxBytes:    config.MaxBytes,
		StartOffset: config.AutoOffsetReset,
		GroupBalancers: []kafka.GroupBalancer{
			kafka.RangeGroupBalancer{},
		},
	})

	commitFunc := func(ctx context.Context, message kafka.Message) error {
		return c.reader.CommitMessages(ctx, message)
	}
	c.offsetManager = NewOffsetManager(OffsetManagerConfig{
		Topic:           config.Topic,
		CommitFunc:      commitFunc,
		FetchOffsetFunc: c.fetchCommittedOffset,
		Logger:          logger,
		MinCommitCount:  config.MinOffsetCommitCount,
		MaxInterval:     config.MaxCommitInterval,
		ForceInterval:   config.ForceCommitInterval,
		MaxRetries:      config.MaxCommitRetries,
		RetryDelay:      config.CommitRetryDelay,
	})

	return c, nil
}

// Start starts the consumer and begins consuming messages.
func (c *Consumer) Start() error {
	c.runningMutex.Lock()
	if c.running {
		c.runningMutex.Unlock()
		return fmt.Errorf("consumer already running")
	}
	c.running = true
	c.runningMutex.Unlock()

	c.logger.Infof("Starting Kafka consumer for topic: %s", c.config.Topic)

	c.wg.Add(1)
	go c.consumeFromKafka()

	if c.keySequencer != nil {
		c.wg.Add(1)
		go c.consumeFromKeySequencer()
	}

	c.wg.Add(1)
	go c.forceCommitLoop()

	return nil
}

func (c *Consumer) consumeFromKafka() {
	defer c.wg.Done()

	var fetchBackoff time.Duration
	const maxFetchBackoff = 5 * time.Second

	for {
		select {
		case <-c.ctx.Done():
			return
		default:
		}

		if err := c.backpressure.WaitUntilBelowCapacity(c.ctx); err != nil {
			return
		}

		msg, err := c.reader.FetchMessage(c.ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return
			}
			c.logger.Errorf("Failed to fetch message: %v", err)
			fetchBackoff = backoffWithJitter(fetchBackoff, maxFetchBackoff)
			select {
			case <-c.ctx.Done():
				return
			case <-time.After(fetchBackoff):
			}
			continue
		}
		fetchBackoff = 0

		if err := c.offsetManager.Track(msg.Partition, msg.Offset); err != nil {
			c.logger.Errorf("Failed to track offset: %v", err)
		}

		key := c.config.Handler.GetKey(msg.Key, msg.Value)

		if c.keySequencer != nil {
			if !c.keySequencer.TryAcquire(key) {
				c.keySequencer.Enqueue(msg, key)
				continue
			}
		}

		c.backpressure.Acquire()
		c.wg.Add(1)
		go c.processMessage(msg, key)
	}
}

// backoffWithJitter doubles the current backoff up to max, adding random jitter.
// Returns the initial backoff (250ms) if current is zero.
func backoffWithJitter(current, max time.Duration) time.Duration {
	if current == 0 {
		current = 250 * time.Millisecond
	} else {
		current *= 2
	}
	if current > max {
		current = max
	}
	jitter := time.Duration(rand.Int64N(int64(current) / 2))
	return current + jitter
}

func (c *Consumer) consumeFromKeySequencer() {
	defer c.wg.Done()

	for {
		select {
		case <-c.ctx.Done():
			return
		case <-c.keySequencer.ReadyChan():
			if c.backpressure.IsAtCapacity() {
				continue
			}

			msg, key, ok := c.keySequencer.Dequeue()
			if !ok {
				continue
			}

			c.backpressure.Acquire()
			c.wg.Add(1)
			go c.processMessage(msg, key)
		}
	}
}

// processMessage handles a single message in its own goroutine.
// It retries HandleMessage up to config.RetryCount times before giving up.
// After all retries are exhausted, the message is sent to DeadLetterPersister if configured.
func (c *Consumer) processMessage(msg kafka.Message, key string) {
	defer c.wg.Done()

	var handlerErr error
	defer func() {
		if r := recover(); r != nil {
			c.logger.Errorf("Panic in processMessage: topic=%s, partition=%d, offset=%d: %v",
				msg.Topic, msg.Partition, msg.Offset, r)
		}

		if err := c.offsetManager.MarkDone(context.Background(), msg.Partition, msg.Offset); err != nil {
			c.logger.Errorf("Failed to mark offset done: %v", err)
		}

		if c.keySequencer != nil {
			c.keySequencer.Release(key)
		}

		c.backpressure.Release()

		if handlerErr != nil {
			if c.config.DeadLetterPersister != nil {
				if persistErr := c.config.DeadLetterPersister.Save(context.Background(), msg, handlerErr, key); persistErr != nil {
					c.logger.Errorf("Failed to persist dead-letter message: %v", persistErr)
				}
			}
		}
	}()

	for attempt := 0; attempt <= c.config.RetryCount; attempt++ {
		if attempt > 0 {
			select {
			case <-c.ctx.Done():
				c.logger.Warnf("Context canceled during retry: topic=%s, partition=%d, offset=%d",
					msg.Topic, msg.Partition, msg.Offset)
				return
			case <-time.After(c.config.RetryDelay):
			}
		}

		// Use procCtx so the handler can complete during graceful shutdown even after
		// the fetch loop context (c.ctx) has been canceled.
		handlerErr = c.config.Handler.HandleMessage(c.procCtx, msg)
		if handlerErr == nil {
			break
		}
		c.logger.Errorf("Failed to process message (attempt %d/%d): topic=%s, partition=%d, offset=%d: %v",
			attempt+1, c.config.RetryCount+1, msg.Topic, msg.Partition, msg.Offset, handlerErr)
	}
}

// forceCommitLoop periodically forces offset commits.
func (c *Consumer) forceCommitLoop() {
	defer c.wg.Done()

	ticker := time.NewTicker(c.config.ForceCommitInterval)
	defer ticker.Stop()

	for {
		select {
		case <-c.ctx.Done():
			return
		case <-ticker.C:
			c.offsetManager.ForceCommit(c.ctx)
		}
	}
}

// Stop gracefully stops the consumer.
func (c *Consumer) Stop() error {
	c.runningMutex.Lock()
	if !c.running {
		c.runningMutex.Unlock()
		return nil
	}
	c.running = false
	c.runningMutex.Unlock()

	c.logger.Info("Stopping Kafka consumer...")

	// Cancel the fetch context to stop the fetch loop and retry delays.
	// processMessage goroutines continue using procCtx until they finish.
	c.cancel()

	done := make(chan struct{})
	go func() {
		c.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		c.logger.Info("All consumer goroutines stopped")
	case <-time.After(c.config.ShutdownTimeout):
		c.logger.Warn("Shutdown timeout reached, force canceling in-flight message processing")
		c.procCancel()
	}

	c.procCancel()

	c.offsetManager.ForceCommit(context.Background())

	var closeErr error
	if err := c.reader.Close(); err != nil {
		c.logger.Errorf("Failed to close reader: %v", err)
		closeErr = err
	}

	c.logger.Info("Kafka consumer stopped")
	return closeErr
}

// fetchCommittedOffset fetches the committed offset for a partition from Kafka,
// returns the committed offset, or -1 if no offset has been committed yet.
func (c *Consumer) fetchCommittedOffset(partition int) (int64, error) {
	request := &kafka.OffsetFetchRequest{
		GroupID: c.config.GroupID,
		Topics: map[string][]int{
			c.config.Topic: {partition},
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	response, err := c.kafkaClient.OffsetFetch(ctx, request)
	if err != nil {
		return -1, fmt.Errorf("failed to fetch offset: %w", err)
	}

	if topicOffsets, ok := response.Topics[c.config.Topic]; ok {
		for _, partitionOffset := range topicOffsets {
			if partitionOffset.Partition == partition {
				if partitionOffset.CommittedOffset == -1 {
					return -1, nil
				}
				return partitionOffset.CommittedOffset, nil
			}
		}
	}

	return -1, nil
}
