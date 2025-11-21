package turnstile

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"
)

func newTestOffsetManager(t *testing.T) *OffsetManager {
	t.Helper()
	logger, err := zap.NewProduction()
	if err != nil {
		t.Fatalf("failed to create logger: %v", err)
	}
	return NewOffsetManager(OffsetManagerConfig{
		Topic:          "test-topic",
		CommitFunc:     func(ctx context.Context, message kafka.Message) error { return nil },
		Logger:         logger.Sugar(),
		MinCommitCount: 5,
		MaxInterval:    1 * time.Second,
		ForceInterval:  5 * time.Second,
		MaxRetries:     3,
		RetryDelay:     10 * time.Millisecond,
	})
}

func TestTrack_InitializesLastCommitOffset(t *testing.T) {
	m := newTestOffsetManager(t)

	if err := m.Track(0, 10); err != nil {
		t.Fatal(err)
	}

	val, ok := m.lastCommitOffsetByPartition.Load(0)
	if !ok {
		t.Fatal("expected lastCommitOffsetPerPartition to be initialized")
	}
	if val.(int64) != 9 {
		t.Errorf("expected lastCommitOffset to be 9 (offset-1), got %d", val.(int64))
	}
}

func TestMarkDone_NoPanicOnUnknownPartition(t *testing.T) {
	m := newTestOffsetManager(t)

	// Should not panic or error
	if err := m.MarkDone(context.Background(), 99, 42); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestInitPartitionOffset_SetsInitialOffset(t *testing.T) {
	m := newTestOffsetManager(t)

	m.InitPartitionOffset(0, 100)

	val, ok := m.lastCommitOffsetByPartition.Load(0)
	if !ok {
		t.Fatal("expected partition 0 to be initialized")
	}
	if val.(int64) != 99 {
		t.Errorf("expected lastCommitOffset 99, got %d", val.(int64))
	}
}

func TestInitPartitionOffset_DeletesZeroOffset(t *testing.T) {
	m := newTestOffsetManager(t)

	if err := m.Track(0, 5); err != nil {
		t.Fatal(err)
	}
	m.InitPartitionOffset(0, 0)

	_, ok := m.lastCommitOffsetByPartition.Load(0)
	if ok {
		t.Error("expected partition 0 to be deleted after InitPartitionOffset with offset 0")
	}
}

func TestForceCommit_CommitsDoneOffsets(t *testing.T) {
	var committed sync.Map

	logger, _ := zap.NewProduction()
	m := NewOffsetManager(OffsetManagerConfig{
		Topic: "test-topic",
		CommitFunc: func(ctx context.Context, msg kafka.Message) error {
			committed.Store(msg.Partition, msg.Offset)
			return nil
		},
		Logger:         logger.Sugar(),
		MinCommitCount: 1,
		MaxInterval:    1 * time.Second,
		ForceInterval:  5 * time.Second,
		MaxRetries:     1,
		RetryDelay:     10 * time.Millisecond,
	})

	if err := m.Track(0, 0); err != nil {
		t.Fatal(err)
	}
	if err := m.Track(0, 1); err != nil {
		t.Fatal(err)
	}
	if err := m.Track(1, 0); err != nil {
		t.Fatal(err)
	}
	if err := m.Track(1, 1); err != nil {
		t.Fatal(err)
	}

	_ = m.MarkDone(context.Background(), 0, 0)
	_ = m.MarkDone(context.Background(), 0, 1)
	_ = m.MarkDone(context.Background(), 1, 0)
	_ = m.MarkDone(context.Background(), 1, 1)

	m.ForceCommit(context.Background())

	offset0, ok0 := committed.Load(0)
	offset1, ok1 := committed.Load(1)

	if !ok0 {
		t.Error("expected partition 0 to be committed")
	}
	if !ok1 {
		t.Error("expected partition 1 to be committed")
	}

	t.Logf("partition 0 committed offset=%v, partition 1 committed offset=%v", offset0, offset1)
}

func TestCommitLocked_DoesNotAdvanceOffsetOnAllRetriesFailed(t *testing.T) {
	logger, _ := zap.NewProduction()
	m := NewOffsetManager(OffsetManagerConfig{
		Topic: "test-topic",
		CommitFunc: func(ctx context.Context, msg kafka.Message) error {
			return fmt.Errorf("kafka unavailable")
		},
		Logger:         logger.Sugar(),
		MinCommitCount: 1,
		MaxInterval:    1 * time.Second,
		ForceInterval:  5 * time.Second,
		MaxRetries:     2,
		RetryDelay:     1 * time.Millisecond,
	})

	if err := m.Track(0, 0); err != nil {
		t.Fatal(err)
	}
	if err := m.Track(0, 1); err != nil {
		t.Fatal(err)
	}
	if err := m.Track(0, 2); err != nil {
		t.Fatal(err)
	}

	_ = m.MarkDone(context.Background(), 0, 0)
	_ = m.MarkDone(context.Background(), 0, 1)
	_ = m.MarkDone(context.Background(), 0, 2)

	// All commits failed; lastCommitOffset must remain at initial value.
	val, ok := m.lastCommitOffsetByPartition.Load(0)
	if !ok {
		t.Fatal("expected partition 0 to be initialized")
	}
	if val.(int64) != -1 {
		t.Errorf("lastCommitOffset should stay at -1 after all retries fail, got %d", val.(int64))
	}
}

func TestSequential_CommitProgress(t *testing.T) {
	var lastCommitted int64

	logger, _ := zap.NewProduction()
	m := NewOffsetManager(OffsetManagerConfig{
		Topic: "test-topic",
		CommitFunc: func(ctx context.Context, msg kafka.Message) error {
			lastCommitted = msg.Offset
			return nil
		},
		Logger:         logger.Sugar(),
		MinCommitCount: 3,
		MaxInterval:    1 * time.Second,
		ForceInterval:  5 * time.Second,
		MaxRetries:     1,
		RetryDelay:     10 * time.Millisecond,
	})

	// Track 5 messages
	for i := int64(0); i < 5; i++ {
		if err := m.Track(0, i); err != nil {
			t.Fatal(err)
		}
	}

	// Mark first 2 done — not enough for commit (minCommitCount=3)
	_ = m.MarkDone(context.Background(), 0, 0)
	_ = m.MarkDone(context.Background(), 0, 1)
	if lastCommitted != 0 {
		t.Errorf("expected no commit yet, got lastCommitted=%d", lastCommitted)
	}

	// Mark 3rd done — should trigger commit
	_ = m.MarkDone(context.Background(), 0, 2)
	if lastCommitted != 2 {
		t.Errorf("expected lastCommitted=2, got %d", lastCommitted)
	}
}

