package turnstile

import (
	"time"

	"github.com/segmentio/kafka-go"
)

// Config holds the configuration for the Kafka consumer.
type Config struct {
	// Brokers is the list of Kafka broker addresses.
	Brokers []string

	// GroupID is the consumer group ID.
	GroupID string

	// Topic is the topic to consume from.
	Topic string

	// Handler is the message handler implementation.
	Handler MessageHandler

	// AutoOffsetReset determines where to start reading if no offset exists.
	// Use kafka.FirstOffset or kafka.LastOffset.
	// Default: kafka.LastOffset
	AutoOffsetReset int64

	// MaxInFlight is the maximum number of messages being processed concurrently.
	// This provides backpressure control.
	// Default: 1000
	MaxInFlight int

	// MinOffsetCommitCount is the minimum number of messages processed before committing.
	// Default: 5
	MinOffsetCommitCount int64

	// MaxCommitInterval is the maximum time between sequential commits. When elapsed,
	// commit() will trigger even if MinOffsetCommitCount has not been reached, as long
	// as there is a new contiguous done offset to commit.
	// Default: 5 seconds
	MaxCommitInterval time.Duration

	// ForceCommitInterval controls how often ForceCommit runs. ForceCommit advances the
	// committed offset to the highest contiguous done offset even when there are in-flight
	// messages ahead of it — it skips gaps rather than waiting for them to fill in.
	// This is distinct from MaxCommitInterval, which only triggers when the next offset
	// is already done (no gaps).
	// Default: 5 seconds
	ForceCommitInterval time.Duration

	// MaxCommitRetries is the maximum number of retries for committing offsets.
	// Default: 50
	MaxCommitRetries int

	// CommitRetryDelay is the delay between commit retries.
	// Default: 100ms
	CommitRetryDelay time.Duration

	// UnOrdered disables key-based message ordering and sequencing.
	// When true, messages with the same key can be processed concurrently,
	// maximizing throughput at the cost of ordering guarantees.
	// Default: false
	UnOrdered bool

	// RetryCount is the number of times to retry HandleMessage before giving up.
	// Default: 0 (no retries; the zero value means no retries)
	RetryCount int

	// RetryDelay is the delay between retry attempts.
	// Default: 500ms
	RetryDelay time.Duration

	// DeadLetterPersister is an optional persister for messages that failed after all retries.
	// If provided, exhausted messages will be saved for inspection/manual replay.
	DeadLetterPersister DeadLetterPersister

	// MaxQueuedPerKey is the maximum number of messages allowed per key in the
	// KeySequencer queue. When exceeded, the oldest message for that key is dropped.
	// Default: 100
	MaxQueuedPerKey int

	// ShutdownTimeout is the maximum time to wait for in-flight messages during shutdown.
	// Default: 30 seconds
	ShutdownTimeout time.Duration

	// MinBytes is the minimum number of bytes to fetch in a single request.
	// Default: 0 (no minimum)
	MinBytes int

	// MaxBytes is the maximum number of bytes to fetch in a single request.
	// Default: 10MB
	MaxBytes int
}

func (c *Config) applyDefaults() {
	if c.AutoOffsetReset == 0 {
		c.AutoOffsetReset = kafka.LastOffset
	}

	if c.MaxInFlight == 0 {
		c.MaxInFlight = 1000
	}

	if c.MinOffsetCommitCount == 0 {
		c.MinOffsetCommitCount = 5
	}

	if c.MaxCommitInterval == 0 {
		c.MaxCommitInterval = 5 * time.Second
	}

	if c.ForceCommitInterval == 0 {
		c.ForceCommitInterval = 5 * time.Second
	}

	if c.MaxCommitRetries == 0 {
		c.MaxCommitRetries = 50
	}

	if c.CommitRetryDelay == 0 {
		c.CommitRetryDelay = 100 * time.Millisecond
	}

	if c.ShutdownTimeout == 0 {
		c.ShutdownTimeout = 30 * time.Second
	}

	if c.MaxBytes == 0 {
		c.MaxBytes = 10e6 // 10MB
	}

	if c.MaxQueuedPerKey == 0 {
		c.MaxQueuedPerKey = 100
	}

	if c.RetryDelay == 0 {
		c.RetryDelay = 500 * time.Millisecond
	}

	// UnOrdered defaults to false (key sequencing enabled by default)
}

func (c *Config) Validate() error {
	if len(c.Brokers) == 0 {
		return ErrNoBrokers
	}

	if c.GroupID == "" {
		return ErrNoGroupID
	}

	if c.Topic == "" {
		return ErrNoTopic
	}

	if c.Handler == nil {
		return ErrNoHandler
	}

	return nil
}
