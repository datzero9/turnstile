# Turnstile

A high-throughput Kafka consumer library for Go that maximizes concurrency while guaranteeing per-key message ordering. Built on top of [segmentio/kafka-go](https://github.com/segmentio/kafka-go).

## Installation

```bash
go get github.com/datzero9/turnstile
```

## Quick Start

```go
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"

	"github.com/datzero9/turnstile"
)

type OrderHandler struct{}

func (h *OrderHandler) HandleMessage(ctx context.Context, msg turnstile.Message) error {
	fmt.Printf("Processing order: %s\n", string(msg.Value))
	return nil
}

func (h *OrderHandler) GetKey(key []byte, value []byte) string {
	// Messages with the same key are processed in order.
	// Messages with different keys are processed concurrently.
	return string(key)
}

func main() {
	consumer, err := turnstile.NewConsumer(turnstile.Config{
		Brokers:     []string{"localhost:9092"},
		GroupID:     "order-service",
		Topic:       "orders",
		Handler:     &OrderHandler{},
		MaxInFlight: 500,
		RetryCount:  3,
	})
	if err != nil {
		log.Fatal(err)
	}

	consumer.Start()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt)
	<-sigChan

	consumer.Stop()
}
```

## Configuration

| Field                  | Default    | Description                                             |
| ---------------------- | ---------- | ------------------------------------------------------- |
| `Brokers`              | _required_ | Kafka broker addresses                                  |
| `GroupID`              | _required_ | Consumer group ID                                       |
| `Topic`                | _required_ | Topic to consume from                                   |
| `Handler`              | _required_ | Your `MessageHandler` implementation                    |
| `MaxInFlight`          | 1000       | Max concurrent messages being processed                 |
| `UnOrdered`            | false      | Set `true` to disable key ordering (max throughput)     |
| `MaxQueuedPerKey`      | 100        | Max queued messages per key before dropping oldest      |
| `RetryCount`           | 0          | Retry attempts for failed `HandleMessage` calls         |
| `RetryDelay`           | 500ms      | Delay between retries                                   |
| `DeadLetterPersister`  | nil        | Optional handler for messages that exhaust all retries  |
| `AutoOffsetReset`      | LastOffset | Where to start if no committed offset exists            |
| `MinOffsetCommitCount` | 5          | Min messages before triggering an offset commit         |
| `MaxCommitInterval`    | 5s         | Max time between commits (when new offsets are ready)   |
| `ForceCommitInterval`  | 5s         | Interval for force-committing highest contiguous offset |
| `MaxCommitRetries`     | 50         | Max retries for offset commits                          |
| `CommitRetryDelay`     | 100ms      | Delay between commit retries                            |
| `MinBytes`             | 0          | Min bytes per Kafka fetch                               |
| `MaxBytes`             | 10MB       | Max bytes per Kafka fetch                               |
| `ShutdownTimeout`      | 30s        | Max wait for in-flight messages during shutdown         |

## Interfaces

### MessageHandler

```go
type MessageHandler interface {
    HandleMessage(ctx context.Context, message Message) error
    GetKey(key []byte, value []byte) string
}
```

- **`HandleMessage`** — process a single message. Return an error to trigger retries.
- **`GetKey`** — extract a key for ordering. Messages with the same key are never processed concurrently. Return `""` to skip ordering for a specific message.

### DeadLetterPersister

```go
type DeadLetterPersister interface {
    Save(ctx context.Context, message Message, err error, key string) error
}
```

Implement this to capture messages that fail after all retry attempts. Store them in a database, file, or another Kafka topic for later inspection and replay.

## License

MIT
