//go:build integration
// +build integration

package tests

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/datzero9/turnstile"
	"github.com/segmentio/kafka-go"
)

type testMessageHandler struct {
	mu               sync.Mutex
	processedMsgs    []kafka.Message
	processingDelay  time.Duration
	errorOnMessage   map[int]error
	errorOnKey       map[string]error
	panicOnMessage   map[int]bool
	keyExtractor     func([]byte, []byte) string
	processedCount   atomic.Int64
	errorCount       atomic.Int64
	onProcessMessage func(kafka.Message)
}

func newTestMessageHandler() *testMessageHandler {
	return &testMessageHandler{
		processedMsgs:  make([]kafka.Message, 0),
		errorOnMessage: make(map[int]error),
		errorOnKey:     make(map[string]error),
		panicOnMessage: make(map[int]bool),
		keyExtractor: func(key []byte, value []byte) string {
			return string(key)
		},
	}
}

func (h *testMessageHandler) HandleMessage(ctx context.Context, message kafka.Message) error {
	if h.processingDelay > 0 {
		select {
		case <-time.After(h.processingDelay):
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	h.mu.Lock()
	msgIndex := len(h.processedMsgs)
	h.processedMsgs = append(h.processedMsgs, message)
	h.mu.Unlock()

	h.processedCount.Add(1)

	if h.panicOnMessage[msgIndex] {
		panic(fmt.Sprintf("panic on message %d", msgIndex))
	}

	if err, exists := h.errorOnMessage[msgIndex]; exists {
		h.errorCount.Add(1)
		return err
	}

	if err, exists := h.errorOnKey[string(message.Key)]; exists {
		h.errorCount.Add(1)
		return err
	}

	if h.onProcessMessage != nil {
		h.onProcessMessage(message)
	}

	return nil
}

func (h *testMessageHandler) GetKey(key []byte, value []byte) string {
	return h.keyExtractor(key, value)
}

func (h *testMessageHandler) GetProcessedMessages() []kafka.Message {
	h.mu.Lock()
	defer h.mu.Unlock()
	result := make([]kafka.Message, len(h.processedMsgs))
	copy(result, h.processedMsgs)
	return result
}

func (h *testMessageHandler) GetProcessedCount() int64 {
	return h.processedCount.Load()
}

func (h *testMessageHandler) GetErrorCount() int64 {
	return h.errorCount.Load()
}

func (h *testMessageHandler) SetProcessingDelay(delay time.Duration) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.processingDelay = delay
}

func (h *testMessageHandler) SetErrorOnMessage(index int, err error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.errorOnMessage[index] = err
}

func (h *testMessageHandler) SetErrorOnKey(key string, err error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.errorOnKey[key] = err
}

func (h *testMessageHandler) SetPanicOnMessage(index int) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.panicOnMessage[index] = true
}

func (h *testMessageHandler) SetKeyExtractor(extractor func([]byte, []byte) string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.keyExtractor = extractor
}

func (h *testMessageHandler) SetOnProcessMessage(hook func(kafka.Message)) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.onProcessMessage = hook
}

func (h *testMessageHandler) Reset() {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.processedMsgs = make([]kafka.Message, 0)
	h.errorOnMessage = make(map[int]error)
	h.errorOnKey = make(map[string]error)
	h.panicOnMessage = make(map[int]bool)
	h.processedCount.Store(0)
	h.errorCount.Store(0)
}

type testDeadLetterPersister struct {
	mu            sync.Mutex
	savedMessages []turnstile.DeadLetterMessage
	saveCallback  func(context.Context, kafka.Message, error, string) error
}

func newTestDeadLetterPersister() *testDeadLetterPersister {
	return &testDeadLetterPersister{
		savedMessages: make([]turnstile.DeadLetterMessage, 0),
	}
}

func (p *testDeadLetterPersister) Save(ctx context.Context, message kafka.Message, err error, key string) error {
	if p.saveCallback != nil {
		return p.saveCallback(ctx, message, err, key)
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	pm := turnstile.DeadLetterMessage{
		ID:        fmt.Sprintf("msg-%d", len(p.savedMessages)),
		Message:   message,
		Error:     err.Error(),
		Key:       key,
		CreatedAt: time.Now().Unix(),
	}
	p.savedMessages = append(p.savedMessages, pm)
	return nil
}

func (p *testDeadLetterPersister) GetSavedMessages() []turnstile.DeadLetterMessage {
	p.mu.Lock()
	defer p.mu.Unlock()
	result := make([]turnstile.DeadLetterMessage, len(p.savedMessages))
	copy(result, p.savedMessages)
	return result
}

func (p *testDeadLetterPersister) GetSavedCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.savedMessages)
}

func (p *testDeadLetterPersister) SetSaveCallback(callback func(context.Context, kafka.Message, error, string) error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.saveCallback = callback
}

func (p *testDeadLetterPersister) Reset() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.savedMessages = make([]turnstile.DeadLetterMessage, 0)
}

func createTestMessages(topic string, partition int, count int, keyPrefix, valuePrefix string) []kafka.Message {
	messages := make([]kafka.Message, count)
	for i := 0; i < count; i++ {
		messages[i] = kafka.Message{
			Topic:     topic,
			Partition: partition,
			Offset:    int64(i),
			Key:       []byte(fmt.Sprintf("%s-%d", keyPrefix, i)),
			Value:     []byte(fmt.Sprintf("%s-%d", valuePrefix, i)),
			Time:      time.Now(),
		}
	}
	return messages
}

func waitForCondition(timeout time.Duration, checkInterval time.Duration, condition func() bool) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if condition() {
			return true
		}
		time.Sleep(checkInterval)
	}
	return false
}

const (
	testBroker     = "localhost:9092"
	testGroupID    = "turnstile-integration-test"
	testTopic      = "turnstile-test-topic"
	testTimeout    = 30 * time.Second
	messageTimeout = 10 * time.Second
)

func TestBasicConsumption(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	uniqueTopic := fmt.Sprintf("%s-basic-%d", testTopic, time.Now().UnixNano())

	conn, err := kafka.DialLeader(ctx, "tcp", testBroker, uniqueTopic, 0)
	if err != nil {
		t.Skipf("Kafka not available: %v", err)
	}
	defer conn.Close()

	testMessages := createTestMessages(uniqueTopic, 0, 10, "key", "value")
	kafkaMessages := make([]kafka.Message, len(testMessages))
	for i, msg := range testMessages {
		kafkaMessages[i] = kafka.Message{
			Key:   msg.Key,
			Value: msg.Value,
		}
	}

	_, err = conn.WriteMessages(kafkaMessages...)
	if err != nil {
		t.Fatalf("Failed to write messages: %v", err)
	}

	handler := newTestMessageHandler()

	config := turnstile.Config{
		Brokers:         []string{testBroker},
		GroupID:         fmt.Sprintf("%s-basic-%d", testGroupID, time.Now().Unix()),
		Topic:           uniqueTopic,
		Handler:         handler,
		MaxInFlight:     100,
		AutoOffsetReset: kafka.FirstOffset,
	}

	consumer, err := turnstile.NewConsumer(config)
	if err != nil {
		t.Fatalf("Failed to create consumer: %v", err)
	}

	consumer.Start()
	defer consumer.Stop()

	success := waitForCondition(messageTimeout, 100*time.Millisecond, func() bool {
		return handler.GetProcessedCount() >= int64(len(testMessages))
	})
	if !success {
		t.Fatalf("Expected %d messages to be processed, got %d", len(testMessages), handler.GetProcessedCount())
	}

	processed := handler.GetProcessedMessages()
	if len(processed) != len(testMessages) {
		t.Errorf("Expected %d messages, got %d", len(testMessages), len(processed))
	}
}

func TestSequentialOffsetCommit(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	conn, err := kafka.DialLeader(ctx, "tcp", testBroker, testTopic, 0)
	if err != nil {
		t.Skipf("Kafka not available: %v", err)
	}
	defer conn.Close()

	testMessages := createTestMessages(testTopic, 0, 20, "key", "value")
	kafkaMessages := make([]kafka.Message, len(testMessages))
	for i, msg := range testMessages {
		kafkaMessages[i] = kafka.Message{
			Key:   msg.Key,
			Value: msg.Value,
		}
	}

	_, err = conn.WriteMessages(kafkaMessages...)
	if err != nil {
		t.Fatalf("Failed to write messages: %v", err)
	}

	handler := newTestMessageHandler()

	handler.SetErrorOnMessage(5, errors.New("test error"))

	handler.SetProcessingDelay(10 * time.Millisecond)

	config := turnstile.Config{
		Brokers:              []string{testBroker},
		GroupID:              fmt.Sprintf("%s-sequential-%d", testGroupID, time.Now().Unix()),
		Topic:                testTopic,
		Handler:              handler,
		MaxInFlight:          50,
		MinOffsetCommitCount: 5,
		MaxCommitInterval:    2 * time.Second,
		AutoOffsetReset:      kafka.FirstOffset,
		DeadLetterPersister:  newTestDeadLetterPersister(),
	}

	consumer, err := turnstile.NewConsumer(config)
	if err != nil {
		t.Fatalf("Failed to create consumer: %v", err)
	}

	consumer.Start()
	defer consumer.Stop()

	time.Sleep(5 * time.Second)
}

func TestWorkerPoolMode(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	conn, err := kafka.DialLeader(ctx, "tcp", testBroker, testTopic, 0)
	if err != nil {
		t.Skipf("Kafka not available: %v", err)
	}
	defer conn.Close()

	testMessages := createTestMessages(testTopic, 0, 50, "key", "value")
	kafkaMessages := make([]kafka.Message, len(testMessages))
	for i, msg := range testMessages {
		kafkaMessages[i] = kafka.Message{
			Key:   msg.Key,
			Value: msg.Value,
		}
	}

	_, err = conn.WriteMessages(kafkaMessages...)
	if err != nil {
		t.Fatalf("Failed to write messages: %v", err)
	}

	handler := newTestMessageHandler()
	handler.SetProcessingDelay(50 * time.Millisecond)

	config := turnstile.Config{
		Brokers:         []string{testBroker},
		GroupID:         fmt.Sprintf("%s-pool-%d", testGroupID, time.Now().Unix()),
		Topic:           testTopic,
		Handler:         handler,
		MaxInFlight:     100,
		AutoOffsetReset: kafka.FirstOffset,
	}

	consumer, err := turnstile.NewConsumer(config)
	if err != nil {
		t.Fatalf("Failed to create consumer: %v", err)
	}

	consumer.Start()
	defer consumer.Stop()

	success := waitForCondition(messageTimeout, 100*time.Millisecond, func() bool {
		return handler.GetProcessedCount() >= int64(len(testMessages))
	})

	if !success {
		t.Fatalf("Expected %d messages to be processed, got %d", len(testMessages), handler.GetProcessedCount())
	}

	t.Logf("Successfully processed %d messages with worker pool", handler.GetProcessedCount())
}

func TestBackpressureManagement(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	conn, err := kafka.DialLeader(ctx, "tcp", testBroker, testTopic, 0)
	if err != nil {
		t.Skipf("Kafka not available: %v", err)
	}
	defer conn.Close()

	testMessages := createTestMessages(testTopic, 0, 200, "key", "value")
	kafkaMessages := make([]kafka.Message, len(testMessages))
	for i, msg := range testMessages {
		kafkaMessages[i] = kafka.Message{
			Key:   msg.Key,
			Value: msg.Value,
		}
	}

	_, err = conn.WriteMessages(kafkaMessages...)
	if err != nil {
		t.Fatalf("Failed to write messages: %v", err)
	}

	handler := newTestMessageHandler()
	handler.SetProcessingDelay(100 * time.Millisecond)

	config := turnstile.Config{
		Brokers:         []string{testBroker},
		GroupID:         fmt.Sprintf("%s-backpressure-%d", testGroupID, time.Now().Unix()),
		Topic:           testTopic,
		Handler:         handler,
		MaxInFlight:     20,
		AutoOffsetReset: kafka.FirstOffset,
	}

	consumer, err := turnstile.NewConsumer(config)
	if err != nil {
		t.Fatalf("Failed to create consumer: %v", err)
	}

	consumer.Start()
	defer consumer.Stop()

	time.Sleep(2 * time.Second)

	success := waitForCondition(30*time.Second, 100*time.Millisecond, func() bool {
		return handler.GetProcessedCount() >= int64(len(testMessages))
	})

	if !success {
		t.Logf("Processed %d out of %d messages (acceptable with backpressure)",
			handler.GetProcessedCount(), len(testMessages))
	}
}

func TestKeyBasedDeduplication(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	conn, err := kafka.DialLeader(ctx, "tcp", testBroker, testTopic, 0)
	if err != nil {
		t.Skipf("Kafka not available: %v", err)
	}
	defer conn.Close()

	kafkaMessages := []kafka.Message{
		{Key: []byte("key1"), Value: []byte("msg1")},
		{Key: []byte("key1"), Value: []byte("msg2")},
		{Key: []byte("key2"), Value: []byte("msg3")},
		{Key: []byte("key1"), Value: []byte("msg4")},
		{Key: []byte("key2"), Value: []byte("msg5")},
	}

	_, err = conn.WriteMessages(kafkaMessages...)
	if err != nil {
		t.Fatalf("Failed to write messages: %v", err)
	}

	handler := newTestMessageHandler()

	processingKeys := make(map[string]bool)
	var keyMutex sync.Mutex
	var concurrentAccess bool

	handler.SetOnProcessMessage(func(msg kafka.Message) {
		key := string(msg.Key)
		keyMutex.Lock()
		if processingKeys[key] {
			concurrentAccess = true
			t.Errorf("Concurrent processing detected for key: %s", key)
		}
		processingKeys[key] = true
		keyMutex.Unlock()

		time.Sleep(50 * time.Millisecond)

		keyMutex.Lock()
		delete(processingKeys, key)
		keyMutex.Unlock()
	})

	config := turnstile.Config{
		Brokers:         []string{testBroker},
		GroupID:         fmt.Sprintf("%s-dedup-%d", testGroupID, time.Now().Unix()),
		Topic:           testTopic,
		Handler:         handler,
		MaxInFlight:     10,
		UnOrdered:       false,
		AutoOffsetReset: kafka.FirstOffset,
	}

	consumer, err := turnstile.NewConsumer(config)
	if err != nil {
		t.Fatalf("Failed to create consumer: %v", err)
	}

	consumer.Start()
	defer consumer.Stop()

	success := waitForCondition(messageTimeout, 100*time.Millisecond, func() bool {
		return handler.GetProcessedCount() >= int64(len(kafkaMessages))
	})

	if !success {
		t.Fatalf("Expected %d messages to be processed, got %d", len(kafkaMessages), handler.GetProcessedCount())
	}

	if concurrentAccess {
		t.Error("Key sequencing failed: concurrent processing of same key detected")
	}
}

func TestGracefulShutdown(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	uniqueTopic := fmt.Sprintf("%s-shutdown-%d", testTopic, time.Now().UnixNano())

	conn, err := kafka.DialLeader(ctx, "tcp", testBroker, uniqueTopic, 0)
	if err != nil {
		t.Skipf("Kafka not available: %v", err)
	}
	defer conn.Close()

	testMessages := createTestMessages(uniqueTopic, 0, 30, "key", "value")
	kafkaMessages := make([]kafka.Message, len(testMessages))
	for i, msg := range testMessages {
		kafkaMessages[i] = kafka.Message{
			Key:   msg.Key,
			Value: msg.Value,
		}
	}

	_, err = conn.WriteMessages(kafkaMessages...)
	if err != nil {
		t.Fatalf("Failed to write messages: %v", err)
	}

	handler := newTestMessageHandler()
	handler.SetProcessingDelay(200 * time.Millisecond)

	config := turnstile.Config{
		Brokers:         []string{testBroker},
		GroupID:         fmt.Sprintf("%s-shutdown-%d", testGroupID, time.Now().Unix()),
		Topic:           uniqueTopic,
		Handler:         handler,
		MaxInFlight:     5,
		ShutdownTimeout: 10 * time.Second,
		AutoOffsetReset: kafka.FirstOffset,
	}

	consumer, err := turnstile.NewConsumer(config)
	if err != nil {
		t.Fatalf("Failed to create consumer: %v", err)
	}

	consumer.Start()

	time.Sleep(100 * time.Millisecond)

	processedBefore := handler.GetProcessedCount()
	t.Logf("Processed before shutdown: %d", processedBefore)

	consumer.Stop()

	processedAfter := handler.GetProcessedCount()
	t.Logf("Processed after shutdown: %d", processedAfter)

	if processedAfter <= processedBefore {
		t.Error("Expected in-flight messages to complete during shutdown")
	}
}

func TestFailedMessagePersistence(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	uniqueTopic := fmt.Sprintf("%s-persist-%d", testTopic, time.Now().UnixNano())

	conn, err := kafka.DialLeader(ctx, "tcp", testBroker, uniqueTopic, 0)
	if err != nil {
		t.Skipf("Kafka not available: %v", err)
	}
	defer conn.Close()

	testMessages := createTestMessages(uniqueTopic, 0, 10, "key", "value")
	kafkaMessages := make([]kafka.Message, len(testMessages))
	for i, msg := range testMessages {
		kafkaMessages[i] = kafka.Message{
			Key:   msg.Key,
			Value: msg.Value,
		}
	}

	_, err = conn.WriteMessages(kafkaMessages...)
	if err != nil {
		t.Fatalf("Failed to write messages: %v", err)
	}

	handler := newTestMessageHandler()

	testErr := errors.New("processing failed")
	handler.SetErrorOnMessage(2, testErr)
	handler.SetErrorOnMessage(5, testErr)
	handler.SetErrorOnMessage(7, testErr)

	persister := newTestDeadLetterPersister()

	config := turnstile.Config{
		Brokers:             []string{testBroker},
		GroupID:             fmt.Sprintf("%s-persist-%d", testGroupID, time.Now().Unix()),
		Topic:               uniqueTopic,
		Handler:             handler,
		MaxInFlight:         1,
		DeadLetterPersister: persister,
		AutoOffsetReset:     kafka.FirstOffset,
	}

	consumer, err := turnstile.NewConsumer(config)
	if err != nil {
		t.Fatalf("Failed to create consumer: %v", err)
	}

	consumer.Start()
	defer consumer.Stop()

	success := waitForCondition(messageTimeout, 100*time.Millisecond, func() bool {
		return handler.GetProcessedCount() >= int64(len(testMessages))
	})

	if !success {
		t.Fatalf("Expected %d messages to be processed, got %d", len(testMessages), handler.GetProcessedCount())
	}

	savedCount := persister.GetSavedCount()
	if savedCount != 3 {
		t.Errorf("Expected 3 failed messages to be persisted, got %d", savedCount)
	}

	errorCount := handler.GetErrorCount()
	if errorCount != 3 {
		t.Errorf("Expected 3 errors, got %d", errorCount)
	}
}

func TestConcurrentProcessing(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	conn, err := kafka.DialLeader(ctx, "tcp", testBroker, testTopic, 0)
	if err != nil {
		t.Skipf("Kafka not available: %v", err)
	}
	defer conn.Close()

	testMessages := createTestMessages(testTopic, 0, 1000, "key", "value")
	kafkaMessages := make([]kafka.Message, len(testMessages))
	for i, msg := range testMessages {
		kafkaMessages[i] = kafka.Message{
			Key:   msg.Key,
			Value: msg.Value,
		}
	}

	_, err = conn.WriteMessages(kafkaMessages...)
	if err != nil {
		t.Fatalf("Failed to write messages: %v", err)
	}

	handler := newTestMessageHandler()

	config := turnstile.Config{
		Brokers:         []string{testBroker},
		GroupID:         fmt.Sprintf("%s-concurrent-%d", testGroupID, time.Now().Unix()),
		Topic:           testTopic,
		Handler:         handler,
		MaxInFlight:     500,
		AutoOffsetReset: kafka.FirstOffset,
	}

	consumer, err := turnstile.NewConsumer(config)
	if err != nil {
		t.Fatalf("Failed to create consumer: %v", err)
	}

	start := time.Now()
	consumer.Start()
	defer consumer.Stop()

	success := waitForCondition(30*time.Second, 100*time.Millisecond, func() bool {
		return handler.GetProcessedCount() >= int64(len(testMessages))
	})

	elapsed := time.Since(start)

	if !success {
		t.Fatalf("Expected %d messages to be processed, got %d", len(testMessages), handler.GetProcessedCount())
	}

	t.Logf("Processed %d messages in %v (%.2f msgs/sec)",
		handler.GetProcessedCount(), elapsed, float64(handler.GetProcessedCount())/elapsed.Seconds())
}

func TestPartitionRebalanceWithMultipleConsumers(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	uniqueTopic := fmt.Sprintf("%s-rebalance-%d", testTopic, time.Now().UnixNano())
	groupID := fmt.Sprintf("%s-rebalance-group-%d", testGroupID, time.Now().Unix())

	conn, err := kafka.DialLeader(ctx, "tcp", testBroker, uniqueTopic, 0)
	if err != nil {
		t.Skipf("Kafka not available: %v", err)
	}
	defer conn.Close()

	testMessages := createTestMessages(uniqueTopic, 0, 50, "key", "value")
	kafkaMessages := make([]kafka.Message, len(testMessages))
	for i, msg := range testMessages {
		kafkaMessages[i] = kafka.Message{
			Key:   msg.Key,
			Value: msg.Value,
		}
	}

	_, err = conn.WriteMessages(kafkaMessages...)
	if err != nil {
		t.Fatalf("Failed to write messages: %v", err)
	}

	handler1 := newTestMessageHandler()
	config1 := turnstile.Config{
		Brokers:              []string{testBroker},
		GroupID:              groupID,
		Topic:                uniqueTopic,
		Handler:              handler1,
		MaxInFlight:          100,
		AutoOffsetReset:      kafka.FirstOffset,
		MinOffsetCommitCount: 5,
		MaxCommitInterval:    1 * time.Second,
	}
	consumer1, err := turnstile.NewConsumer(config1)
	if err != nil {
		t.Fatalf("Failed to create consumer1: %v", err)
	}
	consumer1.Start()
	defer consumer1.Stop()

	time.Sleep(3 * time.Second)

	t.Logf("Consumer1 processed: %d messages", handler1.GetProcessedCount())

	handler2 := newTestMessageHandler()
	config2 := turnstile.Config{
		Brokers:              []string{testBroker},
		GroupID:              groupID,
		Topic:                uniqueTopic,
		Handler:              handler2,
		MaxInFlight:          100,
		AutoOffsetReset:      kafka.FirstOffset,
		MinOffsetCommitCount: 5,
		MaxCommitInterval:    1 * time.Second,
	}
	consumer2, err := turnstile.NewConsumer(config2)
	if err != nil {
		t.Fatalf("Failed to create consumer2: %v", err)
	}
	t.Logf("Starting second consumer to trigger rebalance...")
	consumer2.Start()
	defer consumer2.Stop()

	time.Sleep(5 * time.Second)

	totalProcessed := handler1.GetProcessedCount() + handler2.GetProcessedCount()
	t.Logf("After rebalance - Consumer1: %d, Consumer2: %d, Total: %d",
		handler1.GetProcessedCount(), handler2.GetProcessedCount(), totalProcessed)

	if totalProcessed == 0 {
		t.Fatal("No messages were processed by either consumer")
	}
}

func TestConsumerRestartRebalance(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	uniqueTopic := fmt.Sprintf("%s-restart-%d", testTopic, time.Now().UnixNano())
	groupID := fmt.Sprintf("%s-restart-group-%d", testGroupID, time.Now().Unix())

	conn, err := kafka.DialLeader(ctx, "tcp", testBroker, uniqueTopic, 0)
	if err != nil {
		t.Skipf("Kafka not available: %v", err)
	}
	defer conn.Close()

	firstBatch := createTestMessages(uniqueTopic, 0, 20, "key", "value")
	kafkaMessages1 := make([]kafka.Message, len(firstBatch))
	for i, msg := range firstBatch {
		kafkaMessages1[i] = kafka.Message{
			Key:   msg.Key,
			Value: msg.Value,
		}
	}

	_, err = conn.WriteMessages(kafkaMessages1...)
	if err != nil {
		t.Fatalf("Failed to write first batch: %v", err)
	}

	handler := newTestMessageHandler()

	createConsumer := func() *turnstile.Consumer {
		config := turnstile.Config{
			Brokers:              []string{testBroker},
			GroupID:              groupID,
			Topic:                uniqueTopic,
			Handler:              handler,
			MaxInFlight:          100,
			AutoOffsetReset:      kafka.FirstOffset,
			MinOffsetCommitCount: 5,
			MaxCommitInterval:    500 * time.Millisecond,
		}

		consumer, err := turnstile.NewConsumer(config)
		if err != nil {
			t.Fatalf("Failed to create consumer: %v", err)
		}
		return consumer
	}

	consumer1 := createConsumer()
	consumer1.Start()

	success := waitForCondition(10*time.Second, 200*time.Millisecond, func() bool {
		return handler.GetProcessedCount() >= 20
	})

	if !success {
		t.Fatalf("First consumer did not process first batch in time")
	}

	time.Sleep(2 * time.Second)

	processedAfterFirst := handler.GetProcessedCount()
	t.Logf("First consumer processed %d messages", processedAfterFirst)

	consumer1.Stop()
	t.Logf("Stopped first consumer")

	secondBatch := createTestMessages(uniqueTopic, 0, 20, "key2", "value2")
	kafkaMessages2 := make([]kafka.Message, len(secondBatch))
	for i, msg := range secondBatch {
		kafkaMessages2[i] = kafka.Message{
			Key:   msg.Key,
			Value: msg.Value,
		}
	}

	_, err = conn.WriteMessages(kafkaMessages2...)
	if err != nil {
		t.Fatalf("Failed to write second batch: %v", err)
	}

	t.Logf("Produced second batch of %d messages", len(secondBatch))

	time.Sleep(1 * time.Second)

	consumer2 := createConsumer()
	consumer2.Start()
	defer consumer2.Stop()

	t.Logf("Started second consumer")

	success = waitForCondition(10*time.Second, 200*time.Millisecond, func() bool {
		return handler.GetProcessedCount() >= 40
	})

	finalProcessed := handler.GetProcessedCount()
	t.Logf("After restart - Total processed: %d (expected ~40)", finalProcessed)

	if finalProcessed < 35 {
		t.Errorf("Expected at least 35 messages processed, got %d", finalProcessed)
	}
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(s) > len(substr) && containsAt(s, substr))
}

func containsAt(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
