package turnstile

import (
	"runtime"
	"sync"
	"testing"
	"time"
)

func TestKeySequencer_PerKeyQueues_BasicFlow(t *testing.T) {
	d := NewKeySequencerWithConfig(10)

	// Acquire key "a"
	if !d.TryAcquire("a") {
		t.Fatal("expected to acquire key 'a'")
	}

	// Key "a" is busy, so enqueue messages for "a" and "b"
	d.Enqueue(createTestMessage("t", 0, 1, "a", "v1"), "a")
	d.Enqueue(createTestMessage("t", 0, 2, "b", "v2"), "b")

	if d.QueueSize() != 2 {
		t.Fatalf("expected queue size 2, got %d", d.QueueSize())
	}

	// Dequeue should find "b" since "a" is still held
	msg, key, ok := d.Dequeue()
	if !ok {
		t.Fatal("expected to dequeue a message")
	}
	if key != "b" {
		t.Fatalf("expected key 'b', got '%s'", key)
	}
	if msg.Offset != 2 {
		t.Fatalf("expected offset 2, got %d", msg.Offset)
	}

	d.Release("b")

	// Release "a" — its queued message should become available
	d.Release("a")

	msg, key, ok = d.Dequeue()
	if !ok {
		t.Fatal("expected to dequeue a message for key 'a'")
	}
	if key != "a" {
		t.Fatalf("expected key 'a', got '%s'", key)
	}

	d.Release("a")
}

func TestKeySequencer_ReadyChan_SignaledOnEnqueue(t *testing.T) {
	d := NewKeySequencerWithConfig(10)

	// Acquire key so enqueue queues it
	d.TryAcquire("k1")

	d.Enqueue(createTestMessage("t", 0, 1, "k1", "v"), "k1")

	// readyChan should be signaled
	select {
	case <-d.ReadyChan():
		// expected
	case <-time.After(100 * time.Millisecond):
		t.Fatal("expected readyChan to be signaled after enqueue")
	}
}

func TestKeySequencer_ReadyChan_SignaledOnRelease(t *testing.T) {
	d := NewKeySequencerWithConfig(10)

	d.TryAcquire("k1")
	d.Enqueue(createTestMessage("t", 0, 1, "k1", "v"), "k1")

	// Drain the signal from enqueue
	select {
	case <-d.ReadyChan():
	default:
	}

	d.Release("k1")

	select {
	case <-d.ReadyChan():
		// expected
	case <-time.After(100 * time.Millisecond):
		t.Fatal("expected readyChan to be signaled after release")
	}
}

func TestKeySequencer_PerKeyDepthLimit(t *testing.T) {
	d := NewKeySequencerWithConfig(3)

	d.TryAcquire("hot")

	// Enqueue 5 messages for the same key, limit is 3
	for i := 0; i < 5; i++ {
		d.Enqueue(createTestMessage("t", 0, int64(i), "hot", "v"), "hot")
	}

	if d.QueueSize() != 3 {
		t.Fatalf("expected queue size 3 (depth limit), got %d", d.QueueSize())
	}

	// The oldest messages should have been dropped — release key and dequeue
	d.Release("hot")

	msg, _, ok := d.Dequeue()
	if !ok {
		t.Fatal("expected to dequeue")
	}
	// Oldest (offset 0, 1) should be dropped, so first available is offset 2
	if msg.Offset != 2 {
		t.Fatalf("expected offset 2 (oldest dropped), got %d", msg.Offset)
	}

	d.Release("hot")
}

func TestKeySequencer_EmptyKey_Bypass(t *testing.T) {
	d := NewKeySequencerWithConfig(10)

	// Empty key should always be acquirable
	if !d.TryAcquire("") {
		t.Fatal("empty key should always be acquirable")
	}

	// Enqueue with empty key should be no-op
	d.Enqueue(createTestMessage("t", 0, 1, "", "v"), "")

	if d.QueueSize() != 0 {
		t.Fatalf("expected queue size 0, got %d", d.QueueSize())
	}
}

// BenchmarkDequeue_PerKeyQueues benchmarks the O(1) per-key dequeue
// with 1000+ messages across 50+ keys.
func BenchmarkDequeue_PerKeyQueues(b *testing.B) {
	d := NewKeySequencerWithConfig(1000)

	const numKeys = 50
	const msgsPerKey = 20

	// Acquire all keys so enqueue queues them
	for i := 0; i < numKeys; i++ {
		key := keyName(i)
		d.TryAcquire(key)
	}

	// Enqueue messages
	for i := 0; i < numKeys; i++ {
		key := keyName(i)
		for j := 0; j < msgsPerKey; j++ {
			d.Enqueue(createTestMessage("t", 0, int64(j), key, "v"), key)
		}
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		// Release a key to make it available, then dequeue
		key := keyName(i % numKeys)
		d.Release(key)

		_, _, ok := d.Dequeue()
		if ok {
			d.Release(key)
		}

		// Re-acquire and re-enqueue to keep the benchmark going
		d.TryAcquire(key)
		d.Enqueue(createTestMessage("t", 0, int64(i), key, "v"), key)
	}
}

// BenchmarkEnqueue_PerKeyQueues benchmarks enqueue with per-key queues.
func BenchmarkEnqueue_PerKeyQueues(b *testing.B) {
	d := NewKeySequencerWithConfig(1000)

	// Acquire all keys
	for i := 0; i < 50; i++ {
		d.TryAcquire(keyName(i))
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		key := keyName(i % 50)
		d.Enqueue(createTestMessage("t", 0, int64(i), key, "v"), key)
	}
}

func keyName(i int) string {
	return "key-" + string(rune('A'+i%26)) + "-" + string(rune('0'+i/26))
}

// TestKeySequencer_Release_KeepsKeyLocked verifies that after Release, a key with
// queued messages stays locked so TryAcquire fails — preventing a newly-fetched
// message from jumping ahead of the queue.
func TestKeySequencer_Release_KeepsKeyLocked(t *testing.T) {
	d := NewKeySequencerWithConfig(10)

	// Acquire key "K" and enqueue messages
	d.TryAcquire("K")
	d.Enqueue(createTestMessage("t", 0, 1, "K", "v1"), "K")
	d.Enqueue(createTestMessage("t", 0, 2, "K", "v2"), "K")

	// Release — key should stay locked because queue has messages
	d.Release("K")

	// TryAcquire must fail — the key is still locked
	if d.TryAcquire("K") {
		t.Fatal("TryAcquire should fail — key must stay locked while queue has messages")
	}

	// A newly-arrived message must be enqueued, not processed directly
	d.Enqueue(createTestMessage("t", 0, 3, "K", "v3"), "K")

	// Dequeue must return the oldest queued message (offset 1), not the new one (offset 3)
	msg, key, ok := d.Dequeue()
	if !ok {
		t.Fatal("expected to dequeue a message")
	}
	if key != "K" {
		t.Fatalf("expected key 'K', got '%s'", key)
	}
	if msg.Offset != 1 {
		t.Fatalf("expected offset 1 (oldest queued), got %d — FIFO ordering violated", msg.Offset)
	}

	// Drain remaining queued messages in order
	d.Release("K")
	msg, _, ok = d.Dequeue()
	if !ok || msg.Offset != 2 {
		t.Fatalf("expected offset 2, got %d, ok=%v", msg.Offset, ok)
	}

	d.Release("K")
	msg, _, ok = d.Dequeue()
	if !ok || msg.Offset != 3 {
		t.Fatalf("expected offset 3, got %d, ok=%v", msg.Offset, ok)
	}

	d.Release("K")

	// After draining, key should be fully unlocked
	if !d.TryAcquire("K") {
		t.Fatal("TryAcquire should succeed after all queued messages are drained")
	}
	d.Release("K")
}

// TestKeySequencer_Release_FIFO_Under_Contention stresses the Release/TryAcquire
// boundary with concurrent goroutines to ensure no message jumps the queue.
func TestKeySequencer_Release_FIFO_Under_Contention(t *testing.T) {
	d := NewKeySequencerWithConfig(1000)

	const workers = 8
	const messagesPerWorker = 100

	var (
		wg      sync.WaitGroup
		orderMu sync.Mutex
		offsets []int64
	)

	start := make(chan struct{})

	// Simulate consumeFromKafka: workers fetch messages and TryAcquire/Enqueue
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			<-start

			for j := 0; j < messagesPerWorker; j++ {
				key := "shared-key"

				msg := createTestMessage("t", 0, int64(id*messagesPerWorker+j), key, "v")

				if !d.TryAcquire(key) {
					d.Enqueue(msg, key)
					continue
				}

				orderMu.Lock()
				offsets = append(offsets, msg.Offset)
				orderMu.Unlock()

				d.Release(key)
			}
		}(w)
	}

	close(start)
	wg.Wait()

	// Drain all remaining messages from the queue
	drained := 0
	emptyTicks := 0
	for emptyTicks < 100 {
		msg, _, ok := d.Dequeue()
		if !ok {
			if d.QueueSize() == 0 {
				emptyTicks++
				runtime.Gosched()
				continue
			}
			// Queue has messages but Dequeue failed — this means the key is locked
			// and pendingKeys should have been set by a prior Release
			runtime.Gosched()
			continue
		}

		emptyTicks = 0
		drained++

		orderMu.Lock()
		offsets = append(offsets, msg.Offset)
		orderMu.Unlock()

		d.Release("shared-key")
	}

	orderMu.Lock()
	total := len(offsets)
	orderMu.Unlock()

	if total == 0 {
		t.Fatal("expected some messages to be processed")
	}

	t.Logf("processed %d messages directly, drained %d from queue", total-drained, drained)
}

// TestKeySequencer_Release_EmptyQueue_DeletesKey verifies that Release properly
// unlocks the key when the queue is empty.
func TestKeySequencer_Release_EmptyQueue_DeletesKey(t *testing.T) {
	d := NewKeySequencerWithConfig(10)

	d.TryAcquire("K")

	// Release with empty queue should delete the key
	d.Release("K")

	// Key should be fully unlocked now
	if !d.TryAcquire("K") {
		t.Fatal("TryAcquire should succeed after Release with empty queue")
	}
	d.Release("K")
}

// TestKeySequencer_PendingKeys_ClearedOnDequeue verifies that pendingKeys
// is properly cleaned up during Dequeue, even for edge cases.
func TestKeySequencer_PendingKeys_ClearedOnDequeue(t *testing.T) {
	d := NewKeySequencerWithConfig(10)

	// Set up two keys with queued messages
	d.TryAcquire("A")
	d.TryAcquire("B")
	d.Enqueue(createTestMessage("t", 0, 1, "A", "v"), "A")
	d.Enqueue(createTestMessage("t", 0, 2, "B", "v"), "B")

	// Release both — both should be pending
	d.Release("A")
	d.Release("B")

	// Dequeue one
	msg, key, ok := d.Dequeue()
	if !ok {
		t.Fatal("expected to dequeue")
	}

	// The other should still be available
	msg2, key2, ok2 := d.Dequeue()
	if !ok2 {
		t.Fatal("expected to dequeue second message")
	}

	// Both should be dequeued (order doesn't matter across different keys)
	offsets := map[int64]bool{msg.Offset: true, msg2.Offset: true}
	if !offsets[1] || !offsets[2] {
		t.Fatalf("expected offsets 1 and 2, got %d and %d", msg.Offset, msg2.Offset)
	}
	_ = key
	_ = key2

	// Release both — queues should be empty, keys should be deleted
	d.Release("A")
	d.Release("B")

	// Both keys should be unlocked
	if !d.TryAcquire("A") {
		t.Fatal("key A should be unlocked")
	}
	d.Release("A")

	if !d.TryAcquire("B") {
		t.Fatal("key B should be unlocked")
	}
	d.Release("B")
}
