package turnstile

import (
	"container/list"
	"sync"

	"github.com/segmentio/kafka-go"
)

// MessageWithKey holds a message and its key for queuing.
type MessageWithKey struct {
	Message kafka.Message
	Key     string
}

// KeySequencer prevents concurrent processing of messages with the same key.
// It maintains per-key queues for O(1) dequeue, a notification channel for
// near-zero-latency dispatch, and per-key queue depth limits.
type KeySequencer struct {
	handlingKeys    sync.Map // map[string]bool - keys currently being processed
	maxQueuedPerKey int

	mu          sync.Mutex            // protects fields below
	keyQueues   map[string]*list.List // per-key FIFO queues
	totalQueued int                   // total messages across all key queues
	readyChan   chan struct{}         // signaled when a key becomes available
	pendingKeys map[string]bool       // keys released with queued messages, ready for dequeue
}

// NewKeySequencer creates a new KeySequencer.
func NewKeySequencer() *KeySequencer {
	return NewKeySequencerWithConfig(100)
}

// NewKeySequencerWithConfig creates a new KeySequencer with the given configuration.
func NewKeySequencerWithConfig(maxQueuedPerKey int) *KeySequencer {
	return &KeySequencer{
		handlingKeys:    sync.Map{},
		maxQueuedPerKey: maxQueuedPerKey,
		keyQueues:       make(map[string]*list.List),
		readyChan:       make(chan struct{}, 1),
		pendingKeys:     make(map[string]bool),
	}
}

// ReadyChan returns the channel that is signaled when a message becomes available for dequeue.
// Consumers should select on this instead of polling.
func (d *KeySequencer) ReadyChan() <-chan struct{} {
	return d.readyChan
}

// TryAcquire attempts to acquire a lock on the key.
// Returns true if the key was acquired (not being processed).
// Returns false if the key is already being processed (message should be queued).
func (d *KeySequencer) TryAcquire(key string) bool {
	if key == "" {
		return true
	}
	_, exists := d.handlingKeys.LoadOrStore(key, true)
	return !exists
}

// Release releases the lock on the key. If there are queued messages for that key,
// the key stays locked and the next message is marked as pending for immediate dequeue.
// This prevents a race where a newly-fetched message could jump ahead of queued messages.
func (d *KeySequencer) Release(key string) {
	if key == "" {
		return
	}

	d.mu.Lock()
	if q, ok := d.keyQueues[key]; ok && q.Len() > 0 {
		// Keep key locked in handlingKeys — mark as pending so Dequeue
		// can pick it up without needing TryAcquire.
		d.pendingKeys[key] = true
		d.mu.Unlock()
		d.signalReady()
		return
	}
	d.mu.Unlock()

	d.handlingKeys.Delete(key)
}

// Enqueue adds a message to the per-key queue. If the key's queue exceeds
// maxQueuedPerKey, the oldest message for that key is dropped.
func (d *KeySequencer) Enqueue(msg kafka.Message, key string) {
	if key == "" {
		return
	}

	d.mu.Lock()
	q, ok := d.keyQueues[key]
	if !ok {
		q = list.New()
		d.keyQueues[key] = q
	}

	if q.Len() >= d.maxQueuedPerKey {
		q.Remove(q.Front())
		d.totalQueued--
	}

	q.PushBack(MessageWithKey{
		Message: msg,
		Key:     key,
	})
	d.totalQueued++
	d.mu.Unlock()

	d.signalReady()
}

// Dequeue attempts to dequeue the next available message whose key is not being processed.
// It first checks keys marked as pending (released with queued messages), then checks
// all queues for keys that can be acquired.
// Returns the message, key, and true if found; otherwise returns empty message and false.
func (d *KeySequencer) Dequeue() (kafka.Message, string, bool) {
	d.mu.Lock()
	defer d.mu.Unlock()

	// Priority: dequeue pending keys first (released by Release with queued messages).
	// These keys are still locked in handlingKeys, so TryAcquire would fail —
	// we bypass it since Release already approved the next message.
	for key := range d.pendingKeys {
		delete(d.pendingKeys, key)

		q, ok := d.keyQueues[key]
		if !ok || q.Len() == 0 {
			continue
		}

		front := q.Front()
		mwk := front.Value.(MessageWithKey)
		q.Remove(front)
		d.totalQueued--
		if q.Len() == 0 {
			delete(d.keyQueues, key)
		}
		return mwk.Message, mwk.Key, true
	}

	for _, q := range d.keyQueues {
		if q.Len() == 0 {
			continue
		}

		front := q.Front()
		mwk := front.Value.(MessageWithKey)

		if d.TryAcquire(mwk.Key) {
			q.Remove(front)
			d.totalQueued--
			if q.Len() == 0 {
				delete(d.keyQueues, mwk.Key)
			}
			return mwk.Message, mwk.Key, true
		}
	}

	return kafka.Message{}, "", false
}

// QueueSize returns the current total size of all per-key queues.
func (d *KeySequencer) QueueSize() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.totalQueued
}

// signalReady performs a non-blocking send on readyChan.
func (d *KeySequencer) signalReady() {
	select {
	case d.readyChan <- struct{}{}:
	default:
	}
}
