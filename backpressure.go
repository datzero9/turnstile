package turnstile

import (
	"context"
	"sync"
	"sync/atomic"
)

// BackpressureController manages backpressure for message processing.
// It tracks the number of in-flight messages and blocks when the limit is reached.
type BackpressureController struct {
	maxInFlight int32
	current     atomic.Int32
	cond        *sync.Cond
}

func NewBackpressureController(maxInFlight int) *BackpressureController {
	return &BackpressureController{
		maxInFlight: int32(maxInFlight),
		cond:        sync.NewCond(&sync.Mutex{}),
	}
}

func (c *BackpressureController) Acquire() {
	c.cond.L.Lock()
	defer c.cond.L.Unlock()

	for c.current.Load() >= c.maxInFlight {
		c.cond.Wait()
	}
	c.current.Add(1)
}

func (c *BackpressureController) Release() {
	c.current.Add(-1)
	c.cond.Broadcast()
}

// WaitUntilBelowCapacity blocks until the current in-flight count drops below
// maxInFlight, or the context is cancelled.
func (c *BackpressureController) WaitUntilBelowCapacity(ctx context.Context) error {
	c.cond.L.Lock()
	defer c.cond.L.Unlock()

	if c.current.Load() < c.maxInFlight {
		return nil
	}

	stop := make(chan struct{})
	defer close(stop)

	go func() {
		select {
		case <-ctx.Done():
			c.cond.Broadcast()
		case <-stop:
		}
	}()

	for c.current.Load() >= c.maxInFlight {
		c.cond.Wait()
		if ctx.Err() != nil {
			return ctx.Err()
		}
	}
	return nil
}

func (c *BackpressureController) Current() int {
	return int(c.current.Load())
}

func (c *BackpressureController) Available() int {
	current := c.current.Load()
	available := c.maxInFlight - current
	if available < 0 {
		return 0
	}
	return int(available)
}

func (c *BackpressureController) IsAtCapacity() bool {
	return c.current.Load() >= c.maxInFlight
}
