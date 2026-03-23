package worker

import "sync"

// Throttle controls the level of concurrency using a channel-based semaphore.
// It can be dynamically adjusted at runtime via SetConcurrency.
type Throttle struct {
	sem     chan struct{}
	current int
	max     int
	mu      sync.Mutex
}

// NewThrottle creates a Throttle with the given initial concurrency level.
func NewThrottle(n int) *Throttle {
	if n < 1 {
		n = 1
	}
	t := &Throttle{
		sem:     make(chan struct{}, n),
		current: n,
		max:     n,
	}
	return t
}

// Acquire blocks until a slot is available.
func (t *Throttle) Acquire() {
	t.mu.Lock()
	sem := t.sem
	t.mu.Unlock()
	sem <- struct{}{}
}

// Release frees a slot.
func (t *Throttle) Release() {
	t.mu.Lock()
	sem := t.sem
	t.mu.Unlock()
	<-sem
}

// SetConcurrency adjusts the concurrency level by draining or adding tokens
// to the existing semaphore channel. In-flight workers are not affected because
// they acquired/release on the same channel instance.
// Minimum concurrency is 1.
func (t *Throttle) SetConcurrency(n int) {
	if n < 1 {
		n = 1
	}
	if n > t.max {
		n = t.max // cannot exceed original capacity (channel buffer size)
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if n == t.current {
		return
	}
	if n < t.current {
		// Reduce: fill extra slots so fewer workers can acquire.
		for i := n; i < t.current; i++ {
			t.sem <- struct{}{}
		}
	} else {
		// Increase: drain blocked slots to free capacity.
		for i := t.current; i < n; i++ {
			<-t.sem
		}
	}
	t.current = n
}

// Current returns the current concurrency level.
func (t *Throttle) Current() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.current
}

// Max returns the original (maximum) concurrency level.
func (t *Throttle) Max() int {
	return t.max
}

// Close is a no-op for compatibility; the throttle doesn't need explicit cleanup.
func (t *Throttle) Close() {}
