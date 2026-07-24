// Package coma limits how many goroutines run concurrently and waits
// for all of them to finish.
package coma

import (
	"context"
	"errors"
	"sync"
)

// ErrClosed is returned by Acquire and AcquireContext when Wait has been called.
var ErrClosed = errors.New("coma: manager is shut down")

// ConcurrencyManager limits how many goroutines can run concurrently.
type ConcurrencyManager struct {
	sem     chan struct{}
	closed  chan struct{}
	penging int
	mu      sync.Mutex
	cond    *sync.Cond
}

// New creates a ConcurrencyManager that allows at most max concurrently running goroutines.
func New(max int) *ConcurrencyManager {
	c := &ConcurrencyManager{
		sem:    make(chan struct{}, max),
		closed: make(chan struct{}),
	}
	c.cond = sync.NewCond(&c.mu)
	return c
}

// Acquire blocks until a slot is available and claims it for a new goroutine.
// If Wait has been called, Acquire returns ErrClosed.
func (c *ConcurrencyManager) Acquire() error {
	c.mu.Lock()
	select {
	case <-c.closed:
		c.mu.Unlock()
		return ErrClosed
	default:
	}
	c.penging++
	c.mu.Unlock()

	select {
	case c.sem <- struct{}{}:
		return nil
	case <-c.closed:
		c.decrementPenging()
		return ErrClosed
	}
}

// AcquireContext blocks until a slot is available and claims it for a new goroutine,
// or returns ctx.Err() if the context is done first. If Wait has been called, AcquireContext returns ErrClosed.
func (c *ConcurrencyManager) AcquireContext(ctx context.Context) error {
	c.mu.Lock()
	select {
	case <-c.closed:
		c.mu.Unlock()
		return ErrClosed
	default:
	}
	c.penging++
	c.mu.Unlock()

	select {
	case c.sem <- struct{}{}:
		return nil
	case <-ctx.Done():
		c.decrementPenging()
		return ctx.Err()
	case <-c.closed:
		c.decrementPenging()
		return ErrClosed
	}
}

// Release marks a goroutine as finished and releases one slot.
func (c *ConcurrencyManager) Release() {
	c.decrementPenging()
	<-c.sem
}

// Wait waits until all goroutines are done. Wait is terminal, meaning ConcurrencyManager cannot be reused.
// Wait is not safe for concurrent use - calling it from multiple goroutines at once may panic.
// A repeated call from the same goroutine is a safe no-op.
func (c *ConcurrencyManager) Wait() {
	c.mu.Lock()
	select {
	case <-c.closed:
	default:
		close(c.closed)
	}
	for c.penging > 0 {
		c.cond.Wait()
	}
	c.mu.Unlock()
}

// RunningCount returns the number of currently running goroutines.
func (c *ConcurrencyManager) RunningCount() int {
	return len(c.sem)
}

func (c *ConcurrencyManager) decrementPenging() {
	c.mu.Lock()
	c.penging--
	if c.penging == 0 {
		c.cond.Broadcast()
	}
	c.mu.Unlock()
}
