// Package coma limits how many goroutines run concurrently and waits
// for all of them to finish.
package coma

import (
	"context"
	"errors"
	"sync/atomic"
)

// ErrClosed is returned by Acquire and AcquireContext when Wait has been called.
var ErrClosed = errors.New("coma: manager is shut down")

// ConcurrencyManager limits how many goroutines can run concurrently.
type ConcurrencyManager struct {
	sem        chan struct{}
	closed     chan struct{}
	max        int32
	runningCnt atomic.Int32
}

// New creates a ConcurrencyManager that allows at most max concurrently running goroutines.
func New(max int32) *ConcurrencyManager {
	return &ConcurrencyManager{
		sem:        make(chan struct{}, max),
		closed:     make(chan struct{}),
		max:        max,
		runningCnt: atomic.Int32{},
	}
}

// Acquire blocks until a slot is available and claims it for a new goroutine.
// If Wait has been called, Acquire returns ErrClosed.
func (c *ConcurrencyManager) Acquire() error {
	select {
	case c.sem <- struct{}{}:
		c.runningCnt.Add(1)
		return nil
	case <-c.closed:
		return ErrClosed
	}
}

// AcquireContext blocks until a slot is available and claims it for a new goroutine,
// or returns ctx.Err() if the context is done first. If Wait has been called, AcquireContext returns ErrClosed.
func (c *ConcurrencyManager) AcquireContext(ctx context.Context) error {
	select {
	case c.sem <- struct{}{}:
		c.runningCnt.Add(1)
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-c.closed:
		return ErrClosed
	}
}

// Release marks a goroutine as finished and releases one slot.
func (c *ConcurrencyManager) Release() {
	<-c.sem
	c.runningCnt.Add(-1)
}

// Wait waits until all goroutines are done. Wait is terminal, meaning ConcurrencyManager cannot be reused.
// Wait is not safe for concurrent use - calling it from multiple goroutines at once may panic.
// A repeated call from the same goroutine is a safe no-op.
func (c *ConcurrencyManager) Wait() {
	select {
	case <-c.closed:
		return
	default:
	}
	close(c.closed)
	for range c.max {
		c.sem <- struct{}{}
	}
}

// RunningCount returns the number of currently running goroutines.
func (c *ConcurrencyManager) RunningCount() int32 {
	return c.runningCnt.Load()
}
