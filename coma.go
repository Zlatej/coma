// Package coma limits how many goroutines run concurrently and waits for all of them to finish.
package coma

import (
	"context"
	"errors"
	"sync"
)

// ErrClosed is returned by [ConcurrencyManager.Acquire] and [ConcurrencyManager.AcquireContext] when Wait has been called.
var ErrClosed = errors.New("coma: manager is shut down")

// ConcurrencyManager limits how many goroutines can run concurrently.
type ConcurrencyManager struct {
	sem     chan struct{}
	closed  chan struct{}
	pending int
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
	if err := c.incrementPending(); err != nil {
		return err
	}

	select {
	case c.sem <- struct{}{}:
		return nil
	case <-c.closed:
		c.decrementPending()
		return ErrClosed
	}
}

// AcquireContext blocks until a slot is available and claims it for a new goroutine,
// or returns ctx.Err() if the context is done first. If Wait has been called, [ConcurrencyManager.AcquireContext]
// returns [ErrClosed].
func (c *ConcurrencyManager) AcquireContext(ctx context.Context) error {
	if err := c.incrementPending(); err != nil {
		return err
	}

	select {
	case c.sem <- struct{}{}:
		return nil
	case <-ctx.Done():
		c.decrementPending()
		return ctx.Err()
	case <-c.closed:
		c.decrementPending()
		return ErrClosed
	}
}

// Release marks a goroutine as finished and releases one slot.
func (c *ConcurrencyManager) Release() {
	<-c.sem
	c.decrementPending()
}

// Wait waits until all goroutines are done. Wait is terminal, meaning ConcurrencyManager cannot be reused.
// Wait is safe for concurrent use. A repeated call is a safe no-op.
func (c *ConcurrencyManager) Wait() {
	c.mu.Lock()
	select {
	case <-c.closed:
	default:
		close(c.closed)
	}
	for c.pending > 0 {
		c.cond.Wait()
	}
	c.mu.Unlock()
}

// RunningCount returns the number of currently running goroutines.
func (c *ConcurrencyManager) RunningCount() int {
	return len(c.sem)
}

// incrementPending locks the manager and increments pending, if Wait hasn't been called, else returns [ErrClosed].
func (c *ConcurrencyManager) incrementPending() error {
	c.mu.Lock()
	select {
	case <-c.closed:
		c.mu.Unlock()
		return ErrClosed
	default:
	}
	c.pending++
	c.mu.Unlock()
	return nil
}

// decrementPending locks the manager and decrements pending.
// If there are no other pending goroutines, broadcasts the information.
func (c *ConcurrencyManager) decrementPending() {
	c.mu.Lock()
	c.pending--
	if c.pending <= 0 {
		c.cond.Broadcast()
	}
	c.mu.Unlock()
}
