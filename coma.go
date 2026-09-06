// Package coma limits how many goroutines run concurrently and waits for all of them to finish.
package coma

import (
	"context"
	"errors"
	"sync"
)

// ErrClosed is returned by [ConcurrencyManager.Acquire] and [ConcurrencyManager.AcquireContext]
// when [ConcurrencyManager.Close] or [ConcurrencyManager.Wait] has been called.
var ErrClosed = errors.New("coma: manager is shut down")

// ConcurrencyManager limits how many goroutines can run concurrently.
type ConcurrencyManager struct {
	sem     chan struct{}
	closed  chan struct{}
	pending int
	mu      sync.Mutex
	cond    *sync.Cond
}

// New creates a [ConcurrencyManager] that allows at most max concurrently running goroutines.
// A max < 1 is treated as 1.
func New(max int) *ConcurrencyManager {
	if max < 1 {
		max = 1
	}

	c := &ConcurrencyManager{
		sem:    make(chan struct{}, max),
		closed: make(chan struct{}),
	}
	c.cond = sync.NewCond(&c.mu)
	return c
}

// Acquire blocks until a slot is available and claims it for a new goroutine.
// If [ConcurrencyManager] has been closed, Acquire returns [ErrClosed].
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

// AcquireContext blocks until a slot is available and claims it for a new goroutine.
// If context.Done() has been closed ctx.Err() is returned and if [ConcurrencyManager] has already been closed,
// AcquireContext returns [ErrClosed].
func (c *ConcurrencyManager) AcquireContext(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
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
// Every successful [ConcurrencyManager.Acquire] or [ConcurrencyManager.AcquireContext] must be matched
// by exactly one Release.
//
// Release panics when no slot is held at all.
// An unmatched Release called while other goroutines are holding slots takes one of theirs instead, which allows
// the limit to be exceeded and can make [ConcurrencyManager.Wait] return before those goroutines finish.
// A later Release then panics in its place.
func (c *ConcurrencyManager) Release() {
	select {
	case <-c.sem:
	default:
		panic("coma: Release called when there are no slots to release")
	}
	c.decrementPending()
}

// Close closes [ConcurrencyManager] so it stops accepting acquires. Close is terminal, meaning ConcurrencyManager
// cannot be reused, but [ConcurrencyManager.Wait] still can be called.
func (c *ConcurrencyManager) Close() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.closeLocked()
}

// Wait closes [ConcurrencyManager] and waits until all goroutines are done. Wait is terminal,
// meaning ConcurrencyManager cannot be reused. Wait is safe for concurrent use. A repeated call is a safe no-op.
func (c *ConcurrencyManager) Wait() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.closeLocked()
	for c.pending > 0 {
		c.cond.Wait()
	}
}

// RunningCount returns the number of currently held slots: those for which [ConcurrencyManager.Acquire] or
// [ConcurrencyManager.AcquireContext] returned nil and [ConcurrencyManager.Release] has not yet been called.
// Goroutines blocked in Acquire are not counted, so this is not necessarily the number of goroutines running.
func (c *ConcurrencyManager) RunningCount() int {
	return len(c.sem)
}

// Go calls Acquire, if it succeeds, calls f in a new goroutine. When f returns, Release is called.
// If f panics, Release is not called and the panic propagates as usual.
func (c *ConcurrencyManager) Go(f func()) error {
	if err := c.Acquire(); err != nil {
		return err
	}
	go func() {
		defer func() {
			if x := recover(); x != nil {
				// f panicked. Calling Release here could wake Wait while the panic is still processing.
				// Wait would race the panic and potentially even exit the process before the panic completes.
				// So we don't call Release and let the panic complete. Same behavior as sync.WaitGroup.Go().
				panic(x)
			}

			c.Release()
		}()
		f()
	}()
	return nil
}

// GoContext calls AcquireContext, if it succeeds, calls f in a new goroutine. When f returns, Release is called.
// If f panics, Release is not called and the panic propagates as usual.
func (c *ConcurrencyManager) GoContext(ctx context.Context, f func()) error {
	if err := c.AcquireContext(ctx); err != nil {
		return err
	}
	go func() {
		defer func() {
			if x := recover(); x != nil {
				// f panicked. Calling Release here could wake Wait while the panic is still processing.
				// Wait would race the panic and potentially even exit the process before the panic completes.
				// So we don't call Release and let the panic complete. Same behavior as sync.WaitGroup.Go().
				panic(x)
			}

			c.Release()
		}()
		f()
	}()
	return nil
}

// incrementPending locks the manager and increments pending,
// if [ConcurrencyManager] hasn't been closed, else returns [ErrClosed].
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

// closeLocked closes the c.closed channel if not already closed. c.mu must be locked by caller.
func (c *ConcurrencyManager) closeLocked() {
	select {
	case <-c.closed:
	default:
		close(c.closed)
	}
}
