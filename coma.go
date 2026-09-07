// Package coma limits how many goroutines run concurrently and waits for all of them to finish.
package coma

import (
	"context"
	"errors"
	"sync"
)

// ErrClosed is returned by [Gate.Acquire] and [Gate.AcquireContext]
// when [Gate.Close] or [Gate.Wait] has been called.
var ErrClosed = errors.New("coma: gate is closed")

// Gate limits how many goroutines can run concurrently.
type Gate struct {
	sem     chan struct{}
	closed  chan struct{}
	pending int
	mu      sync.Mutex
	cond    *sync.Cond
}

// ConcurrencyManager limits how many goroutines can run concurrently.
//
// Deprecated: use [Gate] instead
type ConcurrencyManager = Gate

// New creates a [Gate] that allows at most max concurrently running goroutines.
// A max < 1 is treated as 1.
func New(max int) *Gate {
	if max < 1 {
		max = 1
	}

	g := &Gate{
		sem:    make(chan struct{}, max),
		closed: make(chan struct{}),
	}
	g.cond = sync.NewCond(&g.mu)
	return g
}

// Acquire blocks until a slot is available and claims it for a new goroutine.
// If [Gate] has been closed, Acquire returns [ErrClosed].
func (g *Gate) Acquire() error {
	if err := g.incrementPending(); err != nil {
		return err
	}

	select {
	case g.sem <- struct{}{}:
		return nil
	case <-g.closed:
		g.decrementPending()
		return ErrClosed
	}
}

// AcquireContext blocks until a slot is available and claims it for a new goroutine.
// If context.Done() has been closed ctx.Err() is returned and if [Gate] has already been closed,
// AcquireContext returns [ErrClosed].
func (g *Gate) AcquireContext(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := g.incrementPending(); err != nil {
		return err
	}

	select {
	case g.sem <- struct{}{}:
		return nil
	case <-ctx.Done():
		g.decrementPending()
		return ctx.Err()
	case <-g.closed:
		g.decrementPending()
		return ErrClosed
	}
}

// Release marks a goroutine as finished and releases one slot.
// Every successful [Gate.Acquire] or [Gate.AcquireContext] must be matched
// by exactly one Release.
//
// Release panics when no slot is held at all.
// An unmatched Release called while other goroutines are holding slots takes one of theirs instead, which allows
// the limit to be exceeded and can make [Gate.Wait] return before those goroutines finish.
// A later Release then panics in its place.
func (g *Gate) Release() {
	select {
	case <-g.sem:
	default:
		panic("coma: Release called when there are no slots to release")
	}
	g.decrementPending()
}

// Close closes [Gate] so it stops accepting acquires. Close is terminal, meaning Gate
// cannot be reused, but [Gate.Wait] still can be called.
func (g *Gate) Close() {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.closeLocked()
}

// Wait closes [Gate] and waits until all goroutines are done. Wait is terminal,
// meaning Gate cannot be reused. Wait is safe for concurrent use. A repeated call is a safe no-op.
func (g *Gate) Wait() {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.closeLocked()
	for g.pending > 0 {
		g.cond.Wait()
	}
}

// Held returns the number of currently held slots: those for which [Gate.Acquire] or
// [Gate.AcquireContext] returned nil and [Gate.Release] has not yet been called.
// Goroutines blocked in Acquire are not counted, so this is not necessarily the number of goroutines running.
func (g *Gate) Held() int {
	return len(g.sem)
}

// Go calls Acquire, if it succeeds, calls f in a new goroutine. When f returns, Release is called.
// If f panics, Release is not called and the panic propagates as usual.
func (g *Gate) Go(f func()) error {
	if err := g.Acquire(); err != nil {
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

			g.Release()
		}()
		f()
	}()
	return nil
}

// GoContext calls AcquireContext, if it succeeds, calls f in a new goroutine. When f returns, Release is called.
// If f panics, Release is not called and the panic propagates as usual.
func (g *Gate) GoContext(ctx context.Context, f func()) error {
	if err := g.AcquireContext(ctx); err != nil {
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

			g.Release()
		}()
		f()
	}()
	return nil
}

// incrementPending locks the [Gate] and increments pending,
// if Gate hasn't been closed, else returns [ErrClosed].
func (g *Gate) incrementPending() error {
	g.mu.Lock()
	select {
	case <-g.closed:
		g.mu.Unlock()
		return ErrClosed
	default:
	}
	g.pending++
	g.mu.Unlock()
	return nil
}

// decrementPending locks the [Gate] and decrements pending.
// If there are no other pending goroutines, broadcasts the information.
func (g *Gate) decrementPending() {
	g.mu.Lock()
	g.pending--
	if g.pending <= 0 {
		g.cond.Broadcast()
	}
	g.mu.Unlock()
}

// closeLocked closes the c.closed channel if not already closed. c.mu must be locked by caller.
func (g *Gate) closeLocked() {
	select {
	case <-g.closed:
	default:
		close(g.closed)
	}
}
