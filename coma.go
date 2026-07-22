// Package coma limits how many goroutines run concurrently and waits
// for all of them to finish.
package coma

import (
	"context"
	"errors"
	"sync/atomic"
)

// ConcurrencyManager limits how many goroutines can run concurrently
type ConcurrencyManager interface {
	// Wait blocks until a slot is available and claims it for a new goroutine.
	// In case WaitAllDone has been called, Wait returns ErrClosed.
	Wait() error

	// WaitContext blocks until a slot is available and claims it for a new goroutine,
	// or returns ctx.Err() if the context is done first. If WaitAllDone has been called WaitContext returns ErrClosed.
	WaitContext(context.Context) error

	// Done marks a goroutine as finished and releases one slot.
	Done()

	// WaitAllDone waits until all goroutines are done. WaitAllDone is terminal, meaning ConcurrencyManager
	// cannot be reused. It must be called at most once, from a single goroutine, a second call panics.
	WaitAllDone()

	// RunningCount returns the number of currently running goroutines.
	RunningCount() int32
}

// ErrClosed is returned by Wait and WaitContext when WaitAllDone has been called.
var ErrClosed = errors.New("coma: manager is shut down")

type coMa struct {
	sem        chan struct{}
	closed     chan struct{}
	max        int32
	runningCnt atomic.Int32
}

// New creates a ConcurrencyManager that allows at most max concurrently running goroutines.
func New(max int32) ConcurrencyManager {
	return &coMa{
		sem:        make(chan struct{}, max),
		closed:     make(chan struct{}),
		max:        max,
		runningCnt: atomic.Int32{},
	}
}

func (c *coMa) Wait() error {
	select {
	case c.sem <- struct{}{}:
		c.runningCnt.Add(1)
		return nil
	case <-c.closed:
		return ErrClosed
	}
}

func (c *coMa) WaitContext(ctx context.Context) error {
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

func (c *coMa) Done() {
	<-c.sem
	c.runningCnt.Add(-1)
}

func (c *coMa) WaitAllDone() {
	close(c.closed)
	for range c.max {
		c.sem <- struct{}{}
	}
}

func (c *coMa) RunningCount() int32 {
	return c.runningCnt.Load()
}
