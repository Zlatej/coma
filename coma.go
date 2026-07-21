// Package coma limits how many goroutines run concurrently and waits
// for all of them to finish.
package coma

import (
	"context"
	"sync"
	"sync/atomic"
)

// ConcurrencyManager limits how many goroutines can run concurrently
type ConcurrencyManager interface {
	// Wait blocks until a slot is available and claims it for a new goroutine
	Wait()

	// WaitContext blocks until a slot is available and claims it for a new goroutine,
	// or returns ctx.Err() if the context is done first
	WaitContext(context.Context) error

	// Done marks a goroutine as finished
	Done()

	// WaitAllDone waits until all goroutines are done
	WaitAllDone()

	// RunningCount returns the number of currently running goroutines
	RunningCount() int32
}

type coMa struct {
	sem        chan struct{}
	wg         sync.WaitGroup
	max        int32
	runningCnt atomic.Int32
}

// New creates a ConcurrencyManager that allows at most max concurrently running goroutines
func New(max int32) ConcurrencyManager {
	return &coMa{
		sem:        make(chan struct{}, max),
		wg:         sync.WaitGroup{},
		max:        max,
		runningCnt: atomic.Int32{},
	}
}

func (c *coMa) Wait() {
	c.sem <- struct{}{}
	c.wg.Add(1)
	c.runningCnt.Add(1)
}

func (c *coMa) WaitContext(ctx context.Context) error {
	select {
	case c.sem <- struct{}{}:
		c.wg.Add(1)
		c.runningCnt.Add(1)
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (c *coMa) Done() {
	<-c.sem
	c.runningCnt.Add(-1)
	c.wg.Done()
}

func (c *coMa) WaitAllDone() {
	c.wg.Wait()
}

func (c *coMa) RunningCount() int32 {
	return c.runningCnt.Load()
}
