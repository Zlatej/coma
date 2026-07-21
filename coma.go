// Package coma limits how many goroutines run concurrently and waits
// for all of them to finish.
package coma

import (
	"sync"
	"sync/atomic"
)

// ConcurrencyManager limits how many goroutines can run concurrently
type ConcurrencyManager interface {
	// Wait blocks until a slot is available and claims it for a new goroutine
	Wait()

	// Done marks a goroutine as finished
	Done()

	// WaitAllDone waits until all goroutines are done
	WaitAllDone()

	// RunningCount returns the number of currently running goroutines
	RunningCount() int32
}

type concurrencyManager struct {
	sem        chan struct{}
	wg         sync.WaitGroup
	max        int32
	runningCnt atomic.Int32
}

// New creates a ConcurrencyManager that allows at most max concurrently running goroutines
func New(max int32) ConcurrencyManager {
	return &concurrencyManager{
		sem:        make(chan struct{}, max),
		wg:         sync.WaitGroup{},
		max:        max,
		runningCnt: atomic.Int32{},
	}
}

func (c *concurrencyManager) Wait() {
	c.sem <- struct{}{}
	c.wg.Add(1)
	c.runningCnt.Add(1)
}

func (c *concurrencyManager) Done() {
	<-c.sem
	c.runningCnt.Add(-1)
	c.wg.Done()
}

func (c *concurrencyManager) WaitAllDone() {
	c.wg.Wait()
}

func (c *concurrencyManager) RunningCount() int32 {
	return c.runningCnt.Load()
}
