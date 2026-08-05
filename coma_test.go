package coma

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

const limit = 3

func TestAcquire(t *testing.T) {
	cm := New(limit)
	for range limit {
		cm.Acquire()
	}

	acqErr := make(chan error)
	go func() {
		acqErr <- cm.Acquire()
	}()

	// blocked
	select {
	case err := <-acqErr:
		t.Errorf("Acquire returned instead of blocking, err=%v", err)
	case <-time.After(100 * time.Millisecond):
		// expected
	}
	if actual := cm.RunningCount(); actual != limit {
		t.Errorf("RunningCount=%d should be equal to limit=%d", actual, limit)
	}

	cm.Release()

	// acquired
	select {
	case err := <-acqErr:
		if err != nil {
			t.Error("after Release Acquire returned error, should return nil")
		}
	case <-time.After(time.Second):
		t.Errorf("Acquire seems to be blocked even after a slot was released")
	}
	if actual := cm.RunningCount(); actual != limit {
		t.Errorf("after Release RunningCount=%d should be equal to limit=%d", actual, limit)
	}
}

func TestAcquireContext(t *testing.T) {
	const limit = 3
	cm := New(limit)
	ctx, cancel := context.WithCancel(context.Background())
	for range limit {
		cm.Acquire()
	}

	acqErr := make(chan error)
	go func() {
		acqErr <- cm.AcquireContext(ctx)
	}()

	// blocked
	select {
	case err := <-acqErr:
		t.Errorf("AcquireContext returned instead of blocking, err=%v", err)
	case <-time.After(100 * time.Millisecond):
		// expected
	}
	if actual := cm.RunningCount(); actual != limit {
		t.Errorf("RunningCount=%d should be equal to limit=%d", actual, limit)
	}

	cm.Release()

	// acquired
	select {
	case err := <-acqErr:
		if err != nil {
			t.Error("after Release AcquireContext returned error, should return nil")
		}
	case <-time.After(time.Second):
		t.Errorf("AcquireContext seems to be blocked even after a slot was released")
	}
	if actual := cm.RunningCount(); actual != limit {
		t.Errorf("after Release RunningCount=%d should be equal to limit=%d", actual, limit)
	}

	go func() {
		acqErr <- cm.AcquireContext(ctx)
	}()
	cancel()

	// canceled
	select {
	case err := <-acqErr:
		if err != nil {
			if !errors.Is(err, context.Canceled) {
				t.Error("AcquireContext returned is not expected canceled context error")
			}
		} else {
			t.Error("AcquireContext returned nil instead of canceled context error")
		}
	case <-time.After(time.Second):
		t.Errorf("AcquireContext seems to be blocked even after canceling context")
	}
	if actual := cm.RunningCount(); actual != limit {
		t.Errorf("RunningCount=%d after cancel should be equal to limit=%d", actual, limit)
	}
}
