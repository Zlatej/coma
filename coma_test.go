package coma

import (
	"testing"
	"time"
)

func TestAcquire(t *testing.T) {
	const limit = 3
	cm := New(limit)
	for range limit {
		cm.Acquire()
	}

	acqErr := make(chan error)
	go func() {
		acqErr <- cm.Acquire()
	}()

	select {
	case err := <-acqErr:
		t.Errorf("Acquire returned instead of blocking, err=%v", err)
	case <-time.After(300 * time.Microsecond):
		// expected
	}

	if actual := cm.RunningCount(); actual != limit {
		t.Errorf("RunningCount=%d should be equal to limit=%d", actual, limit)
	}

	cm.Release()

	select {
	case err := <-acqErr:
		if err != nil {
			t.Error("after Release Acquire returned error, should return nil")
		}
	case <-time.After(300 * time.Microsecond):
		t.Errorf("Acquire seems to be blocked even after a slot was released")
	}

	if actual := cm.RunningCount(); actual != limit {
		t.Errorf("after Release RunningCount=%d should be equal to limit=%d", actual, limit)
	}
}
