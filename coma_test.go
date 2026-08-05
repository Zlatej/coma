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

func TestRelease(t *testing.T) {
	// other options are implicitely tested in other tests
	t.Run("Release - nothing to release", func(t *testing.T) {
		cm := New(limit)
		done := atomic.Bool{}
		go func() {
			cm.Release()
			done.Store(true)
		}()

		if !done.Load() {
			time.Sleep(time.Second)
			if done.Load() {
				t.Error("Release notblocked when nothing to release")
			}
		}
	})
}

func TestRunningCount(t *testing.T) {
	t.Run("init", func(t *testing.T) {
		cm := New(limit)
		if cnt := cm.RunningCount(); cnt != 0 {
			t.Errorf("RunningCount is %d, should be 0", cnt)
		}
	})
	t.Run("sequential", func(t *testing.T) {
		cm := New(limit)

		for i := range limit {
			if cur := cm.RunningCount(); cur != i {
				t.Errorf("Acquire: RunningCount returned %d, expected is `%d", cur, i)
			}
			if err := cm.Acquire(); err != nil {
				t.Fatalf("Acquire: %v", err)
			}
		}

		for i := limit; i != 0; i-- {
			if cur := cm.RunningCount(); cur != i {
				t.Errorf("Release: RunningCount returned %d, expected is %d", cur, i)
			}
			cm.Release()
		}

		if cur := cm.RunningCount(); cur != 0 {
			t.Errorf("RunningCount returned %d, expected is 0", cur)
		}
	})
	t.Run("parallel", func(t *testing.T) {
		cm := New(limit)
		var workers, monitor sync.WaitGroup
		var done atomic.Bool
		var peak atomic.Int32

		monitor.Go(func() {
			for !done.Load() {
				if cnt := cm.RunningCount(); cnt < 0 || cnt > limit {
					t.Errorf("RunningCount=%d exceeded the limit=%d", cnt, limit)
					return
				} else if cnt > int(peak.Load()) {
					peak.Store(int32(cnt))
				}
				time.Sleep(5 * time.Millisecond)
			}
		})

		for range limit * 3 {
			workers.Go(func() {
				if err := cm.Acquire(); err != nil {
					t.Errorf("Acquire: %v", err)
				}
				time.Sleep(100 * time.Millisecond)
				cm.Release()
			})
		}

		workers.Wait()
		done.Store(true)
		monitor.Wait()

		if cnt := cm.RunningCount(); cnt != 0 {
			t.Errorf("RunningCount is %d, should be 0", cnt)
		}
		if peak.Load() != limit {
			t.Errorf("peak goroutines count=%d is not at the limit=%d", peak.Load(), limit)
		}
	})
	t.Run("with Wait()", func(t *testing.T) {
		cm := New(limit)
		for range limit * 2 {
			if err := cm.Acquire(); err != nil {
				break
			}
			go func() {
				defer cm.Release()
				time.Sleep(100 * time.Millisecond)
			}()
		}
		cm.Wait()
		if cnt := cm.RunningCount(); cnt != 0 {
			t.Errorf("RunningCount is %d, should be 0", cnt)
		}

	})
	t.Run("ctx cancel", func(t *testing.T) {
		cm := New(limit)
		ctx, cancel := context.WithCancel(context.Background())

		var wg sync.WaitGroup
		wg.Go(func() {
			for {
				if err := cm.AcquireContext(ctx); err != nil {
					return
				}
				go func() {
					defer cm.Release()
					time.Sleep(time.Millisecond)
				}()
			}
		})

		time.Sleep(30 * time.Millisecond)
		cancel()
		wg.Wait()
		cm.Wait()

		if cnt := cm.RunningCount(); cnt != 0 {
			t.Errorf("RunningCount is %d, should be 0", cnt)
		}
	})
}
