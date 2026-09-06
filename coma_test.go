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

func TestNew(t *testing.T) {
	for _, max := range []int{1, 0, -1} {
		g := New(max)

		firstErr := make(chan error, 1)
		go func() {
			firstErr <- g.Acquire()
		}()

		select {
		case err := <-firstErr:
			if err != nil {
				t.Fatalf("New(%d): Acquire: %v", max, err)
			}
		case <-time.After(time.Second):
			t.Fatalf("New(%d): Acquire blocked, max was not round up to 1", max)
		}

		if cnt := g.RunningCount(); cnt != 1 {
			t.Errorf("New(%d): RunningCount=%d, want 1", max, cnt)
		}

		secondErr := make(chan error, 1)
		go func() {
			secondErr <- g.Acquire()
		}()

		select {
		case err := <-secondErr:
			t.Errorf("New(%d): second Acquire returned err=%v, should block at limit 1", max, err)
		case <-time.After(100 * time.Millisecond):
			// expected
		}

		g.Release()
	}
}

func TestAcquire(t *testing.T) {
	t.Run("basic functionality", func(t *testing.T) {
		g := New(limit)
		for range limit {
			if err := g.Acquire(); err != nil {
				t.Errorf("Acquire: %v", err)
			}
		}

		acqErr := make(chan error)
		go func() {
			acqErr <- g.Acquire()
		}()

		// blocked
		select {
		case err := <-acqErr:
			t.Errorf("Acquire returned instead of blocking, err=%v", err)
		case <-time.After(100 * time.Millisecond):
			// expected
		}
		if actual := g.RunningCount(); actual != limit {
			t.Errorf("RunningCount=%d should be equal to limit=%d", actual, limit)
		}

		g.Release()

		// acquired
		select {
		case err := <-acqErr:
			if err != nil {
				t.Error("after Release Acquire returned error, should return nil")
			}
		case <-time.After(time.Second):
			t.Errorf("Acquire seems to be blocked even after a slot was released")
		}
		if actual := g.RunningCount(); actual != limit {
			t.Errorf("after Release RunningCount=%d should be equal to limit=%d", actual, limit)
		}
	})
	t.Run("after Wait called", func(t *testing.T) {
		var (
			g        = New(5)
			ctx       = context.Background()
			total     = 2000
			failed    = 0
			failedCtx = 0
		)
		g.Wait()
		for range total {
			if err := g.Acquire(); err == nil {
				failed++
				g.Release()
			}
			if errCtx := g.AcquireContext(ctx); errCtx == nil {
				failedCtx++
				g.Release()
			}
		}
		if failed != 0 || failedCtx != 0 {
			t.Errorf("acquired a slot after wait, total calls: %d, Acquire: %d, AcquireContext: %d",
				total, failed, failedCtx)
		}
	})

	t.Run("after Context canceled", func(t *testing.T) {
		var (
			g        = New(5)
			total     = 2000
			failedCtx = 0
		)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		for range total {
			if errCtx := g.AcquireContext(ctx); errCtx == nil {
				failedCtx++
				g.Release()
			}
		}
		if failedCtx != 0 {
			t.Errorf("acquired a slot after ctx canceled, total calls: %d, AcquireContext: %d", total, failedCtx)
		}
	})
}

func TestAcquireContext(t *testing.T) {
	g := New(limit)
	ctx, cancel := context.WithCancel(context.Background())
	for range limit {
		if err := g.Acquire(); err != nil {
			t.Errorf("Acquire: %v", err)
		}
	}

	acqErr := make(chan error)
	go func() {
		acqErr <- g.AcquireContext(ctx)
	}()

	// blocked
	select {
	case err := <-acqErr:
		t.Errorf("AcquireContext returned instead of blocking, err=%v", err)
	case <-time.After(100 * time.Millisecond):
		// expected
	}
	if actual := g.RunningCount(); actual != limit {
		t.Errorf("RunningCount=%d should be equal to limit=%d", actual, limit)
	}

	g.Release()

	// acquired
	select {
	case err := <-acqErr:
		if err != nil {
			t.Error("after Release AcquireContext returned error, should return nil")
		}
	case <-time.After(time.Second):
		t.Errorf("AcquireContext seems to be blocked even after a slot was released")
	}
	if actual := g.RunningCount(); actual != limit {
		t.Errorf("after Release RunningCount=%d should be equal to limit=%d", actual, limit)
	}

	go func() {
		acqErr <- g.AcquireContext(ctx)
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
	if actual := g.RunningCount(); actual != limit {
		t.Errorf("RunningCount=%d after cancel should be equal to limit=%d", actual, limit)
	}
}

func TestRelease(t *testing.T) {
	t.Run("base usage", func(t *testing.T) {
		g := New(limit)
		want := 0
		for i, step := range []int{+1, -1, +1, -1, +1, +1, -1, +1, +1, -1, -1, -1} {
			if step > 0 {
				if err := g.Acquire(); err != nil {
					t.Fatalf("step %d: Acquire: %v", i, err)
				}
			} else {
				g.Release()
			}
			want += step
			if got := g.RunningCount(); got != want {
				t.Fatalf("step %d: RunningCount=%d, want %d", i, got, want)
			}
		}
		if got := g.RunningCount(); got != 0 {
			t.Errorf("all releases were called, but %d is still held", got)
		}
	})
	t.Run("nothing to release", func(t *testing.T) {
		g := New(limit)
		mustPanic(t, g.Release, "Release")
	})
	t.Run("double release", func(t *testing.T) {
		g := New(limit)
		if err := g.Acquire(); err != nil {
			t.Fatalf("Acquire: %v", err)
		}
		g.Release()
		mustPanic(t, g.Release, "second Release")
	})
}

func TestRunningCount(t *testing.T) {
	t.Run("init", func(t *testing.T) {
		g := New(limit)
		if cnt := g.RunningCount(); cnt != 0 {
			t.Errorf("RunningCount is %d, should be 0", cnt)
		}
	})
	t.Run("sequential", func(t *testing.T) {
		g := New(limit)

		for i := range limit {
			if cur := g.RunningCount(); cur != i {
				t.Errorf("Acquire: RunningCount returned %d, expected is `%d", cur, i)
			}
			if err := g.Acquire(); err != nil {
				t.Fatalf("Acquire: %v", err)
			}
		}

		for i := limit; i != 0; i-- {
			if cur := g.RunningCount(); cur != i {
				t.Errorf("Release: RunningCount returned %d, expected is %d", cur, i)
			}
			g.Release()
		}

		if cur := g.RunningCount(); cur != 0 {
			t.Errorf("RunningCount returned %d, expected is 0", cur)
		}
	})
	t.Run("parallel", func(t *testing.T) {
		g := New(limit)
		var workers, monitor sync.WaitGroup
		var done atomic.Bool
		var peak atomic.Int32

		monitor.Add(1)
		go func() {
			defer monitor.Done()
			for !done.Load() {
				if cnt := g.RunningCount(); cnt < 0 || cnt > limit {
					t.Errorf("RunningCount=%d exceeded the limit=%d", cnt, limit)
					return
				} else if cnt > int(peak.Load()) {
					peak.Store(int32(cnt))
				}
				time.Sleep(5 * time.Millisecond)
			}
		}()

		for range limit * 3 {
			workers.Add(1)
			go func() {
				defer workers.Done()
				if err := g.Acquire(); err != nil {
					t.Errorf("Acquire: %v", err)
				}
				time.Sleep(100 * time.Millisecond)
				g.Release()
			}()
		}

		workers.Wait()
		done.Store(true)
		monitor.Wait()

		if cnt := g.RunningCount(); cnt != 0 {
			t.Errorf("RunningCount is %d, should be 0", cnt)
		}
		if peak.Load() != limit {
			t.Errorf("peak goroutines count=%d is not at the limit=%d", peak.Load(), limit)
		}
	})
	t.Run("with Wait()", func(t *testing.T) {
		g := New(limit)
		for range limit * 2 {
			if err := g.Acquire(); err != nil {
				break
			}
			go func() {
				defer g.Release()
				time.Sleep(100 * time.Millisecond)
			}()
		}
		g.Wait()
		if cnt := g.RunningCount(); cnt != 0 {
			t.Errorf("RunningCount is %d, should be 0", cnt)
		}
	})
	t.Run("ctx cancel", func(t *testing.T) {
		g := New(limit)
		ctx, cancel := context.WithCancel(context.Background())

		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				if err := g.AcquireContext(ctx); err != nil {
					return
				}
				go func() {
					defer g.Release()
					time.Sleep(time.Millisecond)
				}()
			}
		}()

		time.Sleep(30 * time.Millisecond)
		cancel()
		wg.Wait()
		g.Wait()

		if cnt := g.RunningCount(); cnt != 0 {
			t.Errorf("RunningCount is %d, should be 0", cnt)
		}
	})
}

func TestClose(t *testing.T) {
	t.Run("closed while acquiring", func(t *testing.T) {
		g := New(1)
		var wg sync.WaitGroup
		if err := g.Acquire(); err != nil {
			t.Errorf("Acquire: %v", err)
		}

		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := g.Acquire(); !errors.Is(err, ErrClosed) {
				t.Error("Acquire did not return ErrClosed")
			}
		}()

		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := g.AcquireContext(context.Background()); !errors.Is(err, ErrClosed) {
				t.Error("AcquireContext did not return ErrClosed")
			}
		}()

		time.Sleep(50 * time.Millisecond)
		g.Close()
		<-g.closed
		g.Release()
		wg.Wait()
	})
	t.Run("already closed", func(t *testing.T) {
		g := New(limit)
		g.Close()
		acqErr := make(chan error)
		<-g.closed

		go func() {
			acqErr <- g.Acquire()
		}()
		select {
		case err := <-acqErr:
			if !errors.Is(err, ErrClosed) {
				t.Errorf("Acquire returned %v instead of ErrClosed", err)
			}
		case <-time.After(100 * time.Millisecond):
			t.Error("Acquire blocked after closing")
		}

		go func() {
			acqErr <- g.AcquireContext(context.Background())
		}()
		select {
		case err := <-acqErr:
			if !errors.Is(err, ErrClosed) {
				t.Errorf("AcquireContext returned %v instead of ErrClosed", err)
			}
		case <-time.After(100 * time.Millisecond):
			t.Error("Acquire blocked after closing")
		}
	})
	t.Run("called multiple times", func(t *testing.T) {
		g := New(limit)
		if err := g.Acquire(); err != nil {
			t.Errorf("Acquire: %v", err)
		}

		g.Close()
		g.Close()
		g.Close()

		g.Release()
		g.Wait()
	})
	t.Run("concurrent calls", func(t *testing.T) {
		for range 200 {
			g := New(limit)
			var wg sync.WaitGroup
			start := make(chan struct{})
			for range 8 {
				wg.Add(1)
				go func() {
					defer wg.Done()
					<-start
					g.Close()
				}()
			}
			close(start)
			wg.Wait()
		}
	})
}

func TestWait(t *testing.T) {
	t.Run("close blocked Acquires", func(t *testing.T) {
		g := New(1)
		var wg sync.WaitGroup
		if err := g.Acquire(); err != nil {
			t.Errorf("Acquire: %v", err)
		}

		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := g.Acquire(); !errors.Is(err, ErrClosed) {
				t.Error("Acquire did not return ErrClosed")
			}
		}()

		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := g.AcquireContext(context.Background()); !errors.Is(err, ErrClosed) {
				t.Error("AcquireContext did not return ErrClosed")
			}
		}()

		time.Sleep(50 * time.Millisecond)
		go func() {
			g.Wait()
		}()
		<-g.closed
		g.Release()
		wg.Wait()
	})
	t.Run("closing", func(t *testing.T) {
		g := New(limit)
		g.Wait()
		acqErr := make(chan error)

		go func() {
			acqErr <- g.Acquire()
		}()
		select {
		case err := <-acqErr:
			if !errors.Is(err, ErrClosed) {
				t.Errorf("Acquire returned %v instead of ErrClosed", err)
			}
		case <-time.After(100 * time.Millisecond):
			t.Error("Acquire blocked after closing Wait()")
		}

		go func() {
			acqErr <- g.AcquireContext(context.Background())
		}()
		select {
		case err := <-acqErr:
			if !errors.Is(err, ErrClosed) {
				t.Errorf("AcquireContext returned %v instead of ErrClosed", err)
			}
		case <-time.After(100 * time.Millisecond):
			t.Error("Acquire blocked after closing Wait()")
		}
	})
	t.Run("called multiple times", func(t *testing.T) {
		g := New(limit)
		if err := g.Acquire(); err != nil {
			t.Errorf("Acquire: %v", err)
		}
		var wg sync.WaitGroup

		done1 := make(chan bool, 1)
		done2 := make(chan bool, 1)
		done3 := make(chan bool, 1)

		wg.Add(1)
		go func() {
			defer wg.Done()
			g.Wait()
			done1 <- true
		}()
		wg.Add(1)
		go func() {
			defer wg.Done()
			g.Wait()
			done2 <- true
		}()
		wg.Add(1)
		go func() {
			defer wg.Done()
			g.Wait()
			done3 <- true
		}()

		select {
		case <-done1:
			t.Error("first Wait call did not block")
		case <-done2:
			t.Error("second Wait call did not block")
		case <-done3:
			t.Error("third Wait call did not block")
		case <-time.After(300 * time.Millisecond):
		}

		g.Release()
		wg.Wait()
	})
	t.Run("waits", func(t *testing.T) {
		g := New(limit)
		var done atomic.Bool

		if err := g.Acquire(); err != nil {
			t.Errorf("Acquire: %v", err)
		}
		go func() {
			defer g.Release()
			time.Sleep(100 * time.Millisecond)
			done.Store(true)
		}()

		g.Wait()
		if !done.Load() {
			t.Error("goroutine is not done")
		}
	})
	t.Run("concurrent calls", func(t *testing.T) {
		g := New(limit)
		var done atomic.Bool
		var wg sync.WaitGroup
		start := make(chan struct{})

		if err := g.Acquire(); err != nil {
			t.Errorf("Acquire: %v", err)
		}
		go func() {
			defer g.Release()
			time.Sleep(100 * time.Millisecond)
			done.Store(true)
		}()

		for range 20 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				g.Wait()
				if !done.Load() {
					t.Error("goroutine is not done")
				}
			}()
		}
		close(start)
		wg.Wait()
	})
}

func mustPanic(t *testing.T, f func(), what string) (p any) {
	t.Helper()
	defer func() {
		if p = recover(); p == nil {
			t.Errorf("%s: expected a panic, got none", what)
		}
	}()
	f()
	return p
}
