package coma

import (
	"bytes"
	"context"
	"errors"
	"os"
	"os/exec"
	"strings"
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

		if cnt := g.Held(); cnt != 1 {
			t.Errorf("New(%d): Held=%d, want 1", max, cnt)
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
		if actual := g.Held(); actual != limit {
			t.Errorf("Held=%d should be equal to limit=%d", actual, limit)
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
		if actual := g.Held(); actual != limit {
			t.Errorf("after Release Held=%d should be equal to limit=%d", actual, limit)
		}
	})
	t.Run("after Drain called", func(t *testing.T) {
		var (
			g         = New(5)
			ctx       = context.Background()
			total     = 2000
			failed    = 0
			failedCtx = 0
		)
		g.Drain()
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
			g         = New(5)
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
	if actual := g.Held(); actual != limit {
		t.Errorf("Held=%d should be equal to limit=%d", actual, limit)
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
	if actual := g.Held(); actual != limit {
		t.Errorf("after Release Held=%d should be equal to limit=%d", actual, limit)
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
	if actual := g.Held(); actual != limit {
		t.Errorf("Held=%d after cancel should be equal to limit=%d", actual, limit)
	}
}

func TestAcquireContextTimeout(t *testing.T) {
	g := New(limit)
	for range limit {
		if err := g.Acquire(); err != nil {
			t.Fatalf("Acquire: %v", err)
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	start := time.Now()
	err := g.AcquireContext(ctx)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("AcquireContext returned %v instead of context.DeadlineExceeded", err)
	}
	if elapsed := time.Since(start); elapsed < 50*time.Millisecond {
		t.Errorf("AcquireContext returned after %v, want at least the 50ms deadline", elapsed)
	}
	if actual := g.Held(); actual != limit {
		t.Errorf("Held=%d should be equal to limit=%d", actual, limit)
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
			if got := g.Held(); got != want {
				t.Fatalf("step %d: Held=%d, want %d", i, got, want)
			}
		}
		if got := g.Held(); got != 0 {
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

func TestHeld(t *testing.T) {
	t.Run("init", func(t *testing.T) {
		g := New(limit)
		if cnt := g.Held(); cnt != 0 {
			t.Errorf("Held is %d, should be 0", cnt)
		}
	})
	t.Run("sequential", func(t *testing.T) {
		g := New(limit)

		for i := range limit {
			if cur := g.Held(); cur != i {
				t.Errorf("Acquire: Held returned %d, expected is `%d", cur, i)
			}
			if err := g.Acquire(); err != nil {
				t.Fatalf("Acquire: %v", err)
			}
		}

		for i := limit; i != 0; i-- {
			if cur := g.Held(); cur != i {
				t.Errorf("Release: Held returned %d, expected is %d", cur, i)
			}
			g.Release()
		}

		if cur := g.Held(); cur != 0 {
			t.Errorf("Held returned %d, expected is 0", cur)
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
				if cnt := g.Held(); cnt < 0 || cnt > limit {
					t.Errorf("Held=%d exceeded the limit=%d", cnt, limit)
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

		if cnt := g.Held(); cnt != 0 {
			t.Errorf("Held is %d, should be 0", cnt)
		}
		if peak.Load() != limit {
			t.Errorf("peak goroutines count=%d is not at the limit=%d", peak.Load(), limit)
		}
	})
	t.Run("with Drain()", func(t *testing.T) {
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
		g.Drain()
		if cnt := g.Held(); cnt != 0 {
			t.Errorf("Held is %d, should be 0", cnt)
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
		g.Drain()

		if cnt := g.Held(); cnt != 0 {
			t.Errorf("Held is %d, should be 0", cnt)
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
		g.Drain()
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
			g.Drain()
		}()
		<-g.closed
		g.Release()
		wg.Wait()
	})
	t.Run("closing", func(t *testing.T) {
		g := New(limit)
		g.Drain()
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
			t.Error("Acquire blocked after closing Drain()")
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
			t.Error("Acquire blocked after closing Drain()")
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
			g.Drain()
			done1 <- true
		}()
		wg.Add(1)
		go func() {
			defer wg.Done()
			g.Drain()
			done2 <- true
		}()
		wg.Add(1)
		go func() {
			defer wg.Done()
			g.Drain()
			done3 <- true
		}()

		select {
		case <-done1:
			t.Error("first Drain call did not block")
		case <-done2:
			t.Error("second Drain call did not block")
		case <-done3:
			t.Error("third Drain call did not block")
		case <-time.After(300 * time.Millisecond):
		}

		g.Release()
		wg.Wait()
	})
	t.Run("blocks", func(t *testing.T) {
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

		g.Drain()
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
				g.Drain()
				if !done.Load() {
					t.Error("goroutine is not done")
				}
			}()
		}
		close(start)
		wg.Wait()
	})
	t.Run("concurrent calls stress", func(t *testing.T) {
		for range 200 {
			g := New(limit)
			if err := g.Acquire(); err != nil {
				t.Fatalf("Acquire: %v", err)
			}
			var wg sync.WaitGroup
			start := make(chan struct{})
			for range 8 {
				wg.Add(1)
				go func() {
					defer wg.Done()
					<-start
					g.Drain()
				}()
			}
			close(start)
			go g.Release()
			wg.Wait()
		}
	})
}

func TestGo(t *testing.T) {
	t.Run("respects the limit", func(t *testing.T) {
		g := New(limit)
		var monitor sync.WaitGroup
		var done atomic.Bool
		var peak atomic.Int32

		monitor.Add(1)
		go func() {
			defer monitor.Done()
			for !done.Load() {
				if cnt := g.Held(); cnt > int(peak.Load()) {
					peak.Store(int32(cnt))
				}
				time.Sleep(5 * time.Millisecond)
			}
		}()

		for range limit * 3 {
			if err := g.Go(func() {
				time.Sleep(100 * time.Millisecond)
			}); err != nil {
				t.Fatalf("Go: %v", err)
			}
		}

		g.Drain()
		done.Store(true)
		monitor.Wait()

		if peak.Load() > limit {
			t.Errorf("peak concurrency=%d, want %d", peak.Load(), limit)
		}
	})
	t.Run("returns ErrClosed after Wait", func(t *testing.T) {
		g := New(limit)
		g.Drain()
		if err := g.Go(func() {
			t.Error("f should not run once the Gate is closed")
		}); !errors.Is(err, ErrClosed) {
			t.Errorf("Go returned %v instead of ErrClosed", err)
		}
	})
}

// TestGoPanic must run in a child process: an unrecovered panic in the goroutine Go spawns
// is fatal to the whole program, so the only way to observe it is to run it out-of-process
// and inspect the exit. mirrors sync.WaitGroup's own TestIssue76126.
func TestGoPanic(t *testing.T) {
	if os.Getenv("COMA_TEST_GO_PANIC_CHILD") == "1" {
		g := New(limit)
		if err := g.Go(func() {
			panic("boom")
		}); err != nil {
			t.Fatalf("Go: %v", err)
		}
		g.Drain()               // must never return: the panic should terminate the process first
		panic("Drain returned") // unreachable if Release was correctly skipped
	}

	cmd := exec.Command(os.Args[0], "-test.run=^TestGoPanic$")
	cmd.Env = append(os.Environ(), "COMA_TEST_GO_PANIC_CHILD=1")
	var stderr bytes.Buffer
	cmd.Stderr = &stderr

	if err := cmd.Run(); err == nil {
		t.Fatal("child process exited successfully, want a panic")
	}
	if !strings.Contains(stderr.String(), "panic: boom") {
		t.Errorf("missing panic: boom\n%s", stderr.String())
	}
	if strings.Contains(stderr.String(), "Drain returned") {
		t.Error("Drain returned before the panic terminated the process")
	}
}

func TestGoContext(t *testing.T) {
	t.Run("respects the limit", func(t *testing.T) {
		g := New(limit)
		var monitor sync.WaitGroup
		var done atomic.Bool
		var peak atomic.Int32

		monitor.Add(1)
		go func() {
			defer monitor.Done()
			for !done.Load() {
				if cnt := g.Held(); cnt > int(peak.Load()) {
					peak.Store(int32(cnt))
				}
				time.Sleep(5 * time.Millisecond)
			}
		}()

		ctx := context.Background()
		for range limit * 3 {
			if err := g.GoContext(ctx, func() {
				time.Sleep(100 * time.Millisecond)
			}); err != nil {
				t.Fatalf("GoContext: %v", err)
			}
		}

		g.Drain()
		done.Store(true)
		monitor.Wait()

		if peak.Load() != limit {
			t.Errorf("peak concurrency=%d, want %d", peak.Load(), limit)
		}
	})
	t.Run("returns ErrClosed after Wait", func(t *testing.T) {
		g := New(limit)
		g.Drain()
		if err := g.GoContext(context.Background(), func() {
			t.Error("f should not run once the Gate is closed")
		}); !errors.Is(err, ErrClosed) {
			t.Errorf("GoContext returned %v instead of ErrClosed", err)
		}
	})
	t.Run("already canceled context", func(t *testing.T) {
		g := New(limit)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		if err := g.GoContext(ctx, func() {
			t.Error("f should not run when ctx is already canceled")
		}); !errors.Is(err, context.Canceled) {
			t.Errorf("GoContext returned %v instead of context.Canceled", err)
		}
		if cnt := g.Held(); cnt != 0 {
			t.Errorf("Held=%d, want 0", cnt)
		}
	})
	t.Run("canceled while waiting for a slot", func(t *testing.T) {
		g := New(1)
		if err := g.Acquire(); err != nil { // fill the only slot
			t.Fatalf("Acquire: %v", err)
		}

		ctx, cancel := context.WithCancel(context.Background())
		errCh := make(chan error, 1)
		go func() {
			errCh <- g.GoContext(ctx, func() {
				t.Error("f should not run once ctx is canceled while waiting")
			})
		}()

		time.Sleep(30 * time.Millisecond)
		cancel()

		select {
		case err := <-errCh:
			if !errors.Is(err, context.Canceled) {
				t.Errorf("GoContext returned %v instead of context.Canceled", err)
			}
		case <-time.After(time.Second):
			t.Fatal("GoContext did not return after ctx was canceled")
		}

		g.Release()
	})
}

// TestGoContextPanic mirrors TestGoPanic for the AcquireContext-based path.
func TestGoContextPanic(t *testing.T) {
	if os.Getenv("COMA_TEST_GOCONTEXT_PANIC_CHILD") == "1" {
		g := New(limit)
		if err := g.GoContext(context.Background(), func() {
			panic("boom")
		}); err != nil {
			t.Fatalf("GoContext: %v", err)
		}
		g.Drain()               // must never return: the panic should terminate the process first
		panic("Drain returned") // unreachable if Release was correctly skipped
	}

	cmd := exec.Command(os.Args[0], "-test.run=^TestGoContextPanic$")
	cmd.Env = append(os.Environ(), "COMA_TEST_GOCONTEXT_PANIC_CHILD=1")
	var stderr bytes.Buffer
	cmd.Stderr = &stderr

	if err := cmd.Run(); err == nil {
		t.Fatal("child process exited successfully, want a panic")
	}
	if !strings.Contains(stderr.String(), "panic: boom") {
		t.Errorf("missing panic: boom\n%s", stderr.String())
	}
	if strings.Contains(stderr.String(), "Drain returned") {
		t.Error("Drain returned before the panic terminated the process")
	}
}

// every failed Acquire/AcquireContext must leave pending exactly as it found it.
// a leak is invisible until Drain deadlocks, so assert on pending directly.
func TestPendingNotLeakedOnFailure(t *testing.T) {
	t.Run("ctx canceled while blocked", func(t *testing.T) {
		g := New(1)
		if err := g.Acquire(); err != nil { // fill the only slot
			t.Fatalf("Acquire: %v", err)
		}
		for range 50 {
			ctx, cancel := context.WithCancel(context.Background())
			go func() {
				time.Sleep(time.Millisecond)
				cancel()
			}()
			if err := g.AcquireContext(ctx); err == nil {
				t.Fatal("AcquireContext should have failed")
			}
			cancel()
		}
		g.mu.Lock()
		pending := g.pending
		g.mu.Unlock()
		if pending != 1 { // only the one live holder
			t.Errorf("pending=%d after 50 failed AcquireContext, want 1", pending)
		}
		g.Release()
	})

	t.Run("closed while blocked", func(t *testing.T) {
		g := New(1)
		if err := g.Acquire(); err != nil {
			t.Fatalf("Acquire: %v", err)
		}
		errs := make(chan error, 20)
		for range 10 {
			go func() { errs <- g.Acquire() }()
			go func() { errs <- g.AcquireContext(context.Background()) }()
		}
		time.Sleep(50 * time.Millisecond)

		waited := make(chan struct{})
		go func() { g.Drain(); close(waited) }()

		for range 20 {
			if err := <-errs; err == nil {
				t.Error("blocked acquire should have failed once Drain was called")
			}
		}
		g.Release()

		// a leaked pending count makes this Drain hang forever
		select {
		case <-waited:
		case <-time.After(2 * time.Second):
			g.mu.Lock()
			p := g.pending
			g.mu.Unlock()
			t.Fatalf("Drain deadlocked: pending leaked, stuck at %d", p)
		}
	})
}

// TestNoGrantAfterWaitReturns asserts the real invariant behind Wait: it returns only after
// every Release has landed, and nothing is granted afterwards. measured on g.pending under
// g.mu rather than a counter the test keeps itself, since a shadow counter decremented after
// g.Release() returns lags the real release and fails for no reason.
func TestNoGrantAfterWaitReturns(t *testing.T) {
	for trial := range 300 {
		g := New(4)
		var waitDone, lateGrant atomic.Int64
		var wg sync.WaitGroup

		for i := range 12 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for {
					var err error
					if i%2 == 0 {
						err = g.Acquire()
					} else {
						err = g.AcquireContext(context.Background())
					}
					if err != nil {
						return
					}
					if waitDone.Load() == 1 {
						lateGrant.Add(1)
					}
					g.Release()
				}
			}()
		}
		g.Drain()
		waitDone.Store(1)

		g.mu.Lock()
		pending := g.pending
		g.mu.Unlock()
		if pending != 0 {
			t.Fatalf("trial %d: Drain returned with pending=%d", trial, pending)
		}
		if held := g.Held(); held != 0 {
			t.Fatalf("trial %d: Drain returned with %d slots still held", trial, held)
		}
		wg.Wait()
		if n := lateGrant.Load(); n != 0 {
			t.Fatalf("trial %d: %d slots granted after Drain returned", trial, n)
		}
	}
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
