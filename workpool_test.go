package workpool

import (
	"context"
	"runtime"
	"sync"
	"testing"
	"time"
)

func TestWorkPoolSize(t *testing.T) {
	pool := NewPool()
	defer pool.Close()

	if pool.n != runtime.NumCPU() {
		t.Fatalf("expected default num workers %d, got %d", runtime.NumCPU(), pool.n)
	}
	var wg sync.WaitGroup
	for i := 0; i < runtime.NumCPU()*10; i++ {
		wg.Add(1)
		go func() {
			pool.Submit(context.TODO(), func(context.Context) { wg.Done() })
		}()
	}
	wg.Wait()
	if pool.n != 0 {
		t.Fatalf("expected remaining worker count 0, got %d", pool.n)
	}
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			pool.Submit(pool.Context(), func(ctx context.Context) { <-ctx.Done() })
			wg.Done()
		}()
	}
	wg.Wait()
	if x := 100 - pool.q.Len(); x != runtime.NumCPU() {
		t.Fatalf("expected running workers %d, got %d", runtime.NumCPU(), x)
	}
}

func TestWorkPoolSubmit(t *testing.T) {
	pool := NewPool()
	defer pool.Close()

	done := make(chan struct{})
	ctx, cancel := context.WithCancel(pool.Context())

	pool.Submit(ctx, func(ctx context.Context) {
		<-ctx.Done()
		close(done)
	})
	cancel()
	<-done
	if err := ctx.Err(); err != context.Canceled {
		t.Fatalf("expected error %v, got %v", context.Canceled, err)
	}
	if err := pool.Context().Err(); err != nil {
		t.Fatalf("expected nil, got %v", err)
	}

	done = make(chan struct{})
	ctx, cancel = context.WithTimeout(pool.Context(), 500*time.Millisecond)
	defer cancel()

	pool.Submit(ctx, func(ctx context.Context) {
		<-ctx.Done()
		close(done)
	})
	<-done
	if err := ctx.Err(); err != context.DeadlineExceeded {
		t.Fatalf("expected error %v, got %v", context.DeadlineExceeded, err)
	}
	if err := pool.Context().Err(); err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
}

func TestWorkPoolSchedule(t *testing.T) {
	pool := NewPool()
	defer pool.Close()

	var wg sync.WaitGroup
	wg.Add(10)
	when := time.Now()
	task := &task{ctx: pool.Context(), when: when, period: 100 * time.Millisecond, seq: 1}
	task.f = func(context.Context) {
		defer wg.Done()
		if !task.when.Equal(when) {
			t.Fatalf("expected %s, got %s", task.when, when)
		}
		when = when.Add(task.period)
	}
	pool.do(task)
	wg.Wait()
}

func TestWorkPoolClose(t *testing.T) {
	pool := NewPool()
	if err := pool.Close(); err != nil {
		t.Fatalf("expected nil, got %v", err)
	}
	if err := pool.Submit(context.TODO(), func(context.Context) {}); err != ErrClosed {
		t.Fatalf("expected error %v, got %v", ErrClosed, err)
	}
	if err := pool.Close(); err != ErrClosed {
		t.Fatalf("expected error %v, got %v", ErrClosed, err)
	}

	pool = NewPool()
	done := make(chan struct{})
	ctx, cancel := context.WithCancel(pool.Context())
	defer cancel()

	var wg sync.WaitGroup
	wg.Add(1)
	pool.Submit(ctx, func(ctx context.Context) {
		wg.Done()
		<-ctx.Done()
		close(done)
	})
	wg.Wait()
	pool.Close()
	<-done
}
