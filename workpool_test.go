package workpool

import (
	"context"
	"runtime"
	"testing"
	"time"
)

func TestWorkPoolInitOnce(t *testing.T) {
	pool := NewPool(0)
	defer pool.Close()

	if x := cap(pool.ch); x != runtime.NumCPU() {
		t.Fatalf("expected num workers: %d, got %d", runtime.NumCPU(), x)
	}
	if x := len(pool.ch); x != 0 {
		t.Fatalf("expected num idle workers: 0, got %d", x)
	}

	err := pool.Post(context.TODO(), func(context.Context) {})
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(10 * time.Millisecond)
	if x := len(pool.ch); x != runtime.NumCPU() {
		t.Fatalf("expected num idle workers: %d, got %d", runtime.NumCPU(), x)
	}
}

func TestWorkPoolPost(t *testing.T) {
	pool := NewPool(1)
	done := make(chan struct{})
	pool.Post(pool.Context(), func(ctx context.Context) {
		<-ctx.Done()
		close(done)
	})
	pool.Close()
	<-done

	pool = NewPool(1)
	done = make(chan struct{})
	ctx, cancel := context.WithCancel(pool.Context())
	pool.Post(ctx, func(ctx context.Context) {
		<-ctx.Done()
		close(done)
	})
	cancel()
	<-ctx.Done()
	<-done
	if err := ctx.Err(); err != context.Canceled {
		t.Fatalf("expected error %v, got %v", context.Canceled, err)
	}
	if err := pool.Context().Err(); err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
	pool.Close()

	pool = NewPool(1)
	done = make(chan struct{})
	ctx, cancel = context.WithTimeout(pool.Context(), 1*time.Second)
	pool.Post(ctx, func(ctx context.Context) {
		<-ctx.Done()
		close(done)
	})
	<-ctx.Done()
	<-done
	cancel()
	if err := ctx.Err(); err != context.DeadlineExceeded {
		t.Fatalf("expected error %v, got %v", context.DeadlineExceeded, err)
	}
	if err := pool.Context().Err(); err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
	pool.Close()
}

func TestWorkPoolPostUnsafe(t *testing.T) {
	pool := NewPool(1)
	defer pool.Close()

	ctx0, cancel0 := context.WithCancel(context.TODO())
	ctx1, cancel1 := context.WithTimeout(context.TODO(), 500*time.Millisecond)

	err := pool.Post(ctx0, func(ctx context.Context) { <-ctx.Done() })
	if err != nil {
		t.Fatal(err)
	}
	if x := len(pool.ch); x != 0 {
		t.Fatalf("expected num idle workers: 0, got %d", x)
	}

	err = pool.Post(ctx1, func(context.Context) {})
	if err != context.DeadlineExceeded && ctx1.Err() != context.DeadlineExceeded {
		t.Fatalf("expected error %v, got %v", context.DeadlineExceeded, err)
	}
	cancel1()

	if x := len(pool.ch); x != 0 {
		t.Fatalf("expected num idle workers: 0, got %d", x)
	}
	cancel0()
	<-ctx0.Done()
	if err = ctx0.Err(); err != context.Canceled {
		t.Fatalf("expected error %v, got %v", context.Canceled, err)
	}
	time.Sleep(10 * time.Millisecond)
	if x := len(pool.ch); x != 1 {
		t.Fatalf("expected num idle workers: 1, got %d", x)
	}
}

func TestWorkPoolClose(t *testing.T) {
	pool := NewPool(1)
	if err := pool.Close(); err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
	if err := pool.Post(context.TODO(), func(context.Context) {}); err != ErrClosed {
		t.Fatalf("expected error %v, got %v", ErrClosed, err)
	}
	if err := pool.Close(); err != ErrClosed {
		t.Fatalf("expected error %v, got %v", ErrClosed, err)
	}

	pool = NewPool(1)

	pool.Post(pool.Context(), func(ctx context.Context) { <-ctx.Done() })

	go func() {
		ctx, cancel := context.WithTimeout(context.TODO(), 5*time.Second)
		if err := pool.Post(ctx, func(context.Context) {}); err != ErrClosed {
			t.Fatalf("expected error %v, got %v", ErrClosed, err)
		}
		cancel()
		<-ctx.Done()
		if err := ctx.Err(); err != context.Canceled {
			t.Fatalf("expected error %v, got %v", context.Canceled, err)
		}
	}()

	go func() {
		ctx, cancel := context.WithTimeout(context.TODO(), 2*time.Second)
		if err := pool.Post(ctx, func(context.Context) {}); err != ErrClosed {
			t.Fatalf("expected error %v, got %v", ErrClosed, err)
		}
		<-ctx.Done()
		if err := ctx.Err(); err != context.DeadlineExceeded {
			t.Fatalf("expected error %v, got %v", context.DeadlineExceeded, err)
		}
		cancel()
	}()

	pool.Close()

	ctx := pool.Context()
	<-ctx.Done()
	if err := ctx.Err(); err != context.Canceled {
		t.Fatalf("expected error %v, got %v", context.Canceled, err)
	}
}
