package workpool

import (
	"context"
	"errors"
	"runtime"
	"sync"
)

type WorkFunc func(context.Context)

type entry struct {
	work WorkFunc
	ctx  context.Context
}

type worker chan *entry

type WorkPool struct {
	ch     chan worker
	wg     sync.WaitGroup
	once   sync.Once
	ctx    context.Context
	cancel context.CancelFunc
}

func NewPool(size int) *WorkPool {
	if size <= 0 {
		size = runtime.NumCPU()
	}
	ctx, cancel := context.WithCancel(context.TODO())
	return &WorkPool{
		ch:     make(chan worker, size),
		ctx:    ctx,
		cancel: cancel,
	}
}

func (w worker) start(pool *WorkPool) {
	defer func() {
		pool.wg.Done()
		close(w)
	}()

	for {
		select {
		case <-pool.ctx.Done():
			return
		default:
			pool.ch <- w
		}

		select {
		case e := <-w:
			e.work(e.ctx)
		case <-pool.ctx.Done():
			return
		}
	}
}

var ErrClosed = errors.New("workpool has been closed")

func (p *WorkPool) Post(ctx context.Context, work WorkFunc) (err error) {
	if ctx == nil {
		panic("nil context")
	}

	p.once.Do(func() {
		p.wg.Add(cap(p.ch))
		for i := 0; i < cap(p.ch); i++ {
			w := make(worker)
			go w.start(p)
		}
	})

	select {
	case w := <-p.ch:
		select {
		case <-p.ctx.Done():
			err = ErrClosed
		default:
			w <- &entry{work, ctx}
		}
	case <-ctx.Done():
		err = ctx.Err()
	case <-p.ctx.Done():
		err = ErrClosed
	}
	return
}

func (p *WorkPool) Context() context.Context {
	return p.ctx
}

func (p *WorkPool) Close() (err error) {
	select {
	case <-p.ctx.Done():
		err = ErrClosed
	default:
		p.cancel()
		p.wg.Wait()
		close(p.ch)
	}
	return
}

var defaultPool = NewPool(0)

func Post(ctx context.Context, work WorkFunc) error {
	return defaultPool.Post(ctx, work)
}
