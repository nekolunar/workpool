package workpool

import (
	"context"
	"errors"
	"runtime"
	"sync"
)

type WorkFunc func(ctx context.Context)

type entry struct {
	work WorkFunc
	ctx  context.Context
}

type WorkPool struct {
	ch     chan *entry
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
		ch:     make(chan *entry, size),
		ctx:    ctx,
		cancel: cancel,
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
			go func() {
				defer p.wg.Done()
				for e := range p.ch {
					select {
					case <-p.ctx.Done():
						return
					default:
						e.work(e.ctx)
					}
				}
			}()
		}
	})

	select {
	case p.ch <- &entry{work, ctx}:
	case <-p.ctx.Done():
		err = ErrClosed
	case <-ctx.Done():
		err = ctx.Err()
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
		close(p.ch)
		p.wg.Wait()
	}
	return
}

var defaultPool = NewPool(0)

func Post(ctx context.Context, work WorkFunc) error {
	return defaultPool.Post(ctx, work)
}
