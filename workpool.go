package workpool

import (
	"context"
	"errors"
	"runtime"
	"sync"
	"time"
)

type WorkFunc func(context.Context)

type entry struct {
	ctx    context.Context
	work   WorkFunc
	time   time.Time
	period time.Duration
	seq    int64
	idx    int
}

type queue []*entry

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

func (p *WorkPool) Submit(ctx context.Context, work WorkFunc) (err error) {
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
			w <- &entry{ctx: ctx, work: work}
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

func (q queue) Len() int {
	return len(q)
}

func (q queue) Less(i, j int) bool {
	switch {
	case q[i].time.Before(q[j].time):
		return true
	case q[i].time.After(q[j].time):
		return false
	case q[i].seq < q[j].seq:
		return true
	default:
		return false
	}
}

func (q queue) Swap(i, j int) {
	q[i], q[j] = q[j], q[i]
	q[i].idx = i
	q[j].idx = j
}

func (q *queue) Push(x interface{}) {
	n := len(*q)
	e := x.(*entry)
	e.idx = n
	*q = append(*q, e)
}

func (q *queue) Pop() interface{} {
	old := *q
	n := len(old)
	e := old[n-1]
	e.idx = -1
	*q = old[0 : n-1]
	return e
}

var defaultPool = NewPool(0)

func Submit(ctx context.Context, work WorkFunc) error {
	return defaultPool.Submit(ctx, work)
}
