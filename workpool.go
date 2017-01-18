package workpool

import (
	"container/heap"
	"context"
	"errors"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

type WorkFunc func(context.Context)

type task struct {
	ctx    context.Context
	f      WorkFunc
	when   time.Time
	period time.Duration
	seq    uint64
	idx    int
}

type queue []*task

type WorkPool struct {
	l      sync.Mutex
	n      int
	r      int64
	t      *time.Timer
	q      queue
	qwait  sync.Cond
	ctx    context.Context
	cancel context.CancelFunc
}

func NewPool() *WorkPool {
	return NewPoolSize(0)
}

func NewPoolSize(size int) *WorkPool {
	if size <= 0 {
		size = runtime.NumCPU()
	}
	p := new(WorkPool)
	p.n = size
	p.t = time.NewTimer(0)
	p.qwait.L = &p.l
	p.ctx, p.cancel = context.WithCancel(context.Background())

	<-p.t.C

	go func() {
		for {
			select {
			case <-p.ctx.Done():
				return
			case <-p.t.C:
				p.l.Lock()
				p.qwait.Signal()
				p.l.Unlock()
			}
		}
	}()

	return p
}

var seq uint64

func (p *WorkPool) Submit(ctx context.Context, work WorkFunc) error {
	return p.do(&task{ctx: ctx, f: work, when: time.Now(), seq: atomic.AddUint64(&seq, 1)})
}

func (p *WorkPool) Schedule(ctx context.Context, work WorkFunc, delay, period time.Duration) error {
	return p.do(&task{ctx: ctx, f: work, when: time.Now().Add(delay), period: period, seq: atomic.AddUint64(&seq, 1)})
}

var ErrClosed = errors.New("workpool has been closed")

func (p *WorkPool) do(t *task) error {
	select {
	case <-t.ctx.Done():
		return t.ctx.Err()
	case <-p.ctx.Done():
		return ErrClosed
	default:
	}

	p.l.Lock()
	heap.Push(&p.q, t)
	p.qwait.Signal()
	if p.n == 0 {
		p.l.Unlock()
		return nil
	}
	p.n--
	p.l.Unlock()

	go func() {
		var t *task
		for {
			p.l.Lock()
		wait:
			for {
				select {
				case <-p.ctx.Done():
					p.l.Unlock()
					return
				default:
				}
				if p.q.Len() > 0 {
					t = p.q[0]
					d := t.when.Sub(time.Now())
					if d <= 0 {
						heap.Remove(&p.q, 0)
						p.r = 0
						break wait
					}
					r := t.when.UnixNano()
					if p.r == 0 || p.r > r {
						p.t.Reset(d)
						p.r = r
					}
				}
				p.qwait.Wait()
			}
			if p.q.Len() > 0 {
				p.qwait.Signal()
			}
			p.l.Unlock()
			if t != nil {
				t.f(t.ctx)
				if t.period > 0 {
					t.when = t.when.Add(t.period)
					p.do(t)
				}
			}
		}
	}()

	return nil
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
		p.l.Lock()
		p.t.Stop()
		p.qwait.Broadcast()
		p.l.Unlock()
	}
	return
}

func (q queue) Len() int {
	return len(q)
}

func (q queue) Less(i, j int) bool {
	switch {
	case q[i].when.Before(q[j].when):
		return true
	case q[i].when.After(q[j].when):
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
	t := x.(*task)
	t.idx = n
	*q = append(*q, t)
}

func (q *queue) Pop() interface{} {
	old := *q
	n := len(old)
	t := old[n-1]
	t.idx = -1
	*q = old[0 : n-1]
	return t
}
