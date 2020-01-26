package batch

import (
	"sync"
)

type Pool struct {
	p *sync.Pool
}

func NewPool() *Pool {
	return &Pool{
		p: &sync.Pool{
			New: func() interface{} {
				return NewBatch()
			},
		},
	}
}

func (p *Pool) Get() *Batch {
	return p.p.Get().(*Batch)
}

func (p *Pool) Put(b *Batch) {
	b.Reset()
	p.p.Put(b)
}
