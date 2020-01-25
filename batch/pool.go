package batch

import (
	"sync"
)

type Pool struct {
	p *sync.Pool
}

func NewPool(batchSize int) *Pool {
	return &Pool{
		p: &sync.Pool{
			New: func() interface{} {
				return NewBatch(batchSize)
			},
		},
	}
}

func (p *Pool) Get() **Batch {
	b := p.p.Get().(*Batch)
	return &b
}

func (p *Pool) Put(b *Batch) {
	b.Reset()
	p.p.Put(b)
}
