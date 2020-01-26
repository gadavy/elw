package batch

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPool(t *testing.T) {
	const data = "data"
	p := NewPool()

	var wg sync.WaitGroup

	for g := 0; g < 10; g++ {
		wg.Add(1)

		go func() {
			for i := 0; i < 100; i++ {
				batch := p.Get()

				assert.Zero(t, batch.Len(), "expected truncated buffer")

				batch.AppendBytes([]byte(data))

				assert.Equal(t, batch.Len(), len(data), "expected buffer to contain data")

				p.Put(batch)
			}

			wg.Done()
		}()
	}

	wg.Wait()
}
