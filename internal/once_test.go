package internal

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestOnce_Do(t *testing.T) {
	var (
		counter int
		once    = Once{}
		wg      = sync.WaitGroup{}
	)

	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			for j := 0; j < 5; j++ {
				wg.Add(1)

				go once.Do(&wg, func() {
					counter++
					time.Sleep(time.Millisecond * 100)
				})

			}
			wg.Done()
		}()
	}

	wg.Wait()

	assert.Equal(t, 1, counter)
}
