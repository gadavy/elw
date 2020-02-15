package internal

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestOnce_Do(t *testing.T) {
	once := Once{}

	var count int

	for i := 0; i < 5; i++ {
		go once.Do(func() {
			count++
			time.Sleep(time.Second)
		})
	}

	time.Sleep(time.Second)

	assert.Equal(t, 1, count, "expected 1")
}
