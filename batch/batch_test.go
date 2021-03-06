package batch

import (
	"bytes"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestBatch(t *testing.T) {
	batch := NewBatch(0)

	t.Run("AppendBytes", func(t *testing.T) {
		msg := []byte("message")
		expected := []byte("message\n")

		batch.Reset()
		batch.AppendBytes(msg)

		assert.Equal(t, expected, batch.Bytes())
		assert.Equal(t, len(expected), batch.Len())
		assert.Equal(t, string(expected), batch.String())
	})

	t.Run("AppendMeta", func(t *testing.T) {
		indexName := "test-index"
		timeFormat := "2006.01.02"

		expected := []byte(fmt.Sprintf("{\"index\":{\"_type\":\"doc\",\"_index\":\"%s-%s\"}}\n",
			indexName, time.Now().Format(timeFormat)))

		batch.Reset()
		batch.AppendMeta(indexName, timeFormat)

		assert.Equal(t, expected, batch.Bytes())
		assert.Equal(t, len(expected), batch.Len())
		assert.Equal(t, string(expected), batch.String())
	})
}

func BenchmarkBatch_AppendBytes(b *testing.B) {
	str := bytes.Repeat([]byte("a"), 1024)

	slice := make([]byte, 1024)
	buf := bytes.NewBuffer(slice)
	batch := NewBatch(1024)

	b.Run("BytesBuffer", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			buf.Write(str)
			buf.Reset()

			if !bytes.HasSuffix(str, []byte(newline)) {
				buf.WriteString(newline)
			}
		}
	})

	b.Run("CustomBatch", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			batch.AppendBytes(str)
			batch.Reset()
		}
	})
}
