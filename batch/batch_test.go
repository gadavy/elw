package batch

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBatch_Writes(t *testing.T) {
	batch := NewBatch(0)

	tests := []struct {
		name string
		data []byte
		want []byte
	}{
		{
			name: "message 1",
			data: []byte("message 1"),
			want: []byte(defaultMetaData + `message 1`),
		},
		{
			name: "message 2",
			data: []byte("message 2"),
			want: []byte(defaultMetaData + `message 2`),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			batch.Reset()
			batch.AppendBytes(tt.data)

			assert.Equal(t, batch.Bytes(), tt.want)
			assert.Equal(t, batch.String(), string(tt.want))
			assert.Equal(t, batch.Len(), len(tt.want))
			assert.Equal(t, batch.String(), string(tt.want))
		})
	}
}

func BenchmarkBatch_AppendBytes(b *testing.B) {
	str := bytes.Repeat([]byte("a"), 1024)

	slice := make([]byte, 1024)
	buf := bytes.NewBuffer(slice)
	batch := NewBatch(1024)

	b.Run("BytesBuffer", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			buf.Write([]byte(defaultMetaData))
			buf.Write(str)
			buf.Reset()
		}
	})

	b.Run("CustomBatch", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			batch.AppendBytes(str)
			batch.Reset()
		}
	})
}
