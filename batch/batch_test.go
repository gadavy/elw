package batch

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBatch(t *testing.T) {
	message := []byte("message\n")
	batch := NewBatch()

	t.Run("AppendBytes", func(t *testing.T) {
		batch.AppendBytes(message)
		assert.Equal(t, [][]byte{message}, batch.Buffer())
		assert.Equal(t, len(message), batch.Len())
	})

	batch.Reset()

	t.Run("Len", func(t *testing.T) {
		batch.AppendBytes(message)
		assert.Equal(t, len(message), batch.Len())
	})

	batch.Reset()

	t.Run("String", func(t *testing.T) {
		msg1 := "msg 1\n"
		msg2 := "msg 2\n"
		msg3 := "msg 3\n"

		assert.Equal(t, "", batch.String())

		batch.AppendBytes([]byte(msg1))
		assert.Equal(t, msg1, batch.String())

		batch.AppendBytes([]byte(msg2))
		assert.Equal(t, msg1+msg2, batch.String())

		batch.AppendBytes([]byte(msg3))
		assert.Equal(t, msg1+msg2+msg3, batch.String())
	})

	batch.Reset()

	t.Run("Buffer", func(t *testing.T) {
		msg1 := []byte("msg 1\n")
		msg2 := []byte("msg 2\n")
		msg3 := []byte("msg 3\n")

		assert.Equal(t, [][]byte{}, batch.Buffer())

		batch.AppendBytes(msg1)
		assert.Equal(t, [][]byte{msg1}, batch.Buffer())

		batch.AppendBytes(msg2)
		assert.Equal(t, [][]byte{msg1, msg2}, batch.Buffer())

		batch.AppendBytes(msg3)
		assert.Equal(t, [][]byte{msg1, msg2, msg3}, batch.Buffer())
	})

	batch.Reset()
}

func BenchmarkBatch_AppendBytes(b *testing.B) {
	str := bytes.Repeat([]byte("a"), 1024)

	slice := make([]byte, 1024)
	buf := bytes.NewBuffer(slice)
	batch := NewBatch()

	b.Run("BytesBuffer", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
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

func BenchmarkBatch_String(b *testing.B) {
	message := []byte("message\n")
	batch := NewBatch()

	b.Run("1 Message (len = 8)", func(b *testing.B) {
		b.StopTimer()
		batch.AppendBytes(message)
		b.StartTimer()
		for i := 0; i < b.N; i++ {
			_ = batch.String()
		}
	})

	batch.Reset()

	b.Run("10 Messages (len = 8)", func(b *testing.B) {
		b.StopTimer()

		for i := 0; i < 10; i++ {
			batch.AppendBytes(message)
		}

		b.StartTimer()

		for i := 0; i < b.N; i++ {
			_ = batch.String()
		}
	})

	batch.Reset()

	b.Run("100 Messages (len = 8)", func(b *testing.B) {
		b.StopTimer()

		for i := 0; i < 100; i++ {
			batch.AppendBytes(message)
		}

		b.StartTimer()

		for i := 0; i < b.N; i++ {
			_ = batch.String()
		}
	})

	batch.Reset()

	b.Run("1000 Messages (len = 8)", func(b *testing.B) {
		b.StopTimer()

		for i := 0; i < 100; i++ {
			batch.AppendBytes(message)
		}

		b.StartTimer()

		for i := 0; i < b.N; i++ {
			_ = batch.String()
		}
	})

	batch.Reset()
}
