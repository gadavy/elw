package batch

import (
	"time"
)

type Batch struct {
	Time time.Time
	buf  [][]byte
	len  int
}

func NewBatch() *Batch {
	return &Batch{
		buf: make([][]byte, 0),
		len: 0,
	}
}

func (b *Batch) AppendBytes(p []byte) {
	b.buf = append(b.buf, p)
	b.len += len(p)
}

// Len returns the number of bytes.
func (b *Batch) Len() int {
	return b.len
}

func (b *Batch) Buffer() [][]byte {
	return b.buf
}

// String returns the contents of the buffer as a string.
func (b *Batch) String() string {
	return string(b.join())
}

// Reset resets the batch buffer to be empty.
func (b *Batch) Reset() {
	b.buf = b.buf[:0]
	b.len = 0
}

func (b *Batch) join() (p []byte) {
	if len(b.buf) == 0 {
		return []byte{}
	}

	if len(b.buf) == 1 {
		// Just return a copy.
		return append([]byte(nil), b.buf[0]...)
	}

	p = make([]byte, 0, b.len)
	for _, v := range b.buf {
		p = append(p, v...)
	}

	return p
}
