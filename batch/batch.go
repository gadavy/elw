package batch

import (
	"bytes"
	"time"
)

const (
	firstPartOfMetadata = "{\"index\":{\"_type\":\"doc\",\"_index\":\""
	lastPartOfMetadata  = "\"}}\n"
	delimiter           = "-"
	newline             = "\n"
)

type Batch struct {
	buf []byte
}

func NewBatch(size int) *Batch {
	return &Batch{buf: make([]byte, 0, size)}
}

func (b *Batch) AppendBytes(e []byte) {
	b.buf = append(b.buf, e...)

	if !bytes.HasSuffix(e, []byte(newline)) {
		b.buf = append(b.buf, newline...)
	}
}

func (b *Batch) AppendMeta(indexName, timeFormat string) {
	b.buf = append(b.buf, firstPartOfMetadata...)
	b.buf = append(b.buf, indexName...)
	b.buf = append(b.buf, delimiter...)
	b.buf = append(b.buf, time.Now().Format(timeFormat)...)
	b.buf = append(b.buf, lastPartOfMetadata...)
}

func (b *Batch) Bytes() []byte {
	return b.buf[0:]
}

func (b *Batch) String() string {
	return string(b.buf)
}

func (b *Batch) Len() int {
	return len(b.buf)
}

func (b *Batch) Reset() {
	b.buf = b.buf[:0]
}
