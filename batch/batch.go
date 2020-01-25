package batch

const (
	defaultMetaData = "{\"index\":{\"_type\":\"doc\"}}\n"
)

type Batch struct {
	index string
	buf   []byte
}

func NewBatch(size int) *Batch {
	return &Batch{buf: make([]byte, 0, size)}
}

func (b *Batch) SetIndex(index string) {
	b.index = index
}

func (b *Batch) Index() string {
	return b.index
}

// Append binary data to batch with meta info.
func (b *Batch) AppendBytes(p []byte) {
	if len(p) > 0 {
		b.buf = append(b.buf, defaultMetaData...)
		b.buf = append(b.buf, p...)
	}
}

func (b *Batch) Bytes() []byte {
	return b.buf[0:]
}

// String returns the contents of the buffer as a string.
func (b *Batch) String() string {
	return string(b.buf)
}

// Len returns the number of bytes
func (b *Batch) Len() int {
	return len(b.buf)
}

// Reset resets the buffer to be empty.
func (b *Batch) Reset() {
	b.buf = b.buf[:0]
}
