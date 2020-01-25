package elw

import (
	"io"
	"time"
)

// Default writer settings.
const (
	DefaultSendInterval = time.Second
	DefaultBatchSize    = 1 << 10 << 10 // 1MB
	DefaultIndexFormat  = "test-index-2006.01.02"
)

type Config struct {
	// SendInterval
	SendInterval time.Duration
	// BatchSize is maximum size in bytes of logs batch before it get sending. Minimal is 512 bytes, by default it is 1MB.
	BatchSize int
	// Index. Default value is "test-index-2006.01.02"
	IndexFormat string
	// FailureOut
	FailureOut io.Writer
}

const (
	minimalBatchSize    = 512
	minimalSendInterval = time.Millisecond * 100
)

func (c *Config) validate() {
	if c.BatchSize < minimalBatchSize {
		c.BatchSize = DefaultBatchSize
	}

	if c.SendInterval < minimalSendInterval {
		c.SendInterval = DefaultSendInterval
	}

	if c.IndexFormat == "" {
		c.IndexFormat = DefaultIndexFormat
	}
}
