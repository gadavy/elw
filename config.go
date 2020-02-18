package elw

import (
	"net/http"
	"time"

	"github.com/gadavy/elw/transport"
)

const (
	// Default writer settings
	DefaultBatchSize    = 1024 * 1024
	DefaultRotatePeriod = time.Second
	DefaultIndexName    = "test-index"
	DefaultTimeFormat   = "2006.01.02"

	// Default transport settings
	DefaultPingInterval   = time.Second
	DefaultRequestTimeout = 2 * time.Second
	DefaultUserAgent      = "go-elastic-log-writer"

	// Default storage settings
	DefaultFilepath = "logs/app.log"
)

const (
	MinimalBatchSize = 512
)

type Config struct {
	// Writer settings
	BatchSize    int
	RotatePeriod time.Duration
	IndexName    string
	TimeFormat   string

	// Transport settings
	NodeURIs       []string
	RequestTimeout time.Duration
	PingInterval   time.Duration
	SuccessCodes   []int
	UserAgent      string

	// Storage settings
	Filepath    string
	DropStorage bool
}

func (c *Config) validate() {
	// Check writer settings
	if c.BatchSize <= MinimalBatchSize {
		c.BatchSize = MinimalBatchSize
	}

	if c.IndexName == "" {
		c.IndexName = DefaultIndexName
	}

	if c.TimeFormat == "" {
		c.TimeFormat = DefaultTimeFormat
	}

	// Check transport settings
	if c.RotatePeriod <= 0 {
		c.RotatePeriod = DefaultRotatePeriod
	}

	if c.RequestTimeout <= 0 {
		c.RequestTimeout = DefaultRequestTimeout
	}

	if c.PingInterval <= 0 {
		c.PingInterval = DefaultPingInterval
	}

	if len(c.SuccessCodes) == 0 {
		c.SuccessCodes = []int{
			http.StatusOK,
			http.StatusCreated,
			http.StatusAccepted,
		}
	}

	if c.UserAgent == "" {
		c.UserAgent = DefaultUserAgent
	}

	// Check storage settings
	if c.Filepath == "" {
		c.Filepath = DefaultFilepath
	}
}

func (c *Config) getTransportConfig() transport.Config {
	return transport.Config{
		NodeURIs:       c.NodeURIs,
		RequestTimeout: c.RequestTimeout,
		PingInterval:   c.PingInterval,
		SuccessCodes:   c.SuccessCodes,
		UserAgent:      c.UserAgent,
	}
}
