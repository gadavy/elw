package elw

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/gadavy/elw/transport"
)

func TestConfig(t *testing.T) {
	t.Run("Default values", func(t *testing.T) {
		expected := Config{
			BatchSize:      MinimalBatchSize,
			RotatePeriod:   DefaultRotatePeriod,
			IndexName:      DefaultIndexName,
			TimeFormat:     DefaultTimeFormat,
			RequestTimeout: DefaultRequestTimeout,
			PingInterval:   DefaultPingInterval,
			SuccessCodes:   []int{200, 201, 202},
			UserAgent:      DefaultUserAgent,
			Filepath:       DefaultFilepath,
		}

		config := Config{}
		config.validate()

		assert.Equal(t, expected, config)
	})

	t.Run("GetTransportConfig", func(t *testing.T) {
		expected := transport.Config{
			NodeURIs:       []string{"http://127.0.0.1:9200", "http://127.0.0.1:9201"},
			RequestTimeout: DefaultRequestTimeout,
			PingInterval:   DefaultPingInterval,
			SuccessCodes:   []int{200, 201, 202},
			UserAgent:      DefaultUserAgent,
		}

		config := Config{
			NodeURIs:       []string{"http://127.0.0.1:9200", "http://127.0.0.1:9201"},
			RequestTimeout: DefaultRequestTimeout,
			PingInterval:   DefaultPingInterval,
			SuccessCodes:   []int{200, 201, 202},
			UserAgent:      DefaultUserAgent,
		}

		assert.Equal(t, expected, config.getTransportConfig())
	})
}
