package elw

import (
	"testing"
	"time"
)

func BenchmarkElasticWriter_Write(b *testing.B) {
	b.StopTimer()

	writer := ElasticWriter{
		transport:    new(stubTransport),
		storage:      new(stubStorage),
		batchSize:    1024 * 1024,
		indexName:    "test-index-",
		timeFormat:   "2006.01.02",
		rotatePeriod: time.Second,
		timer:        time.NewTimer(time.Second),
	}

	writer.batch = writer.acquireBatch()

	b.StartTimer()

	b.Run("write", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			writer.Write([]byte(
				"1234567890QWERTYUIOP{}ASDFGHJKL:ZXCVBBNM<>?" +
					"1234567890QWERTYUIOP{}ASDFGHJKL:ZXCVBBNM<>?"))
		}
	})

	writer.rotateBatch()

	b.Run("write", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			writer.Write([]byte(
				"1234567890QWERTYUIOP{}ASDFGHJKL:ZXCVBBNM<>?" +
					"1234567890QWERTYUIOP{}ASDFGHJKL:ZXCVBBNM<>?" +
					"1234567890QWERTYUIOP{}ASDFGHJKL:ZXCVBBNM<>?"))
		}
	})

	writer.rotateBatch()

	b.Run("write", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			writer.Write([]byte(
				"1234567890QWERTYUIOP{}ASDFGHJKL:ZXCVBBNM<>?" +
					"1234567890QWERTYUIOP{}ASDFGHJKL:ZXCVBBNM<>?" +
					"1234567890QWERTYUIOP{}ASDFGHJKL:ZXCVBBNM<>?" +
					"1234567890QWERTYUIOP{}ASDFGHJKL:ZXCVBBNM<>?"))
		}
	})

	writer.rotateBatch()

	b.Run("write", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			writer.Write([]byte(
				"1234567890QWERTYUIOP{}ASDFGHJKL:ZXCVBBNM<>?" +
					"1234567890QWERTYUIOP{}ASDFGHJKL:ZXCVBBNM<>?" +
					"1234567890QWERTYUIOP{}ASDFGHJKL:ZXCVBBNM<>?" +
					"1234567890QWERTYUIOP{}ASDFGHJKL:ZXCVBBNM<>?" +
					"1234567890QWERTYUIOP{}ASDFGHJKL:ZXCVBBNM<>?"))
		}
	})

	writer.rotateBatch()
}

type stubTransport struct{}

func (s *stubTransport) SendBulk(body []byte) error {
	return nil
}

func (s *stubTransport) IsConnected() bool {
	return true
}

func (s *stubTransport) IsReconnected() <-chan struct{} {
	return nil
}

type stubStorage struct{}

func (s *stubStorage) Put(data []byte) error {
	return nil
}

func (s *stubStorage) Pop() ([]byte, error) {
	return nil, nil
}

func (s *stubStorage) Drop() error {
	return nil
}

func (s *stubStorage) IsUsed() bool {
	return false
}
