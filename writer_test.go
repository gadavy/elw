package elw

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/TermiusOne/elw/batch"
)

func BenchmarkElasticWriter_Write(b *testing.B) {
	b.StopTimer()
	b.ReportAllocs()

	var (
		err error

		message = []byte(strings.Repeat("test", 50))
		pool    = batch.NewPool(512)
	)

	writer := ElasticWriter{
		transport:    new(stubTransport),
		storage:      new(stubStorage),
		batch:        pool.Get(),
		pool:         pool,
		timer:        time.NewTimer(time.Second),
		batchSize:    512,
		sendInterval: time.Second,
	}

	b.StartTimer()

	b.Run("write()", func(b *testing.B) {
		b.Run("Single", func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				if _, err = writer.Write(message); err != nil {
					b.Error(err)
				}
			}
		})

		writer.rotateBatch()

		b.Run("Parallel (10)", func(b *testing.B) {
			b.SetParallelism(10)
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					if _, err = writer.Write(message); err != nil {
						b.Error(err)
					}
				}
			})
		})

		writer.rotateBatch()

		b.Run("Parallel (100)", func(b *testing.B) {
			b.SetParallelism(100)
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					if _, err = writer.Write(message); err != nil {
						b.Error(err)
					}
				}
			})
		})

		writer.rotateBatch()

		b.Run("Parallel (1000)", func(b *testing.B) {
			b.SetParallelism(1000)
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					if _, err = writer.Write(message); err != nil {
						b.Error(err)
					}
				}
			})
		})
	})
}

type stubStorage struct {
	StoreOut  error
	GetOut    *batch.Batch
	IsUsedOut bool
	DropOut   error
}

func (s *stubStorage) Store(b *batch.Batch) error { return s.StoreOut }
func (s *stubStorage) Get() (*batch.Batch, error) { return s.GetOut, nil }
func (s *stubStorage) IsUsed() bool               { return s.IsUsedOut }
func (s *stubStorage) Drop() error                { return s.DropOut }

type stubTransport struct {
	SendOut   error
	IsLiveOut bool
}

func (s *stubTransport) SendBulk(*batch.Batch) error  { return s.SendOut }
func (s *stubTransport) IsLive() bool                 { return s.IsLiveOut }
func (s *stubTransport) Reconnected() <-chan struct{} { return nil }

func TestElasticWriter_Write(t *testing.T) {
	fmt.Println(time.Now().Format("test-index-2006.01.02"))
}
