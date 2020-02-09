package main

import (
	"fmt"
	"os"
	"os/signal"
	"runtime"
	"sync"
	"syscall"
	"time"

	"github.com/gadavy/elw"
	"github.com/gadavy/elw/core"
)

var addr = []string{
	"http://0.0.0.0:9200",
	"http://192.168.1.112:9200",
	"http://192.168.1.112:9201",
	"http://192.168.1.112:9202",
}

func main() {
	exit := make(chan os.Signal, 1)
	signal.Notify(exit, syscall.SIGINT, syscall.SIGTERM)

	writer := elw.NewElasticWriter(core.NewTransport(addr...), NewStubStorage())
	writer.IndexName = "test-index-"
	writer.BatchSize = 1024 * 1024

	var idx int64

	defer writer.Close()

	go func() {
		for {
			fmt.Println(runtime.NumGoroutine())
			time.Sleep(time.Second)
		}
	}()

loop:
	for {
		select {
		case <-exit:
			break loop
		default:
			idx++

			msg := fmt.Sprintf("{\"message\": \"%d\", \"@timestamp\": \"%s\"}\n",
				idx,
				time.Now().Format(time.RFC3339Nano),
			)

			_, _ = writer.Write([]byte(msg))

			time.Sleep(time.Microsecond * 5)
		}
	}

	fmt.Println("stop")
}

func NewStubStorage() *stubStorage {
	return &stubStorage{buf: make([][]byte, 0)}
}

type stubStorage struct {
	mu sync.Mutex

	buf [][]byte
}

func (s *stubStorage) Put(data []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.buf = append(s.buf, data)

	return nil
}

func (s *stubStorage) Pop() (b []byte, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	b, s.buf = s.buf[0], s.buf[1:]

	return b, nil
}

func (s *stubStorage) Drop() error {
	return nil
}

func (s *stubStorage) IsUsed() bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	return len(s.buf) > 0
}
