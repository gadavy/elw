package elw

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/TermiusOne/elw/batch"
	"github.com/TermiusOne/elw/core"
	"github.com/TermiusOne/elw/internal"
)

type ElasticWriter struct {
	transport core.Transport
	storage   core.Storage

	pool *batch.Pool

	mu    sync.Mutex
	batch **batch.Batch
	timer *time.Timer

	sendInterval time.Duration
	batchSize    int
	indexFormat  string
	failureOut   io.Writer

	once internal.Once

	wg     sync.WaitGroup
	ctx    context.Context
	cancel context.CancelFunc
}

func NewElasticWriter(cfg Config) *ElasticWriter {
	w := &ElasticWriter{
		transport:    nil,
		storage:      nil,
		pool:         batch.NewPool(cfg.BatchSize),
		timer:        time.NewTimer(cfg.SendInterval),
		sendInterval: cfg.SendInterval,
		batchSize:    cfg.BatchSize,
		failureOut:   cfg.FailureOut,
	}

	w.batch = w.pool.Get()

	w.ctx, w.cancel = context.WithCancel(context.Background())

	w.wg.Add(1)

	go w.worker()

	return w
}

func (w *ElasticWriter) Write(p []byte) (n int, err error) {
	writeLen := len(p)

	if writeLen > w.batchSize {
		return 0, errors.New("write length exceeds maximum batch size")
	}

	w.mu.Lock()

	if (*w.batch).Len()+writeLen > w.batchSize {
		w.rotateBatch()
	}

	(*w.batch).AppendBytes(p)

	w.mu.Unlock()

	return len(p), nil
}

func (w *ElasticWriter) worker() {
	for {
		select {
		case <-w.timer.C:
			w.mu.Lock()

			if (*w.batch).Len() > 0 {
				w.rotateBatch()
			}

			w.mu.Unlock()
		case <-w.transport.Reconnected():
			w.wg.Add(1)

			go w.once.Do(&w.wg, func() {
				w.releaseStorage()
			})
		case <-w.ctx.Done():
			return
		}
	}
}

func (w *ElasticWriter) rotateBatch() {
	w.wg.Add(1)

	go w.releaseBatch(*w.batch)

	w.batch = w.pool.Get()

	w.timer.Reset(w.sendInterval)
}

func (w *ElasticWriter) releaseBatch(b *batch.Batch) {
	defer w.wg.Done()
	defer w.pool.Put(b)

	b.SetIndex(w.index())

	var err error

	switch w.transport.IsLive() {
	case true:
		if err = w.transport.SendBulk(b); err == nil {
			return
		}

		fallthrough
	case false:
		if err = w.storage.Store(b); err != nil {
			return
		}
	}

	if w.failureOut != nil {
		_, _ = fmt.Fprintf(w.failureOut, "store batch %s failed: %v", b.String(), err)
	}
}

func (w *ElasticWriter) releaseStorage() {
	var (
		b   *batch.Batch
		err error
	)

	for w.storage.IsUsed() && w.transport.IsLive() {
		if b, err = w.storage.Get(); err != nil {
			continue
		}

		if err = w.transport.SendBulk(b); err == nil {
			w.pool.Put(b)
			continue
		}

		if err = w.storage.Store(b); err == nil {
			w.pool.Put(b)
			continue
		}

		if w.failureOut != nil {
			_, _ = fmt.Fprintf(w.failureOut, "store batch %s failed: %v", b.String(), err)
		}
	}
}

func (w *ElasticWriter) Sync() error {
	return w.close()
}

func (w *ElasticWriter) Close() error {
	return w.close()
}

func (w *ElasticWriter) close() error {
	w.mu.Lock()

	w.cancel()

	w.releaseBatch(*w.batch)
	w.releaseStorage()

	w.wg.Wait()
	w.mu.Unlock()

	return nil
}

func (w *ElasticWriter) index() string {
	return time.Now().Format(w.indexFormat)
}
