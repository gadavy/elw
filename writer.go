package elw

import (
	"sync"
	"time"

	"github.com/gadavy/elw/core"
	"github.com/gadavy/elw/internal"
)

type ElasticWriter struct {
	noCopy noCopy // nolint:unused,structcheck

	BatchSize    int
	RotatePeriod time.Duration
	IndexName    string
	TimeFormat   string

	transport core.Transport
	storage   core.Storage
	logger    core.Logger

	mu    sync.Mutex
	batch **core.Batch
	timer *time.Timer

	batchPool sync.Pool

	once internal.Once

	done chan struct{}
}

func (w *ElasticWriter) Init() {
	go w.worker()
}

func (w *ElasticWriter) Write(p []byte) (n int, err error) {
	w.mu.Lock()

	if (*w.batch).Len() > w.BatchSize {
		w.rotateBatch()
	}

	(*w.batch).AppendMeta(w.IndexName, w.TimeFormat)
	(*w.batch).AppendBytes(p)

	w.mu.Unlock()

	return len(p), nil
}

func (w *ElasticWriter) Sync() error {
	w.mu.Lock()

	if (*w.batch).Len() > 0 {
		w.rotateBatch()
	}

	w.mu.Unlock()

	return nil
}

func (w *ElasticWriter) Close() error {
	w.done <- struct{}{}
	w.rotateBatch()
	w.releaseStorage()

	return nil
}

func (w *ElasticWriter) rotateBatch() {
	go w.releaseBatch(*w.batch)

	w.batch = w.acquireBatch()

	w.timer.Reset(w.RotatePeriod)
}

func (w *ElasticWriter) acquireBatch() **core.Batch {
	b, ok := w.batchPool.Get().(*core.Batch)
	if !ok {
		b = core.NewBatch(w.BatchSize)
	}

	return &b
}

func (w *ElasticWriter) releaseBatch(b *core.Batch) {
	defer w.batchPool.Put(b)
	defer b.Reset()

	var err error

	switch w.transport.IsConnected() {
	case true:
		if err = w.transport.SendBulk(b.Bytes()); err == nil {
			return
		}

		fallthrough
	case false:
		if err = w.storage.Put(b.Bytes()); err == nil {
			return
		}

		if w.logger != nil {
			w.logger.Printf("release batch = %s failed: %v", b.String(), err)
		}
	}
}

func (w *ElasticWriter) releaseStorage() {
	var (
		buf []byte
		err error
	)

	for w.transport.IsConnected() && w.storage.IsUsed() {
		if buf, err = w.storage.Pop(); err != nil {
			continue
		}

		if err = w.transport.SendBulk(buf); err == nil {
			continue
		}

		if err = w.storage.Put(buf); err == nil {
			continue
		}

		if w.logger != nil {
			w.logger.Printf("release batch = %s failed: %v", buf, err)
		}
	}
}

func (w *ElasticWriter) worker() {
	w.timer = time.NewTimer(w.RotatePeriod)

	for {
		select {
		case <-w.transport.IsReconnected():
			go w.once.Do(w.releaseStorage)
		case <-w.timer.C:
			w.mu.Lock()

			if (*w.batch).Len() > 0 {
				w.rotateBatch()
			}

			w.mu.Unlock()
		case <-w.done:
			return
		}
	}
}
