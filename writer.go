package elw

import (
	"sync"
	"time"

	"github.com/gadavy/elw/batch"
	"github.com/gadavy/elw/internal"
	"github.com/gadavy/elw/storage"
	"github.com/gadavy/elw/transport"
)

func NewElasticWriter(cfg Config) (*ElasticWriter, error) {
	cfg.validate()

	tr, err := transport.New(cfg.getTransportConfig())
	if err != nil {
		return nil, err
	}

	st, err := storage.New(cfg.Filepath)
	if err != nil {
		return nil, err
	}

	ew := &ElasticWriter{
		batchSize:    cfg.BatchSize,
		indexName:    cfg.IndexName,
		timeFormat:   cfg.TimeFormat,
		rotatePeriod: cfg.RotatePeriod,
		dropStorage:  cfg.DropStorage,

		transport: tr,
		storage:   st,

		done: make(internal.Signal, 1),
		wg:   new(sync.WaitGroup),
	}

	ew.batch = ew.acquireBatch()
	ew.timer = time.NewTimer(ew.rotatePeriod)

	go ew.worker()

	return ew, nil
}

type ElasticWriter struct {
	noCopy noCopy // nolint:unused,structcheck

	transport transport.Transport
	storage   storage.Storage
	logger    Logger

	batchSize    int
	rotatePeriod time.Duration
	indexName    string
	timeFormat   string
	dropStorage  bool

	once internal.Once
	done internal.Signal

	mu    sync.Mutex
	batch **batch.Batch
	timer *time.Timer

	wg *sync.WaitGroup

	batchPool sync.Pool
}

func (w *ElasticWriter) Write(p []byte) (n int, err error) {
	w.mu.Lock()

	if (*w.batch).Len()+len(p) > w.batchSize {
		w.rotateBatch()
	}

	(*w.batch).AppendMeta(w.indexName, w.timeFormat)
	(*w.batch).AppendBytes(p)

	w.mu.Unlock()

	return len(p), nil
}

func (w *ElasticWriter) Sync() error {
	w.mu.Lock()

	w.rotateBatch()

	w.mu.Unlock()

	return nil
}

func (w *ElasticWriter) Close() error {
	w.done.Send()

	w.mu.Lock()

	w.rotateBatch()

	w.wg.Add(1)
	w.once.DoWG(w.wg, w.releaseStorage)

	w.wg.Wait()

	w.mu.Unlock()

	if w.dropStorage {
		return w.storage.Drop()
	}

	return nil
}

func (w *ElasticWriter) rotateBatch() {
	if (*w.batch).Len() > 0 {
		w.wg.Add(1)

		go w.releaseBatch(*w.batch)

		w.batch = w.acquireBatch()
	}

	w.timer.Reset(w.rotatePeriod)
}

func (w *ElasticWriter) acquireBatch() **batch.Batch {
	b, ok := w.batchPool.Get().(*batch.Batch)
	if !ok {
		b = batch.NewBatch(w.batchSize)
	}

	return &b
}

func (w *ElasticWriter) releaseBatch(b *batch.Batch) {
	defer w.wg.Done()
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
	for {
		select {
		case <-w.transport.IsReconnected():
			w.wg.Add(1)

			go w.once.DoWG(w.wg, w.releaseStorage)
		case <-w.timer.C:
			w.rotateBatch()
		case <-w.done:
			return
		}
	}
}
