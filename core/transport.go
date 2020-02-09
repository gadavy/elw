package core

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/valyala/fasthttp"
)

type Transport interface {
	SendBulk(body []byte) error
	IsConnected() bool
	IsReconnected() <-chan struct{}
}

const (
	defaultUserAgent      = "go-elasticsearch"
	defaultPingInterval   = time.Second
	defaultRequestTimeout = 2 * time.Second
)

type transport struct {
	connStatus  uint32
	clientsPool ClientsPool

	successStatuses map[int]bool

	deadSignal *sync.Cond
	liveSignal chan struct{}
}

func NewTransport(urls ...string) Transport {
	transport := &transport{
		connStatus:  1,
		clientsPool: NewClientsPool(urls...),
		successStatuses: map[int]bool{
			fasthttp.StatusOK:       true,
			fasthttp.StatusCreated:  true,
			fasthttp.StatusAccepted: true,
		},

		liveSignal: make(chan struct{}, 1),
		deadSignal: sync.NewCond(&sync.Mutex{}),
	}

	go transport.pingDeadNodes()

	return transport
}

func (t *transport) IsConnected() (ok bool) {
	return atomic.LoadUint32(&t.connStatus) == isLive
}

func (t *transport) IsReconnected() <-chan struct{} {
	return t.liveSignal
}

func (t *transport) SendBulk(body []byte) (err error) {
	const (
		contentType = "application/x-ndjson"
		requestURI  = "/_bulk"
	)

	req := fasthttp.AcquireRequest()
	defer fasthttp.ReleaseRequest(req)

	resp := fasthttp.AcquireResponse()
	defer fasthttp.ReleaseResponse(resp)

	req.Header.SetMethod(fasthttp.MethodPost)
	req.Header.SetUserAgent(defaultUserAgent)
	req.Header.SetContentType(contentType)
	req.Header.SetRequestURI(requestURI)

	req.SetBody(body)

	for {
		client, err := t.clientsPool.NextLive()
		if err != nil {
			t.setDead()

			return err
		}

		resp.Reset()

		err = client.DoTimeout(req, resp, defaultRequestTimeout)
		if err != nil || !t.successStatuses[resp.StatusCode()] {
			t.clientsPool.OnFailure(client)
			t.deadSignal.Signal()

			continue
		}

		return nil
	}
}

func (t *transport) pingDeadNodes() {
	const requestURI = "/"

	req := fasthttp.AcquireRequest()
	defer fasthttp.ReleaseRequest(req)

	resp := fasthttp.AcquireResponse()
	defer fasthttp.ReleaseResponse(resp)

	req.Header.SetMethod(fasthttp.MethodHead)
	req.Header.SetUserAgent(defaultUserAgent)
	req.Header.SetRequestURI(requestURI)

	for {
		client, err := t.clientsPool.NextDead()
		if err != nil {
			t.deadSignal.L.Lock()
			t.deadSignal.Wait()
			t.deadSignal.L.Unlock()

			continue
		}

		err = client.DoTimeout(req, resp, defaultRequestTimeout)
		if err == nil && t.successStatuses[resp.StatusCode()] {
			t.clientsPool.OnSuccess(client)

			t.setLive()
		}

		resp.Reset()

		time.Sleep(defaultPingInterval)
	}
}

func (t *transport) setDead() {
	atomic.StoreUint32(&t.connStatus, isDead)

	t.deadSignal.Signal()
}

func (t *transport) setLive() {
	select {
	case t.liveSignal <- struct{}{}:
		atomic.StoreUint32(&t.connStatus, isLive)
	default:
		atomic.StoreUint32(&t.connStatus, isLive)
	}
}
