package transport

import (
	"sync/atomic"
	"time"

	"github.com/valyala/fasthttp"

	"github.com/gadavy/elw/internal"
)

type Transport interface {
	SendBulk(body []byte) error
	IsConnected() bool
	IsReconnected() <-chan struct{}
}

type Config struct {
	NodeURIs       []string
	RequestTimeout time.Duration
	PingInterval   time.Duration
	SuccessCodes   []int
	UserAgent      string
}

type httpTransport struct {
	connStatus  uint32
	clientsPool ClientsPool

	requestTimeout time.Duration
	pingInterval   time.Duration
	successCodes   map[int]bool

	deadSignal internal.Signal
	liveSignal internal.Signal
}

func New(cfg Config) (Transport, error) {
	pool, err := NewClientsPool(cfg.NodeURIs, cfg.UserAgent)
	if err != nil {
		return nil, err
	}

	transport := &httpTransport{
		clientsPool:    pool,
		connStatus:     isLive,
		pingInterval:   cfg.PingInterval,
		requestTimeout: cfg.RequestTimeout,
		successCodes:   make(map[int]bool),

		liveSignal: make(internal.Signal, 1),
		deadSignal: make(internal.Signal, 1),
	}

	for _, code := range cfg.SuccessCodes {
		transport.successCodes[code] = true
	}

	go transport.pingDeadNodes()

	return transport, nil
}

func (t *httpTransport) IsConnected() (ok bool) {
	return atomic.LoadUint32(&t.connStatus) == isLive
}

func (t *httpTransport) IsReconnected() <-chan struct{} {
	return t.liveSignal
}

func (t *httpTransport) SendBulk(body []byte) error {
	var (
		client *NodeClient
		code   int
		err    error
	)

	for {
		client, err = t.clientsPool.NextLive()
		if err != nil {
			atomic.StoreUint32(&t.connStatus, isDead)

			t.deadSignal.Send()

			return err
		}

		code, err = client.BulkRequest(body, t.requestTimeout)
		if err == nil && t.successCodes[code] {
			return nil
		}

		if err != fasthttp.ErrNoFreeConns {
			t.clientsPool.OnFailure(client)
			t.deadSignal.Send()
		}
	}
}

func (t *httpTransport) pingDeadNodes() {
	var (
		client *NodeClient
		code   int
		err    error
	)

	for {
		client, err = t.clientsPool.NextDead()
		if err != nil {
			<-t.deadSignal
			continue
		}

		code, err = client.PingRequest(t.requestTimeout)
		if err == nil && t.successCodes[code] {
			t.clientsPool.OnSuccess(client)

			atomic.StoreUint32(&t.connStatus, isLive)

			t.liveSignal.Send()
		}

		time.Sleep(t.pingInterval)
	}
}
