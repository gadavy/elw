package core

import (
	"errors"
	"math"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/valyala/fasthttp"
)

const (
	isDead uint32 = iota
	isLive
)

type NodeClient struct {
	status uint32
	host   string
	*fasthttp.HostClient
}

func NewNodeClient(url string) *NodeClient {
	hostClient := &fasthttp.HostClient{
		Addr:                strings.TrimLeft(url, "http://"),
		MaxConnDuration:     time.Second,
		MaxIdleConnDuration: time.Second,
	}

	return &NodeClient{
		host:       url,
		HostClient: hostClient,
	}
}

func (c *NodeClient) Do(req *fasthttp.Request, resp *fasthttp.Response) error {
	req.SetHost(c.host)

	return c.HostClient.Do(req, resp)
}

func (c *NodeClient) DoTimeout(req *fasthttp.Request, resp *fasthttp.Response, timeout time.Duration) error {
	req.SetHost(c.host)

	return c.HostClient.DoTimeout(req, resp, timeout)
}

type ClientsPool interface {
	Append(c *NodeClient)
	NextLive() (*NodeClient, error)
	NextDead() (*NodeClient, error)
	OnFailure(c *NodeClient)
	OnSuccess(c *NodeClient)
}

func NewClientsPool(urls ...string) ClientsPool {
	cs := make([]*NodeClient, 0, len(urls))

	for _, url := range urls {
		cs = append(cs, NewNodeClient(url))
	}

	return &clientsPool{clients: cs}
}

type clientsPool struct {
	mu      sync.RWMutex
	clients []*NodeClient
}

func (p *clientsPool) Append(c *NodeClient) {
	p.mu.Lock()
	p.clients = append(p.clients, c)
	p.mu.Unlock()
}

var (
	ErrNoAvailableClients = errors.New("no available clients")
)

func (p *clientsPool) NextLive() (*NodeClient, error) {
	return p.next(isLive)
}

func (p *clientsPool) NextDead() (*NodeClient, error) {
	return p.next(isDead)
}

func (p *clientsPool) OnFailure(c *NodeClient) {
	atomic.StoreUint32(&c.status, isDead)
}

func (p *clientsPool) OnSuccess(c *NodeClient) {
	atomic.StoreUint32(&c.status, isLive)
}

func (p *clientsPool) next(status uint32) (*NodeClient, error) {
	p.mu.RLock()

	clients := p.clients

	p.mu.RUnlock()

	var (
		minC *NodeClient
		minR = math.MaxInt32
		minT = time.Now()
	)

	for _, client := range clients {
		if client.status != status {
			continue
		}

		r := client.PendingRequests()
		t := client.LastUseTime()

		if r < minR || (r == minR && t.Before(minT)) {
			minC = client
			minR = r
			minT = t
		}
	}

	if minC == nil {
		return nil, ErrNoAvailableClients
	}

	return minC, nil
}
