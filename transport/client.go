package transport

import (
	"strings"
	"sync/atomic"
	"time"

	"github.com/valyala/fasthttp"
)

const (
	isDead uint32 = iota
	isLive
)

const (
	MaxIdleConnDuration = 5 * time.Second
)

type NodeClient struct {
	host      string
	useragent string

	status      uint32
	lastUseTime int64

	client fasthttp.HostClient
}

// NewNodeClient create elastic node client with small api.
func NewNodeClient(url, useragent string) *NodeClient {
	client := &NodeClient{
		host:      url,
		useragent: useragent,
		status:    isLive,
		client: fasthttp.HostClient{
			Addr:                strings.TrimPrefix(url, "http://"),
			MaxIdleConnDuration: MaxIdleConnDuration,
		},
	}

	return client
}

// Bulk request allows to perform multiple index operations in a single request.
// Full documentation at https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-bulk.html
func (c *NodeClient) BulkRequest(body []byte, timeout time.Duration) (code int, err error) {
	const (
		contentType = "application/x-ndjson"
		requestURI  = "/_bulk"
	)

	req := fasthttp.AcquireRequest()
	defer fasthttp.ReleaseRequest(req)

	resp := fasthttp.AcquireResponse()
	defer fasthttp.ReleaseResponse(resp)

	req.Header.SetMethod(fasthttp.MethodPost)
	req.Header.SetUserAgent(c.useragent)
	req.Header.SetContentType(contentType)
	req.Header.SetRequestURI(requestURI)
	req.Header.SetHost(c.host)

	req.SetBody(body)

	atomic.StoreInt64(&c.lastUseTime, time.Now().UnixNano())

	err = c.client.DoTimeout(req, resp, timeout)

	return resp.StatusCode(), err
}

// Ping request allows to check connection status.
func (c *NodeClient) PingRequest(timeout time.Duration) (code int, err error) {
	const requestURI = "/"

	req := fasthttp.AcquireRequest()
	defer fasthttp.ReleaseRequest(req)

	resp := fasthttp.AcquireResponse()
	defer fasthttp.ReleaseResponse(resp)

	req.Header.SetMethod(fasthttp.MethodHead)
	req.Header.SetUserAgent(c.useragent)
	req.Header.SetRequestURI(requestURI)
	req.Header.SetHost(c.host)

	atomic.StoreInt64(&c.lastUseTime, time.Now().UnixNano())

	err = c.client.DoTimeout(req, resp, timeout)

	return resp.StatusCode(), err
}

// PendingRequests returns all pending request of node client.
func (c *NodeClient) PendingRequests() int {
	return c.client.PendingRequests()
}

// LastUseTime returns time of last started request.
func (c *NodeClient) LastUseTime() int {
	return int(atomic.LoadInt64(&c.lastUseTime))
}
