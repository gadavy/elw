package core

import (
	"errors"

	"github.com/valyala/fasthttp"
)

type Transport interface {
	SendBulk(body []byte) error
	IsConnected() bool
	IsReconnected() <-chan struct{}
}

type transport struct {
	url    string
	status int

	successStatuses map[int]struct{}
	client          *fasthttp.Client
}

func NewTransport(url string) Transport {
	return &transport{
		url:             url,
		successStatuses: map[int]struct{}{200: {}, 201: {}, 202: {}},
		client:          &fasthttp.Client{},
	}
}

func (t *transport) SendBulk(body []byte) (err error) {
	const (
		userAgent   = "go-elasticsearch"
		contentType = "application/x-ndjson"
	)

	req := fasthttp.AcquireRequest()
	defer fasthttp.ReleaseRequest(req)

	resp := fasthttp.AcquireResponse()
	defer fasthttp.ReleaseResponse(resp)

	req.Header.SetMethod(fasthttp.MethodPost)
	req.Header.SetUserAgent(userAgent)
	req.Header.SetContentType(contentType)
	req.Header.SetRequestURI(t.url + "/_bulk")

	req.SetBody(body)

	return t.doRequest(req, resp)
}

func (t *transport) doRequest(req *fasthttp.Request, resp *fasthttp.Response) (err error) {
	if err = t.client.Do(req, resp); err != nil {
		return err
	}

	if _, ok := t.successStatuses[resp.StatusCode()]; !ok {
		return errors.New("send bulk failed")
	}

	return nil
}

func (t *transport) IsConnected() bool { return true }

func (t *transport) IsReconnected() <-chan struct{} { return nil }
