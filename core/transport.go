package core

import (
	"github.com/TermiusOne/elw/batch"
)

type Transport interface {
	IsLive() bool
	SendBulk(b *batch.Batch) error
	Reconnected() <-chan struct{}
}

type httpTransport struct{}

func (t *httpTransport) IsLive() bool {
	return false
}

func (t *httpTransport) SendBulk(b *batch.Batch) error {
	return nil
}

func (t *httpTransport) Reconnected() <-chan struct{} {
	return nil
}
