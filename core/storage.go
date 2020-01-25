package core

import (
	"os"

	"github.com/TermiusOne/elw/batch"
)

type Storage interface {
	Store(b *batch.Batch) error
	Get() (*batch.Batch, error)
	IsUsed() bool
	Drop() error
}

type fileStorage struct {
	file os.File
}

func (s *fileStorage) Store(b *batch.Batch) error {
	return nil
}

func (s *fileStorage) Get() (*batch.Batch, error) {
	return nil, nil
}

func (s *fileStorage) IsUsed() bool {
	return false
}

func (s *fileStorage) Drop() error {
	return nil
}
