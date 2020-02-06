package core

type Storage interface {
	Put(data []byte) error
	Pop() ([]byte, error)
	Drop() error
	IsUsed() bool
}
