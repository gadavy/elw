package storage

import (
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

type Storage interface {
	Put(data []byte) error
	Pop() ([]byte, error)
	Drop() error
	IsUsed() bool
}

func NewStorage(path string) (Storage, error) {
	if path == ":memory:" {
		return newMemoryStorage(), nil
	}

	return newFileStorage(path)
}

type MemoryStorage struct {
	mu      sync.Mutex
	storage [][]byte
}

func newMemoryStorage() *MemoryStorage {
	return &MemoryStorage{storage: make([][]byte, 0)}
}

func (s *MemoryStorage) Put(data []byte) error {
	s.mu.Lock()
	s.storage = append(s.storage, append(data[:0:0], data...))
	s.mu.Unlock()

	return nil
}

func (s *MemoryStorage) Pop() (b []byte, err error) {
	s.mu.Lock()
	b, s.storage = s.storage[0], s.storage[1:]
	s.mu.Unlock()

	return b, nil
}

func (s *MemoryStorage) Drop() error {
	s.mu.Lock()
	s.storage = make([][]byte, 0)
	s.mu.Unlock()

	return nil
}

func (s *MemoryStorage) IsUsed() (ok bool) {
	s.mu.Lock()
	ok = len(s.storage) > 0
	s.mu.Unlock()

	return ok
}

type FileStorage struct {
	path  string
	file  string
	count int64
}

func newFileStorage(filepath string) (*FileStorage, error) {
	dir, file := path.Split(filepath)

	if err := os.MkdirAll(dir, os.ModePerm); err != nil {
		return nil, err
	}

	if file == "" {
		file = "app.log"
	}

	storage := &FileStorage{
		path: dir,
		file: file,
	}

	return storage, nil
}

func (s *FileStorage) Put(data []byte) (err error) {
	err = ioutil.WriteFile(s.filename(), data, os.ModePerm)
	if err != nil {
		return err
	}

	atomic.AddInt64(&s.count, 1)

	return nil
}

func (s *FileStorage) Pop() (data []byte, err error) {
	files, err := ioutil.ReadDir(s.path)
	if err != nil {
		return nil, err
	}

	if len(files) == 0 {
		atomic.StoreInt64(&s.count, 0)

		return nil, errors.New("no such files")
	}

	filename := fmt.Sprint(s.path, files[0].Name())

	data, err = ioutil.ReadFile(filename)
	if err != nil {
		return nil, err
	}

	if err = os.Remove(filename); err != nil {
		return nil, err
	}

	atomic.AddInt64(&s.count, -1)

	return data, nil
}

func (s *FileStorage) Drop() error {
	files, err := ioutil.ReadDir(s.path)
	if err != nil {
		return err
	}

	if len(files) == 0 {
		atomic.StoreInt64(&s.count, 0)

		return errors.New("no such files")
	}

	for _, file := range files {
		filename := fmt.Sprint(s.path, file.Name())

		if err = os.Remove(filename); err != nil {
			return err
		}

		atomic.AddInt64(&s.count, -1)
	}

	return nil
}

func (s *FileStorage) IsUsed() bool {
	return atomic.LoadInt64(&s.count) > 0
}

func (s *FileStorage) filename() string {
	t := strconv.FormatInt(time.Now().UnixNano(), 10)

	buf := make([]byte, 0, len(s.path)+len(t)+len(s.file)+1)
	buf = append(buf, s.path...)
	buf = append(buf, s.file...)
	buf = append(buf, "."...)
	buf = append(buf, t...)

	return string(buf)
}
