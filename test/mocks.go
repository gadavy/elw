package test

import (
	"fmt"

	"github.com/stretchr/testify/mock"
)

type MockTransport struct {
	mock.Mock
}

func (m *MockTransport) SendBulk(body []byte) error {
	return m.Called(body).Error(0)
}

func (m *MockTransport) IsConnected() bool {
	return m.Called().Bool(0)
}

func (m *MockTransport) IsReconnected() <-chan struct{} {
	return m.Called().Get(0).(<-chan struct{})
}

type MockStorage struct {
	mock.Mock
}

func (m *MockStorage) Put(data []byte) error {
	return m.Called(data).Error(0)
}

func (m *MockStorage) Pop() ([]byte, error) {
	args := m.Called(m)
	return args.Get(0).([]byte), args.Error(1)
}

func (m *MockStorage) Drop() error {
	return m.Called().Error(0)
}

func (m *MockStorage) IsUsed() bool {
	return m.Called().Bool(0)
}

type MockLogger struct {
	mock.Mock
}

func (m *MockLogger) Printf(format string, v ...interface{}) {
	m.Called(fmt.Sprintf(format, v...))
}
