package elw

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/gadavy/elw/batch"
	"github.com/gadavy/elw/test"
)

func TestElasticWriter_releaseBatch(t *testing.T) {
	tests := []struct {
		name                    string
		input                   []byte
		transport               *test.MockTransport
		transportIsConnectedOut []interface{}
		transportSendBulkIn     []interface{}
		transportSendBulkOut    []interface{}
		storage                 *test.MockStorage
		storagePutIn            []interface{}
		storagePutOut           []interface{}
		logger                  *test.MockLogger
		loggerIn                []interface{}
	}{
		{
			name:      "IsConnectedSendPass",
			input:     []byte("Message\n"),
			transport: &test.MockTransport{},
			transportIsConnectedOut: []interface{}{
				true,
			},
			transportSendBulkIn: []interface{}{
				[]byte("Message\n"),
			},
			transportSendBulkOut: []interface{}{
				(error)(nil),
			},
			storage:       &test.MockStorage{},
			storagePutIn:  nil,
			storagePutOut: nil,
			logger:        &test.MockLogger{},
			loggerIn:      nil,
		},
		{
			name:      "IsConnectedPutPass",
			input:     []byte("Message\n"),
			transport: &test.MockTransport{},
			transportIsConnectedOut: []interface{}{
				true,
			},
			transportSendBulkIn: []interface{}{
				[]byte("Message\n"),
			},
			transportSendBulkOut: []interface{}{
				errors.New("transport error"),
			},
			storage: &test.MockStorage{},
			storagePutIn: []interface{}{
				[]byte("Message\n"),
			},
			storagePutOut: []interface{}{
				(error)(nil),
			},
			logger:   &test.MockLogger{},
			loggerIn: nil,
		},
		{
			name:      "IsConnectedPutError",
			input:     []byte("Message\n"),
			transport: &test.MockTransport{},
			transportIsConnectedOut: []interface{}{
				true,
			},
			transportSendBulkIn: []interface{}{
				[]byte("Message\n"),
			},
			transportSendBulkOut: []interface{}{
				errors.New("transport error"),
			},
			storage: &test.MockStorage{},
			storagePutIn: []interface{}{
				[]byte("Message\n"),
			},
			storagePutOut: []interface{}{
				errors.New("storage error"),
			},
			logger: &test.MockLogger{},
			loggerIn: []interface{}{
				"release batch = Message\n failed: storage error",
			},
		},
		{
			name:      "NotConnectedPutPass",
			input:     []byte("Message\n"),
			transport: &test.MockTransport{},
			transportIsConnectedOut: []interface{}{
				false,
			},
			transportSendBulkIn:  nil,
			transportSendBulkOut: nil,
			storage:              &test.MockStorage{},
			storagePutIn: []interface{}{
				[]byte("Message\n"),
			},
			storagePutOut: []interface{}{
				errors.New("storage error"),
			},
			logger: &test.MockLogger{},
			loggerIn: []interface{}{
				"release batch = Message\n failed: storage error",
			},
		},
		{
			name:      "NotConnectedPutError",
			input:     []byte("Message\n"),
			transport: &test.MockTransport{},
			transportIsConnectedOut: []interface{}{
				false,
			},
			transportSendBulkIn:  nil,
			transportSendBulkOut: nil,
			storage:              &test.MockStorage{},
			storagePutIn: []interface{}{
				[]byte("Message\n"),
			},
			storagePutOut: []interface{}{
				(error)(nil),
			},
			logger:   &test.MockLogger{},
			loggerIn: nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.transport.On("IsConnected").Return(tt.transportIsConnectedOut...)
			tt.transport.On("SendBulk", tt.transportSendBulkIn...).Return(tt.transportSendBulkOut...)

			tt.storage.On("Put", tt.storagePutIn...).Return(tt.storagePutOut...)

			tt.logger.On("Printf", tt.loggerIn...)

			b := batch.NewBatch(len(tt.input))
			b.AppendBytes(tt.input)

			writer := ElasticWriter{
				transport: tt.transport,
				storage:   tt.storage,
				logger:    tt.logger,
				wg:        new(sync.WaitGroup),
			}

			writer.wg.Add(1)

			writer.releaseBatch(b)
		})
	}
}

func TestElasticWriter_AcquireAndRotateBatch(t *testing.T) {
	writer := ElasticWriter{
		transport: new(test.StubTransport),
		timer:     time.NewTimer(time.Second),
		wg:        new(sync.WaitGroup),
	}

	res := writer.acquireBatch()

	b := &batch.Batch{}
	expected := &b

	assert.IsType(t, expected, res)

	writer.batch = expected

	(*expected).AppendBytes([]byte("1"))

	writer.rotateBatch()

	assert.NotEqual(t, *expected, *writer.batch)
}

func TestElasticWriter_releaseStorage(t *testing.T) {
	tests := []struct {
		name                    string
		transport               *test.MockTransport
		transportIsConnectedOut []interface{}
		transportSendBulkIn     []interface{}
		transportSendBulkOut    []interface{}
		storage                 *test.MockStorage
		storageIsUsedOut        []interface{}
		storagePopOut           []interface{}
		storagePutIn            []interface{}
		storagePutOut           []interface{}
		logger                  *test.MockLogger
		loggerIn                []interface{}
	}{
		{
			name:      "PopError",
			transport: &test.MockTransport{},
			transportIsConnectedOut: []interface{}{
				true,
				true,
			},
			storage: &test.MockStorage{},
			storageIsUsedOut: []interface{}{
				true,
				false,
			},

			storagePopOut: []interface{}{
				([]byte)(nil),
				errors.New("storage error"),
			},
			storagePutIn:  nil,
			storagePutOut: nil,
			logger:        &test.MockLogger{},
			loggerIn:      nil,
		},
		{
			name:      "SendBulkPass",
			transport: &test.MockTransport{},
			transportIsConnectedOut: []interface{}{
				true,
				true,
			},
			transportSendBulkIn: []interface{}{
				[]byte("message"),
			},
			transportSendBulkOut: []interface{}{
				(error)(nil),
			},
			storage: &test.MockStorage{},
			storageIsUsedOut: []interface{}{
				true,
				false,
			},
			storagePopOut: []interface{}{
				[]byte("message"),
				(error)(nil),
			},
			storagePutIn:  nil,
			storagePutOut: nil,
			logger:        &test.MockLogger{},
			loggerIn:      nil,
		},
		{
			name:      "PutPass",
			transport: &test.MockTransport{},
			transportIsConnectedOut: []interface{}{
				true,
				true,
			},
			transportSendBulkIn: []interface{}{
				[]byte("message"),
			},
			transportSendBulkOut: []interface{}{
				errors.New("transport error"),
			},
			storage: &test.MockStorage{},
			storageIsUsedOut: []interface{}{
				true,
				false,
			},
			storagePopOut: []interface{}{
				[]byte("message"),
				(error)(nil),
			},
			storagePutIn: []interface{}{
				[]byte("message"),
			},
			storagePutOut: []interface{}{
				(error)(nil),
			},
			logger:   &test.MockLogger{},
			loggerIn: nil,
		},
		{
			name:      "PutError",
			transport: &test.MockTransport{},
			transportIsConnectedOut: []interface{}{
				true,
				true,
			},
			transportSendBulkIn: []interface{}{
				[]byte("message"),
			},
			transportSendBulkOut: []interface{}{
				errors.New("transport error"),
			},
			storage: &test.MockStorage{},
			storageIsUsedOut: []interface{}{
				true,
				false,
			},
			storagePopOut: []interface{}{
				[]byte("message"),
				(error)(nil),
			},
			storagePutIn: []interface{}{
				[]byte("message"),
			},
			storagePutOut: []interface{}{
				errors.New("storage error"),
			},
			logger: &test.MockLogger{},
			loggerIn: []interface{}{
				"release batch = message failed: storage error",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.transport.On("IsConnected").Return(tt.transportIsConnectedOut...)
			tt.transport.On("SendBulk", tt.transportSendBulkIn...).Return(tt.transportSendBulkOut...)

			tt.storage.On("IsUsed").Return(tt.storageIsUsedOut...)
			tt.storage.On("Pop").Return(tt.storagePopOut...)
			tt.storage.On("Put", tt.storagePutIn...).Return(tt.storagePutOut...)

			tt.logger.On("Printf", tt.loggerIn...)

			writer := ElasticWriter{
				transport: tt.transport,
				storage:   tt.storage,
				logger:    tt.logger,
				wg:        new(sync.WaitGroup),
			}

			writer.releaseStorage()
		})
	}
}
