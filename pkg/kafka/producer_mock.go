// Code generated by MockGen. DO NOT EDIT.
// Source: producer.go

// Package kafka is a generated GoMock package.
package kafka

import (
	context "context"
	reflect "reflect"

	gomock "github.com/golang/mock/gomock"
	message "github.com/Trendyol/go-kafka-partition-scaler/pkg/kafka/message"
)

// MockProducer is a mock of Producer interface.
type MockProducer struct {
	ctrl     *gomock.Controller
	recorder *MockProducerMockRecorder
}

// MockProducerMockRecorder is the mock recorder for MockProducer.
type MockProducerMockRecorder struct {
	mock *MockProducer
}

// NewMockProducer creates a new mock instance.
func NewMockProducer(ctrl *gomock.Controller) *MockProducer {
	mock := &MockProducer{ctrl: ctrl}
	mock.recorder = &MockProducerMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockProducer) EXPECT() *MockProducerMockRecorder {
	return m.recorder
}

// ProduceAsync mocks base method.
func (m *MockProducer) ProduceAsync(ctx context.Context, message *message.ProducerMessage) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "ProduceAsync", ctx, message)
	ret0, _ := ret[0].(error)
	return ret0
}

// ProduceAsync indicates an expected call of ProduceAsync.
func (mr *MockProducerMockRecorder) ProduceAsync(ctx, message interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ProduceAsync", reflect.TypeOf((*MockProducer)(nil).ProduceAsync), ctx, message)
}

// ProduceSync mocks base method.
func (m *MockProducer) ProduceSync(ctx context.Context, message *message.ProducerMessage) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "ProduceSync", ctx, message)
	ret0, _ := ret[0].(error)
	return ret0
}

// ProduceSync indicates an expected call of ProduceSync.
func (mr *MockProducerMockRecorder) ProduceSync(ctx, message interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ProduceSync", reflect.TypeOf((*MockProducer)(nil).ProduceSync), ctx, message)
}

// ProduceSyncBulk mocks base method.
func (m *MockProducer) ProduceSyncBulk(ctx context.Context, messages []*message.ProducerMessage, size int) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "ProduceSyncBulk", ctx, messages, size)
	ret0, _ := ret[0].(error)
	return ret0
}

// ProduceSyncBulk indicates an expected call of ProduceSyncBulk.
func (mr *MockProducerMockRecorder) ProduceSyncBulk(ctx, messages, size interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ProduceSyncBulk", reflect.TypeOf((*MockProducer)(nil).ProduceSyncBulk), ctx, messages, size)
}
