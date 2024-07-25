// Code generated by MockGen. DO NOT EDIT.
// Source: processed_message_listener.go

// Package internal is a generated GoMock package.
package internal

import (
	reflect "reflect"
	time "time"

	gomock "github.com/golang/mock/gomock"
)

// MockProcessedMessageListener is a mock of ProcessedMessageListener interface.
type MockProcessedMessageListener struct {
	ctrl     *gomock.Controller
	recorder *MockProcessedMessageListenerMockRecorder
}

// MockProcessedMessageListenerMockRecorder is the mock recorder for MockProcessedMessageListener.
type MockProcessedMessageListenerMockRecorder struct {
	mock *MockProcessedMessageListener
}

// NewMockProcessedMessageListener creates a new mock instance.
func NewMockProcessedMessageListener(ctrl *gomock.Controller) *MockProcessedMessageListener {
	mock := &MockProcessedMessageListener{ctrl: ctrl}
	mock.recorder = &MockProcessedMessageListenerMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockProcessedMessageListener) EXPECT() *MockProcessedMessageListenerMockRecorder {
	return m.recorder
}

// Close mocks base method.
func (m *MockProcessedMessageListener) Close() {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "Close")
}

// Close indicates an expected call of Close.
func (mr *MockProcessedMessageListenerMockRecorder) Close() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Close", reflect.TypeOf((*MockProcessedMessageListener)(nil).Close))
}

// IsChannelClosed mocks base method.
func (m *MockProcessedMessageListener) IsChannelClosed() bool {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "IsChannelClosed")
	ret0, _ := ret[0].(bool)
	return ret0
}

// IsChannelClosed indicates an expected call of IsChannelClosed.
func (mr *MockProcessedMessageListenerMockRecorder) IsChannelClosed() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "IsChannelClosed", reflect.TypeOf((*MockProcessedMessageListener)(nil).IsChannelClosed))
}

// LastCommittedOffset mocks base method.
func (m *MockProcessedMessageListener) LastCommittedOffset() int64 {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "LastCommittedOffset")
	ret0, _ := ret[0].(int64)
	return ret0
}

// LastCommittedOffset indicates an expected call of LastCommittedOffset.
func (mr *MockProcessedMessageListenerMockRecorder) LastCommittedOffset() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "LastCommittedOffset", reflect.TypeOf((*MockProcessedMessageListener)(nil).LastCommittedOffset))
}

// Publish mocks base method.
func (m *MockProcessedMessageListener) Publish(message *ConsumerMessage) {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "Publish", message)
}

// Publish indicates an expected call of Publish.
func (mr *MockProcessedMessageListenerMockRecorder) Publish(message interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Publish", reflect.TypeOf((*MockProcessedMessageListener)(nil).Publish), message)
}

// ResetTicker mocks base method.
func (m *MockProcessedMessageListener) ResetTicker(duration time.Duration) {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "ResetTicker", duration)
}

// ResetTicker indicates an expected call of ResetTicker.
func (mr *MockProcessedMessageListenerMockRecorder) ResetTicker(duration interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ResetTicker", reflect.TypeOf((*MockProcessedMessageListener)(nil).ResetTicker), duration)
}

// SetFirstConsumedMessage mocks base method.
func (m *MockProcessedMessageListener) SetFirstConsumedMessage(topic string, partition int32, offset int64) {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "SetFirstConsumedMessage", topic, partition, offset)
}

// SetFirstConsumedMessage indicates an expected call of SetFirstConsumedMessage.
func (mr *MockProcessedMessageListenerMockRecorder) SetFirstConsumedMessage(topic, partition, offset interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "SetFirstConsumedMessage", reflect.TypeOf((*MockProcessedMessageListener)(nil).SetFirstConsumedMessage), topic, partition, offset)
}