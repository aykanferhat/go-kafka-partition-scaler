// Code generated by MockGen. DO NOT EDIT.
// Source: consumer_group.go

// Package internal is a generated GoMock package.
package internal

import (
	reflect "reflect"

	gomock "github.com/golang/mock/gomock"
)

// MockConsumerGroup is a mock of ConsumerGroup interface.
type MockConsumerGroup struct {
	ctrl     *gomock.Controller
	recorder *MockConsumerGroupMockRecorder
}

// MockConsumerGroupMockRecorder is the mock recorder for MockConsumerGroup.
type MockConsumerGroupMockRecorder struct {
	mock *MockConsumerGroup
}

// NewMockConsumerGroup creates a new mock instance.
func NewMockConsumerGroup(ctrl *gomock.Controller) *MockConsumerGroup {
	mock := &MockConsumerGroup{ctrl: ctrl}
	mock.recorder = &MockConsumerGroupMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockConsumerGroup) EXPECT() *MockConsumerGroupMockRecorder {
	return m.recorder
}

// GetGroupID mocks base method.
func (m *MockConsumerGroup) GetGroupID() string {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetGroupID")
	ret0, _ := ret[0].(string)
	return ret0
}

// GetGroupID indicates an expected call of GetGroupID.
func (mr *MockConsumerGroupMockRecorder) GetGroupID() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetGroupID", reflect.TypeOf((*MockConsumerGroup)(nil).GetGroupID))
}

// GetLastCommittedOffset mocks base method.
func (m *MockConsumerGroup) GetLastCommittedOffset(topic string, partition int32) int64 {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetLastCommittedOffset", topic, partition)
	ret0, _ := ret[0].(int64)
	return ret0
}

// GetLastCommittedOffset indicates an expected call of GetLastCommittedOffset.
func (mr *MockConsumerGroupMockRecorder) GetLastCommittedOffset(topic, partition interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetLastCommittedOffset", reflect.TypeOf((*MockConsumerGroup)(nil).GetLastCommittedOffset), topic, partition)
}

// Subscribe mocks base method.
func (m *MockConsumerGroup) Subscribe() error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Subscribe")
	ret0, _ := ret[0].(error)
	return ret0
}

// Subscribe indicates an expected call of Subscribe.
func (mr *MockConsumerGroupMockRecorder) Subscribe() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Subscribe", reflect.TypeOf((*MockConsumerGroup)(nil).Subscribe))
}

// Unsubscribe mocks base method.
func (m *MockConsumerGroup) Unsubscribe() {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "Unsubscribe")
}

// Unsubscribe indicates an expected call of Unsubscribe.
func (mr *MockConsumerGroupMockRecorder) Unsubscribe() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Unsubscribe", reflect.TypeOf((*MockConsumerGroup)(nil).Unsubscribe))
}

// WaitConsumerStart mocks base method.
func (m *MockConsumerGroup) WaitConsumerStart() {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "WaitConsumerStart")
}

// WaitConsumerStart indicates an expected call of WaitConsumerStart.
func (mr *MockConsumerGroupMockRecorder) WaitConsumerStart() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "WaitConsumerStart", reflect.TypeOf((*MockConsumerGroup)(nil).WaitConsumerStart))
}

// WaitConsumerStop mocks base method.
func (m *MockConsumerGroup) WaitConsumerStop() {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "WaitConsumerStop")
}

// WaitConsumerStop indicates an expected call of WaitConsumerStop.
func (mr *MockConsumerGroupMockRecorder) WaitConsumerStop() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "WaitConsumerStop", reflect.TypeOf((*MockConsumerGroup)(nil).WaitConsumerStop))
}