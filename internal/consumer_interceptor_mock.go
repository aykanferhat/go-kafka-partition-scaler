// Code generated by MockGen. DO NOT EDIT.
// Source: consumer_interceptor.go

// Package internal is a generated GoMock package.
package internal

import (
	context "context"
	reflect "reflect"

	gomock "github.com/golang/mock/gomock"
)

// MockConsumerInterceptor is a mock of ConsumerInterceptor interface.
type MockConsumerInterceptor struct {
	ctrl     *gomock.Controller
	recorder *MockConsumerInterceptorMockRecorder
}

// MockConsumerInterceptorMockRecorder is the mock recorder for MockConsumerInterceptor.
type MockConsumerInterceptorMockRecorder struct {
	mock *MockConsumerInterceptor
}

// NewMockConsumerInterceptor creates a new mock instance.
func NewMockConsumerInterceptor(ctrl *gomock.Controller) *MockConsumerInterceptor {
	mock := &MockConsumerInterceptor{ctrl: ctrl}
	mock.recorder = &MockConsumerInterceptorMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockConsumerInterceptor) EXPECT() *MockConsumerInterceptorMockRecorder {
	return m.recorder
}

// OnConsume mocks base method.
func (m *MockConsumerInterceptor) OnConsume(ctx context.Context, message *ConsumerMessage) context.Context {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "OnConsume", ctx, message)
	ret0, _ := ret[0].(context.Context)
	return ret0
}

// OnConsume indicates an expected call of OnConsume.
func (mr *MockConsumerInterceptorMockRecorder) OnConsume(ctx, message interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "OnConsume", reflect.TypeOf((*MockConsumerInterceptor)(nil).OnConsume), ctx, message)
}