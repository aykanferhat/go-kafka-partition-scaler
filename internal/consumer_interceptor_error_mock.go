// Code generated by MockGen. DO NOT EDIT.
// Source: consumer_interceptor_error.go

// Package internal is a generated GoMock package.
package internal

import (
	context "context"
	reflect "reflect"

	gomock "github.com/golang/mock/gomock"
)

// MockConsumerErrorInterceptor is a mock of ConsumerErrorInterceptor interface.
type MockConsumerErrorInterceptor struct {
	ctrl     *gomock.Controller
	recorder *MockConsumerErrorInterceptorMockRecorder
}

// MockConsumerErrorInterceptorMockRecorder is the mock recorder for MockConsumerErrorInterceptor.
type MockConsumerErrorInterceptorMockRecorder struct {
	mock *MockConsumerErrorInterceptor
}

// NewMockConsumerErrorInterceptor creates a new mock instance.
func NewMockConsumerErrorInterceptor(ctrl *gomock.Controller) *MockConsumerErrorInterceptor {
	mock := &MockConsumerErrorInterceptor{ctrl: ctrl}
	mock.recorder = &MockConsumerErrorInterceptorMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockConsumerErrorInterceptor) EXPECT() *MockConsumerErrorInterceptorMockRecorder {
	return m.recorder
}

// OnError mocks base method.
func (m *MockConsumerErrorInterceptor) OnError(ctx context.Context, message *ConsumerMessage, err error) {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "OnError", ctx, message, err)
}

// OnError indicates an expected call of OnError.
func (mr *MockConsumerErrorInterceptorMockRecorder) OnError(ctx, message, err interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "OnError", reflect.TypeOf((*MockConsumerErrorInterceptor)(nil).OnError), ctx, message, err)
}
