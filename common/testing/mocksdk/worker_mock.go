// The MIT License
//
// Copyright (c) 2020 Temporal Technologies Inc.  All rights reserved.
//
// Copyright (c) 2020 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

// Code generated by MockGen. DO NOT EDIT.
// Source: go.temporal.io/sdk/worker (interfaces: Worker)
//
// Generated by this command:
//
//	mockgen -copyright_file ../../../LICENSE -package mocksdk go.temporal.io/sdk/worker Worker
//

// Package mocksdk is a generated GoMock package.
package mocksdk

import (
	reflect "reflect"

	nexus "github.com/nexus-rpc/sdk-go/nexus"
	activity "go.temporal.io/sdk/activity"
	workflow "go.temporal.io/sdk/workflow"
	gomock "go.uber.org/mock/gomock"
)

// MockWorker is a mock of Worker interface.
type MockWorker struct {
	ctrl     *gomock.Controller
	recorder *MockWorkerMockRecorder
}

// MockWorkerMockRecorder is the mock recorder for MockWorker.
type MockWorkerMockRecorder struct {
	mock *MockWorker
}

// NewMockWorker creates a new mock instance.
func NewMockWorker(ctrl *gomock.Controller) *MockWorker {
	mock := &MockWorker{ctrl: ctrl}
	mock.recorder = &MockWorkerMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockWorker) EXPECT() *MockWorkerMockRecorder {
	return m.recorder
}

// RegisterActivity mocks base method.
func (m *MockWorker) RegisterActivity(arg0 any) {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "RegisterActivity", arg0)
}

// RegisterActivity indicates an expected call of RegisterActivity.
func (mr *MockWorkerMockRecorder) RegisterActivity(arg0 any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "RegisterActivity", reflect.TypeOf((*MockWorker)(nil).RegisterActivity), arg0)
}

// RegisterActivityWithOptions mocks base method.
func (m *MockWorker) RegisterActivityWithOptions(arg0 any, arg1 activity.RegisterOptions) {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "RegisterActivityWithOptions", arg0, arg1)
}

// RegisterActivityWithOptions indicates an expected call of RegisterActivityWithOptions.
func (mr *MockWorkerMockRecorder) RegisterActivityWithOptions(arg0, arg1 any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "RegisterActivityWithOptions", reflect.TypeOf((*MockWorker)(nil).RegisterActivityWithOptions), arg0, arg1)
}

// RegisterNexusService mocks base method.
func (m *MockWorker) RegisterNexusService(arg0 *nexus.Service) {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "RegisterNexusService", arg0)
}

// RegisterNexusService indicates an expected call of RegisterNexusService.
func (mr *MockWorkerMockRecorder) RegisterNexusService(arg0 any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "RegisterNexusService", reflect.TypeOf((*MockWorker)(nil).RegisterNexusService), arg0)
}

// RegisterWorkflow mocks base method.
func (m *MockWorker) RegisterWorkflow(arg0 any) {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "RegisterWorkflow", arg0)
}

// RegisterWorkflow indicates an expected call of RegisterWorkflow.
func (mr *MockWorkerMockRecorder) RegisterWorkflow(arg0 any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "RegisterWorkflow", reflect.TypeOf((*MockWorker)(nil).RegisterWorkflow), arg0)
}

// RegisterWorkflowWithOptions mocks base method.
func (m *MockWorker) RegisterWorkflowWithOptions(arg0 any, arg1 workflow.RegisterOptions) {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "RegisterWorkflowWithOptions", arg0, arg1)
}

// RegisterWorkflowWithOptions indicates an expected call of RegisterWorkflowWithOptions.
func (mr *MockWorkerMockRecorder) RegisterWorkflowWithOptions(arg0, arg1 any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "RegisterWorkflowWithOptions", reflect.TypeOf((*MockWorker)(nil).RegisterWorkflowWithOptions), arg0, arg1)
}

// Run mocks base method.
func (m *MockWorker) Run(arg0 <-chan any) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Run", arg0)
	ret0, _ := ret[0].(error)
	return ret0
}

// Run indicates an expected call of Run.
func (mr *MockWorkerMockRecorder) Run(arg0 any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Run", reflect.TypeOf((*MockWorker)(nil).Run), arg0)
}

// Start mocks base method.
func (m *MockWorker) Start() error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Start")
	ret0, _ := ret[0].(error)
	return ret0
}

// Start indicates an expected call of Start.
func (mr *MockWorkerMockRecorder) Start() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Start", reflect.TypeOf((*MockWorker)(nil).Start))
}

// Stop mocks base method.
func (m *MockWorker) Stop() {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "Stop")
}

// Stop indicates an expected call of Stop.
func (mr *MockWorkerMockRecorder) Stop() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Stop", reflect.TypeOf((*MockWorker)(nil).Stop))
}
