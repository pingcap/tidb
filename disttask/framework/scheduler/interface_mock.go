// Copyright 2023 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package scheduler

import (
	"context"

	"github.com/pingcap/tidb/disttask/framework/proto"
	"github.com/stretchr/testify/mock"
)

// MockTaskTable is a mock of TaskTable.
// TODO(gmhdbjd): move this to storage package.
type MockTaskTable struct {
	mock.Mock
}

// GetGlobalTasksInStates implements TaskTable.GetTasksInStates.
func (t *MockTaskTable) GetGlobalTasksInStates(states ...interface{}) ([]*proto.Task, error) {
	args := t.Called(states...)
	if args.Error(1) != nil {
		return nil, args.Error(1)
	} else if args.Get(0) == nil {
		return nil, nil
	} else {
		return args.Get(0).([]*proto.Task), nil
	}
}

// GetGlobalTaskByID implements TaskTable.GetTaskByID.
func (t *MockTaskTable) GetGlobalTaskByID(id int64) (*proto.Task, error) {
	args := t.Called(id)
	if args.Error(1) != nil {
		return nil, args.Error(1)
	} else if args.Get(0) == nil {
		return nil, nil
	} else {
		return args.Get(0).(*proto.Task), nil
	}
}

// GetSubtaskInStates implements SubtaskTable.GetSubtaskInStates.
func (t *MockTaskTable) GetSubtaskInStates(instanceID string, taskID int64, states ...interface{}) (*proto.Subtask, error) {
	args := t.Called(instanceID, taskID, states)
	if args.Error(1) != nil {
		return nil, args.Error(1)
	} else if args.Get(0) == nil {
		return nil, nil
	} else {
		return args.Get(0).(*proto.Subtask), nil
	}
}

// UpdateSubtaskState implements SubtaskTable.UpdateSubtaskState.
func (t *MockTaskTable) UpdateSubtaskState(id int64, state string) error {
	args := t.Called(id, state)
	return args.Error(0)
}

// HasSubtasksInStates implements SubtaskTable.HasSubtasksInStates.
func (t *MockTaskTable) HasSubtasksInStates(instanceID string, taskID int64, states ...interface{}) (bool, error) {
	args := t.Called(instanceID, taskID, states)
	return args.Bool(0), args.Error(1)
}

// MockPool is a mock of Pool.
type MockPool struct {
	mock.Mock
}

// NewMockPool creates a new mock pool.
func NewMockPool(int) Pool {
	return &MockPool{}
}

// Run implements Pool.Run.
func (m *MockPool) Run(f func()) error {
	args := m.Called()
	if args.Error(0) == nil {
		go f()
	}
	return args.Error(0)
}

// RunWithConcurrency implements Pool.RunWithConcurrency.
func (m *MockPool) RunWithConcurrency(funcs chan func(), _ uint32) error {
	args := m.Called()
	if args.Error(0) == nil {
		go func() {
			for f := range funcs {
				go f()
			}
		}()
	}
	return args.Error(0)
}

// ReleaseAndWait implements Pool.ReleaseAndWait.
func (m *MockPool) ReleaseAndWait() {
	m.Called()
}

// MockScheduler is a mock of Scheduler.
type MockScheduler struct {
	mock.Mock
}

// InitSubtaskExecEnv implements Scheduler.InitSubtaskExecEnv.
func (m *MockScheduler) InitSubtaskExecEnv(ctx context.Context) error {
	args := m.Called(ctx)
	return args.Error(0)
}

// SplitSubtask implements Scheduler.SplitSubtask.
func (m *MockScheduler) SplitSubtask(_ context.Context, subtask []byte) ([]proto.MinimalTask, error) {
	args := m.Called(subtask)
	if args.Error(1) != nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]proto.MinimalTask), nil
}

// CleanupSubtaskExecEnv implements Scheduler.CleanupSubtaskExecEnv.
func (m *MockScheduler) CleanupSubtaskExecEnv(ctx context.Context) error {
	args := m.Called(ctx)
	return args.Error(0)
}

// Rollback implements Scheduler.Rollback.
func (m *MockScheduler) Rollback(ctx context.Context) error {
	args := m.Called(ctx)
	return args.Error(0)
}

// MockInternalScheduler is a mock of InternalScheduler.
type MockInternalScheduler struct {
	mock.Mock
}

// Start implements InternalScheduler.Start.
func (m *MockInternalScheduler) Start() {
	m.Called()
}

// Stop implements InternalScheduler.Stop.
func (m *MockInternalScheduler) Stop() {
	m.Called()
}

// Run implements InternalScheduler.Run.
func (m *MockInternalScheduler) Run(ctx context.Context, task *proto.Task) error {
	args := m.Called(ctx, task)
	return args.Error(0)
}

// Rollback implements InternalScheduler.Rollback.
func (m *MockInternalScheduler) Rollback(ctx context.Context, task *proto.Task) error {
	args := m.Called(ctx, task)
	return args.Error(0)
}

// MockSubtaskExecutor is a mock of SubtaskExecutor.
type MockSubtaskExecutor struct {
	mock.Mock
}

// Run implements SubtaskExecutor.Run.
func (m *MockSubtaskExecutor) Run(ctx context.Context) error {
	args := m.Called(ctx)
	return args.Error(0)
}

// MockMinimalTask is a mock of MinimalTask.
type MockMinimalTask struct{}

// IsMinimalTask implements MinimalTask.IsMinimalTask.
func (MockMinimalTask) IsMinimalTask() {}
