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
)

// TaskTable defines the interface to access task table.
type TaskTable interface {
	GetGlobalTasksInStates(states ...interface{}) (task []*proto.Task, err error)
	GetGlobalTaskByID(taskID int64) (task *proto.Task, err error)

	GetSubtaskInStates(instanceID string, taskID int64, step int64, states ...interface{}) (*proto.Subtask, error)
	StartSubtask(id int64) error
	UpdateSubtaskStateAndError(id int64, state string, err error) error
	FinishSubtask(id int64, meta []byte) error
	HasSubtasksInStates(instanceID string, taskID int64, step int64, states ...interface{}) (bool, error)
	UpdateErrorToSubtask(tidbID string, err error) error
}

// Pool defines the interface of a pool.
type Pool interface {
	Run(func()) error
	RunWithConcurrency(chan func(), uint32) error
	ReleaseAndWait()
}

// InternalScheduler defines the interface of an internal scheduler.
type InternalScheduler interface {
	Start()
	Stop()
	Run(context.Context, *proto.Task) error
	Rollback(context.Context, *proto.Task) error
}

// Scheduler defines the interface of a scheduler.
// User should implement this interface to define their own scheduler.
type Scheduler interface {
	// InitSubtaskExecEnv is used to initialize the environment for the subtask executor.
	InitSubtaskExecEnv(context.Context) error
	// SplitSubtask is used to split the subtask into multiple minimal tasks.
	SplitSubtask(ctx context.Context, subtask []byte) ([]proto.MinimalTask, error)
	// CleanupSubtaskExecEnv is used to clean up the environment for the subtask executor.
	CleanupSubtaskExecEnv(context.Context) error
	// OnSubtaskFinished is used to handle the subtask when it is finished.
	// return the result of the subtask.
	// MUST return subtask meta back on success.
	OnSubtaskFinished(ctx context.Context, subtask []byte) ([]byte, error)
	// Rollback is used to rollback all subtasks.
	Rollback(context.Context) error
}

// SubtaskExecutor defines the interface of a subtask executor.
// User should implement this interface to define their own subtask executor.
// TODO: Rename to minimal task executor.
type SubtaskExecutor interface {
	Run(ctx context.Context) error
}

// EmptyScheduler is an empty scheduler.
// it can be used for the task that does not need to split into subtasks.
type EmptyScheduler struct {
}

var _ Scheduler = &EmptyScheduler{}

// InitSubtaskExecEnv implements the Scheduler interface.
func (*EmptyScheduler) InitSubtaskExecEnv(context.Context) error {
	return nil
}

// SplitSubtask implements the Scheduler interface.
func (*EmptyScheduler) SplitSubtask(context.Context, []byte) ([]proto.MinimalTask, error) {
	return nil, nil
}

// CleanupSubtaskExecEnv implements the Scheduler interface.
func (*EmptyScheduler) CleanupSubtaskExecEnv(context.Context) error {
	return nil
}

// OnSubtaskFinished implements the Scheduler interface.
func (*EmptyScheduler) OnSubtaskFinished(_ context.Context, metaBytes []byte) ([]byte, error) {
	return metaBytes, nil
}

// Rollback implements the Scheduler interface.
func (*EmptyScheduler) Rollback(context.Context) error {
	return nil
}

// EmptyExecutor is an empty minimal task executor.
// it can be used for the task that does not need to split into minimal tasks.
type EmptyExecutor struct {
}

var _ SubtaskExecutor = &EmptyExecutor{}

// Run implements the SubtaskExecutor interface.
func (*EmptyExecutor) Run(context.Context) error {
	return nil
}
