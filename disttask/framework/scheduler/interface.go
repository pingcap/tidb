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
	GetSubtaskInStates(instanceID string, taskID int64, states ...interface{}) (*proto.Subtask, error)
	UpdateSubtaskStateAndError(id int64, state string, error string) error
	HasSubtasksInStates(instanceID string, taskID int64, states ...interface{}) (bool, error)
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
	InitSubtaskExecEnv(context.Context) error
	SplitSubtask(subtask []byte) ([]proto.MinimalTask, error)
	CleanupSubtaskExecEnv(context.Context) error
	Rollback(context.Context) error
}

// SubtaskExecutor defines the interface of a subtask executor.
// User should implement this interface to define their own subtask executor.
type SubtaskExecutor interface {
	Run(ctx context.Context) error
}
