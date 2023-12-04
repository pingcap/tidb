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

package taskexecutor

import (
	"context"

	"github.com/pingcap/tidb/pkg/disttask/framework/proto"
	"github.com/pingcap/tidb/pkg/disttask/framework/taskexecutor/execute"
)

type taskTypeOptions struct {
	// Summary is the summary of all tasks of the task type.
	// TODO: better have a summary per task/subtask.
	Summary *execute.Summary
}

// TaskTypeOption is the option of TaskType.
type TaskTypeOption func(opts *taskTypeOptions)

var (
	// key is task type
	taskTypes             = make(map[proto.TaskType]taskTypeOptions)
	taskExecutorFactories = make(map[proto.TaskType]taskExecutorFactoryFn)
	taskHookFactories     = make(map[proto.TaskType]taskHookFactoryFn)
)

type taskExecutorFactoryFn func(ctx context.Context, id string, task *proto.Task, taskTable TaskTable) TaskExecutor
type taskHookFactoryFn func() Callback

// RegisterTaskType registers the task type.
func RegisterTaskType(taskType proto.TaskType, factory taskExecutorFactoryFn, opts ...TaskTypeOption) {
	var option taskTypeOptions
	for _, opt := range opts {
		opt(&option)
	}
	taskTypes[taskType] = option
	taskExecutorFactories[taskType] = factory
	// register default callback.
	taskHookFactories[taskType] = func() Callback {
		return &BaseCallback{}
	}
}

// GetTaskExecutorFactory gets taskExecutorFactory by task type.
func GetTaskExecutorFactory(taskType proto.TaskType) taskExecutorFactoryFn {
	return taskExecutorFactories[taskType]
}

// RegisterHook registers the task type's hook.
func RegisterHook(taskType proto.TaskType, factory taskHookFactoryFn) {
	taskHookFactories[taskType] = factory
}

// GetHookFactory gets taskHookFactoryFn by task type.
func GetHookFactory(taskType proto.TaskType) taskHookFactoryFn {
	return taskHookFactories[taskType]
}

// ClearTaskExecutors is only used in test
func ClearTaskExecutors() {
	taskTypes = make(map[proto.TaskType]taskTypeOptions)
	taskExecutorFactories = make(map[proto.TaskType]taskExecutorFactoryFn)
}

// WithSummary is the option of TaskExecutor to set the summary.
var WithSummary TaskTypeOption = func(opts *taskTypeOptions) {
	opts.Summary = execute.NewSummary()
}
