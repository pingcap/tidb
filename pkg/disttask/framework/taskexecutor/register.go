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

	"github.com/pingcap/errors"
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
)

type taskExecutorFactoryFn func(ctx context.Context, id string, task *proto.Task, taskTable execute.TaskTable) execute.TaskExecutor

// RegisterTaskType registers the task type.
func RegisterTaskType(taskType proto.TaskType, factory taskExecutorFactoryFn, opts ...TaskTypeOption) {
	var option taskTypeOptions
	for _, opt := range opts {
		opt(&option)
	}
	taskTypes[taskType] = option
	taskExecutorFactories[taskType] = factory
}

// GetTaskExecutorFactory gets taskExecutorFactory by task type.
func GetTaskExecutorFactory(taskType proto.TaskType) taskExecutorFactoryFn {
	return taskExecutorFactories[taskType]
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

func getSummary(
	ctx context.Context,
	task *proto.Task,
	taskTable execute.TaskTable,
) (summary *execute.Summary, err error) {
	opt, ok := taskTypes[task.Type]
	if !ok {
		return nil, errors.Errorf("task executor option for type %s not found", task.Type)
	}
	if opt.Summary != nil {
		opt.Summary.TaskTable = taskTable
		return opt.Summary, nil
	}
	return nil, nil
}
