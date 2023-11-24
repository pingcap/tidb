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

	"github.com/pingcap/tidb/pkg/disttask/framework/proto"
	"github.com/pingcap/tidb/pkg/disttask/framework/scheduler/execute"
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
	taskTypes              = make(map[proto.TaskType]taskTypeOptions)
	taskSchedulerFactories = make(map[proto.TaskType]schedulerFactoryFn)
)

type schedulerFactoryFn func(ctx context.Context, id string, task *proto.Task, taskTable TaskTable) Scheduler

// RegisterTaskType registers the task type.
func RegisterTaskType(taskType proto.TaskType, factory schedulerFactoryFn, opts ...TaskTypeOption) {
	var option taskTypeOptions
	for _, opt := range opts {
		opt(&option)
	}
	taskTypes[taskType] = option
	taskSchedulerFactories[taskType] = factory
}

func getSchedulerFactory(taskType proto.TaskType) schedulerFactoryFn {
	return taskSchedulerFactories[taskType]
}

// ClearSchedulers is only used in test
func ClearSchedulers() {
	taskTypes = make(map[proto.TaskType]taskTypeOptions)
	taskSchedulerFactories = make(map[proto.TaskType]schedulerFactoryFn)
}

// WithSummary is the option of Scheduler to set the summary.
var WithSummary TaskTypeOption = func(opts *taskTypeOptions) {
	opts.Summary = execute.NewSummary()
}
