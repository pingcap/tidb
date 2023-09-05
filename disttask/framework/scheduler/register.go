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
)

type taskTypeOptions struct {
	PoolSize int32
}

// TaskTypeOption is the option of TaskType.
type TaskTypeOption func(opts *taskTypeOptions)

// WithPoolSize is the option of TaskType to set the pool size.
func WithPoolSize(poolSize int32) TaskTypeOption {
	return func(opts *taskTypeOptions) {
		opts.PoolSize = poolSize
	}
}

var (
	// key is task type
	taskTypes              = make(map[string]taskTypeOptions)
	taskSchedulerFactories = make(map[string]schedulerFactoryFn)
)

type schedulerFactoryFn func(ctx context.Context, id string, taskID int64, taskTable TaskTable, pool Pool) Scheduler

// RegisterTaskType registers the task type.
func RegisterTaskType(taskType string, factory schedulerFactoryFn, opts ...TaskTypeOption) {
	var option taskTypeOptions
	for _, opt := range opts {
		opt(&option)
	}
	taskTypes[taskType] = option
	taskSchedulerFactories[taskType] = factory
}

func getSchedulerFactory(taskType string) schedulerFactoryFn {
	return taskSchedulerFactories[taskType]
}

// ClearSchedulers is only used in test
func ClearSchedulers() {
	taskTypes = make(map[string]taskTypeOptions)
	taskSchedulerFactories = make(map[string]schedulerFactoryFn)
}
