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
	"path"
	"strconv"

	"github.com/pingcap/tidb/disttask/framework/proto"
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

type schedulerRegisterOptions struct {
}

// Constructor is the constructor of Scheduler.
type Constructor func(context context.Context, taskID int64, taskMeta []byte, step int64) (Scheduler, error)

// RegisterOption is the register option of Scheduler.
type RegisterOption func(opts *schedulerRegisterOptions)

// SubtaskExecutorConstructor is the constructor of SubtaskExecutor.
type SubtaskExecutorConstructor func(minimalTask proto.MinimalTask, step int64) (SubtaskExecutor, error)

type subtaskExecutorRegisterOptions struct {
}

// SubtaskExecutorRegisterOption is the register option of SubtaskExecutor.
type SubtaskExecutorRegisterOption func(opts *subtaskExecutorRegisterOptions)

var (
	// key is task type
	taskTypes = make(map[string]taskTypeOptions)

	// key is task type + step
	schedulerConstructors = make(map[string]Constructor)
	schedulerOptions      = make(map[string]schedulerRegisterOptions)

	// key is task type + step
	subtaskExecutorConstructors = make(map[string]SubtaskExecutorConstructor)
	subtaskExecutorOptions      = make(map[string]subtaskExecutorRegisterOptions)
)

// RegisterTaskType registers the task type.
func RegisterTaskType(taskType string, opts ...TaskTypeOption) {
	var option taskTypeOptions
	for _, opt := range opts {
		opt(&option)
	}
	taskTypes[taskType] = option
}

// RegisterSchedulerConstructor registers the constructor of Scheduler.
func RegisterSchedulerConstructor(taskType string, step int64, constructor Constructor, opts ...RegisterOption) {
	taskKey := getKey(taskType, step)
	schedulerConstructors[taskKey] = constructor

	var option schedulerRegisterOptions
	for _, opt := range opts {
		opt(&option)
	}
	schedulerOptions[taskKey] = option
}

// RegisterSubtaskExectorConstructor registers the constructor of SubtaskExecutor.
func RegisterSubtaskExectorConstructor(taskType string, step int64, constructor SubtaskExecutorConstructor, opts ...SubtaskExecutorRegisterOption) {
	taskKey := getKey(taskType, step)
	subtaskExecutorConstructors[taskKey] = constructor

	var option subtaskExecutorRegisterOptions
	for _, opt := range opts {
		opt(&option)
	}
	subtaskExecutorOptions[taskKey] = option
}

func getKey(taskType string, step int64) string {
	return path.Join(taskType, strconv.FormatInt(step, 10))
}

// ClearSchedulers is only used in test
func ClearSchedulers() {
	taskTypes = make(map[string]taskTypeOptions)
	schedulerConstructors = make(map[string]Constructor)
	schedulerOptions = make(map[string]schedulerRegisterOptions)
	subtaskExecutorConstructors = make(map[string]SubtaskExecutorConstructor)
	subtaskExecutorOptions = make(map[string]subtaskExecutorRegisterOptions)
}
