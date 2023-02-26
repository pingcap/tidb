// Copyright 2022 PingCAP, Inc.
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

import "context"

type SubtaskExecutor interface {
	Run(ctx context.Context) error
}

type schedulerRegisterOptions struct{}

type SchedulerConstructor func(task *Task) Scheduler

type SchedulerRegisterOption func(opts *schedulerRegisterOptions)

type SubtaskExecutorConstructor func(subtask *Subtask) SubtaskExecutor

type subtaskExecutorRegisterOptions struct{}

type SubtaskExecutorRegisterOption func(opts *subtaskExecutorRegisterOptions)

var (
	schedulerConstructors = make(map[TaskType]SchedulerConstructor)
	schedulerOptions      = make(map[TaskType]schedulerRegisterOptions)

	subtaskExecutorConstructors = make(map[TaskType]SubtaskExecutorConstructor)
	subtaskExecutorOptions      = make(map[TaskType]subtaskExecutorRegisterOptions)
)

func RegisterSchedulerConstructor(taskType TaskType, constructor SchedulerConstructor, opts ...SchedulerRegisterOption) {
	schedulerConstructors[taskType] = constructor

	var option schedulerRegisterOptions
	for _, opt := range opts {
		opt(&option)
	}
	schedulerOptions[taskType] = option
}

func RegisterSubtaskExectorConstructor(taskType TaskType, constructor SubtaskExecutorConstructor, opts ...SubtaskExecutorRegisterOption) {
	subtaskExecutorConstructors[taskType] = constructor

	var option subtaskExecutorRegisterOptions
	for _, opt := range opts {
		opt(&option)
	}
	subtaskExecutorOptions[taskType] = option
}
