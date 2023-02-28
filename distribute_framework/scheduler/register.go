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

import (
	"context"

	"github.com/pingcap/tidb/distribute_framework/proto"
)

type SubtaskExecutor interface {
	Run(ctx context.Context) error
}

type schedulerRegisterOptions struct{}

type SchedulerConstructor func(task *proto.Task, step proto.TaskStep) (Scheduler, error)

type SchedulerRegisterOption func(opts *schedulerRegisterOptions)

type SubtaskExecutorConstructor func(subtask *proto.Subtask, step proto.TaskStep) (SubtaskExecutor, error)

type subtaskExecutorRegisterOptions struct{}

type SubtaskExecutorRegisterOption func(opts *subtaskExecutorRegisterOptions)

var (
	schedulerConstructors = make(map[proto.TaskType]SchedulerConstructor)
	schedulerOptions      = make(map[proto.TaskType]schedulerRegisterOptions)

	subtaskExecutorConstructors = make(map[proto.TaskType]SubtaskExecutorConstructor)
	subtaskExecutorOptions      = make(map[proto.TaskType]subtaskExecutorRegisterOptions)
)

func RegisterSchedulerConstructor(taskType proto.TaskType, constructor SchedulerConstructor, opts ...SchedulerRegisterOption) {
	schedulerConstructors[taskType] = constructor

	var option schedulerRegisterOptions
	for _, opt := range opts {
		opt(&option)
	}
	schedulerOptions[taskType] = option
}

func RegisterSubtaskExectorConstructor(taskType proto.TaskType, constructor SubtaskExecutorConstructor, opts ...SubtaskExecutorRegisterOption) {
	subtaskExecutorConstructors[taskType] = constructor

	var option subtaskExecutorRegisterOptions
	for _, opt := range opts {
		opt(&option)
	}
	subtaskExecutorOptions[taskType] = option
}
