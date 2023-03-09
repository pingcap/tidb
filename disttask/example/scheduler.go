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

package example

import (
	"context"
	"encoding/json"
	"sync/atomic"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/disttask/framework/proto"
	"github.com/pingcap/tidb/disttask/framework/scheduler"
	"github.com/pingcap/tidb/util/logutil"
)

var globalNumberCounter atomic.Int64

// StepOneScheduler is a scheduler for step one.
type StepOneScheduler struct {
	task *proto.Task
}

// StepTwoScheduler is a scheduler for step two.
type StepTwoScheduler struct {
	task *proto.Task
}

// InitSubtaskExecEnv is used to initialize the environment for the subtask executor.
func (*StepOneScheduler) InitSubtaskExecEnv(context.Context) error {
	return nil
}

// CleanupSubtaskExecEnv is used to clean up the environment for the subtask executor.
func (*StepOneScheduler) CleanupSubtaskExecEnv(context.Context) error { return nil }

// SplitSubtask is used to split the subtask into multiple minimal tasks.
func (*StepOneScheduler) SplitSubtask(subtask *proto.Subtask) []proto.MinimalTask {
	var subtaskExample SubtaskExample
	_ = json.Unmarshal(subtask.Meta, &subtaskExample)
	miniTask := make([]proto.MinimalTask, 0, len(subtaskExample.Numbers))
	for _, number := range subtaskExample.Numbers {
		miniTask = append(miniTask, int64(number))
	}
	return miniTask
}

// Rollback is used to rollback all subtasks.
func (*StepOneScheduler) Rollback(context.Context) error {
	logutil.BgLogger().Info("rollback step one")
	return nil
}

// InitSubtaskExecEnv is used to initialize the environment for the subtask executor.
func (*StepTwoScheduler) InitSubtaskExecEnv(context.Context) error { return nil }

// CleanupSubtaskExecEnv is used to clean up the environment for the subtask executor.
func (*StepTwoScheduler) CleanupSubtaskExecEnv(context.Context) error { return nil }

// SplitSubtask is used to split the subtask into multiple minimal tasks.
func (*StepTwoScheduler) SplitSubtask(subtask *proto.Subtask) []proto.MinimalTask {
	var subtaskExample SubtaskExample
	_ = json.Unmarshal(subtask.Meta, &subtaskExample)
	miniTask := make([]proto.MinimalTask, 0, len(subtaskExample.Numbers))
	for _, number := range subtaskExample.Numbers {
		miniTask = append(miniTask, int64(number))
	}
	return miniTask
}

// Rollback is used to rollback all subtasks.
func (*StepTwoScheduler) Rollback(context.Context) error {
	logutil.BgLogger().Info("rollback step two")
	return nil
}

func init() {
	scheduler.RegisterSchedulerConstructor(
		proto.TaskTypeExample,
		// The order of the scheduler is the same as the order of the subtasks.
		func(task *proto.Task, step int64) (scheduler.Scheduler, error) {
			switch step {
			case StepOne:
				return &StepOneScheduler{task: task}, nil
			case StepTwo:
				return &StepTwoScheduler{task: task}, nil
			}
			return nil, errors.Errorf("unknown step %d", step)
		},
	)
}
