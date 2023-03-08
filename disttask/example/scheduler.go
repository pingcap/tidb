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

// ExampleStepOneScheduler is a scheduler for step one.
type ExampleStepOneScheduler struct {
	task *proto.Task
}

// ExampleStepTwoScheduler is a scheduler for step two.
type ExampleStepTwoScheduler struct {
	task *proto.Task
}

// InitSubtaskExecEnv is used to initialize the environment for the subtask executor.
func (s *ExampleStepOneScheduler) InitSubtaskExecEnv(ctx context.Context) error {
	return nil
}

// CleanupSubtaskExecEnv is used to clean up the environment for the subtask executor.
func (s *ExampleStepOneScheduler) CleanupSubtaskExecEnv(ctx context.Context) error { return nil }

// SplitSubtask is used to split the subtask into multiple minimal tasks.
func (s *ExampleStepOneScheduler) SplitSubtask(subtask *proto.Subtask) []proto.MinimalTask {
	var subtaskExample SubtaskExample
	_ = json.Unmarshal(subtask.Meta, &subtaskExample)
	miniTask := make([]proto.MinimalTask, 0, len(subtaskExample.Numbers))
	for _, number := range subtaskExample.Numbers {
		miniTask = append(miniTask, int64(number))
	}
	return miniTask
}

// Rollback is used to rollback all subtasks.
func (s *ExampleStepOneScheduler) Rollback(ctx context.Context) error {
	logutil.BgLogger().Info("rollback step one")
	return nil
}

// InitSubtaskExecEnv is used to initialize the environment for the subtask executor.
func (s *ExampleStepTwoScheduler) InitSubtaskExecEnv(ctx context.Context) error { return nil }

// CleanupSubtaskExecEnv is used to clean up the environment for the subtask executor.
func (s *ExampleStepTwoScheduler) CleanupSubtaskExecEnv(ctx context.Context) error { return nil }

// SplitSubtask is used to split the subtask into multiple minimal tasks.
func (s *ExampleStepTwoScheduler) SplitSubtask(subtask *proto.Subtask) []proto.MinimalTask {
	var subtaskExample SubtaskExample
	_ = json.Unmarshal(subtask.Meta, &subtaskExample)
	miniTask := make([]proto.MinimalTask, 0, len(subtaskExample.Numbers))
	for _, number := range subtaskExample.Numbers {
		miniTask = append(miniTask, int64(number))
	}
	return miniTask
}

// Rollback is used to rollback all subtasks.
func (s *ExampleStepTwoScheduler) Rollback(ctx context.Context) error {
	logutil.BgLogger().Info("rollback step two")
	return nil
}

func init() {
	scheduler.RegisterSchedulerConstructor(
		TaskTypeExample,
		// The order of the scheduler is the same as the order of the subtasks.
		func(task *proto.Task, step int64) (scheduler.Scheduler, error) {
			switch step {
			case StepOne:
				return &ExampleStepOneScheduler{task: task}, nil
			case StepTwo:
				return &ExampleStepTwoScheduler{task: task}, nil
			}
			return nil, errors.Errorf("unknown step %d", step)
		},
	)
}
