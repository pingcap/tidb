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

type ExampleStepOneScheduler struct {
	task *proto.Task
}

type ExampleStepTwoScheduler struct {
	task *proto.Task
}

func (s *ExampleStepOneScheduler) InitSubtaskExecEnv(ctx context.Context) error {
	return nil
}

func (s *ExampleStepOneScheduler) CleanupSubtaskExecEnv(ctx context.Context) error { return nil }

func (s *ExampleStepOneScheduler) SplitSubtask(subtask *proto.Subtask) []proto.MinimalTask {
	var subtaskExample SubtaskExample
	_ = json.Unmarshal(subtask.Meta, &subtaskExample)
	miniTask := make([]proto.MinimalTask, 0, len(subtaskExample.Numbers))
	for _, number := range subtaskExample.Numbers {
		miniTask = append(miniTask, int64(number))
	}
	return miniTask
}

func (s *ExampleStepOneScheduler) Rollback(ctx context.Context) error {
	logutil.BgLogger().Info("rollback step one")
	return nil
}

func (s *ExampleStepTwoScheduler) InitSubtaskExecEnv(ctx context.Context) error { return nil }

func (s *ExampleStepTwoScheduler) CleanupSubtaskExecEnv(ctx context.Context) error { return nil }

func (s *ExampleStepTwoScheduler) SplitSubtask(subtask *proto.Subtask) []proto.MinimalTask {
	var subtaskExample SubtaskExample
	_ = json.Unmarshal(subtask.Meta, &subtaskExample)
	miniTask := make([]proto.MinimalTask, 0, len(subtaskExample.Numbers))
	for _, number := range subtaskExample.Numbers {
		miniTask = append(miniTask, int64(number))
	}
	return miniTask
}

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
