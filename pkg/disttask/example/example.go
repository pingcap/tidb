// Copyright 2024 PingCAP, Inc.
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

	"github.com/pingcap/tidb/pkg/disttask/framework/proto"
	"github.com/pingcap/tidb/pkg/disttask/framework/scheduler"
	"github.com/pingcap/tidb/pkg/disttask/framework/taskexecutor"
	"github.com/pingcap/tidb/pkg/disttask/framework/taskexecutor/execute"
)

var (
	_ scheduler.Extension      = (*exampleSchedulerExtension)(nil)
	_ scheduler.CleanUpRoutine = (*exampleCleanUp)(nil)

	_ execute.SubtaskExecutor = (*exampleSubtaskExecutor)(nil)
	_ taskexecutor.Extension  = (*exampleSubtaskExtension)(nil)
)

type exampleSchedulerExtension struct {
}

func (*exampleSchedulerExtension) OnTick(ctx context.Context, task *proto.Task) {}

func (*exampleSchedulerExtension) OnNextSubtasksBatch(ctx context.Context,
	h scheduler.TaskHandle,
	task *proto.Task,
	execIDs []string, step proto.Step) (subtaskMetas [][]byte, err error) {
	if task.Step == proto.StepInit {
		return [][]byte{
			[]byte("task1"),
			[]byte("task2"),
			[]byte("task3"),
		}, nil
	}
	if task.Step == proto.StepOne {
		return [][]byte{
			[]byte("task4"),
		}, nil
	}
	return nil, nil
}

func (*exampleSchedulerExtension) OnDone(ctx context.Context, h scheduler.TaskHandle, task *proto.Task) error {
	return nil
}

func (*exampleSchedulerExtension) GetEligibleInstances(ctx context.Context, task *proto.Task) ([]string, error) {
	return nil, nil
}

func (*exampleSchedulerExtension) IsRetryableErr(err error) bool {
	return false
}

func (*exampleSchedulerExtension) GetNextStep(task *proto.Task) proto.Step {
	switch task.Step {
	case proto.StepInit:
		return proto.StepOne
	case proto.StepOne:
		return proto.StepTwo
	default:
		return proto.StepDone
	}
}

type exampleSubtaskExecutor struct {
}

func (*exampleSubtaskExecutor) Init(context.Context) error {
	return nil
}

func (*exampleSubtaskExecutor) RunSubtask(ctx context.Context, subtask *proto.Subtask) error {
	return nil
}

func (*exampleSubtaskExecutor) Cleanup(context.Context) error {
	return nil
}

func (*exampleSubtaskExecutor) OnFinished(ctx context.Context, subtask *proto.Subtask) error {
	return nil
}

func (*exampleSubtaskExecutor) Rollback(context.Context) error {
	return nil
}

type exampleSubtaskExtension struct {
}

func (*exampleSubtaskExtension) IsIdempotent(subtask *proto.Subtask) bool {
	return false
}

func (*exampleSubtaskExtension) GetSubtaskExecutor(ctx context.Context, task *proto.Task, summary *execute.Summary) (execute.SubtaskExecutor, error) {
	return &exampleSubtaskExecutor{}, nil
}

func (*exampleSubtaskExtension) IsRetryableError(err error) bool {
	return false
}

type exampleCleanUp struct{}

// CleanUp implements the CleanUpRoutine.CleanUp interface.
func (*exampleCleanUp) CleanUp(ctx context.Context, task *proto.Task) error {

	return nil
}

func registerExample() {
	schedulerExt := &exampleSchedulerExtension{}
	subtaskExecutorExt := &exampleSubtaskExtension{}
	cleanUp := &exampleCleanUp{}

	scheduler.RegisterSchedulerFactory(proto.TaskTypeExample,
		func(ctx context.Context, taskMgr scheduler.TaskManager, nodeMgr *scheduler.NodeManager, task *proto.Task) scheduler.Scheduler {
			baseScheduler := scheduler.NewBaseScheduler(ctx, taskMgr, nodeMgr, task)
			baseScheduler.Extension = schedulerExt
			return baseScheduler
		})

	taskexecutor.RegisterTaskType(proto.TaskTypeExample,
		func(ctx context.Context, id string, task *proto.Task, taskTable taskexecutor.TaskTable) taskexecutor.TaskExecutor {
			s := taskexecutor.NewBaseTaskExecutor(ctx, id, task.ID, taskTable)
			s.Extension = subtaskExecutorExt
			return s
		},
	)
	// optional.
	scheduler.RegisterSchedulerCleanUpFactory(proto.TaskTypeExample,
		func() scheduler.CleanUpRoutine {
			return cleanUp
		})
}
