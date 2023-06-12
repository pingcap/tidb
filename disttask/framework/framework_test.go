// Copyright 2023 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package framework_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pingcap/tidb/disttask/framework/dispatcher"
	"github.com/pingcap/tidb/disttask/framework/proto"
	"github.com/pingcap/tidb/disttask/framework/scheduler"
	"github.com/pingcap/tidb/disttask/framework/storage"
	"github.com/pingcap/tidb/domain/infosync"
	"github.com/pingcap/tidb/testkit"
	"github.com/stretchr/testify/require"
)

type testFlowHandle struct{}

var _ dispatcher.TaskFlowHandle = (*testFlowHandle)(nil)

func (*testFlowHandle) OnTicker(_ context.Context, _ *proto.Task) {
}

func (*testFlowHandle) ProcessNormalFlow(_ context.Context, _ dispatcher.TaskHandle, gTask *proto.Task) (metas [][]byte, err error) {
	if gTask.State == proto.TaskStatePending {
		gTask.Step = proto.StepOne
		return [][]byte{
			[]byte("task1"),
			[]byte("task2"),
		}, nil
	}
	return nil, nil
}

func (*testFlowHandle) ProcessErrFlow(_ context.Context, _ dispatcher.TaskHandle, _ *proto.Task, _ [][]byte) (meta []byte, err error) {
	return nil, nil
}

func (*testFlowHandle) GetEligibleInstances(ctx context.Context, _ *proto.Task) ([]*infosync.ServerInfo, error) {
	return dispatcher.GenerateSchedulerNodes(ctx)
}

func (*testFlowHandle) IsRetryableErr(error) bool {
	return true
}

type testMiniTask struct{}

func (testMiniTask) IsMinimalTask() {}

type testScheduler struct{}

func (*testScheduler) InitSubtaskExecEnv(_ context.Context) error { return nil }

func (t *testScheduler) CleanupSubtaskExecEnv(_ context.Context) error { return nil }

func (t *testScheduler) Rollback(_ context.Context) error { return nil }

func (t *testScheduler) SplitSubtask(_ context.Context, subtask []byte) ([]proto.MinimalTask, error) {
	return []proto.MinimalTask{
		testMiniTask{},
		testMiniTask{},
		testMiniTask{},
	}, nil
}

func (t *testScheduler) OnSubtaskFinished(_ context.Context, meta []byte) ([]byte, error) {
	return meta, nil
}

type testSubtaskExecutor struct {
	v *atomic.Int64
}

func (e *testSubtaskExecutor) Run(_ context.Context) error {
	e.v.Add(1)
	return nil
}

func TestFrameworkStartUp(t *testing.T) {
	defer dispatcher.ClearTaskFlowHandle()
	defer scheduler.ClearSchedulers()

	var v atomic.Int64
	dispatcher.ClearTaskFlowHandle()
	dispatcher.RegisterTaskFlowHandle(proto.TaskTypeExample, &testFlowHandle{})
	scheduler.ClearSchedulers()
	scheduler.RegisterTaskType(proto.TaskTypeExample)
	scheduler.RegisterSchedulerConstructor(proto.TaskTypeExample, proto.StepOne, func(_ int64, _ []byte, _ int64) (scheduler.Scheduler, error) {
		return &testScheduler{}, nil
	})
	scheduler.RegisterSubtaskExectorConstructor(proto.TaskTypeExample, proto.StepOne, func(_ proto.MinimalTask, _ int64) (scheduler.SubtaskExecutor, error) {
		return &testSubtaskExecutor{v: &v}, nil
	})

	_ = testkit.CreateMockStore(t)
	mgr, err := storage.GetTaskManager()
	require.NoError(t, err)
	taskID, err := mgr.AddNewGlobalTask("key1", proto.TaskTypeExample, 8, nil)
	require.NoError(t, err)
	start := time.Now()

	var task *proto.Task
	for {
		if time.Since(start) > 2*time.Minute {
			require.FailNow(t, "timeout")
		}

		time.Sleep(time.Second)
		task, err = mgr.GetGlobalTaskByID(taskID)
		require.NoError(t, err)
		require.NotNil(t, task)
		if task.State != proto.TaskStatePending && task.State != proto.TaskStateRunning {
			break
		}
	}

	require.Equal(t, proto.TaskStateSucceed, task.State)
	require.Equal(t, int64(6), v.Load())
}
