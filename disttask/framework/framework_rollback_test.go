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

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/disttask/framework/dispatcher"
	"github.com/pingcap/tidb/disttask/framework/proto"
	"github.com/pingcap/tidb/disttask/framework/scheduler"
	"github.com/pingcap/tidb/domain/infosync"
	"github.com/pingcap/tidb/testkit"
	disttaskutil "github.com/pingcap/tidb/util/disttask"
	"github.com/pingcap/tidb/util/logutil"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

type rollbackFlowHandle struct{}

var _ dispatcher.TaskFlowHandle = (*rollbackFlowHandle)(nil)
var rollbackCnt atomic.Int32

func (*rollbackFlowHandle) OnTicker(_ context.Context, _ *proto.Task) {
}

func (*rollbackFlowHandle) ProcessNormalFlow(_ context.Context, _ dispatcher.TaskHandle, gTask *proto.Task) (metas [][]byte, err error) {
	if gTask.State == proto.TaskStatePending {
		gTask.Step = proto.StepOne
		return [][]byte{
			[]byte("task1"),
			[]byte("task2"),
			[]byte("task3"),
		}, nil
	}
	return nil, nil
}

func (*rollbackFlowHandle) ProcessErrFlow(_ context.Context, _ dispatcher.TaskHandle, _ *proto.Task, _ []error) (meta []byte, err error) {
	return []byte("rollbacktask1"), nil
}

func (*rollbackFlowHandle) GetEligibleInstances(_ context.Context, _ *proto.Task) ([]*infosync.ServerInfo, error) {
	return generateSchedulerNodes4Test()
}

func generateSubtasks4Test(ctx context.Context, gTask *proto.Task, serverNodes []*infosync.ServerInfo, subtaskMetas [][]byte) ([][]*proto.Subtask, error) {
	subTasks := make([][]*proto.Subtask, len(serverNodes))
	for i, meta := range subtaskMetas {
		// we assign the subtask to the instance in a round-robin way.
		pos := i % len(serverNodes)
		instanceID := disttaskutil.GenerateExecID(serverNodes[pos].IP, serverNodes[pos].Port)
		logutil.BgLogger().Debug("create subtasks",
			zap.Int("gTask.ID", int(gTask.ID)), zap.String("type", gTask.Type), zap.String("instanceID", instanceID))
		subTasks[pos] = append(subTasks[pos], proto.NewSubtask(gTask.ID, gTask.Type, instanceID, meta))
	}
	return subTasks, nil
}

func (*rollbackFlowHandle) GenerateSubtasks(ctx context.Context, gTask *proto.Task, serverNodes []*infosync.ServerInfo, subtaskMetas [][]byte) ([][]*proto.Subtask, error) {
	return generateSubtasks4Test(ctx, gTask, serverNodes, subtaskMetas)
}

func (*rollbackFlowHandle) IsRetryableErr(error) bool {
	return true
}

type testRollbackMiniTask struct{}

func (testRollbackMiniTask) IsMinimalTask() {}

func (testRollbackMiniTask) String() string {
	return ""
}

type rollbackScheduler struct {
	v *atomic.Int64
}

func (*rollbackScheduler) InitSubtaskExecEnv(_ context.Context) error { return nil }

func (t *rollbackScheduler) CleanupSubtaskExecEnv(_ context.Context) error { return nil }

func (t *rollbackScheduler) Rollback(_ context.Context) error {
	t.v.Store(0)
	rollbackCnt.Add(1)
	return nil
}

func (t *rollbackScheduler) SplitSubtask(_ context.Context, subtask []byte) ([]proto.MinimalTask, error) {
	return []proto.MinimalTask{
		testRollbackMiniTask{},
		testRollbackMiniTask{},
		testRollbackMiniTask{},
	}, nil
}

func (t *rollbackScheduler) OnSubtaskFinished(_ context.Context, meta []byte) ([]byte, error) {
	return meta, nil
}

type rollbackSubtaskExecutor struct {
	v *atomic.Int64
}

func (e *rollbackSubtaskExecutor) Run(_ context.Context) error {
	e.v.Add(1)
	return nil
}

func RegisterRollbackTaskMeta(v *atomic.Int64) {
	dispatcher.ClearTaskFlowHandle()
	dispatcher.RegisterTaskFlowHandle(proto.TaskTypeRollbackExample, &rollbackFlowHandle{})
	scheduler.ClearSchedulers()
	scheduler.RegisterTaskType(proto.TaskTypeRollbackExample)
	scheduler.RegisterSchedulerConstructor(proto.TaskTypeRollbackExample, proto.StepOne, func(_ int64, _ []byte, _ int64) (scheduler.Scheduler, error) {
		return &rollbackScheduler{v: v}, nil
	}, scheduler.WithConcurrentSubtask())
	scheduler.RegisterSubtaskExectorConstructor(proto.TaskTypeRollbackExample, proto.StepOne, func(_ proto.MinimalTask, _ int64) (scheduler.SubtaskExecutor, error) {
		return &rollbackSubtaskExecutor{v: v}, nil
	})
	rollbackCnt.Store(0)
}

func TestFrameworkRollback(t *testing.T) {
	defer dispatcher.ClearTaskFlowHandle()
	defer scheduler.ClearSchedulers()
	var v atomic.Int64
	RegisterRollbackTaskMeta(&v)
	distContext := testkit.NewDistExecutionContext(t, 2)
	// 1. cancel global task.
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/disttask/framework/dispatcher/cancelTaskBeforeProbe", "1*return(true)"))
	DispatchTaskAndCheckFail("key1", proto.TaskTypeRollbackExample, t, &v)
	require.Equal(t, int32(2), rollbackCnt.Load())
	rollbackCnt.Store(0)
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/disttask/framework/dispatcher/cancelTaskBeforeProbe"))

	// 2. processNormalFlowErr and not retryable.
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/disttask/framework/dispatcher/processNormalFlowErrNotRetryable", "1*return(true)"))
	DispatchTaskAndCheckFail("key2", proto.TaskTypeRollbackExample, t, &v)
	require.Equal(t, int32(0), rollbackCnt.Load())
	rollbackCnt.Store(0)
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/disttask/framework/dispatcher/processNormalFlowErrNotRetryable"))

	// 3. dispatch normal subtasks fail.
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/disttask/framework/dispatcher/dispatchSubTasksFail", "1*return(true)"))
	DispatchTaskAndCheckFail("key3", proto.TaskTypeRollbackExample, t, &v)
	require.Equal(t, int32(0), rollbackCnt.Load())
	rollbackCnt.Store(0)
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/disttask/framework/dispatcher/dispatchSubTasksFail"))

	// 4. insert revert subtasks fail.
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/disttask/framework/dispatcher/cancelTaskBeforeProbe", "1*return(true)"))
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/disttask/framework/dispatcher/insertSubtasksFail", "return(true)"))
	DispatchTaskAndCheckFail("key4", proto.TaskTypeRollbackExample, t, &v)
	require.Equal(t, int32(2), rollbackCnt.Load())
	rollbackCnt.Store(0)
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/disttask/framework/dispatcher/insertSubtasksFail"))
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/disttask/framework/dispatcher/cancelTaskBeforeProbe"))
	distContext.Close()
}
