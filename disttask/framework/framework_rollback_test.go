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
	"github.com/stretchr/testify/require"
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

func (t *rollbackScheduler) SplitSubtask(_ context.Context, _ []byte) ([]proto.MinimalTask, error) {
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
	dispatcher.RegisterTaskFlowHandle(proto.TaskTypeExample, &rollbackFlowHandle{})
	scheduler.ClearSchedulers()
	scheduler.RegisterTaskType(proto.TaskTypeExample)
	scheduler.RegisterSchedulerConstructor(proto.TaskTypeExample, proto.StepOne, func(_ int64, _ []byte, _ int64) (scheduler.Scheduler, error) {
		return &rollbackScheduler{v: v}, nil
	})
	scheduler.RegisterSubtaskExectorConstructor(proto.TaskTypeExample, proto.StepOne, func(_ proto.MinimalTask, _ int64) (scheduler.SubtaskExecutor, error) {
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
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/disttask/framework/dispatcher/cancelTaskAfterRefreshTask", "2*return(true)"))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/disttask/framework/dispatcher/cancelTaskAfterRefreshTask"))
	}()

	DispatchTaskAndCheckFail("key2", t, &v)
	require.Equal(t, int32(2), rollbackCnt.Load())
	rollbackCnt.Store(0)
	distContext.Close()
}
