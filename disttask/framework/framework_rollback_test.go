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

package framework_test

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/disttask/framework/dispatcher"
	"github.com/pingcap/tidb/disttask/framework/mock"
	"github.com/pingcap/tidb/disttask/framework/proto"
	"github.com/pingcap/tidb/domain/infosync"
	"github.com/pingcap/tidb/testkit"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

type rollbackDispatcherExt struct {
	cnt int
}

var _ dispatcher.Extension = (*rollbackDispatcherExt)(nil)
var rollbackCnt atomic.Int32

func (*rollbackDispatcherExt) OnTick(_ context.Context, _ *proto.Task) {
}

func (dsp *rollbackDispatcherExt) OnNextSubtasksBatch(_ context.Context, _ dispatcher.TaskHandle, gTask *proto.Task) (metas [][]byte, err error) {
	if gTask.Step == proto.StepInit {
		dsp.cnt = 3
		return [][]byte{
			[]byte("task1"),
			[]byte("task2"),
			[]byte("task3"),
		}, nil
	}
	return nil, nil
}

func (*rollbackDispatcherExt) OnErrStage(_ context.Context, _ dispatcher.TaskHandle, _ *proto.Task, _ []error) (meta []byte, err error) {
	return []byte("rollbacktask1"), nil
}

func (*rollbackDispatcherExt) GetEligibleInstances(_ context.Context, _ *proto.Task) ([]*infosync.ServerInfo, error) {
	return generateSchedulerNodes4Test()
}

func (*rollbackDispatcherExt) IsRetryableErr(error) bool {
	return true
}

func (dsp *rollbackDispatcherExt) StageFinished(task *proto.Task) bool {
	return task.Step == proto.StepInit && dsp.cnt >= 3
}

func (dsp *rollbackDispatcherExt) Finished(task *proto.Task) bool {
	return task.Step == proto.StepInit && dsp.cnt >= 3
}

type testRollbackMiniTask struct{}

func (testRollbackMiniTask) IsMinimalTask() {}

func (testRollbackMiniTask) String() string {
	return ""
}

type rollbackScheduler struct {
	m *sync.Map
}

func (*rollbackScheduler) Init(_ context.Context) error { return nil }

func (t *rollbackScheduler) Cleanup(_ context.Context) error { return nil }

func (t *rollbackScheduler) Rollback(_ context.Context) error {
	t.m = &sync.Map{}
	rollbackCnt.Add(1)
	return nil
}

func (t *rollbackScheduler) SplitSubtask(_ context.Context, _ *proto.Subtask) ([]proto.MinimalTask, error) {
	return []proto.MinimalTask{
		testRollbackMiniTask{},
		testRollbackMiniTask{},
		testRollbackMiniTask{},
	}, nil
}

func (t *rollbackScheduler) OnFinished(_ context.Context, _ *proto.Subtask) error {
	return nil
}

type rollbackSubtaskExecutor struct {
	m *sync.Map
}

func (e *rollbackSubtaskExecutor) Run(_ context.Context) error {
	e.m.Store("1", "1")
	return nil
}

func registerRollbackTaskMeta(t *testing.T, ctrl *gomock.Controller, m *sync.Map) {
	mockExtension := mock.NewMockExtension(ctrl)
	mockExtension.EXPECT().GetSubtaskExecutor(gomock.Any(), gomock.Any(), gomock.Any()).Return(&rollbackScheduler{m: m}, nil).AnyTimes()
	mockExtension.EXPECT().GetMiniTaskExecutor(gomock.Any(), gomock.Any(), gomock.Any()).Return(&rollbackSubtaskExecutor{m: m}, nil).AnyTimes()
	registerTaskMetaInner(t, mockExtension, &rollbackDispatcherExt{})
	rollbackCnt.Store(0)
}

func TestFrameworkRollback(t *testing.T) {
	m := sync.Map{}

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	registerRollbackTaskMeta(t, ctrl, &m)
	distContext := testkit.NewDistExecutionContext(t, 2)
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/disttask/framework/dispatcher/cancelTaskAfterRefreshTask", "2*return(true)"))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/disttask/framework/dispatcher/cancelTaskAfterRefreshTask"))
	}()

	DispatchTaskAndCheckState("key1", t, &m, proto.TaskStateReverted)
	require.Equal(t, int32(2), rollbackCnt.Load())
	rollbackCnt.Store(0)
	distContext.Close()
}
