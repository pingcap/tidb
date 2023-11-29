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
	"testing"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/disttask/framework/dispatcher"
	mockDispatch "github.com/pingcap/tidb/pkg/disttask/framework/dispatcher/mock"
	"github.com/pingcap/tidb/pkg/disttask/framework/proto"
	"github.com/pingcap/tidb/pkg/domain/infosync"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/util"
	"go.uber.org/mock/gomock"
)

func getMockHATestDispatcherExt(ctrl *gomock.Controller) dispatcher.Extension {
	mockDispatcher := mockDispatch.NewMockExtension(ctrl)
	mockDispatcher.EXPECT().OnTick(gomock.Any(), gomock.Any()).Return().AnyTimes()
	mockDispatcher.EXPECT().GetEligibleInstances(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, _ *proto.Task) ([]*infosync.ServerInfo, bool, error) {
			return generateSchedulerNodes4Test()
		},
	).AnyTimes()
	mockDispatcher.EXPECT().IsRetryableErr(gomock.Any()).Return(true).AnyTimes()
	mockDispatcher.EXPECT().GetNextStep(gomock.Any()).DoAndReturn(
		func(task *proto.Task) proto.Step {
			switch task.Step {
			case proto.StepInit:
				return proto.StepOne
			case proto.StepOne:
				return proto.StepTwo
			default:
				return proto.StepDone
			}
		},
	).AnyTimes()
	mockDispatcher.EXPECT().OnNextSubtasksBatch(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, _ dispatcher.TaskHandle, gTask *proto.Task, _ []*infosync.ServerInfo, _ proto.Step) (metas [][]byte, err error) {
			if gTask.Step == proto.StepInit {
				return [][]byte{
					[]byte("task1"),
					[]byte("task2"),
					[]byte("task3"),
					[]byte("task4"),
					[]byte("task5"),
					[]byte("task6"),
					[]byte("task7"),
					[]byte("task8"),
					[]byte("task9"),
					[]byte("task10"),
				}, nil
			}
			if gTask.Step == proto.StepOne {
				return [][]byte{
					[]byte("task11"),
					[]byte("task12"),
					[]byte("task13"),
					[]byte("task14"),
					[]byte("task15"),
				}, nil
			}
			return nil, nil
		},
	).AnyTimes()

	mockDispatcher.EXPECT().OnDone(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).AnyTimes()

	return mockDispatcher
}

func TestHABasic(t *testing.T) {
	var m sync.Map
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	ctx := context.Background()
	ctx = util.WithInternalSourceType(ctx, "dispatcher")

	RegisterTaskMeta(t, ctrl, &m, getMockHATestDispatcherExt(ctrl))
	distContext := testkit.NewDistExecutionContext(t, 4)
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/disttask/framework/scheduler/mockCleanScheduler", "return()"))
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/disttask/framework/scheduler/mockStopManager", "4*return()"))
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/disttask/framework/scheduler/mockTiDBDown", "return(\":4000\")"))
	DispatchTaskAndCheckSuccess(ctx, "😊", t, &m)
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/disttask/framework/scheduler/mockTiDBDown"))
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/disttask/framework/scheduler/mockStopManager"))
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/disttask/framework/scheduler/mockCleanScheduler"))
	distContext.Close()
}

func TestHAManyNodes(t *testing.T) {
	var m sync.Map
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	ctx := context.Background()
	ctx = util.WithInternalSourceType(ctx, "dispatcher")

	RegisterTaskMeta(t, ctrl, &m, getMockHATestDispatcherExt(ctrl))
	distContext := testkit.NewDistExecutionContext(t, 30)
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/disttask/framework/scheduler/mockCleanScheduler", "return()"))
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/disttask/framework/scheduler/mockStopManager", "30*return()"))
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/disttask/framework/scheduler/mockTiDBDown", "return(\":4000\")"))
	DispatchTaskAndCheckSuccess(ctx, "😊", t, &m)
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/disttask/framework/scheduler/mockTiDBDown"))
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/disttask/framework/scheduler/mockStopManager"))
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/disttask/framework/scheduler/mockCleanScheduler"))
	distContext.Close()
}

func TestHAFailInDifferentStage(t *testing.T) {
	var m sync.Map
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	ctx := context.Background()
	ctx = util.WithInternalSourceType(ctx, "dispatcher")

	RegisterTaskMeta(t, ctrl, &m, getMockHATestDispatcherExt(ctrl))
	distContext := testkit.NewDistExecutionContext(t, 6)
	// stage1 : server num from 6 to 3.
	// stage2 : server num from 3 to 2.
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/disttask/framework/scheduler/mockCleanScheduler", "return()"))
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/disttask/framework/scheduler/mockStopManager", "6*return()"))
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/disttask/framework/scheduler/mockTiDBDown", "return(\":4000\")"))
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/disttask/framework/scheduler/mockTiDBDown2", "return()"))

	DispatchTaskAndCheckSuccess(ctx, "😊", t, &m)
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/disttask/framework/scheduler/mockTiDBDown"))
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/disttask/framework/scheduler/mockTiDBDown2"))
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/disttask/framework/scheduler/mockStopManager"))
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/disttask/framework/scheduler/mockCleanScheduler"))
	distContext.Close()
}

func TestHAFailInDifferentStageManyNodes(t *testing.T) {
	var m sync.Map

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	ctx := context.Background()
	ctx = util.WithInternalSourceType(ctx, "dispatcher")

	RegisterTaskMeta(t, ctrl, &m, getMockHATestDispatcherExt(ctrl))
	distContext := testkit.NewDistExecutionContext(t, 30)
	// stage1 : server num from 30 to 27.
	// stage2 : server num from 27 to 26.
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/disttask/framework/scheduler/mockCleanScheduler", "return()"))
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/disttask/framework/scheduler/mockStopManager", "30*return()"))
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/disttask/framework/scheduler/mockTiDBDown", "return(\":4000\")"))
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/disttask/framework/scheduler/mockTiDBDown2", "return()"))

	DispatchTaskAndCheckSuccess(ctx, "😊", t, &m)
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/disttask/framework/scheduler/mockTiDBDown"))
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/disttask/framework/scheduler/mockTiDBDown2"))
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/disttask/framework/scheduler/mockStopManager"))
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/disttask/framework/scheduler/mockCleanScheduler"))
	distContext.Close()
}

func TestHAReplacedButRunning(t *testing.T) {
	var m sync.Map

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	ctx := context.Background()
	ctx = util.WithInternalSourceType(ctx, "dispatcher")

	RegisterTaskMeta(t, ctrl, &m, getMockHATestDispatcherExt(ctrl))
	distContext := testkit.NewDistExecutionContext(t, 4)
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/disttask/framework/scheduler/mockTiDBPartitionThenResume", "10*return(true)"))
	DispatchTaskAndCheckSuccess(ctx, "😊", t, &m)
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/disttask/framework/scheduler/mockTiDBPartitionThenResume"))
	distContext.Close()
}

func TestHAReplacedButRunningManyNodes(t *testing.T) {
	var m sync.Map

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	ctx := context.Background()
	ctx = util.WithInternalSourceType(ctx, "dispatcher")

	RegisterTaskMeta(t, ctrl, &m, getMockHATestDispatcherExt(ctrl))
	distContext := testkit.NewDistExecutionContext(t, 30)
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/disttask/framework/scheduler/mockTiDBPartitionThenResume", "30*return(true)"))
	DispatchTaskAndCheckSuccess(ctx, "😊", t, &m)
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/disttask/framework/scheduler/mockTiDBPartitionThenResume"))
	distContext.Close()
}
