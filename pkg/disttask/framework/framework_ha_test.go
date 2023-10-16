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
	"github.com/pingcap/tidb/pkg/disttask/framework/proto"
	"github.com/pingcap/tidb/pkg/domain/infosync"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

type haTestDispatcherExt struct {
	cnt int
}

var _ dispatcher.Extension = (*haTestDispatcherExt)(nil)

func (*haTestDispatcherExt) OnTick(_ context.Context, _ *proto.Task) {
}

func (dsp *haTestDispatcherExt) OnNextSubtasksBatch(_ context.Context, _ dispatcher.TaskHandle, gTask *proto.Task, _ proto.Step) (metas [][]byte, err error) {
	if gTask.Step == proto.StepInit {
		dsp.cnt = 10
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
		dsp.cnt = 15
		return [][]byte{
			[]byte("task11"),
			[]byte("task12"),
			[]byte("task13"),
			[]byte("task14"),
			[]byte("task15"),
		}, nil
	}
	return nil, nil
}

func (*haTestDispatcherExt) OnErrStage(ctx context.Context, h dispatcher.TaskHandle, gTask *proto.Task, receiveErr []error) (subtaskMeta []byte, err error) {
	return nil, nil
}

func (*haTestDispatcherExt) GetEligibleInstances(_ context.Context, _ *proto.Task) ([]*infosync.ServerInfo, error) {
	return generateSchedulerNodes4Test()
}

func (*haTestDispatcherExt) IsRetryableErr(error) bool {
	return true
}

func (dsp *haTestDispatcherExt) GetNextStep(_ dispatcher.TaskHandle, task *proto.Task) proto.Step {
	switch task.Step {
	case proto.StepInit:
		return proto.StepOne
	case proto.StepOne:
		return proto.StepTwo
	default:
		return proto.StepDone
	}
}

func TestHABasic(t *testing.T) {
	var m sync.Map
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	RegisterTaskMeta(t, ctrl, &m, &haTestDispatcherExt{})
	distContext := testkit.NewDistExecutionContext(t, 4)
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/disttask/framework/scheduler/mockCleanScheduler", "return()"))
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/disttask/framework/scheduler/mockStopManager", "4*return()"))
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/disttask/framework/scheduler/mockTiDBDown", "return(\":4000\")"))
	DispatchTaskAndCheckSuccess("😊", t, &m)
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/disttask/framework/scheduler/mockTiDBDown"))
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/disttask/framework/scheduler/mockStopManager"))
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/disttask/framework/scheduler/mockCleanScheduler"))
	distContext.Close()
}

func TestHAManyNodes(t *testing.T) {
	var m sync.Map
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	RegisterTaskMeta(t, ctrl, &m, &haTestDispatcherExt{})
	distContext := testkit.NewDistExecutionContext(t, 30)
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/disttask/framework/scheduler/mockCleanScheduler", "return()"))
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/disttask/framework/scheduler/mockStopManager", "30*return()"))
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/disttask/framework/scheduler/mockTiDBDown", "return(\":4000\")"))
	DispatchTaskAndCheckSuccess("😊", t, &m)
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/disttask/framework/scheduler/mockTiDBDown"))
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/disttask/framework/scheduler/mockStopManager"))
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/disttask/framework/scheduler/mockCleanScheduler"))
	distContext.Close()
}

func TestHAFailInDifferentStage(t *testing.T) {
	var m sync.Map
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	RegisterTaskMeta(t, ctrl, &m, &haTestDispatcherExt{})
	distContext := testkit.NewDistExecutionContext(t, 6)
	// stage1 : server num from 6 to 3.
	// stage2 : server num from 3 to 2.
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/disttask/framework/scheduler/mockCleanScheduler", "return()"))
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/disttask/framework/scheduler/mockStopManager", "6*return()"))
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/disttask/framework/scheduler/mockTiDBDown", "return(\":4000\")"))
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/disttask/framework/scheduler/mockTiDBDown2", "return()"))

	DispatchTaskAndCheckSuccess("😊", t, &m)
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
	RegisterTaskMeta(t, ctrl, &m, &haTestDispatcherExt{})
	distContext := testkit.NewDistExecutionContext(t, 30)
	// stage1 : server num from 30 to 27.
	// stage2 : server num from 27 to 26.
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/disttask/framework/scheduler/mockCleanScheduler", "return()"))
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/disttask/framework/scheduler/mockStopManager", "30*return()"))
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/disttask/framework/scheduler/mockTiDBDown", "return(\":4000\")"))
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/disttask/framework/scheduler/mockTiDBDown2", "return()"))

	DispatchTaskAndCheckSuccess("😊", t, &m)
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
	RegisterTaskMeta(t, ctrl, &m, &haTestDispatcherExt{})
	distContext := testkit.NewDistExecutionContext(t, 4)
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/disttask/framework/scheduler/mockTiDBPartitionThenResume", "10*return(true)"))
	DispatchTaskAndCheckSuccess("😊", t, &m)
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/disttask/framework/scheduler/mockTiDBPartitionThenResume"))
	distContext.Close()
}

func TestHAReplacedButRunningManyNodes(t *testing.T) {
	var m sync.Map

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	RegisterTaskMeta(t, ctrl, &m, &haTestDispatcherExt{})
	distContext := testkit.NewDistExecutionContext(t, 30)
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/disttask/framework/scheduler/mockTiDBPartitionThenResume", "30*return(true)"))
	DispatchTaskAndCheckSuccess("😊", t, &m)
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/disttask/framework/scheduler/mockTiDBPartitionThenResume"))
	distContext.Close()
}
