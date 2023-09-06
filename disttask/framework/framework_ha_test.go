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
	"github.com/pingcap/tidb/disttask/framework/dispatcher"
	"github.com/pingcap/tidb/disttask/framework/proto"
	"github.com/pingcap/tidb/disttask/framework/scheduler"
	"github.com/pingcap/tidb/disttask/framework/storage"
	"github.com/pingcap/tidb/domain/infosync"
	"github.com/pingcap/tidb/testkit"
	"github.com/stretchr/testify/require"
)

type haTestFlowHandle struct{}

var _ dispatcher.Extension = (*haTestFlowHandle)(nil)

func (*haTestFlowHandle) OnTick(_ context.Context, _ *proto.Task) {
}

func (*haTestFlowHandle) OnNextStage(_ context.Context, _ dispatcher.TaskHandle, gTask *proto.Task) (metas [][]byte, err error) {
	if gTask.State == proto.TaskStatePending {
		gTask.Step = proto.StepOne
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
		gTask.Step = proto.StepTwo
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

func (*haTestFlowHandle) OnErrStage(ctx context.Context, h dispatcher.TaskHandle, gTask *proto.Task, receiveErr []error) (subtaskMeta []byte, err error) {
	return nil, nil
}

func (*haTestFlowHandle) GetEligibleInstances(_ context.Context, _ *proto.Task) ([]*infosync.ServerInfo, error) {
	return generateSchedulerNodes4Test()
}

func (*haTestFlowHandle) IsRetryableErr(error) bool {
	return true
}

func RegisterHATaskMeta(m *sync.Map) {
	dispatcher.ClearDispatcherFactory()
	dispatcher.RegisterDispatcherFactory(proto.TaskTypeExample,
		func(ctx context.Context, taskMgr *storage.TaskManager, serverID string, task *proto.Task) dispatcher.Dispatcher {
			baseDispatcher := dispatcher.NewBaseDispatcher(ctx, taskMgr, serverID, task)
			baseDispatcher.Extension = &haTestFlowHandle{}
			return baseDispatcher
		})
	scheduler.ClearSchedulers()
	scheduler.RegisterTaskType(proto.TaskTypeExample)
	scheduler.RegisterSchedulerConstructor(proto.TaskTypeExample, proto.StepOne, func(_ context.Context, _ *proto.Task, _ *scheduler.Summary) (scheduler.Scheduler, error) {
		return &testScheduler{}, nil
	})
	scheduler.RegisterSchedulerConstructor(proto.TaskTypeExample, proto.StepTwo, func(_ context.Context, _ *proto.Task, _ *scheduler.Summary) (scheduler.Scheduler, error) {
		return &testScheduler{}, nil
	})
	scheduler.RegisterSubtaskExectorConstructor(proto.TaskTypeExample, proto.StepOne, func(_ proto.MinimalTask, _ int64) (scheduler.SubtaskExecutor, error) {
		return &testSubtaskExecutor{m: m}, nil
	})
	scheduler.RegisterSubtaskExectorConstructor(proto.TaskTypeExample, proto.StepTwo, func(_ proto.MinimalTask, _ int64) (scheduler.SubtaskExecutor, error) {
		return &testSubtaskExecutor1{m: m}, nil
	})
}

func TestHABasic(t *testing.T) {
	defer dispatcher.ClearDispatcherFactory()
	defer scheduler.ClearSchedulers()
	var m sync.Map
	RegisterHATaskMeta(&m)
	distContext := testkit.NewDistExecutionContext(t, 4)
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/disttask/framework/scheduler/mockCleanScheduler", "return()"))
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/disttask/framework/scheduler/mockStopManager", "4*return()"))
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/disttask/framework/scheduler/mockTiDBDown", "return(\":4000\")"))
	DispatchTaskAndCheckSuccess("😊", t, &m)
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/disttask/framework/scheduler/mockTiDBDown"))
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/disttask/framework/scheduler/mockStopManager"))
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/disttask/framework/scheduler/mockCleanScheduler"))
	distContext.Close()
}

func TestHAManyNodes(t *testing.T) {
	defer dispatcher.ClearDispatcherFactory()
	defer scheduler.ClearSchedulers()
	var m sync.Map

	RegisterHATaskMeta(&m)
	distContext := testkit.NewDistExecutionContext(t, 30)
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/disttask/framework/scheduler/mockCleanScheduler", "return()"))
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/disttask/framework/scheduler/mockStopManager", "30*return()"))
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/disttask/framework/scheduler/mockTiDBDown", "return(\":4000\")"))
	DispatchTaskAndCheckSuccess("😊", t, &m)
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/disttask/framework/scheduler/mockTiDBDown"))
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/disttask/framework/scheduler/mockStopManager"))
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/disttask/framework/scheduler/mockCleanScheduler"))
	distContext.Close()
}

func TestHAFailInDifferentStage(t *testing.T) {
	defer dispatcher.ClearDispatcherFactory()
	defer scheduler.ClearSchedulers()
	var m sync.Map

	RegisterHATaskMeta(&m)
	distContext := testkit.NewDistExecutionContext(t, 6)
	// stage1 : server num from 6 to 3.
	// stage2 : server num from 3 to 2.
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/disttask/framework/scheduler/mockCleanScheduler", "return()"))
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/disttask/framework/scheduler/mockStopManager", "6*return()"))
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/disttask/framework/scheduler/mockTiDBDown", "return(\":4000\")"))
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/disttask/framework/scheduler/mockTiDBDown2", "return()"))

	DispatchTaskAndCheckSuccess("😊", t, &m)
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/disttask/framework/scheduler/mockTiDBDown"))
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/disttask/framework/scheduler/mockTiDBDown2"))
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/disttask/framework/scheduler/mockStopManager"))
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/disttask/framework/scheduler/mockCleanScheduler"))
	distContext.Close()
}

func TestHAFailInDifferentStageManyNodes(t *testing.T) {
	defer dispatcher.ClearDispatcherFactory()
	defer scheduler.ClearSchedulers()
	var m sync.Map

	RegisterHATaskMeta(&m)
	distContext := testkit.NewDistExecutionContext(t, 30)
	// stage1 : server num from 30 to 27.
	// stage2 : server num from 27 to 26.
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/disttask/framework/scheduler/mockCleanScheduler", "return()"))
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/disttask/framework/scheduler/mockStopManager", "30*return()"))
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/disttask/framework/scheduler/mockTiDBDown", "return(\":4000\")"))
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/disttask/framework/scheduler/mockTiDBDown2", "return()"))

	DispatchTaskAndCheckSuccess("😊", t, &m)
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/disttask/framework/scheduler/mockTiDBDown"))
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/disttask/framework/scheduler/mockTiDBDown2"))
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/disttask/framework/scheduler/mockStopManager"))
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/disttask/framework/scheduler/mockCleanScheduler"))
	distContext.Close()
}

func TestHAReplacedButRunning(t *testing.T) {
	defer dispatcher.ClearDispatcherFactory()
	defer scheduler.ClearSchedulers()
	var m sync.Map

	RegisterHATaskMeta(&m)
	distContext := testkit.NewDistExecutionContext(t, 4)
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/disttask/framework/scheduler/mockTiDBPartitionThenResume", "10*return(true)"))
	DispatchTaskAndCheckSuccess("😊", t, &m)
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/disttask/framework/scheduler/mockTiDBPartitionThenResume"))
	distContext.Close()
}

func TestHAReplacedButRunningManyNodes(t *testing.T) {
	defer dispatcher.ClearDispatcherFactory()
	defer scheduler.ClearSchedulers()
	var m sync.Map

	RegisterHATaskMeta(&m)
	distContext := testkit.NewDistExecutionContext(t, 30)
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/disttask/framework/scheduler/mockTiDBPartitionThenResume", "30*return(true)"))
	DispatchTaskAndCheckSuccess("😊", t, &m)
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/disttask/framework/scheduler/mockTiDBPartitionThenResume"))
	distContext.Close()
}
