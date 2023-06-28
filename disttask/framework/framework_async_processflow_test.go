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

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/disttask/framework/dispatcher"
	"github.com/pingcap/tidb/disttask/framework/proto"
	"github.com/pingcap/tidb/disttask/framework/scheduler"
	"github.com/pingcap/tidb/domain/infosync"
	"github.com/pingcap/tidb/testkit"
	"github.com/stretchr/testify/require"
)

type asyncFlowHandle struct {
}

var _ dispatcher.TaskFlowHandle = (*asyncFlowHandle)(nil)

func (*asyncFlowHandle) OnTicker(_ context.Context, _ *proto.Task) {
}

func (f *asyncFlowHandle) ProcessNormalFlow(_ context.Context, _ dispatcher.TaskHandle, gTask *proto.Task,
	metasChan chan [][]byte, errChan chan error, doneChan chan bool) {
	switch gTask.Step {
	case proto.StepOne:
		metasChan <- [][]byte{
			[]byte("task1"),
			[]byte("task2"),
			[]byte("task3"),
		}
		time.Sleep(3 * time.Second)
		metasChan <- [][]byte{
			[]byte("task4"),
			[]byte("task5"),
			[]byte("task6"),
		}
		doneChan <- true
	case proto.StepTwo:
		metasChan <- [][]byte{
			[]byte("task7"),
			[]byte("task8"),
		}
		doneChan <- true
	default:
		doneChan <- true
	}
}

func (*asyncFlowHandle) ProcessErrFlow(ctx context.Context, h dispatcher.TaskHandle, gTask *proto.Task, receiveErr []error, metasChan chan [][]byte, errChan chan error, doneChan chan bool) {
	metasChan <- [][]byte{[]byte("rollbacktask1")}
	time.Sleep(3 * time.Second)
	metasChan <- [][]byte{[]byte("rollbacktask1")}
	doneChan <- true
}

func (*asyncFlowHandle) GetEligibleInstances(_ context.Context, _ *proto.Task) ([]*infosync.ServerInfo, error) {
	return generateSchedulerNodes4Test()
}

func (*asyncFlowHandle) IsRetryableErr(error) bool {
	return true
}

func RegisterAsyncTaskMeta(v *atomic.Int64) {
	dispatcher.ClearTaskFlowHandle()
	dispatcher.RegisterTaskFlowHandle(proto.TaskTypeAsyncExample, &asyncFlowHandle{})
	scheduler.ClearSchedulers()
	scheduler.RegisterTaskType(proto.TaskTypeAsyncExample)
	scheduler.RegisterSchedulerConstructor(proto.TaskTypeAsyncExample, proto.StepOne, func(_ int64, _ []byte, _ int64) (scheduler.Scheduler, error) {
		return &rollbackScheduler{v: v}, nil
	}, scheduler.WithConcurrentSubtask())
	scheduler.RegisterSchedulerConstructor(proto.TaskTypeAsyncExample, proto.StepTwo, func(_ int64, _ []byte, _ int64) (scheduler.Scheduler, error) {
		return &rollbackScheduler{v: v}, nil
	}, scheduler.WithConcurrentSubtask())
	scheduler.RegisterSubtaskExectorConstructor(proto.TaskTypeAsyncExample, proto.StepOne, func(_ proto.MinimalTask, _ int64) (scheduler.SubtaskExecutor, error) {
		return &rollbackSubtaskExecutor{v: v}, nil
	})
	scheduler.RegisterSubtaskExectorConstructor(proto.TaskTypeAsyncExample, proto.StepTwo, func(_ proto.MinimalTask, _ int64) (scheduler.SubtaskExecutor, error) {
		return &rollbackSubtaskExecutor{v: v}, nil
	})
	rollbackCnt.Store(0)
}

func TestFrameworkAsyncBasic(t *testing.T) {
	defer dispatcher.ClearTaskFlowHandle()
	defer scheduler.ClearSchedulers()
	var v atomic.Int64
	RegisterAsyncTaskMeta(&v)
	distContext := testkit.NewDistExecutionContext(t, 2)
	task := DispatchTask("key1", proto.TaskTypeAsyncExample, t)
	require.Equal(t, proto.TaskStateSucceed, task.State)
	require.Equal(t, int64(24), v.Load())
	distContext.Close()
}

func TestFrameworkAsyncRollback(t *testing.T) {
	defer dispatcher.ClearTaskFlowHandle()
	defer scheduler.ClearSchedulers()
	var v atomic.Int64
	RegisterAsyncTaskMeta(&v)
	distContext := testkit.NewDistExecutionContext(t, 2)
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/disttask/framework/dispatcher/cancelTaskBeforeProbe", "1*return(true)"))
	DispatchTaskAndCheckFail("key1", proto.TaskTypeAsyncExample, t, &v)
	require.Equal(t, int32(4), rollbackCnt.Load())
	rollbackCnt.Store(0)
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/disttask/framework/dispatcher/cancelTaskBeforeProbe"))
	distContext.Close()
}
