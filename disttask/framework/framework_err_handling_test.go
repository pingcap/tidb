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
	"errors"
	"sync"
	"testing"

	"github.com/pingcap/tidb/disttask/framework/dispatcher"
	"github.com/pingcap/tidb/disttask/framework/proto"
	"github.com/pingcap/tidb/disttask/framework/scheduler"
	"github.com/pingcap/tidb/domain/infosync"
	"github.com/pingcap/tidb/testkit"
)

type planErrDispatcher struct {
	callTime int
	cnt      int
}

var (
	_ dispatcher.Dispatcher = (*planErrDispatcher)(nil)
	_ dispatcher.Dispatcher = (*planNotRetryableErrDispatcher)(nil)
)

func (*planErrDispatcher) OnTick(_ context.Context, _ *proto.Task) {
}

func (dsp *planErrDispatcher) OnNextStage(_ context.Context, _ dispatcher.TaskHandle, gTask *proto.Task) (metas [][]byte, err error) {
	if gTask.State == proto.TaskStatePending {
		if dsp.callTime == 0 {
			dsp.callTime++
			return nil, errors.New("retryable err")
		}
		gTask.Step = proto.StepOne
		dsp.cnt = 3
		return [][]byte{
			[]byte("task1"),
			[]byte("task2"),
			[]byte("task3"),
		}, nil
	}
	if gTask.Step == proto.StepOne {
		gTask.Step = proto.StepTwo
		dsp.cnt = 4
		return [][]byte{
			[]byte("task4"),
		}, nil
	}
	return nil, nil
}

func (dsp *planErrDispatcher) OnErrStage(_ context.Context, _ dispatcher.TaskHandle, _ *proto.Task, _ []error) (meta []byte, err error) {
	if dsp.callTime == 1 {
		dsp.callTime++
		return nil, errors.New("not retryable err")
	}
	return []byte("planErrTask"), nil
}

func (*planErrDispatcher) GetEligibleInstances(_ context.Context, _ *proto.Task) ([]*infosync.ServerInfo, error) {
	return generateSchedulerNodes4Test()
}

func (*planErrDispatcher) IsRetryableErr(error) bool {
	return true
}

func (dsp *planErrDispatcher) AllDispatched(task *proto.Task) bool {
	if task.Step == proto.StepInit {
		return true
	}
	if task.Step == proto.StepOne && dsp.cnt == 3 {
		return true
	}
	if task.Step == proto.StepTwo && dsp.cnt == 4 {
		return true
	}
	return false
}

func (dsp *planErrDispatcher) Finished(task *proto.Task) bool {
	if task.Step == proto.StepTwo && dsp.cnt == 4 {
		return true
	}
	return false
}

type planNotRetryableErrDispatcher struct {
	cnt int
}

func (*planNotRetryableErrDispatcher) OnTick(_ context.Context, _ *proto.Task) {
}

func (p *planNotRetryableErrDispatcher) OnNextStage(_ context.Context, _ dispatcher.TaskHandle, gTask *proto.Task) (metas [][]byte, err error) {
	return nil, errors.New("not retryable err")
}

func (*planNotRetryableErrDispatcher) OnNextStageBatch(_ context.Context, _ dispatcher.TaskHandle, _ *proto.Task) (subtaskMetas [][]byte, err error) {
	return nil, nil
}

func (*planNotRetryableErrDispatcher) OnErrStage(_ context.Context, _ dispatcher.TaskHandle, _ *proto.Task, _ []error) (meta []byte, err error) {
	return nil, errors.New("not retryable err")
}

func (*planNotRetryableErrDispatcher) GetEligibleInstances(_ context.Context, _ *proto.Task) ([]*infosync.ServerInfo, error) {
	return generateSchedulerNodes4Test()
}

func (*planNotRetryableErrDispatcher) IsRetryableErr(error) bool {
	return false
}

func (dsp *planNotRetryableErrDispatcher) AllDispatched(task *proto.Task) bool {
	if task.Step == proto.StepInit {
		return true
	}
	if task.Step == proto.StepOne && dsp.cnt == 3 {
		return true
	}
	if task.Step == proto.StepTwo && dsp.cnt == 4 {
		return true
	}
	return false
}

func (dsp *planNotRetryableErrDispatcher) Finished(task *proto.Task) bool {
	if task.Step == proto.StepTwo && dsp.cnt == 4 {
		return true
	}
	return false
}
func TestPlanErr(t *testing.T) {
	defer dispatcher.ClearTaskDispatcher()
	defer scheduler.ClearSchedulers()
	m := sync.Map{}

	RegisterTaskMeta(&m, &planErrDispatcher{0, 0})
	distContext := testkit.NewDistExecutionContext(t, 2)
	DispatchTaskAndCheckSuccess("key1", t, &m)
	distContext.Close()
}

func TestRevertPlanErr(t *testing.T) {
	defer dispatcher.ClearTaskDispatcher()
	defer scheduler.ClearSchedulers()
	m := sync.Map{}

	RegisterTaskMeta(&m, &planErrDispatcher{0, 0})
	distContext := testkit.NewDistExecutionContext(t, 2)
	DispatchTaskAndCheckSuccess("key1", t, &m)
	distContext.Close()
}

func TestPlanNotRetryableErr(t *testing.T) {
	defer dispatcher.ClearTaskDispatcher()
	defer scheduler.ClearSchedulers()
	m := sync.Map{}

	RegisterTaskMeta(&m, &planNotRetryableErrDispatcher{0})
	distContext := testkit.NewDistExecutionContext(t, 2)
	DispatchTaskAndCheckState("key1", t, &m, proto.TaskStateFailed)
	distContext.Close()
}
