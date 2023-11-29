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

	"github.com/pingcap/tidb/pkg/disttask/framework/dispatcher"
	"github.com/pingcap/tidb/pkg/disttask/framework/proto"
	"github.com/pingcap/tidb/pkg/domain/infosync"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/tikv/client-go/v2/util"
	"go.uber.org/mock/gomock"
)

<<<<<<< HEAD
type planErrDispatcherExt struct {
	callTime int
	cnt      int
=======
var (
	callTime = 0
)

func getPlanNotRetryableErrDispatcherExt(ctrl *gomock.Controller) dispatcher.Extension {
	mockDispatcher := mockDispatch.NewMockExtension(ctrl)
	mockDispatcher.EXPECT().OnTick(gomock.Any(), gomock.Any()).Return().AnyTimes()
	mockDispatcher.EXPECT().GetEligibleInstances(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, _ *proto.Task) ([]*infosync.ServerInfo, bool, error) {
			return generateSchedulerNodes4Test()
		},
	).AnyTimes()
	mockDispatcher.EXPECT().IsRetryableErr(gomock.Any()).Return(false).AnyTimes()
	mockDispatcher.EXPECT().GetNextStep(gomock.Any()).DoAndReturn(
		func(task *proto.Task) proto.Step {
			return proto.StepDone
		},
	).AnyTimes()
	mockDispatcher.EXPECT().OnNextSubtasksBatch(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, _ dispatcher.TaskHandle, gTask *proto.Task, _ []*infosync.ServerInfo, _ proto.Step) (metas [][]byte, err error) {
			return nil, errors.New("not retryable err")
		},
	).AnyTimes()

	mockDispatcher.EXPECT().OnDone(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	return mockDispatcher
>>>>>>> 86df166bd32 (importinto: make cancel wait task done and some fixes (#48928))
}

var (
	_ dispatcher.Extension = (*planErrDispatcherExt)(nil)
	_ dispatcher.Extension = (*planNotRetryableErrDispatcherExt)(nil)
)

<<<<<<< HEAD
func (*planErrDispatcherExt) OnTick(_ context.Context, _ *proto.Task) {
}

func (p *planErrDispatcherExt) OnNextSubtasksBatch(_ context.Context, _ dispatcher.TaskHandle, gTask *proto.Task, _ []*infosync.ServerInfo, _ proto.Step) (metas [][]byte, err error) {
	if gTask.Step == proto.StepInit {
		if p.callTime == 0 {
			p.callTime++
			return nil, errors.New("retryable err")
		}
		p.cnt = 3
		return [][]byte{
			[]byte("task1"),
			[]byte("task2"),
			[]byte("task3"),
		}, nil
	}
	if gTask.Step == proto.StepOne {
		p.cnt = 4
		return [][]byte{
			[]byte("task4"),
		}, nil
	}
	return nil, nil
}

func (p *planErrDispatcherExt) OnErrStage(_ context.Context, _ dispatcher.TaskHandle, _ *proto.Task, _ []error) (meta []byte, err error) {
	if p.callTime == 1 {
		p.callTime++
		return nil, errors.New("not retryable err")
	}
	return []byte("planErrTask"), nil
}

func (*planErrDispatcherExt) GetEligibleInstances(_ context.Context, _ *proto.Task) ([]*infosync.ServerInfo, bool, error) {
	return generateSchedulerNodes4Test()
}

func (*planErrDispatcherExt) IsRetryableErr(error) bool {
	return true
}

func (p *planErrDispatcherExt) GetNextStep(task *proto.Task) proto.Step {
	switch task.Step {
	case proto.StepInit:
		return proto.StepOne
	case proto.StepOne:
		return proto.StepTwo
	default:
		return proto.StepDone
	}
}

type planNotRetryableErrDispatcherExt struct {
	cnt int
}

func (*planNotRetryableErrDispatcherExt) OnTick(_ context.Context, _ *proto.Task) {
}

func (p *planNotRetryableErrDispatcherExt) OnNextSubtasksBatch(_ context.Context, _ dispatcher.TaskHandle, _ *proto.Task, _ []*infosync.ServerInfo, _ proto.Step) (metas [][]byte, err error) {
	return nil, errors.New("not retryable err")
}

func (*planNotRetryableErrDispatcherExt) OnErrStage(_ context.Context, _ dispatcher.TaskHandle, _ *proto.Task, _ []error) (meta []byte, err error) {
	return nil, errors.New("not retryable err")
}

func (*planNotRetryableErrDispatcherExt) GetEligibleInstances(_ context.Context, _ *proto.Task) ([]*infosync.ServerInfo, bool, error) {
	return generateSchedulerNodes4Test()
}

func (*planNotRetryableErrDispatcherExt) IsRetryableErr(error) bool {
	return false
}

func (p *planNotRetryableErrDispatcherExt) GetNextStep(*proto.Task) proto.Step {
	return proto.StepDone
=======
	gomock.InOrder(
		mockDispatcher.EXPECT().OnDone(gomock.Any(), gomock.Any(), gomock.Any()).Return(errors.New("not retryable err")),
		mockDispatcher.EXPECT().OnDone(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).AnyTimes(),
	)
	return mockDispatcher
>>>>>>> 86df166bd32 (importinto: make cancel wait task done and some fixes (#48928))
}

func TestPlanErr(t *testing.T) {
	m := sync.Map{}
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	ctx := context.Background()
	ctx = util.WithInternalSourceType(ctx, "dispatcher")

	RegisterTaskMeta(t, ctrl, &m, &planErrDispatcherExt{0, 0})
	distContext := testkit.NewDistExecutionContext(t, 2)
	DispatchTaskAndCheckSuccess(ctx, "key1", t, &m)
	distContext.Close()
}

func TestRevertPlanErr(t *testing.T) {
	m := sync.Map{}

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	ctx := context.Background()
	ctx = util.WithInternalSourceType(ctx, "dispatcher")

	RegisterTaskMeta(t, ctrl, &m, &planErrDispatcherExt{0, 0})
	distContext := testkit.NewDistExecutionContext(t, 2)
	DispatchTaskAndCheckSuccess(ctx, "key1", t, &m)
	distContext.Close()
}

func TestPlanNotRetryableErr(t *testing.T) {
	m := sync.Map{}
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	ctx := context.Background()
	ctx = util.WithInternalSourceType(ctx, "dispatcher")

	RegisterTaskMeta(t, ctrl, &m, &planNotRetryableErrDispatcherExt{})
	distContext := testkit.NewDistExecutionContext(t, 2)
	DispatchTaskAndCheckState(ctx, "key1", t, &m, proto.TaskStateFailed)
	distContext.Close()
}
