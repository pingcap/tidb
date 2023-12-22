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

package testutil

import (
	"context"
	"errors"

	"github.com/pingcap/tidb/pkg/disttask/framework/proto"
	"github.com/pingcap/tidb/pkg/disttask/framework/scheduler"
	mockDispatch "github.com/pingcap/tidb/pkg/disttask/framework/scheduler/mock"
	"github.com/pingcap/tidb/pkg/domain/infosync"
	"go.uber.org/mock/gomock"
)

// GetMockBasicSchedulerExt returns mock scheduler.Extension with basic functionalities.
func GetMockBasicSchedulerExt(ctrl *gomock.Controller) scheduler.Extension {
	mockScheduler := mockDispatch.NewMockExtension(ctrl)
	mockScheduler.EXPECT().OnTick(gomock.Any(), gomock.Any()).Return().AnyTimes()
	mockScheduler.EXPECT().GetEligibleInstances(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, _ *proto.Task) ([]*infosync.ServerInfo, bool, error) {
			return generateTaskExecutorNodes4Test()
		},
	).AnyTimes()
	mockScheduler.EXPECT().IsRetryableErr(gomock.Any()).Return(true).AnyTimes()
	mockScheduler.EXPECT().GetNextStep(gomock.Any()).DoAndReturn(
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
	mockScheduler.EXPECT().OnNextSubtasksBatch(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, _ scheduler.TaskHandle, task *proto.Task, _ []*infosync.ServerInfo, _ proto.Step) (metas [][]byte, err error) {
			if task.Step == proto.StepInit {
				return [][]byte{
					[]byte("task1"),
					[]byte("task2"),
					[]byte("task3"),
				}, nil
			}
			if task.Step == proto.StepOne {
				return [][]byte{
					[]byte("task4"),
				}, nil
			}
			return nil, nil
		},
	).AnyTimes()

	mockScheduler.EXPECT().OnDone(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	return mockScheduler
}

// GetMockHATestSchedulerExt returns mock scheduler.Extension for HA testing with multiple steps.
func GetMockHATestSchedulerExt(ctrl *gomock.Controller) scheduler.Extension {
	mockScheduler := mockDispatch.NewMockExtension(ctrl)
	mockScheduler.EXPECT().OnTick(gomock.Any(), gomock.Any()).Return().AnyTimes()
	mockScheduler.EXPECT().GetEligibleInstances(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, _ *proto.Task) ([]*infosync.ServerInfo, bool, error) {
			return generateTaskExecutorNodes4Test()
		},
	).AnyTimes()
	mockScheduler.EXPECT().IsRetryableErr(gomock.Any()).Return(true).AnyTimes()
	mockScheduler.EXPECT().GetNextStep(gomock.Any()).DoAndReturn(
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
	mockScheduler.EXPECT().OnNextSubtasksBatch(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, _ scheduler.TaskHandle, task *proto.Task, _ []*infosync.ServerInfo, _ proto.Step) (metas [][]byte, err error) {
			if task.Step == proto.StepInit {
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
			if task.Step == proto.StepOne {
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

	mockScheduler.EXPECT().OnDone(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).AnyTimes()

	return mockScheduler
}

func generateTaskExecutorNodes4Test() ([]*infosync.ServerInfo, bool, error) {
	serverInfos := infosync.MockGlobalServerInfoManagerEntry.GetAllServerInfo()
	if len(serverInfos) == 0 {
		return nil, true, errors.New("not found instance")
	}

	serverNodes := make([]*infosync.ServerInfo, 0, len(serverInfos))
	for _, serverInfo := range serverInfos {
		serverNodes = append(serverNodes, serverInfo)
	}
	return serverNodes, true, nil
}

// GetPlanNotRetryableErrSchedulerExt returns mock scheduler.Extension which will generate non retryable error when planning.
func GetPlanNotRetryableErrSchedulerExt(ctrl *gomock.Controller) scheduler.Extension {
	mockScheduler := mockDispatch.NewMockExtension(ctrl)
	mockScheduler.EXPECT().OnTick(gomock.Any(), gomock.Any()).Return().AnyTimes()
	mockScheduler.EXPECT().GetEligibleInstances(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, _ *proto.Task) ([]*infosync.ServerInfo, bool, error) {
			return generateTaskExecutorNodes4Test()
		},
	).AnyTimes()
	mockScheduler.EXPECT().IsRetryableErr(gomock.Any()).Return(false).AnyTimes()
	mockScheduler.EXPECT().GetNextStep(gomock.Any()).DoAndReturn(
		func(task *proto.Task) proto.Step {
			if task.Step == proto.StepInit {
				return proto.StepOne
			}
			return proto.StepDone
		},
	).AnyTimes()
	mockScheduler.EXPECT().OnNextSubtasksBatch(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, _ scheduler.TaskHandle, _ *proto.Task, _ []*infosync.ServerInfo, _ proto.Step) (metas [][]byte, err error) {
			return nil, errors.New("not retryable err")
		},
	).AnyTimes()

	mockScheduler.EXPECT().OnDone(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	return mockScheduler
}

// GetPlanErrSchedulerExt returns mock scheduler.Extension which will generate error when planning.
func GetPlanErrSchedulerExt(ctrl *gomock.Controller, testContext *TestContext) scheduler.Extension {
	mockScheduler := mockDispatch.NewMockExtension(ctrl)
	mockScheduler.EXPECT().OnTick(gomock.Any(), gomock.Any()).Return().AnyTimes()
	mockScheduler.EXPECT().GetEligibleInstances(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, _ *proto.Task) ([]*infosync.ServerInfo, bool, error) {
			return generateTaskExecutorNodes4Test()
		},
	).AnyTimes()
	mockScheduler.EXPECT().IsRetryableErr(gomock.Any()).Return(true).AnyTimes()
	mockScheduler.EXPECT().GetNextStep(gomock.Any()).DoAndReturn(
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
	mockScheduler.EXPECT().OnNextSubtasksBatch(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, _ scheduler.TaskHandle, task *proto.Task, _ []*infosync.ServerInfo, _ proto.Step) (metas [][]byte, err error) {
			if task.Step == proto.StepInit {
				if testContext.CallTime == 0 {
					testContext.CallTime++
					return nil, errors.New("retryable err")
				}
				return [][]byte{
					[]byte("task1"),
					[]byte("task2"),
					[]byte("task3"),
				}, nil
			}
			if task.Step == proto.StepOne {
				return [][]byte{
					[]byte("task4"),
				}, nil
			}
			return nil, nil
		},
	).AnyTimes()

	gomock.InOrder(
		mockScheduler.EXPECT().OnDone(gomock.Any(), gomock.Any(), gomock.Any()).Return(errors.New("not retryable err")),
		mockScheduler.EXPECT().OnDone(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).AnyTimes(),
	)
	return mockScheduler
}

// GetMockRollbackSchedulerExt returns mock scheduler.Extension which will generate rollback subtasks.
func GetMockRollbackSchedulerExt(ctrl *gomock.Controller) scheduler.Extension {
	mockScheduler := mockDispatch.NewMockExtension(ctrl)
	mockScheduler.EXPECT().OnTick(gomock.Any(), gomock.Any()).Return().AnyTimes()
	mockScheduler.EXPECT().GetEligibleInstances(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, _ *proto.Task) ([]*infosync.ServerInfo, bool, error) {
			return generateTaskExecutorNodes4Test()
		},
	).AnyTimes()
	mockScheduler.EXPECT().IsRetryableErr(gomock.Any()).Return(true).AnyTimes()
	mockScheduler.EXPECT().GetNextStep(gomock.Any()).DoAndReturn(
		func(task *proto.Task) proto.Step {
			switch task.Step {
			case proto.StepInit:
				return proto.StepOne
			default:
				return proto.StepDone
			}
		},
	).AnyTimes()
	mockScheduler.EXPECT().OnNextSubtasksBatch(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, _ scheduler.TaskHandle, task *proto.Task, _ []*infosync.ServerInfo, _ proto.Step) (metas [][]byte, err error) {
			if task.Step == proto.StepInit {
				return [][]byte{
					[]byte("task1"),
					[]byte("task2"),
					[]byte("task3"),
				}, nil
			}
			return nil, nil
		},
	).AnyTimes()

	mockScheduler.EXPECT().OnDone(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	return mockScheduler
}

// GetMockDynamicDispatchExt returns mock scheduler.Extension which will generate subtask in multiple batches.
func GetMockDynamicDispatchExt(ctrl *gomock.Controller) scheduler.Extension {
	mockScheduler := mockDispatch.NewMockExtension(ctrl)
	mockScheduler.EXPECT().OnTick(gomock.Any(), gomock.Any()).Return().AnyTimes()
	mockScheduler.EXPECT().GetEligibleInstances(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, _ *proto.Task) ([]*infosync.ServerInfo, bool, error) {
			return generateTaskExecutorNodes4Test()
		},
	).AnyTimes()
	mockScheduler.EXPECT().IsRetryableErr(gomock.Any()).Return(true).AnyTimes()
	mockScheduler.EXPECT().GetNextStep(gomock.Any()).DoAndReturn(
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
	mockScheduler.EXPECT().OnNextSubtasksBatch(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, _ scheduler.TaskHandle, task *proto.Task, _ []*infosync.ServerInfo, _ proto.Step) (metas [][]byte, err error) {
			if task.Step == proto.StepInit {
				return [][]byte{
					[]byte("task"),
					[]byte("task"),
				}, nil
			}

			// step2
			if task.Step == proto.StepOne {
				return [][]byte{
					[]byte("task"),
				}, nil
			}
			return nil, nil
		},
	).AnyTimes()

	mockScheduler.EXPECT().OnDone(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).AnyTimes()

	return mockScheduler
}
