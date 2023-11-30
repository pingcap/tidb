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

	"github.com/pingcap/tidb/pkg/disttask/framework/dispatcher"
	mockDispatch "github.com/pingcap/tidb/pkg/disttask/framework/dispatcher/mock"
	"github.com/pingcap/tidb/pkg/disttask/framework/proto"
	"github.com/pingcap/tidb/pkg/domain/infosync"
	"go.uber.org/mock/gomock"
)

// GetMockBasicDispatcherExt returns mock dispatcher.Extension with basic functionalities.
func GetMockBasicDispatcherExt(ctrl *gomock.Controller) dispatcher.Extension {
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
				}, nil
			}
			if gTask.Step == proto.StepOne {
				return [][]byte{
					[]byte("task4"),
				}, nil
			}
			return nil, nil
		},
	).AnyTimes()

	mockDispatcher.EXPECT().OnDone(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	return mockDispatcher
}

// GetMockHATestDispatcherExt returns mock dispatcher.Extension for HA testing with multiple steps.
func GetMockHATestDispatcherExt(ctrl *gomock.Controller) dispatcher.Extension {
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

func generateSchedulerNodes4Test() ([]*infosync.ServerInfo, bool, error) {
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

// GetPlanNotRetryableErrDispatcherExt returns mock dispatcher.Extension which will generate non retryable error when planning.
func GetPlanNotRetryableErrDispatcherExt(ctrl *gomock.Controller) dispatcher.Extension {
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
}

// GetPlanErrDispatcherExt returns mock dispatcher.Extension which will generate error when planning.
func GetPlanErrDispatcherExt(ctrl *gomock.Controller, testContext *TestContext) dispatcher.Extension {
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
			if gTask.Step == proto.StepOne {
				return [][]byte{
					[]byte("task4"),
				}, nil
			}
			return nil, nil
		},
	).AnyTimes()

	gomock.InOrder(
		mockDispatcher.EXPECT().OnDone(gomock.Any(), gomock.Any(), gomock.Any()).Return(errors.New("not retryable err")),
		mockDispatcher.EXPECT().OnDone(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).AnyTimes(),
	)
	return mockDispatcher
}

// GetMockRollbackDispatcherExt returns mock dispatcher.Extension which will generate rollback subtasks.
func GetMockRollbackDispatcherExt(ctrl *gomock.Controller) dispatcher.Extension {
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
				}, nil
			}
			return nil, nil
		},
	).AnyTimes()

	mockDispatcher.EXPECT().OnDone(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	return mockDispatcher
}

// GetMockDynamicDispatchExt returns mock dispatcher.Extension which will generate subtask in multiple batches.
func GetMockDynamicDispatchExt(ctrl *gomock.Controller) dispatcher.Extension {
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
					[]byte("task"),
					[]byte("task"),
				}, nil
			}

			// step2
			if gTask.Step == proto.StepOne {
				return [][]byte{
					[]byte("task"),
				}, nil
			}
			return nil, nil
		},
	).AnyTimes()

	mockDispatcher.EXPECT().OnDone(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).AnyTimes()

	return mockDispatcher
}
