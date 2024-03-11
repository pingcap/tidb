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
	"fmt"
	"math"

	"github.com/pingcap/tidb/pkg/disttask/framework/proto"
	"github.com/pingcap/tidb/pkg/disttask/framework/scheduler"
	mockScheduler "github.com/pingcap/tidb/pkg/disttask/framework/scheduler/mock"
	"github.com/pingcap/tidb/pkg/disttask/framework/storage"
	"go.uber.org/mock/gomock"
)

// SchedulerInfo is used for mocking scheduler.Extension.
type SchedulerInfo struct {
	AllErrorRetryable bool
	StepInfos         []StepInfo
}

// StepInfo is used for mocking scheduler.Extension.
type StepInfo struct {
	Step proto.Step
	// the semantic of below fields is
	//  for the first ErrRepeatCount calls to NextSubtasksBatch, return the corresponding error in Errors.
	// 	for the rest calls to NextSubtasksBatch, return SubtaskCnt subtasks.
	callCount      int64 // for internal counting.
	Err            error
	ErrRepeatCount int64
	SubtaskCnt     int
}

// GetMockBasicSchedulerExt returns mock scheduler.Extension with basic functionalities.
func GetMockBasicSchedulerExt(ctrl *gomock.Controller) scheduler.Extension {
	return GetMockSchedulerExt(ctrl, SchedulerInfo{
		AllErrorRetryable: true,
		StepInfos: []StepInfo{
			{Step: proto.StepOne, SubtaskCnt: 3},
			{Step: proto.StepTwo, SubtaskCnt: 1},
		},
	})
}

// GetMockSchedulerExt returns mock scheduler.Extension with input stepInfos.
// the returned scheduler.Extension will go from StepInit -> each step in stepInfos -> StepDone.
func GetMockSchedulerExt(ctrl *gomock.Controller, schedulerInfo SchedulerInfo) scheduler.Extension {
	stepInfos := schedulerInfo.StepInfos
	if len(stepInfos) == 0 {
		panic("stepInfos should not be empty")
	}
	stepTransition := make(map[proto.Step]proto.Step, len(stepInfos)+1)
	currStep := proto.StepInit
	stepInfoMap := make(map[proto.Step]StepInfo, len(stepInfos))
	for _, stepInfo := range stepInfos {
		stepTransition[currStep] = stepInfo.Step
		currStep = stepInfo.Step
		stepInfoMap[stepInfo.Step] = stepInfo
	}
	stepTransition[currStep] = proto.StepDone

	mockScheduler := mockScheduler.NewMockExtension(ctrl)
	mockScheduler.EXPECT().OnTick(gomock.Any(), gomock.Any()).Return().AnyTimes()
	mockScheduler.EXPECT().GetEligibleInstances(gomock.Any(), gomock.Any()).Return(nil, nil).AnyTimes()
	mockScheduler.EXPECT().IsRetryableErr(gomock.Any()).Return(schedulerInfo.AllErrorRetryable).AnyTimes()
	mockScheduler.EXPECT().GetNextStep(gomock.Any()).DoAndReturn(
		func(task *proto.TaskBase) proto.Step {
			return stepTransition[task.Step]
		},
	).AnyTimes()
	mockScheduler.EXPECT().OnNextSubtasksBatch(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, _ storage.TaskHandle, _ *proto.Task, _ []string, nextStep proto.Step) (metas [][]byte, err error) {
			stepInfo, ok := stepInfoMap[nextStep]
			if !ok {
				return nil, nil
			}
			if stepInfo.callCount < stepInfo.ErrRepeatCount {
				stepInfo.callCount++
				return nil, stepInfo.Err
			}
			res := make([][]byte, stepInfo.SubtaskCnt)
			for i := 0; i < stepInfo.SubtaskCnt; i++ {
				res[i] = []byte(fmt.Sprintf("subtask-%d", i))
			}
			return res, nil
		},
	).AnyTimes()

	mockScheduler.EXPECT().OnDone(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	return mockScheduler
}

// GetMockHATestSchedulerExt returns mock scheduler.Extension for HA testing with multiple steps.
func GetMockHATestSchedulerExt(ctrl *gomock.Controller) scheduler.Extension {
	return GetMockSchedulerExt(ctrl, SchedulerInfo{
		AllErrorRetryable: true,
		StepInfos: []StepInfo{
			{Step: proto.StepOne, SubtaskCnt: 10},
			{Step: proto.StepTwo, SubtaskCnt: 5},
		},
	})
}

// GetPlanNotRetryableErrSchedulerExt returns mock scheduler.Extension which will generate non retryable error when planning.
func GetPlanNotRetryableErrSchedulerExt(ctrl *gomock.Controller) scheduler.Extension {
	return GetMockSchedulerExt(ctrl, SchedulerInfo{
		AllErrorRetryable: false,
		StepInfos: []StepInfo{
			{Step: proto.StepOne, Err: errors.New("not retryable err"), ErrRepeatCount: math.MaxInt64},
		},
	})
}

// GetStepTwoPlanNotRetryableErrSchedulerExt returns mock scheduler.Extension which will generate non retryable error when planning for step two.
func GetStepTwoPlanNotRetryableErrSchedulerExt(ctrl *gomock.Controller) scheduler.Extension {
	return GetMockSchedulerExt(ctrl, SchedulerInfo{
		AllErrorRetryable: false,
		StepInfos: []StepInfo{
			{Step: proto.StepOne, SubtaskCnt: 10},
			{Step: proto.StepTwo, Err: errors.New("not retryable err"), ErrRepeatCount: math.MaxInt64},
		},
	})
}

// GetPlanErrSchedulerExt returns mock scheduler.Extension which will generate error when planning.
func GetPlanErrSchedulerExt(ctrl *gomock.Controller, testContext *TestContext) scheduler.Extension {
	mockScheduler := mockScheduler.NewMockExtension(ctrl)
	mockScheduler.EXPECT().OnTick(gomock.Any(), gomock.Any()).Return().AnyTimes()
	mockScheduler.EXPECT().GetEligibleInstances(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, _ *proto.Task) ([]string, error) {
			return nil, nil
		},
	).AnyTimes()
	mockScheduler.EXPECT().IsRetryableErr(gomock.Any()).Return(true).AnyTimes()
	mockScheduler.EXPECT().GetNextStep(gomock.Any()).DoAndReturn(
		func(task *proto.TaskBase) proto.Step {
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
		func(_ context.Context, _ storage.TaskHandle, task *proto.Task, _ []string, _ proto.Step) (metas [][]byte, err error) {
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
	return GetMockSchedulerExt(ctrl, SchedulerInfo{
		AllErrorRetryable: true,
		StepInfos: []StepInfo{
			{Step: proto.StepOne, SubtaskCnt: 3},
		},
	})
}
