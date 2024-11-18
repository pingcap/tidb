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

	"github.com/pingcap/tidb/pkg/disttask/framework/mock"
	mockexecute "github.com/pingcap/tidb/pkg/disttask/framework/mock/execute"
	"github.com/pingcap/tidb/pkg/disttask/framework/proto"
	"github.com/pingcap/tidb/pkg/disttask/framework/taskexecutor"
	"go.uber.org/mock/gomock"
)

// GetMockStepExecutor returns one mock subtaskExecutor.
func GetMockStepExecutor(ctrl *gomock.Controller) *mockexecute.MockStepExecutor {
	executor := mockexecute.NewMockStepExecutor(ctrl)
	executor.EXPECT().Init(gomock.Any()).Return(nil).AnyTimes()
	executor.EXPECT().Cleanup(gomock.Any()).Return(nil).AnyTimes()
	executor.EXPECT().OnFinished(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	return executor
}

// GetMockTaskExecutorExtension returns one mock TaskExecutorExtension.
func GetMockTaskExecutorExtension(ctrl *gomock.Controller, mockStepExecutor *mockexecute.MockStepExecutor) *mock.MockExtension {
	mockExtension := mock.NewMockExtension(ctrl)
	mockExtension.EXPECT().
		GetStepExecutor(gomock.Any()).
		Return(mockStepExecutor, nil).AnyTimes()
	mockExtension.EXPECT().IsRetryableError(gomock.Any()).Return(false).AnyTimes()
	return mockExtension
}

// InitTaskExecutor inits all mock components for TaskExecutor.
func InitTaskExecutor(ctrl *gomock.Controller, runSubtaskFn func(ctx context.Context, subtask *proto.Subtask) error) {
	mockStepExecutor := GetMockStepExecutor(ctrl)
	mockStepExecutor.EXPECT().RunSubtask(gomock.Any(), gomock.Any()).DoAndReturn(
		runSubtaskFn,
	).AnyTimes()
	mockStepExecutor.EXPECT().RealtimeSummary().Return(nil).AnyTimes()

	mockExtension := GetMockTaskExecutorExtension(ctrl, mockStepExecutor)
	taskexecutor.RegisterTaskType(proto.TaskTypeExample,
		func(ctx context.Context, id string, task *proto.Task, taskTable taskexecutor.TaskTable) taskexecutor.TaskExecutor {
			s := taskexecutor.NewBaseTaskExecutor(ctx, id, task, taskTable)
			s.Extension = mockExtension
			return s
		},
	)
}
