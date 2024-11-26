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

	"github.com/pingcap/tidb/pkg/disttask/framework/proto"
	"github.com/pingcap/tidb/pkg/disttask/framework/taskexecutor"
	"go.uber.org/mock/gomock"
)

// InitTaskExecutor inits all mock components for TaskExecutor.
func InitTaskExecutor(ctrl *gomock.Controller, runSubtaskFn func(ctx context.Context, subtask *proto.Subtask) error) {
	executorExt := GetCommonTaskExecutorExt(ctrl, GetCommonStepExecutor(ctrl, runSubtaskFn))
	taskexecutor.RegisterTaskType(proto.TaskTypeExample,
		func(ctx context.Context, id string, task *proto.Task, taskTable taskexecutor.TaskTable) taskexecutor.TaskExecutor {
			s := taskexecutor.NewBaseTaskExecutor(ctx, id, task, taskTable)
			s.Extension = executorExt
			return s
		},
	)
}
