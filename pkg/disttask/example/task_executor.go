// Copyright 2025 PingCAP, Inc.
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

package example

import (
	"context"
	"encoding/json"

	"github.com/pingcap/tidb/pkg/disttask/framework/proto"
	"github.com/pingcap/tidb/pkg/disttask/framework/taskexecutor"
	"github.com/pingcap/tidb/pkg/disttask/framework/taskexecutor/execute"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
)

type taskExecutor struct {
	*taskexecutor.BaseTaskExecutor
}

var _ taskexecutor.TaskExecutor = (*taskExecutor)(nil)

func newTaskExecutor(ctx context.Context, task *proto.Task, param taskexecutor.Param) *taskExecutor {
	e := &taskExecutor{
		BaseTaskExecutor: taskexecutor.NewBaseTaskExecutor(ctx, task, param),
	}
	e.BaseTaskExecutor.Extension = e
	return e
}

func (*taskExecutor) IsIdempotent(*proto.Subtask) bool {
	return true
}

func (*taskExecutor) GetStepExecutor(*proto.Task) (execute.StepExecutor, error) {
	return &stepExecutor{}, nil
}

func (*taskExecutor) IsRetryableError(error) bool {
	return true
}

// Note: you can have different types of stepExecutor for different steps.
type stepExecutor struct {
	taskexecutor.BaseStepExecutor
}

func (*stepExecutor) RunSubtask(_ context.Context, subtask *proto.Subtask) error {
	stMeta := subtaskMeta{}
	if err := json.Unmarshal(subtask.Meta, &stMeta); err != nil {
		return err
	}
	logutil.BgLogger().Info("RunSubtask", zap.Int64("subtaskID", subtask.ID),
		zap.String("message", stMeta.Message))
	return nil
}
