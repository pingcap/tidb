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

package txn

import (
	"context"
	"encoding/json"

	"github.com/pingcap/tidb/pkg/disttask/framework/proto"
	"github.com/pingcap/tidb/pkg/disttask/framework/taskexecutor"
	"github.com/pingcap/tidb/pkg/disttask/framework/taskexecutor/execute"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
)

type taskExecutor struct {
	*taskexecutor.BaseTaskExecutor
	ctx         context.Context
	sessionPool util.DestroyableSessionPool
}

var _ taskexecutor.TaskExecutor = (*taskExecutor)(nil)

func NewTaskExecutor(ctx context.Context, task *proto.Task, param taskexecutor.Param, sessionPool util.DestroyableSessionPool) *taskExecutor {
	e := &taskExecutor{
		BaseTaskExecutor: taskexecutor.NewBaseTaskExecutor(ctx, task, param),
		ctx:              ctx,
		sessionPool:      sessionPool,
	}
	e.BaseTaskExecutor.Extension = e
	return e
}

func (*taskExecutor) IsIdempotent(*proto.Subtask) bool {
	return true
}

func (t *taskExecutor) GetStepExecutor(*proto.Task) (execute.StepExecutor, error) {
	return &stepExecutor{
		ctx:         t.ctx,
		sessionPool: t.sessionPool,
	}, nil
}

func (*taskExecutor) IsRetryableError(error) bool {
	return false
}

type stepExecutor struct {
	ctx context.Context
	taskexecutor.BaseStepExecutor
	sessionPool util.DestroyableSessionPool
}

func (s *stepExecutor) RunSubtask(_ context.Context, subtask *proto.Subtask) error {
	stMeta := subtaskMeta{}
	if err := json.Unmarshal(subtask.Meta, &stMeta); err != nil {
		return err
	}
	logutil.BgLogger().Info("RunSubtask", zap.Int64("subtaskID", subtask.ID),
		zap.String("sql", stMeta.SQL))

	session, err := s.sessionPool.Get()
	if err != nil {
		return err
	}
	defer s.sessionPool.Put(session)

	sql := stMeta.SQL
	sctx := session.(sessionctx.Context)
	sqlExec := sctx.GetSQLExecutor()
	_, err = sqlExec.ExecuteInternal(s.ctx, sql)
	if err != nil {
		return err
	}

	return nil
}
