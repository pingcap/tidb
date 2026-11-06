// Copyright 2026 PingCAP, Inc.
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

package ddl

import (
	"context"
	"encoding/json"
	goerrors "errors"
	"fmt"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/dxf/framework/handle"
	"github.com/pingcap/tidb/pkg/dxf/framework/proto"
	"github.com/pingcap/tidb/pkg/dxf/framework/storage"
	importcore "github.com/pingcap/tidb/pkg/importinto"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/util/dbterror"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
)

// prepareMViewBuildTask uses the existing internal SQL entrypoint. Preparation
// returns a task description; submitting and waiting remain DDL responsibilities.
func prepareMViewBuildTask(ctx context.Context, sctx sessionctx.Context, sql string, jobID int64) (*proto.Task, error) {
	rows, _, err := sctx.GetRestrictedSQLExecutor().ExecRestrictedStmt(ctx,
		&ast.PrepareMViewBuildStmt{ImportSQL: sql, DDLJobID: jobID}, sqlexec.ExecOptionUseCurSession)
	if err != nil {
		return nil, err
	}
	if len(rows) != 1 {
		return nil, errors.New("MV build preparation did not return one task")
	}
	var task proto.Task
	if err := json.Unmarshal(rows[0].GetBytes(0), &task); err != nil {
		return nil, err
	}
	return &task, nil
}

func (w *worker) mviewBuildTaskKey(jobID int64) string {
	return fmt.Sprintf("%s/ddl/MViewBuild/%d", w.store.GetKeyspace(), jobID)
}

func (w *worker) executeMViewDistTask(ctx context.Context, job *model.Job, prepare func() (*proto.Task, error)) error {
	ctx = kv.WithInternalSourceType(ctx, kv.InternalDistTask)
	mgr, err := storage.GetDXFSvcTaskMgr()
	if err != nil {
		return err
	}
	key := w.mviewBuildTaskKey(job.ID)
	task, err := mgr.GetTaskByKeyWithHistory(ctx, key)
	if err != nil && !goerrors.Is(err, storage.ErrTaskNotFound) {
		return err
	}
	if task == nil {
		spec, err := prepare()
		if err != nil {
			return err
		}
		task, err = handle.SubmitTask(ctx, key, proto.MViewBuild, w.store.GetKeyspace(), spec.RequiredSlots, "", spec.MaxNodeCount, spec.Meta)
		if err != nil {
			// A response may be lost after the task has committed.
			task, err = mgr.GetTaskByKeyWithHistory(ctx, key)
			if err != nil {
				return err
			}
		}
	}
	if task.State == proto.TaskStatePaused {
		if err := handle.ResumeTask(ctx, key); err != nil {
			return err
		}
	}
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		if err := w.checkRunnableOrHandlePauseOrCanceled(ctx, key); err != nil {
			return err
		}
		task, err = mgr.GetTaskByIDWithHistory(ctx, task.ID)
		if err != nil {
			return err
		}
		switch task.State {
		case proto.TaskStateSucceed:
			readTS, err := importcore.MViewBuildReadTS(task.Meta)
			if err != nil {
				return err
			}
			return w.setCreateMaterializedViewBuildReadTSInReorgCtx(job.ID, readTS)
		case proto.TaskStateFailed, proto.TaskStateReverted:
			if task.Error != nil {
				return task.Error
			}
			return errors.Errorf("MV build task %d stopped in state %s", task.ID, task.State)
		case proto.TaskStatePaused:
			return dbterror.ErrPausedDDLJob.GenWithStackByArgs(job.ID)
		}
		select {
		case <-ctx.Done():
			return context.Cause(ctx)
		case <-ticker.C:
		}
	}
}

func (w *worker) stopMViewDistTask(ctx context.Context, jobID int64) error {
	mgr, err := storage.GetDXFSvcTaskMgr()
	if err != nil {
		return err
	}
	key := w.mviewBuildTaskKey(jobID)
	task, err := mgr.GetTaskByKeyWithHistory(ctx, key)
	if goerrors.Is(err, storage.ErrTaskNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	if task.IsDone() {
		return nil
	}
	if err := handle.CancelTask(ctx, key); err != nil {
		return err
	}
	return handle.WaitTaskDoneByKey(ctx, key)
}
