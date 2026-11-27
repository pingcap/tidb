// Copyright 2019 PingCAP, Inc.
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
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/config/kerneltype"
	"github.com/pingcap/tidb/pkg/dxf/framework/handle"
	"github.com/pingcap/tidb/pkg/dxf/framework/proto"
	"github.com/pingcap/tidb/pkg/dxf/framework/storage"
	"github.com/pingcap/tidb/pkg/dxf/importinto/taskinfo"
	"github.com/pingcap/tidb/pkg/dxf/importinto/taskkey"
	"github.com/pingcap/tidb/pkg/executor/importer"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/util/dbterror"
)

func (w *worker) usesMViewDistTask() bool {
	failpoint.Inject("mockMViewBuildImportJob", func() {
		failpoint.Return(true)
	})
	return kerneltype.IsNextGen() && w.store.Name() == "TiKV"
}

func (w *worker) prepareMViewBuildTask(ctx context.Context, job *model.Job, mviewTableInfo *model.TableInfo,
	importJobID int64) (*proto.Task, error) {
	sctx, err := w.sessPool.Get()
	if err != nil {
		return nil, err
	}
	defer w.sessPool.Put(sctx)
	restore, err := initCreateMaterializedViewBuildSession(sctx, job, mviewTableInfo, job.SchemaName)
	if err != nil {
		return nil, err
	}
	defer restore()
	vars := sctx.GetSessionVars() //nolint:forbidigo
	sql, err := buildCreateMaterializedViewImportSQL(job.SchemaName, mviewTableInfo, vars.MViewMaintainImportThreads, vars.MViewMaintainImportDiskQuota)
	if err != nil {
		return nil, err
	}
	is := sctx.GetLatestInfoSchema().(infoschema.InfoSchema)
	tbl, ok := is.TableByID(ctx, mviewTableInfo.ID)
	if !ok {
		return nil, infoschema.ErrTableNotExists.FastGenByArgs(job.SchemaName, mviewTableInfo.Name.O)
	}
	plan, err := importer.BuildQueryImportPlan(ctx, sctx, &importer.QueryImportRequest{
		SQL:            sql,
		DBName:         ast.NewCIStr(job.SchemaName).L,
		DBID:           job.SchemaID,
		Table:          tbl,
		SourceTableIDs: mviewTableInfo.MaterializedView.BaseTableIDs,
	})
	if err != nil {
		return nil, err
	}
	meta, err := json.Marshal(&taskinfo.TaskMeta{JobID: importJobID, Plan: *plan, Stmt: sql})
	if err != nil {
		return nil, err
	}
	return &proto.Task{
		TaskBase: proto.TaskBase{
			Type:          proto.ImportInto,
			RequiredSlots: plan.ThreadCnt,
			MaxNodeCount:  plan.MaxNodeCnt,
		},
		Meta: meta,
	}, nil
}

func mviewBuildReadTS(data []byte) (uint64, error) {
	var meta taskinfo.TaskMeta
	if err := json.Unmarshal(data, &meta); err != nil {
		return 0, err
	}
	if meta.Plan.QuerySource == nil || meta.Plan.QuerySource.ReadTS == 0 {
		return 0, errors.New("MV build task has no read timestamp")
	}
	return meta.Plan.QuerySource.ReadTS, nil
}

// Reuse the import task across owner changes.
func (w *worker) findOrSubmitMViewDistTask(ctx context.Context, mgr *storage.TaskManager, job *model.Job,
	mviewTableInfo *model.TableInfo, importJobID int64) (*proto.Task, error) {
	key := taskkey.ForJob(importJobID)
	task, err := mgr.GetTaskByKeyWithHistory(ctx, key)
	if err == nil {
		return task, nil
	}
	if !goerrors.Is(err, storage.ErrTaskNotFound) {
		return nil, errors.Trace(err)
	}
	spec, err := w.prepareMViewBuildTask(ctx, job, mviewTableInfo, importJobID)
	if err != nil {
		return nil, err
	}
	return w.submitMViewDistTask(ctx, mgr, key, spec)
}

func (w *worker) submitMViewDistTask(ctx context.Context, mgr *storage.TaskManager, key string, spec *proto.Task) (*proto.Task, error) {
	task, submitErr := handle.SubmitTask(ctx, key, proto.ImportInto, w.store.GetKeyspace(), spec.RequiredSlots,
		handle.GetTargetScope(), spec.MaxNodeCount, spec.Meta)
	if submitErr == nil {
		return task, nil
	}
	// A response may be lost after the task has committed.
	task, err := mgr.GetTaskByKeyWithHistory(ctx, key)
	if err != nil {
		return nil, errors.Trace(submitErr)
	}
	return task, nil
}

func (w *worker) executeDistCreateMV(stepCtx context.Context, job *model.Job, mviewTableInfo *model.TableInfo) error {
	ctx := kv.WithInternalSourceType(w.workCtx, kv.InternalDistTask)
	args, err := model.GetCreateMaterializedViewArgs(job)
	if err != nil {
		return err
	}
	if args.ImportJobID == 0 {
		return dbterror.ErrInvalidDDLJob.GenWithStackByArgs("create materialized view: import job of the build not found")
	}
	mgr, err := storage.GetDXFSvcTaskMgr()
	if err != nil {
		return errors.Trace(err)
	}
	task, err := w.findOrSubmitMViewDistTask(ctx, mgr, job, mviewTableInfo, args.ImportJobID)
	if err != nil {
		return err
	}
	key := task.Key
	var finished *proto.Task
	err = w.waitDistTask(ctx, stepCtx, key, func(waitCtx context.Context) error {
		if task.State == proto.TaskStatePaused {
			if _, err := mgr.ResumeTask(waitCtx, key); err != nil {
				return errors.Trace(err)
			}
		}
		var err error
		finished, err = handle.WaitTaskDoneOrPausedWithResult(waitCtx, task.ID)
		if finished != nil && (finished.State == proto.TaskStateFailed || finished.State == proto.TaskStateReverted) && finished.Error != nil {
			return finished.Error
		}
		return err
	})
	if err != nil {
		return err
	}
	if finished.State == proto.TaskStatePaused {
		return dbterror.ErrPausedDDLJob.GenWithStackByArgs(job.ID)
	}
	readTS, err := mviewBuildReadTS(finished.Meta)
	if err != nil {
		return err
	}
	return w.setCreateMaterializedViewBuildReadTSInReorgCtx(job.ID, readTS)
}

// Wait for task cleanup before the DDL rollback drops the MV table.
func (w *worker) stopMViewDistTask(ctx context.Context, job *model.Job) error {
	args, err := model.GetCreateMaterializedViewArgs(job)
	if err != nil || args.ImportJobID == 0 {
		return err
	}
	key := taskkey.ForJob(args.ImportJobID)
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		done, err := cancelDistTask(ctx, key)
		if goerrors.Is(err, storage.ErrTaskNotFound) {
			sctx, err := w.sessPool.Get()
			if err != nil {
				return err
			}
			defer w.sessPool.Put(sctx)
			return errors.Trace(importer.CancelJob(ctx, sctx.GetSQLExecutor(), args.ImportJobID))
		}
		if err != nil {
			return err
		}
		if done {
			return nil
		}
		select {
		case <-ctx.Done():
			return context.Cause(ctx)
		case <-ticker.C:
		}
	}
}
