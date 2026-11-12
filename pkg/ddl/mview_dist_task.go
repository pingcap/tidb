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
	"fmt"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/config/kerneltype"
	"github.com/pingcap/tidb/pkg/ddl/logutil"
	"github.com/pingcap/tidb/pkg/dxf/framework/handle"
	"github.com/pingcap/tidb/pkg/dxf/framework/proto"
	"github.com/pingcap/tidb/pkg/dxf/framework/storage"
	"github.com/pingcap/tidb/pkg/dxf/importinto/taskkey"
	"github.com/pingcap/tidb/pkg/executor/importer"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/util/dbterror"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
	"go.uber.org/zap"
)

// The build of a NextGen CREATE MATERIALIZED VIEW job is an ordinary IMPORT
// INTO job and DXF task. The DDL job creates the import job in its own
// transaction, and finds it again by group key, so a new owner reconnects to
// the same job and task without persisting anything in the DDL job.

// usesMViewDistTask reports whether the MV build of this store runs as an
// IMPORT INTO job on DXF.
func (w *worker) usesMViewDistTask() bool {
	failpoint.Inject("mockMViewBuildImportJob", func() {
		failpoint.Return(true)
	})
	return kerneltype.IsNextGen() && w.store.Name() == "TiKV"
}

// mviewBuildGroupKey returns the group key of the import job of a CREATE
// MATERIALIZED VIEW job. Users must not use group keys in this form.
func mviewBuildGroupKey(ddlJobID int64) string {
	return fmt.Sprintf("mview-build-%d", ddlJobID)
}

// createMViewBuildImportJob creates the import job of the build in the
// transaction of the DDL job. The import job therefore exists exactly when the
// DDL job has reached the build state, and a stale owner cannot create one.
func (w *worker) createMViewBuildImportJob(ctx context.Context, job *model.Job, mviewTableInfo *model.TableInfo) error {
	_, err := importer.CreateJob(ctx, w.sess.GetSQLExecutor(), ast.NewCIStr(job.SchemaName).L,
		mviewTableInfo.Name.L, mviewTableInfo.ID, "", mviewBuildGroupKey(job.ID), &importer.ImportParameters{}, 0)
	return errors.Trace(err)
}

// getMViewBuildImportJobID returns the ID of the import job of the build, or
// 0 if the DDL job has not created it.
func getMViewBuildImportJobID(ctx context.Context, exec sqlexec.SQLExecutor, ddlJobID, tableID int64) (int64, error) {
	jobs, err := importer.GetJobsByGroupKey(ctx, exec, "", mviewBuildGroupKey(ddlJobID), true)
	if err != nil {
		return 0, errors.Trace(err)
	}
	var importJobID int64
	for _, j := range jobs {
		// The table ID rules out an import job of a user that took this group key.
		if j.TableID != tableID {
			continue
		}
		if importJobID != 0 {
			return 0, errors.Errorf("create materialized view: multiple import jobs for DDL job %d", ddlJobID)
		}
		importJobID = j.ID
	}
	return importJobID, nil
}

// getMViewBuildImportJobIDInNewSession is getMViewBuildImportJobID outside the
// transaction of the DDL job.
func (w *worker) getMViewBuildImportJobIDInNewSession(ctx context.Context, ddlJobID, tableID int64) (int64, error) {
	sctx, err := w.sessPool.Get()
	if err != nil {
		return 0, err
	}
	defer w.sessPool.Put(sctx)
	return getMViewBuildImportJobID(ctx, sctx.GetSQLExecutor(), ddlJobID, tableID)
}

// prepareMViewBuildTask returns the description of the build task of an
// import job without submitting it.
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
	vars := sctx.GetSessionVars()
	sql, err := buildCreateMaterializedViewImportSQL(job.SchemaName, mviewTableInfo, vars.MViewMaintainImportThreads, vars.MViewMaintainImportDiskQuota)
	if err != nil {
		return nil, err
	}
	is := sctx.GetLatestInfoSchema().(infoschema.InfoSchema)
	tbl, ok := is.TableByID(ctx, mviewTableInfo.ID)
	if !ok {
		return nil, infoschema.ErrTableNotExists.FastGenByArgs(job.SchemaName, mviewTableInfo.Name.O)
	}
	plan, err := importer.PrepareQueryImport(ctx, sctx, &importer.QueryImportRequest{
		SQL:    sql,
		DBName: ast.NewCIStr(job.SchemaName).L,
		DBID:   job.SchemaID,
		Table:  tbl,
		// CREATE MATERIALIZED VIEW allows only one base table, in the same schema
		// as the view, and rejects views and subqueries in its definition.
		SourceTableIDs: mviewTableInfo.MaterializedView.BaseTableIDs,
		JobID:          importJobID,
	})
	if err != nil {
		return nil, err
	}
	meta, err := json.Marshal(&importer.TaskMeta{JobID: importJobID, Plan: *plan, Stmt: sql})
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
	var meta importer.TaskMeta
	if err := json.Unmarshal(data, &meta); err != nil {
		return 0, err
	}
	if meta.JobID == 0 || meta.Plan.QuerySource == nil || meta.Plan.QuerySource.ReadTS == 0 {
		return 0, errors.New("MV build task has no read timestamp")
	}
	return meta.Plan.QuerySource.ReadTS, nil
}

// findOrSubmitMViewDistTask reconnects to the task of the import job, or
// submits it. The task key comes from the import job, like other imports.
func (w *worker) findOrSubmitMViewDistTask(ctx context.Context, mgr *storage.TaskManager, job *model.Job,
	mviewTableInfo *model.TableInfo, importJobID int64) (*proto.Task, error) {
	key := taskkey.ForJob(importJobID)
	task, err := mgr.GetTaskByKeyWithHistory(ctx, key)
	if err == nil {
		return task, nil
	}
	if !goerrors.Is(err, storage.ErrTaskNotFound) {
		return nil, err
	}
	spec, err := w.prepareMViewBuildTask(ctx, job, mviewTableInfo, importJobID)
	if err != nil {
		return nil, err
	}
	task, submitErr := handle.SubmitTask(ctx, key, proto.ImportInto, w.store.GetKeyspace(), spec.RequiredSlots,
		handle.GetTargetScope(), spec.MaxNodeCount, spec.Meta)
	if submitErr == nil {
		return task, nil
	}
	// A response may be lost after the task has committed.
	task, err = mgr.GetTaskByKeyWithHistory(ctx, key)
	if err != nil {
		return nil, errors.Trace(submitErr)
	}
	return task, nil
}

func (w *worker) executeMViewDistTask(ctx context.Context, job *model.Job, mviewTableInfo *model.TableInfo) error {
	ctx = kv.WithInternalSourceType(ctx, kv.InternalDistTask)
	importJobID, err := w.getMViewBuildImportJobIDInNewSession(ctx, job.ID, mviewTableInfo.ID)
	if err != nil {
		return err
	}
	if importJobID == 0 {
		return dbterror.ErrInvalidDDLJob.GenWithStackByArgs("create materialized view: import job of the build not found")
	}
	mgr, err := storage.GetDXFSvcTaskMgr()
	if err != nil {
		return err
	}
	task, err := w.findOrSubmitMViewDistTask(ctx, mgr, job, mviewTableInfo, importJobID)
	if err != nil {
		return err
	}
	key := task.Key
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
		latest, err := mgr.GetTaskByIDWithHistory(ctx, task.ID)
		if err != nil {
			// The build keeps running without this read. An error here must not
			// roll back the DDL job and cancel the task.
			logutil.DDLLogger().Warn("get MV build task failed", zap.Int64("jobID", job.ID), zap.Error(err))
		} else {
			task = latest
			switch task.State {
			case proto.TaskStateSucceed:
				readTS, err := mviewBuildReadTS(task.Meta)
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
		}
		select {
		case <-ctx.Done():
			return context.Cause(ctx)
		case <-ticker.C:
		}
	}
}

// stopMViewDistTask cancels the build of a rolling back DDL job and waits for
// it to stop. An import job without a task is cancelled directly.
func (w *worker) stopMViewDistTask(ctx context.Context, job *model.Job) error {
	sctx, err := w.sessPool.Get()
	if err != nil {
		return err
	}
	defer w.sessPool.Put(sctx)
	exec := sctx.GetSQLExecutor()
	importJobID, err := getMViewBuildImportJobID(ctx, exec, job.ID, job.TableID)
	if err != nil || importJobID == 0 {
		return err
	}
	mgr, err := storage.GetDXFSvcTaskMgr()
	if err != nil {
		return err
	}
	key := taskkey.ForJob(importJobID)
	task, err := mgr.GetTaskByKeyWithHistory(ctx, key)
	if goerrors.Is(err, storage.ErrTaskNotFound) {
		return errors.Trace(importer.CancelJob(ctx, exec, importJobID))
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
