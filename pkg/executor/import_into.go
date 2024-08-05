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

package executor

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/pkg/disttask/framework/handle"
	"github.com/pingcap/tidb/pkg/disttask/framework/proto"
	fstorage "github.com/pingcap/tidb/pkg/disttask/framework/storage"
	"github.com/pingcap/tidb/pkg/disttask/importinto"
	"github.com/pingcap/tidb/pkg/executor/importer"
	"github.com/pingcap/tidb/pkg/executor/internal/exec"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/lightning/log"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	plannercore "github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/privilege"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/dbterror/exeerrors"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/tikv/client-go/v2/util"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

const unknownImportedRowCount = -1

// ImportIntoExec represents a IMPORT INTO executor.
type ImportIntoExec struct {
	exec.BaseExecutor
	selectExec exec.Executor
	userSctx   sessionctx.Context
	controller *importer.LoadDataController
	stmt       string

	plan       *plannercore.ImportInto
	tbl        table.Table
	dataFilled bool
}

var (
	_ exec.Executor = (*ImportIntoExec)(nil)
)

func newImportIntoExec(b exec.BaseExecutor, selectExec exec.Executor, userSctx sessionctx.Context,
	plan *plannercore.ImportInto, tbl table.Table) (*ImportIntoExec, error) {
	return &ImportIntoExec{
		BaseExecutor: b,
		selectExec:   selectExec,
		userSctx:     userSctx,
		stmt:         plan.Stmt,
		plan:         plan,
		tbl:          tbl,
	}, nil
}

// Next implements the Executor Next interface.
func (e *ImportIntoExec) Next(ctx context.Context, req *chunk.Chunk) (err error) {
	req.GrowAndReset(e.MaxChunkSize())
	ctx = kv.WithInternalSourceType(ctx, kv.InternalImportInto)
	if e.dataFilled {
		// need to return an empty req to indicate all results have been written
		return nil
	}
	importPlan, err := importer.NewImportPlan(ctx, e.userSctx, e.plan, e.tbl)
	if err != nil {
		return err
	}
	astArgs := importer.ASTArgsFromImportPlan(e.plan)
	controller, err := importer.NewLoadDataController(importPlan, e.tbl, astArgs)
	if err != nil {
		return err
	}
	e.controller = controller

	if e.selectExec != nil {
		// `import from select` doesn't return rows, so no need to set dataFilled.
		return e.importFromSelect(ctx)
	}

	if err2 := e.controller.InitDataFiles(ctx); err2 != nil {
		return err2
	}

	// must use a new session to pre-check, else the stmt in show processlist will be changed.
	newSCtx, err2 := CreateSession(e.userSctx)
	if err2 != nil {
		return err2
	}
	defer CloseSession(newSCtx)
	sqlExec := newSCtx.GetSQLExecutor()
	if err2 = e.controller.CheckRequirements(ctx, sqlExec); err2 != nil {
		return err2
	}

	if err := e.controller.InitTiKVConfigs(ctx, newSCtx); err != nil {
		return err
	}

	failpoint.InjectCall("cancellableCtx", &ctx)

	jobID, task, err := e.submitTask(ctx)
	if err != nil {
		return err
	}

	if !e.controller.Detached {
		if err = e.waitTask(ctx, jobID, task); err != nil {
			return err
		}
	}
	return e.fillJobInfo(ctx, jobID, req)
}

func (e *ImportIntoExec) fillJobInfo(ctx context.Context, jobID int64, req *chunk.Chunk) error {
	e.dataFilled = true
	// we use taskManager to get job, user might not have the privilege to system tables.
	taskManager, err := fstorage.GetTaskManager()
	ctx = util.WithInternalSourceType(ctx, kv.InternalDistTask)
	if err != nil {
		return err
	}
	var info *importer.JobInfo
	if err = taskManager.WithNewSession(func(se sessionctx.Context) error {
		sqlExec := se.GetSQLExecutor()
		var err2 error
		info, err2 = importer.GetJob(ctx, sqlExec, jobID, e.Ctx().GetSessionVars().User.String(), false)
		return err2
	}); err != nil {
		return err
	}
	fillOneImportJobInfo(info, req, unknownImportedRowCount)
	return nil
}

func (e *ImportIntoExec) submitTask(ctx context.Context) (int64, *proto.TaskBase, error) {
	importFromServer, err := storage.IsLocalPath(e.controller.Path)
	if err != nil {
		// since we have checked this during creating controller, this should not happen.
		return 0, nil, exeerrors.ErrLoadDataInvalidURI.FastGenByArgs(plannercore.ImportIntoDataSource, err.Error())
	}
	logutil.Logger(ctx).Info("get job importer", zap.Stringer("param", e.controller.Parameters),
		zap.Bool("dist-task-enabled", variable.EnableDistTask.Load()))
	if importFromServer {
		ecp, err2 := e.controller.PopulateChunks(ctx)
		if err2 != nil {
			return 0, nil, err2
		}
		return importinto.SubmitStandaloneTask(ctx, e.controller.Plan, e.stmt, ecp)
	}
	// if tidb_enable_dist_task=true, we import distributively, otherwise we import on current node.
	if variable.EnableDistTask.Load() {
		return importinto.SubmitTask(ctx, e.controller.Plan, e.stmt)
	}
	return importinto.SubmitStandaloneTask(ctx, e.controller.Plan, e.stmt, nil)
}

// waitTask waits for the task to finish.
// NOTE: WaitTaskDoneOrPaused also return error when task fails.
func (*ImportIntoExec) waitTask(ctx context.Context, jobID int64, task *proto.TaskBase) error {
	err := handle.WaitTaskDoneOrPaused(ctx, task.ID)
	// when user KILL the connection, the ctx will be canceled, we need to cancel the import job.
	if errors.Cause(err) == context.Canceled {
		taskManager, err2 := fstorage.GetTaskManager()
		if err2 != nil {
			return err2
		}
		// use background, since ctx is canceled already.
		return cancelAndWaitImportJob(context.Background(), taskManager, jobID)
	}
	return err
}

func (e *ImportIntoExec) importFromSelect(ctx context.Context) error {
	e.dataFilled = true
	// must use a new session as:
	// 	- pre-check will execute other sql, the stmt in show processlist will be changed.
	// 	- userSctx might be in stale read, we cannot do write.
	newSCtx, err2 := CreateSession(e.userSctx)
	if err2 != nil {
		return err2
	}
	defer CloseSession(newSCtx)

	sqlExec := newSCtx.GetSQLExecutor()
	if err2 = e.controller.CheckRequirements(ctx, sqlExec); err2 != nil {
		return err2
	}
	if err := e.controller.InitTiKVConfigs(ctx, newSCtx); err != nil {
		return err
	}

	importID := uuid.New().String()
	logutil.Logger(ctx).Info("importing data from select statement",
		zap.String("import-id", importID), zap.Int("concurrency", e.controller.ThreadCnt),
		zap.String("target-table", e.controller.FullTableName()),
		zap.Int64("target-table-id", e.controller.TableInfo.ID))
	ti, err2 := importer.NewTableImporter(ctx, e.controller, importID, e.Ctx().GetStore())
	if err2 != nil {
		return err2
	}
	defer func() {
		if err := ti.Close(); err != nil {
			logutil.Logger(ctx).Error("close importer failed", zap.Error(err))
		}
	}()
	selectedRowCh := make(chan importer.QueryRow)
	ti.SetSelectedRowCh(selectedRowCh)

	var importResult *importer.JobImportResult
	eg, egCtx := errgroup.WithContext(ctx)
	eg.Go(func() error {
		var err error
		importResult, err = ti.ImportSelectedRows(egCtx, newSCtx)
		return err
	})
	eg.Go(func() error {
		defer close(selectedRowCh)
		fields := exec.RetTypes(e.selectExec)
		var idAllocator int64
		for {
			// rows will be consumed concurrently, we cannot use chunk pool in session ctx.
			chk := exec.NewFirstChunk(e.selectExec)
			iter := chunk.NewIterator4Chunk(chk)
			err := exec.Next(egCtx, e.selectExec, chk)
			if err != nil {
				return err
			}
			if chk.NumRows() == 0 {
				break
			}
			for innerChunkRow := iter.Begin(); innerChunkRow != iter.End(); innerChunkRow = iter.Next() {
				idAllocator++
				select {
				case selectedRowCh <- importer.QueryRow{
					ID:   idAllocator,
					Data: innerChunkRow.GetDatumRow(fields),
				}:
				case <-egCtx.Done():
					return egCtx.Err()
				}
			}
		}
		return nil
	})
	if err := eg.Wait(); err != nil {
		return err
	}

	if err2 = importer.FlushTableStats(ctx, newSCtx, e.controller.TableInfo.ID, importResult); err2 != nil {
		logutil.Logger(ctx).Error("flush stats failed", zap.Error(err2))
	}

	stmtCtx := e.userSctx.GetSessionVars().StmtCtx
	stmtCtx.SetAffectedRows(importResult.Affected)
	// TODO: change it after spec is ready.
	stmtCtx.SetMessage(fmt.Sprintf("Records: %d, ID: %s", importResult.Affected, importID))
	return nil
}

// ImportIntoActionExec represents a import into action executor.
type ImportIntoActionExec struct {
	exec.BaseExecutor
	tp    ast.ImportIntoActionTp
	jobID int64
}

var (
	_ exec.Executor = (*ImportIntoActionExec)(nil)
)

// Next implements the Executor Next interface.
func (e *ImportIntoActionExec) Next(ctx context.Context, _ *chunk.Chunk) (err error) {
	ctx = kv.WithInternalSourceType(ctx, kv.InternalImportInto)

	var hasSuperPriv bool
	if pm := privilege.GetPrivilegeManager(e.Ctx()); pm != nil {
		hasSuperPriv = pm.RequestVerification(e.Ctx().GetSessionVars().ActiveRoles, "", "", "", mysql.SuperPriv)
	}
	// we use sessionCtx from GetTaskManager, user ctx might not have enough privileges.
	taskManager, err := fstorage.GetTaskManager()
	ctx = util.WithInternalSourceType(ctx, kv.InternalDistTask)
	if err != nil {
		return err
	}
	if err = e.checkPrivilegeAndStatus(ctx, taskManager, hasSuperPriv); err != nil {
		return err
	}

	task := log.BeginTask(logutil.Logger(ctx).With(zap.Int64("jobID", e.jobID),
		zap.Any("action", e.tp)), "import into action")
	defer func() {
		task.End(zap.ErrorLevel, err)
	}()
	return cancelAndWaitImportJob(ctx, taskManager, e.jobID)
}

func (e *ImportIntoActionExec) checkPrivilegeAndStatus(ctx context.Context, manager *fstorage.TaskManager, hasSuperPriv bool) error {
	var info *importer.JobInfo
	if err := manager.WithNewSession(func(se sessionctx.Context) error {
		exec := se.GetSQLExecutor()
		var err2 error
		info, err2 = importer.GetJob(ctx, exec, e.jobID, e.Ctx().GetSessionVars().User.String(), hasSuperPriv)
		return err2
	}); err != nil {
		return err
	}
	if !info.CanCancel() {
		return exeerrors.ErrLoadDataInvalidOperation.FastGenByArgs("CANCEL")
	}
	return nil
}

func cancelAndWaitImportJob(ctx context.Context, manager *fstorage.TaskManager, jobID int64) error {
	if err := manager.WithNewTxn(ctx, func(se sessionctx.Context) error {
		ctx = util.WithInternalSourceType(ctx, kv.InternalDistTask)
		return manager.CancelTaskByKeySession(ctx, se, importinto.TaskKey(jobID))
	}); err != nil {
		return err
	}
	return handle.WaitTaskDoneByKey(ctx, importinto.TaskKey(jobID))
}
