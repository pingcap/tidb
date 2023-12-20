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
	"sync/atomic"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/br/pkg/lightning/log"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/pkg/disttask/framework/handle"
	"github.com/pingcap/tidb/pkg/disttask/framework/proto"
	fstorage "github.com/pingcap/tidb/pkg/disttask/framework/storage"
	"github.com/pingcap/tidb/pkg/disttask/importinto"
	"github.com/pingcap/tidb/pkg/executor/importer"
	"github.com/pingcap/tidb/pkg/executor/internal/exec"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	plannercore "github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/privilege"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/sessiontxn"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/dbterror/exeerrors"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
	"github.com/tikv/client-go/v2/util"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

var (
	// TestDetachedTaskFinished is a flag for test.
	TestDetachedTaskFinished atomic.Bool
	// TestCancelFunc for test.
	TestCancelFunc context.CancelFunc
)

const unknownImportedRowCount = -1

// ImportIntoExec represents a IMPORT INTO executor.
type ImportIntoExec struct {
	exec.BaseExecutor
	userSctx   sessionctx.Context
	importPlan *importer.Plan
	controller *importer.LoadDataController
	stmt       string

	dataFilled bool
}

var (
	_ exec.Executor = (*ImportIntoExec)(nil)
)

func newImportIntoExec(b exec.BaseExecutor, userSctx sessionctx.Context, plan *plannercore.ImportInto, tbl table.Table) (
	*ImportIntoExec, error) {
	importPlan, err := importer.NewImportPlan(userSctx, plan, tbl)
	if err != nil {
		return nil, err
	}
	astArgs := importer.ASTArgsFromImportPlan(plan)
	controller, err := importer.NewLoadDataController(importPlan, tbl, astArgs)
	if err != nil {
		return nil, err
	}
	return &ImportIntoExec{
		BaseExecutor: b,
		userSctx:     userSctx,
		importPlan:   importPlan,
		controller:   controller,
		stmt:         plan.Stmt,
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
	if err2 := e.controller.InitDataFiles(ctx); err2 != nil {
		return err2
	}

	// must use a new session to pre-check, else the stmt in show processlist will be changed.
	newSCtx, err2 := CreateSession(e.userSctx)
	if err2 != nil {
		return err2
	}
	defer CloseSession(newSCtx)
	sqlExec := newSCtx.(sqlexec.SQLExecutor)
	if err2 = e.controller.CheckRequirements(ctx, sqlExec); err2 != nil {
		return err2
	}

	if err := e.importPlan.InitTiKVConfigs(ctx, newSCtx); err != nil {
		return err
	}

	failpoint.Inject("cancellableCtx", func() {
		// KILL is not implemented in testkit, so we use a fail-point to simulate it.
		newCtx, cancel := context.WithCancel(ctx)
		ctx = newCtx
		TestCancelFunc = cancel
	})
	// todo: we don't need Job now, remove it later.
	parentCtx := ctx
	if e.controller.Detached {
		parentCtx = context.Background()
	}
	group, groupCtx := errgroup.WithContext(parentCtx)
	groupCtx = kv.WithInternalSourceType(groupCtx, kv.InternalDistTask)

	param := &importer.JobImportParam{
		Job:      &importer.Job{},
		Group:    group,
		GroupCtx: groupCtx,
		Done:     make(chan struct{}),
		Progress: importer.NewProgress(),
	}
	distImporter, err := e.getJobImporter(ctx, param)
	if err != nil {
		return err
	}
	defer func() {
		_ = distImporter.Close()
	}()
	param.Progress.SourceFileSize = e.controller.TotalFileSize
	jobID, task, err := distImporter.SubmitTask(ctx)
	if err != nil {
		return err
	}

	if e.controller.Detached {
		ctx := kv.WithInternalSourceType(context.Background(), kv.InternalImportInto)
		se, err := CreateSession(e.userSctx)
		if err != nil {
			return err
		}
		go func() {
			defer CloseSession(se)
			// error is stored in system table, so we can ignore it here
			//nolint: errcheck
			_ = e.doImport(ctx, se, distImporter, task)
			failpoint.Inject("testDetachedTaskFinished", func() {
				TestDetachedTaskFinished.Store(true)
			})
		}()
		return e.fillJobInfo(ctx, jobID, req)
	}
	if err = e.doImport(ctx, e.userSctx, distImporter, task); err != nil {
		return err
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
		sqlExec := se.(sqlexec.SQLExecutor)
		var err2 error
		info, err2 = importer.GetJob(ctx, sqlExec, jobID, e.Ctx().GetSessionVars().User.String(), false)
		return err2
	}); err != nil {
		return err
	}
	fillOneImportJobInfo(info, req, unknownImportedRowCount)
	return nil
}

func (e *ImportIntoExec) getJobImporter(ctx context.Context, param *importer.JobImportParam) (*importinto.DistImporter, error) {
	importFromServer, err := storage.IsLocalPath(e.controller.Path)
	if err != nil {
		// since we have checked this during creating controller, this should not happen.
		return nil, exeerrors.ErrLoadDataInvalidURI.FastGenByArgs(plannercore.ImportIntoDataSource, err.Error())
	}
	logutil.Logger(ctx).Info("get job importer", zap.Stringer("param", e.controller.Parameters),
		zap.Bool("dist-task-enabled", variable.EnableDistTask.Load()))
	if importFromServer {
		ecp, err2 := e.controller.PopulateChunks(ctx)
		if err2 != nil {
			return nil, err2
		}
		return importinto.NewDistImporterServerFile(param, e.importPlan, e.stmt, ecp, e.controller.TotalFileSize)
	}
	// if tidb_enable_dist_task=true, we import distributively, otherwise we import on current node.
	if variable.EnableDistTask.Load() {
		return importinto.NewDistImporter(param, e.importPlan, e.stmt, e.controller.TotalFileSize)
	}
	return importinto.NewDistImporterCurrNode(param, e.importPlan, e.stmt, e.controller.TotalFileSize)
}

func (e *ImportIntoExec) doImport(ctx context.Context, se sessionctx.Context, distImporter *importinto.DistImporter, task *proto.Task) error {
	distImporter.ImportTask(task)
	group := distImporter.Param().Group
	err := group.Wait()
	// when user KILL the connection, the ctx will be canceled, we need to cancel the import job.
	if errors.Cause(err) == context.Canceled {
		taskManager, err2 := fstorage.GetTaskManager()
		if err2 != nil {
			return err2
		}
		// use background, since ctx is canceled already.
		return cancelAndWaitImportJob(context.Background(), taskManager, distImporter.JobID())
	}
	if err2 := flushStats(ctx, se, e.importPlan.TableInfo.ID, distImporter.Result(ctx)); err2 != nil {
		logutil.Logger(ctx).Error("flush stats failed", zap.Error(err2))
	}
	return err
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
		exec := se.(sqlexec.SQLExecutor)
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

// flushStats flushes the stats of the table.
func flushStats(ctx context.Context, se sessionctx.Context, tableID int64, result importer.JobImportResult) error {
	if err := sessiontxn.NewTxn(ctx, se); err != nil {
		return err
	}
	sessionVars := se.GetSessionVars()
	sessionVars.TxnCtxMu.Lock()
	defer sessionVars.TxnCtxMu.Unlock()
	sessionVars.TxnCtx.UpdateDeltaForTable(tableID, int64(result.Affected), int64(result.Affected), result.ColSizeMap)
	se.StmtCommit(ctx)
	return se.CommitTxn(ctx)
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
