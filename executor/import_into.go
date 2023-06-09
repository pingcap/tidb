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

	"github.com/pingcap/failpoint"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/disttask/framework/proto"
	fstorage "github.com/pingcap/tidb/disttask/framework/storage"
	"github.com/pingcap/tidb/disttask/loaddata"
	"github.com/pingcap/tidb/executor/asyncloaddata"
	"github.com/pingcap/tidb/executor/importer"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/mysql"
	plannercore "github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/privilege"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/dbterror/exeerrors"
	"github.com/pingcap/tidb/util/sqlexec"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

var (
	// TestDetachedTaskFinished is a flag for test.
	TestDetachedTaskFinished atomic.Bool
)

// ImportIntoExec represents a IMPORT INTO executor.
type ImportIntoExec struct {
	baseExecutor
	userSctx   sessionctx.Context
	importPlan *importer.Plan
	controller *importer.LoadDataController
	stmt       string

	dataFilled bool
}

var (
	_ Executor = (*ImportIntoExec)(nil)
)

func newImportIntoExec(b baseExecutor, userSctx sessionctx.Context, plan *plannercore.ImportInto, tbl table.Table) (
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
		baseExecutor: b,
		userSctx:     userSctx,
		importPlan:   importPlan,
		controller:   controller,
		stmt:         plan.Stmt,
	}, nil
}

// Next implements the Executor Next interface.
func (e *ImportIntoExec) Next(ctx context.Context, req *chunk.Chunk) (err error) {
	req.GrowAndReset(e.maxChunkSize)
	ctx = kv.WithInternalSourceType(ctx, kv.InternalImportInto)
	if e.dataFilled {
		// need to return an empty req to indicate all results have been written
		return nil
	}

	// todo: we don't need to do it here, remove it.
	if err2 := e.controller.InitDataFiles(ctx); err2 != nil {
		return err2
	}

	sqlExec := e.userSctx.(sqlexec.SQLExecutor)
	if err2 := e.controller.CheckRequirements(ctx, sqlExec); err2 != nil {
		return err2
	}

	// todo: we don't need Job now, remove it later.
	group, groupCtx := errgroup.WithContext(ctx)
	param := &importer.JobImportParam{
		Job:      &asyncloaddata.Job{},
		Group:    group,
		GroupCtx: groupCtx,
		Done:     make(chan struct{}),
		Progress: asyncloaddata.NewProgress(false),
	}
	distImporter, err := e.getJobImporter(param)
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
		go func() {
			// todo: there's no need to wait for the import to finish, we can just return here.
			// error is stored in system table, so we can ignore it here
			//nolint: errcheck
			_ = e.doImport(distImporter, task)
			failpoint.Inject("testDetachedTaskFinished", func() {
				TestDetachedTaskFinished.Store(true)
			})
		}()
		return e.fillJobInfo(ctx, jobID, req)
	}
	if err = e.doImport(distImporter, task); err != nil {
		return err
	}
	return e.fillJobInfo(ctx, jobID, req)
}

func (e *ImportIntoExec) fillJobInfo(ctx context.Context, jobID int64, req *chunk.Chunk) error {
	e.dataFilled = true
	// we use globalTaskManager to get job, user might not have the privilege to system tables.
	globalTaskManager, err := fstorage.GetTaskManager()
	if err != nil {
		return err
	}
	var info *importer.JobInfo
	if err = globalTaskManager.WithNewSession(func(se sessionctx.Context) error {
		sqlExec := se.(sqlexec.SQLExecutor)
		var err2 error
		info, err2 = importer.GetJob(ctx, sqlExec, jobID, e.ctx.GetSessionVars().User.String(), false)
		return err2
	}); err != nil {
		return err
	}
	fillOneImportJobInfo(info, req, -1)
	return nil
}

func (e *ImportIntoExec) getJobImporter(param *importer.JobImportParam) (*loaddata.DistImporter, error) {
	importFromServer, err := storage.IsLocalPath(e.controller.Path)
	if err != nil {
		// since we have checked this during creating controller, this should not happen.
		return nil, exeerrors.ErrLoadDataInvalidURI.FastGenByArgs(err.Error())
	}
	if importFromServer {
		ecp, err2 := e.controller.PopulateChunks(param.GroupCtx)
		if err2 != nil {
			return nil, err2
		}
		return loaddata.NewDistImporterServerFile(param, e.importPlan, e.stmt, ecp, e.controller.TotalFileSize)
	}
	// if tidb_enable_dist_task=true, we import distributively, otherwise we import on current node.
	if variable.EnableDistTask.Load() {
		return loaddata.NewDistImporter(param, e.importPlan, e.stmt, e.controller.TotalFileSize)
	}
	return loaddata.NewDistImporterCurrNode(param, e.importPlan, e.stmt, e.controller.TotalFileSize)
}

func (e *ImportIntoExec) doImport(distImporter *loaddata.DistImporter, task *proto.Task) error {
	distImporter.ImportTask(task)
	group := distImporter.Param().Group
	return group.Wait()
}

// ImportIntoActionExec represents a import into action executor.
type ImportIntoActionExec struct {
	baseExecutor
	tp    ast.ImportIntoActionTp
	jobID int64
}

var (
	_ Executor = (*ImportIntoActionExec)(nil)
)

// Next implements the Executor Next interface.
func (e *ImportIntoActionExec) Next(ctx context.Context, _ *chunk.Chunk) error {
	ctx = kv.WithInternalSourceType(ctx, kv.InternalImportInto)

	var hasSuperPriv bool
	if pm := privilege.GetPrivilegeManager(e.ctx); pm != nil {
		hasSuperPriv = pm.RequestVerification(e.ctx.GetSessionVars().ActiveRoles, "", "", "", mysql.SuperPriv)
	}
	// we use sessionCtx from GetTaskManager, user ctx might not have enough privileges.
	globalTaskManager, err := fstorage.GetTaskManager()
	if err != nil {
		return err
	}
	if err = e.checkPrivilegeAndStatus(ctx, globalTaskManager, hasSuperPriv); err != nil {
		return err
	}

	log.L().Info("import into action", zap.Int64("jobID", e.jobID), zap.Any("action", e.tp))
	// todo: validating step is not run in a subtask, the framework don't support cancel it, we can make it run in a subtask later.
	// todo: cancel is async operation, we don't wait here now, maybe add a wait syntax later.
	// todo: after CANCEL, user can see the job status is Canceled immediately, but the job might still running.
	// and the state of framework task might became finished since framework don't force state change DAG when update task.
	// todo: add a CANCELLING status?
	return globalTaskManager.WithNewTxn(func(se sessionctx.Context) error {
		exec := se.(sqlexec.SQLExecutor)
		if err2 := importer.CancelJob(ctx, exec, e.jobID); err2 != nil {
			return err2
		}
		return globalTaskManager.CancelGlobalTaskByKeySession(se, loaddata.TaskKey(e.jobID))
	})
}

func (e *ImportIntoActionExec) checkPrivilegeAndStatus(ctx context.Context, manager *fstorage.TaskManager, hasSuperPriv bool) error {
	var info *importer.JobInfo
	if err := manager.WithNewSession(func(se sessionctx.Context) error {
		exec := se.(sqlexec.SQLExecutor)
		var err2 error
		info, err2 = importer.GetJob(ctx, exec, e.jobID, e.ctx.GetSessionVars().User.String(), hasSuperPriv)
		return err2
	}); err != nil {
		return err
	}
	if !info.CanCancel() {
		return exeerrors.ErrLoadDataInvalidOperation.FastGenByArgs("CANCEL")
	}
	return nil
}
