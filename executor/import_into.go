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
	"github.com/pingcap/tidb/disttask/framework/proto"
	"github.com/pingcap/tidb/disttask/framework/storage"
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

	sqlExec := e.userSctx.(sqlexec.SQLExecutor)
	job := importer.NewJob(jobID, sqlExec, e.ctx.GetSessionVars().User.String(), false)
	info, err := job.Get(ctx)
	if err != nil {
		return err
	}
	fillOneImportJobInfo(info, req, 0)
	return nil
}

func (e *ImportIntoExec) getJobImporter(param *importer.JobImportParam) (*loaddata.DistImporter, error) {
	// if tidb_enable_dist_task=true, we import distributively, otherwise we import on current node.
	// todo: if we import from local directory, we should also import on current node.
	if variable.EnableDistTask.Load() {
		return loaddata.NewDistImporter(param, e.importPlan, e.stmt, e.controller.TotalFileSize)
	}
	return loaddata.NewDistImporterCurrNode(param, e.importPlan, e.stmt, e.controller.TotalFileSize)
}

func (e *ImportIntoExec) doImport(distImporter *loaddata.DistImporter, task *proto.Task) error {
	distImporter.ImportTask(task)
	group := distImporter.Param().Group
	err := group.Wait()
	if !e.controller.Detached {
		importResult := distImporter.Result()
		userStmtCtx := e.userSctx.GetSessionVars().StmtCtx
		userStmtCtx.SetMessage(importResult.Msg)
		userStmtCtx.SetAffectedRows(importResult.Affected)
	}
	return err
}

type ImportIntoActionExec struct {
	baseExecutor
	tp    ast.ImportIntoActionTp
	jobID int64
}

var (
	_ Executor = (*ImportIntoActionExec)(nil)
)

func (e *ImportIntoActionExec) Next(ctx context.Context, _ *chunk.Chunk) error {
	ctx = kv.WithInternalSourceType(ctx, kv.InternalImportInto)

	var hasSuperPriv bool
	if pm := privilege.GetPrivilegeManager(e.ctx); pm != nil {
		hasSuperPriv = pm.RequestVerification(e.ctx.GetSessionVars().ActiveRoles, "", "", "", mysql.SuperPriv)
	}
	if err := e.checkPrivilegeAndStatus(ctx, hasSuperPriv); err != nil {
		return err
	}

	// we use sessionCtx from GetTaskManager, user ctx might not have enough privileges.
	globalTaskManager, err := storage.GetTaskManager()
	if err != nil {
		return err
	}
	return globalTaskManager.WithNewTxn(func(se sessionctx.Context) error {
		exec := e.ctx.(sqlexec.SQLExecutor)
		if err2 := importer.CancelJob(ctx, exec, e.jobID); err2 != nil {
			return err2
		}
		return globalTaskManager.CancelGlobalTaskByKeySession(se, loaddata.TaskKey(e.jobID))
	})
}

func (e *ImportIntoActionExec) checkPrivilegeAndStatus(ctx context.Context, hasSuperPriv bool) error {
	exec := e.ctx.(sqlexec.SQLExecutor)
	job := importer.NewJob(e.jobID, exec, e.ctx.GetSessionVars().User.String(), hasSuperPriv)
	info, err := job.Get(ctx)
	if err != nil {
		return err
	}
	if !info.CanCancel() {
		return exeerrors.ErrLoadDataInvalidOperation.FastGenByArgs("CANCEL")
	}
	return nil
}
