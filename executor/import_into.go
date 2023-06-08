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
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/disttask/framework/proto"
	"github.com/pingcap/tidb/disttask/loaddata"
	"github.com/pingcap/tidb/executor/asyncloaddata"
	"github.com/pingcap/tidb/executor/importer"
	"github.com/pingcap/tidb/kv"
	plannercore "github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/sessiontxn"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/dbterror/exeerrors"
	"github.com/pingcap/tidb/util/logutil"
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

	detachHandled bool
}

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
	if e.detachHandled {
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
	task, err := distImporter.SubmitTask()
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
			// todo: there's no need to wait for the import to finish, we can just return here.
			// error is stored in system table, so we can ignore it here
			//nolint: errcheck
			_ = e.doImport(ctx, se, distImporter, task)
			failpoint.Inject("testDetachedTaskFinished", func() {
				TestDetachedTaskFinished.Store(true)
			})
		}()
		req.AppendInt64(0, task.ID)
		e.detachHandled = true
		return nil
	}
	return e.doImport(ctx, e.userSctx, distImporter, task)
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
		return loaddata.NewDistImporterServerFile(param, e.importPlan, e.stmt, ecp)
	}
	// if tidb_enable_dist_task=true, we import distributively, otherwise we import on current node.
	if variable.EnableDistTask.Load() {
		return loaddata.NewDistImporter(param, e.importPlan, e.stmt)
	}
	return loaddata.NewDistImporterCurrNode(param, e.importPlan, e.stmt)
}

func (e *ImportIntoExec) doImport(ctx context.Context, se sessionctx.Context, distImporter *loaddata.DistImporter, task *proto.Task) error {
	distImporter.ImportTask(task)
	group := distImporter.Param().Group
	err := group.Wait()
	if !e.controller.Detached {
		importResult := distImporter.Result(task)
		userStmtCtx := e.userSctx.GetSessionVars().StmtCtx
		userStmtCtx.SetMessage(importResult.Msg)
		userStmtCtx.SetAffectedRows(importResult.Affected)
	}
	if err2 := flushStats(ctx, se, e.importPlan.TableInfo.ID, distImporter.Result(task)); err2 != nil {
		logutil.BgLogger().Error("flush stats failed", zap.Error(err2))
	}
	return err
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
