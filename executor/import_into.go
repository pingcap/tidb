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
	"sync/atomic"

	"github.com/google/uuid"
	"github.com/pingcap/tidb/executor/asyncloaddata"
	"github.com/pingcap/tidb/executor/importer"
	"github.com/pingcap/tidb/kv"
	plannercore "github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessiontxn"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/sqlexec"
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
	baseExecutor
	selectExec Executor
	userSctx   sessionctx.Context
	importPlan *importer.Plan
	controller *importer.LoadDataController
	stmt       string

	dataFilled bool
}

var (
	_ Executor = (*ImportIntoExec)(nil)
)

func newImportIntoExec(b baseExecutor, selectExec Executor, userSctx sessionctx.Context, plan *plannercore.ImportInto, tbl table.Table) (*ImportIntoExec, error) {
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
		selectExec:   selectExec,
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
	return e.importFromSelect(ctx)
}

func (e *ImportIntoExec) importFromSelect(ctx context.Context) error {
	e.dataFilled = true
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

	group, groupCtx := errgroup.WithContext(ctx)
	param := &importer.JobImportParam{
		Job:      &asyncloaddata.Job{},
		Group:    group,
		GroupCtx: groupCtx,
		Done:     make(chan struct{}),
		Progress: asyncloaddata.NewProgress(false),
	}
	importID := uuid.New().String()
	logutil.Logger(ctx).Info("importing data from select statement",
		zap.String("importID", importID))
	ti, err2 := importer.NewTableImporter(param, e.controller, importID)
	if err2 != nil {
		return err2
	}
	defer ti.Close()
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
		fields := retTypes(e.selectExec)
		chk := tryNewCacheChunk(e.selectExec)
		iter := chunk.NewIterator4Chunk(chk)
		var idAllocator int64
		for {
			err := Next(egCtx, e.selectExec, chk)
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

	if err2 = flushStats(ctx, e.userSctx, e.importPlan.TableInfo.ID, importResult); err2 != nil {
		logutil.Logger(ctx).Error("flush stats failed", zap.Error(err2))
	}

	stmtCtx := e.userSctx.GetSessionVars().StmtCtx
	stmtCtx.SetAffectedRows(importResult.Affected)
	stmtCtx.SetMessage(fmt.Sprintf("Records: %d", importResult.Affected))
	return nil
}

// flushStats flushes the stats of the table.
func flushStats(ctx context.Context, se sessionctx.Context, tableID int64, result *importer.JobImportResult) error {
	if err := sessiontxn.NewTxn(ctx, se); err != nil {
		return err
	}
	sessionVars := se.GetSessionVars()
	sessionVars.TxnCtxMu.Lock()
	defer sessionVars.TxnCtxMu.Unlock()
	sessionVars.TxnCtx.UpdateDeltaForTable(tableID, int64(result.Affected), int64(result.Affected), result.ColSizeMap)
	se.StmtCommit()
	return se.CommitTxn(ctx)
}
