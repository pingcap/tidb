// Copyright 2018 PingCAP, Inc.
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
	"runtime/trace"
	"time"

	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta/autoid"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/table/tables"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/memory"
)

// ReplaceExec represents a replace executor.
type ReplaceExec struct {
	*InsertValues
	Priority int
}

// Close implements the Executor Close interface.
func (e *ReplaceExec) Close() error {
	e.setMessage()
	if e.runtimeStats != nil && e.stats != nil {
		defer e.ctx.GetSessionVars().StmtCtx.RuntimeStatsColl.RegisterStats(e.id, e.stats)
	}
	if e.SelectExec != nil {
		return e.SelectExec.Close()
	}
	return nil
}

// Open implements the Executor Open interface.
func (e *ReplaceExec) Open(ctx context.Context) error {
	e.memTracker = memory.NewTracker(e.id, -1)
	e.memTracker.AttachTo(e.ctx.GetSessionVars().StmtCtx.MemTracker)

	if e.SelectExec != nil {
		return e.SelectExec.Open(ctx)
	}
	e.initEvalBuffer()
	return nil
}

// replaceRow removes all duplicate rows for one row, then inserts it.
func (e *ReplaceExec) replaceRow(ctx context.Context, r toBeCheckedRow) error {
	txn, err := e.ctx.Txn(true)
	if err != nil {
		return err
	}

	if r.handleKey != nil {
		handle, err := tablecodec.DecodeRowKey(r.handleKey.newKey)
		if err != nil {
			return err
		}

		if _, err := txn.Get(ctx, r.handleKey.newKey); err == nil {
			rowUnchanged, err := e.removeRow(ctx, txn, handle, r, true)
			if err != nil {
				return err
			}
			if rowUnchanged {
				return nil
			}
		} else {
			if !kv.IsErrNotFound(err) {
				return err
			}
		}
	}

	// Keep on removing duplicated rows.
	for {
		rowUnchanged, foundDupKey, err := e.removeIndexRow(ctx, txn, r)
		if err != nil {
			return err
		}
		if rowUnchanged {
			return nil
		}
		if foundDupKey {
			continue
		}
		break
	}

	// No duplicated rows now, insert the row.
	err = e.addRecord(ctx, r.row)
	if err != nil {
		return err
	}
	return nil
}

// removeIndexRow removes the row which has a duplicated key.
// the return values:
//  1. bool: true when the row is unchanged. This means no need to remove, and then add the row.
//  2. bool: true when found the duplicated key. This only means that duplicated key was found,
//     and the row was removed.
//  3. error: the error.
func (e *ReplaceExec) removeIndexRow(ctx context.Context, txn kv.Transaction, r toBeCheckedRow) (bool, bool, error) {
	for _, uk := range r.uniqueKeys {
		_, handle, err := tables.FetchDuplicatedHandle(ctx, uk.newKey, true, txn, e.Table.Meta().ID, uk.commonHandle)
		if err != nil {
			return false, false, err
		}
		if handle == nil {
			continue
		}
		rowUnchanged, err := e.removeRow(ctx, txn, handle, r, true)
		if err != nil {
			return false, true, err
		}
		return rowUnchanged, true, nil
	}
	return false, false, nil
}

func (e *ReplaceExec) exec(ctx context.Context, newRows [][]types.Datum) error {
	/*
	 * MySQL uses the following algorithm for REPLACE (and LOAD DATA ... REPLACE):
	 *  1. Try to insert the new row into the table
	 *  2. While the insertion fails because a duplicate-key error occurs for a primary key or unique index:
	 *  3. Delete from the table the conflicting row that has the duplicate key value
	 *  4. Try again to insert the new row into the table
	 * See http://dev.mysql.com/doc/refman/5.7/en/replace.html
	 *
	 * For REPLACE statements, the affected-rows value is 2 if the new row replaced an old row,
	 * because in this case, one row was inserted after the duplicate was deleted.
	 * See http://dev.mysql.com/doc/refman/5.7/en/mysql-affected-rows.html
	 */

	defer trace.StartRegion(ctx, "ReplaceExec").End()
	// Get keys need to be checked.
	toBeCheckedRows, err := getKeysNeedCheck(ctx, e.ctx, e.Table, newRows)
	if err != nil {
		return err
	}

	txn, err := e.ctx.Txn(true)
	if err != nil {
		return err
	}

	if e.collectRuntimeStatsEnabled() {
		if snapshot := txn.GetSnapshot(); snapshot != nil {
			snapshot.SetOption(kv.CollectRuntimeStats, e.stats.SnapshotRuntimeStats)
			defer snapshot.SetOption(kv.CollectRuntimeStats, nil)
		}
	}
	setOptionForTopSQL(e.ctx.GetSessionVars().StmtCtx, txn)
	prefetchStart := time.Now()
	// Use BatchGet to fill cache.
	// It's an optimization and could be removed without affecting correctness.
	if err = e.prefetchDataCache(ctx, txn, toBeCheckedRows); err != nil {
		return err
	}

	if e.stats != nil {
		e.stats.Prefetch = time.Since(prefetchStart)
	}
	e.ctx.GetSessionVars().StmtCtx.AddRecordRows(uint64(len(newRows)))
	for _, r := range toBeCheckedRows {
		err = e.replaceRow(ctx, r)
		if err != nil {
			return err
		}
	}
	return nil
}

// Next implements the Executor Next interface.
func (e *ReplaceExec) Next(ctx context.Context, req *chunk.Chunk) error {
	req.Reset()
	if e.collectRuntimeStatsEnabled() {
		ctx = context.WithValue(ctx, autoid.AllocatorRuntimeStatsCtxKey, e.stats.AllocatorRuntimeStats)
	}

	if len(e.children) > 0 && e.children[0] != nil {
		return insertRowsFromSelect(ctx, e)
	}
	return insertRows(ctx, e)
}

// setMessage sets info message(ERR_INSERT_INFO) generated by REPLACE statement
func (e *ReplaceExec) setMessage() {
	stmtCtx := e.ctx.GetSessionVars().StmtCtx
	numRecords := stmtCtx.RecordRows()
	if e.SelectExec != nil || numRecords > 1 {
		numWarnings := stmtCtx.WarningCount()
		numDuplicates := stmtCtx.AffectedRows() - numRecords
		msg := fmt.Sprintf(mysql.MySQLErrName[mysql.ErrInsertInfo].Raw, numRecords, numDuplicates, numWarnings)
		stmtCtx.SetMessage(msg)
	}
}

// GetFKChecks implements WithForeignKeyTrigger interface.
func (e *ReplaceExec) GetFKChecks() []*FKCheckExec {
	return e.fkChecks
}

// GetFKCascades implements WithForeignKeyTrigger interface.
func (e *ReplaceExec) GetFKCascades() []*FKCascadeExec {
	return e.fkCascades
}

// HasFKCascades implements WithForeignKeyTrigger interface.
func (e *ReplaceExec) HasFKCascades() bool {
	return len(e.fkCascades) > 0
}