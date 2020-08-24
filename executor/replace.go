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
// See the License for the specific language governing permissions and
// limitations under the License.

package executor

import (
	"context"
	"fmt"
	"runtime/trace"

	"github.com/pingcap/errors"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/memory"
	"go.uber.org/zap"
)

// ReplaceExec represents a replace executor.
type ReplaceExec struct {
	*InsertValues
	Priority int
}

// Close implements the Executor Close interface.
func (e *ReplaceExec) Close() error {
	e.setMessage()
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

// removeRow removes the duplicate row and cleanup its keys in the key-value map,
// but if the to-be-removed row equals to the to-be-added row, no remove or add things to do.
func (e *ReplaceExec) removeRow(ctx context.Context, txn kv.Transaction, handle kv.Handle, r toBeCheckedRow) (bool, error) {
	newRow := r.row
	oldRow, err := getOldRow(ctx, e.ctx, txn, r.t, handle, e.GenExprs)
	if err != nil {
		logutil.BgLogger().Error("get old row failed when replace",
			zap.String("handle", handle.String()),
			zap.String("toBeInsertedRow", types.DatumsToStrNoErr(r.row)))
		if kv.IsErrNotFound(err) {
			err = errors.NotFoundf("can not be duplicated row, due to old row not found. handle %s", handle)
		}
		return false, err
	}

	rowUnchanged, err := types.EqualDatums(e.ctx.GetSessionVars().StmtCtx, oldRow, newRow)
	if err != nil {
		return false, err
	}
	if rowUnchanged {
		e.ctx.GetSessionVars().StmtCtx.AddAffectedRows(1)
		return true, nil
	}

	err = r.t.RemoveRecord(e.ctx, handle, oldRow)
	if err != nil {
		return false, err
	}
	e.ctx.GetSessionVars().StmtCtx.AddAffectedRows(1)
	return false, nil
}

// replaceRow removes all duplicate rows for one row, then inserts it.
func (e *ReplaceExec) replaceRow(ctx context.Context, r toBeCheckedRow) error {
	txn, err := e.ctx.Txn(true)
	if err != nil {
		return err
	}

	if r.handleKey != nil {
		handle, err := tablecodec.DecodeRowKey(r.handleKey.newKV.key)
		if err != nil {
			return err
		}

		if _, err := txn.Get(ctx, r.handleKey.newKV.key); err == nil {
			rowUnchanged, err := e.removeRow(ctx, txn, handle, r)
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
//     1. bool: true when the row is unchanged. This means no need to remove, and then add the row.
//     2. bool: true when found the duplicated key. This only means that duplicated key was found,
//              and the row was removed.
//     3. error: the error.
func (e *ReplaceExec) removeIndexRow(ctx context.Context, txn kv.Transaction, r toBeCheckedRow) (bool, bool, error) {
	for _, uk := range r.uniqueKeys {
		val, err := txn.Get(ctx, uk.newKV.key)
		if err != nil {
			if kv.IsErrNotFound(err) {
				continue
			}
			return false, false, err
		}
		handle, err := tablecodec.DecodeHandleInUniqueIndexValue(val, uk.commonHandle)
		if err != nil {
			return false, true, err
		}
		rowUnchanged, err := e.removeRow(ctx, txn, handle, r)
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
	txnSize := txn.Size()

	// Use BatchGet to fill cache.
	// It's an optimization and could be removed without affecting correctness.
	if err = prefetchDataCache(ctx, txn, toBeCheckedRows); err != nil {
		return err
	}

	e.ctx.GetSessionVars().StmtCtx.AddRecordRows(uint64(len(newRows)))
	for _, r := range toBeCheckedRows {
		err = e.replaceRow(ctx, r)
		if err != nil {
			return err
		}
	}
	e.memTracker.Consume(int64(txn.Size() - txnSize))
	return nil
}

// Next implements the Executor Next interface.
func (e *ReplaceExec) Next(ctx context.Context, req *chunk.Chunk) error {
	req.Reset()
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
		msg := fmt.Sprintf(mysql.MySQLErrName[mysql.ErrInsertInfo], numRecords, numDuplicates, numWarnings)
		stmtCtx.SetMessage(msg)
	}
}
