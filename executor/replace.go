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
	"github.com/juju/errors"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/table/tables"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"golang.org/x/net/context"
)

// ReplaceExec represents a replace executor.
type ReplaceExec struct {
	*InsertValues
	Priority int
	finished bool
}

// Close implements the Executor Close interface.
func (e *ReplaceExec) Close() error {
	if e.SelectExec != nil {
		return e.SelectExec.Close()
	}
	return nil
}

// Open implements the Executor Open interface.
func (e *ReplaceExec) Open(ctx context.Context) error {
	if e.SelectExec != nil {
		return e.SelectExec.Open(ctx)
	}
	return nil
}

// removeRow removes the duplicate row and cleanup its keys in the key-value map,
// but if the to-be-removed row equals to the to-be-added row, no remove or add things to do.
func (e *ReplaceExec) removeRow(handle int64, r toBeCheckedRow) (bool, error) {
	newRow := r.row
	oldRow, err := e.batchChecker.getOldRow(e.ctx, r.t, handle)
	if err != nil {
		return false, errors.Trace(err)
	}
	rowUnchanged, err := types.EqualDatums(e.ctx.GetSessionVars().StmtCtx, oldRow, newRow)
	if err != nil {
		return false, errors.Trace(err)
	}
	if rowUnchanged {
		e.ctx.GetSessionVars().StmtCtx.AddAffectedRows(1)
		return true, nil
	}

	err = r.t.RemoveRecord(e.ctx, handle, oldRow)
	if err != nil {
		return false, errors.Trace(err)
	}
	e.ctx.GetSessionVars().StmtCtx.AddAffectedRows(1)

	// Cleanup keys map, because the record was removed.
	cleanupRows, err := e.getKeysNeedCheck(e.ctx, r.t, [][]types.Datum{oldRow})
	if err != nil {
		return false, errors.Trace(err)
	}
	if len(cleanupRows) > 0 {
		// The length of need-to-cleanup rows should be at most 1, due to we only input 1 row.
		e.deleteDupKeys(cleanupRows[0])
	}
	return false, nil
}

// addRow adds a row when all the duplicate key were checked.
func (e *ReplaceExec) addRow(row []types.Datum) (int64, error) {
	// Set kv.PresumeKeyNotExists is safe here, because we've already removed all duplicated rows.
	e.ctx.Txn().SetOption(kv.PresumeKeyNotExists, nil)
	h, err := e.Table.AddRecord(e.ctx, row, false)
	e.ctx.Txn().DelOption(kv.PresumeKeyNotExists)
	if err != nil {
		return 0, errors.Trace(err)
	}
	return h, nil
}

// replaceRow removes all duplicate rows for one row, then inserts it.
func (e *ReplaceExec) replaceRow(r toBeCheckedRow) error {
	if r.handleKey != nil {
		if _, found := e.dupKVs[string(r.handleKey.newKV.key)]; found {
			handle, err := tablecodec.DecodeRowKey(r.handleKey.newKV.key)
			if err != nil {
				return errors.Trace(err)
			}
			rowUnchanged, err := e.removeRow(handle, r)
			if err != nil {
				return errors.Trace(err)
			}
			if rowUnchanged {
				return nil
			}
		}
	}

	// Keep on removing duplicated rows.
	for {
		rowUnchanged, foundDupKey, err := e.removeIndexRow(r)
		if err != nil {
			return errors.Trace(err)
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
	newHandle, err := e.addRow(r.row)
	if err != nil {
		return errors.Trace(err)
	}
	e.fillBackKeys(r.t, r, newHandle)
	return nil
}

// removeIndexRow removes the row which has a duplicated key.
// the return values:
//     1. bool: true when the row is unchanged. This means no need to remove, and then add the row.
//     2. bool: true when found the duplicated key. This only means that duplicated key was found,
//              and the row was removed.
//     3. error: the error.
func (e *ReplaceExec) removeIndexRow(r toBeCheckedRow) (bool, bool, error) {
	for _, uk := range r.uniqueKeys {
		if val, found := e.dupKVs[string(uk.newKV.key)]; found {
			handle, err := tables.DecodeHandle(val)
			if err != nil {
				return false, found, errors.Trace(err)
			}
			rowUnchanged, err := e.removeRow(handle, r)
			if err != nil {
				return false, found, errors.Trace(err)
			}
			return rowUnchanged, found, nil
		}
	}
	return false, false, nil
}

func (e *ReplaceExec) exec(newRows [][]types.Datum) error {
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
	err := e.batchGetInsertKeys(e.ctx, e.Table, newRows)
	if err != nil {
		return errors.Trace(err)
	}

	// Batch get the to-be-replaced rows in storage.
	err = e.initDupOldRowValue(e.ctx, e.Table, newRows)
	if err != nil {
		return errors.Trace(err)
	}

	for _, r := range e.toBeCheckedRows {
		err = e.replaceRow(r)
		if err != nil {
			return errors.Trace(err)
		}
	}
	if e.lastInsertID != 0 {
		e.ctx.GetSessionVars().SetLastInsertID(e.lastInsertID)
	}
	e.finished = true
	return nil
}

// Next implements the Executor Next interface.
func (e *ReplaceExec) Next(ctx context.Context, chk *chunk.Chunk) error {
	chk.Reset()
	if e.finished {
		return nil
	}
	cols, err := e.getColumns(e.Table.Cols())
	if err != nil {
		return errors.Trace(err)
	}

	if len(e.children) > 0 && e.children[0] != nil {
		return errors.Trace(e.insertRowsFromSelect(ctx, cols, e.exec))
	}
	return errors.Trace(e.insertRows(cols, e.exec))
}
