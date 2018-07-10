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

func (e *ReplaceExec) removeRow(handle int64, newRow types.DatumRow) error {
	oldRow, err := e.decodeOldRow(e.ctx, e.Table, handle)
	if err != nil {
		return errors.Trace(err)
	}
	rowUnchanged, err := types.EqualDatums(e.ctx.GetSessionVars().StmtCtx, oldRow, newRow)
	if err != nil {
		return errors.Trace(err)
	}
	err = e.Table.RemoveRecord(e.ctx, handle, oldRow)
	if err != nil {
		return errors.Trace(err)
	}
	e.ctx.StmtAddDirtyTableOP(DirtyTableDeleteRow, e.Table.Meta().ID, handle, nil)
	if !rowUnchanged {
		e.ctx.GetSessionVars().StmtCtx.AddAffectedRows(1)
	}

	// Cleanup keys map, because the record was removed.
	cleanupRows, err := e.getKeysNeedCheck(e.ctx, e.Table, []types.DatumRow{oldRow})
	if err != nil {
		return errors.Trace(err)
	}
	if len(cleanupRows) > 0 {
		// The length of need-to-cleanup rows should be at most 1, due to we only input 1 row.
		e.deleteDupKeys(cleanupRows[0])
	}
	return nil
}

func (e *ReplaceExec) insertRow(row types.DatumRow) (int64, error) {
	// Set kv.PresumeKeyNotExists is safe here, because we've already removed all duplicated rows.
	e.ctx.Txn().SetOption(kv.PresumeKeyNotExists, nil)
	h, err := e.Table.AddRecord(e.ctx, row, false)
	e.ctx.Txn().DelOption(kv.PresumeKeyNotExists)
	if err != nil {
		return 0, errors.Trace(err)
	}
	if !e.ctx.GetSessionVars().ImportingData {
		e.ctx.StmtAddDirtyTableOP(DirtyTableAddRow, e.Table.Meta().ID, h, row)
	}
	return h, nil
}

func (e *ReplaceExec) exec(newRows []types.DatumRow) error {
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

	for i, r := range e.toBeCheckedRows {
		for {
			if r.handleKey != nil {
				if _, found := e.dupKVs[string(r.handleKey.newKV.key)]; found {
					handle, err := tablecodec.DecodeRowKey(r.handleKey.newKV.key)
					if err != nil {
						return errors.Trace(err)
					}
					err = e.removeRow(handle, newRows[i])
					if err != nil {
						return errors.Trace(err)
					}
					continue
				}
			}

			foundDupKey := false
			for _, uk := range r.uniqueKeys {
				if val, found := e.dupKVs[string(uk.newKV.key)]; found {
					handle, err := tables.DecodeHandle(val)
					if err != nil {
						return errors.Trace(err)
					}
					err = e.removeRow(handle, newRows[i])
					if err != nil {
						return errors.Trace(err)
					}
					foundDupKey = true
					break
				}
			}
			if foundDupKey {
				continue
			}
			// No duplicated rows now, insert the row and go to the next row.
			newHandle, err := e.insertRow(newRows[i])
			if err != nil {
				return errors.Trace(err)
			}
			e.fillBackKeys(e.Table, r, newHandle)
			break
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
