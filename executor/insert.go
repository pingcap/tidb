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
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/table/tables"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"golang.org/x/net/context"
)

// InsertExec represents an insert executor.
type InsertExec struct {
	*InsertValues

	OnDuplicate []*expression.Assignment

	Priority mysql.PriorityEnum

	finished bool
}

func (e *InsertExec) insertOneRow(row types.DatumRow) (int64, error) {
	if err := e.checkBatchLimit(); err != nil {
		return 0, errors.Trace(err)
	}
	e.ctx.Txn().SetOption(kv.PresumeKeyNotExists, nil)
	h, err := e.Table.AddRecord(e.ctx, row, false)
	e.ctx.Txn().DelOption(kv.PresumeKeyNotExists)
	if err != nil {
		return 0, errors.Trace(err)
	}
	if !e.ctx.GetSessionVars().ImportingData {
		e.ctx.StmtAddDirtyTableOP(DirtyTableAddRow, e.Table.Meta().ID, h, row)
	}
	e.rowCount++
	return h, nil
}

func (e *InsertExec) exec(rows []types.DatumRow) error {
	// If tidb_batch_insert is ON and not in a transaction, we could use BatchInsert mode.
	sessVars := e.ctx.GetSessionVars()
	defer sessVars.CleanBuffers()
	ignoreErr := sessVars.StmtCtx.IgnoreErr

	e.rowCount = 0
	if !sessVars.ImportingData {
		sessVars.GetWriteStmtBufs().BufStore = kv.NewBufferStore(e.ctx.Txn(), kv.TempTxnMemBufCap)
	}

	// If you use the IGNORE keyword, duplicate-key error that occurs while executing the INSERT statement are ignored.
	// For example, without IGNORE, a row that duplicates an existing UNIQUE index or PRIMARY KEY value in
	// the table causes a duplicate-key error and the statement is aborted. With IGNORE, the row is discarded and no error occurs.
	// However, if the `on duplicate update` is also specified, the duplicated row will be updated.
	// Using BatchGet in insert ignore to mark rows as duplicated before we add records to the table.
	// If `ON DUPLICATE KEY UPDATE` is specified, and no `IGNORE` keyword,
	// the to-be-insert rows will be check on duplicate keys and update to the new rows.
	if len(e.OnDuplicate) > 0 {
		err := e.batchUpdateDupRows(rows, ignoreErr)
		if err != nil {
			return errors.Trace(err)
		}
	} else if ignoreErr {
		err := e.batchCheckAndInsert(rows, e.insertOneRow)
		if err != nil {
			return errors.Trace(err)
		}
	} else {
		for _, row := range rows {
			if _, err := e.insertOneRow(row); err != nil {
				return errors.Trace(err)
			}
		}
	}
	if e.lastInsertID != 0 {
		sessVars.SetLastInsertID(e.lastInsertID)
	}
	e.finished = true
	return nil
}

// checkBatchLimit check the batchSize limitation.
func (e *InsertExec) checkBatchLimit() error {
	sessVars := e.ctx.GetSessionVars()
	batchInsert := sessVars.BatchInsert && !sessVars.InTxn()
	batchSize := sessVars.DMLBatchSize
	if batchInsert && e.rowCount >= uint64(batchSize) {
		e.ctx.StmtCommit()
		if err := e.ctx.NewTxn(); err != nil {
			// We should return a special error for batch insert.
			return ErrBatchInsertFail.Gen("BatchInsert failed with error: %v", err)
		}
		e.rowCount = 0
		if !sessVars.ImportingData {
			sessVars.GetWriteStmtBufs().BufStore = kv.NewBufferStore(e.ctx.Txn(), kv.TempTxnMemBufCap)
		}
	}
	return nil
}

// batchUpdateDupRows updates multi-rows in batch if they are duplicate with rows in table.
func (e *InsertExec) batchUpdateDupRows(newRows []types.DatumRow, ignoreErr bool) error {
	err := e.batchGetInsertKeys(e.ctx, e.Table, newRows)
	if err != nil {
		return errors.Trace(err)
	}

	// Batch get the to-be-updated rows in storage.
	err = e.initDupOldRowValue(e.ctx, e.Table, newRows)
	if err != nil {
		return errors.Trace(err)
	}

	for i, r := range e.toBeCheckedRows {
		if r.handleKey != nil {
			if _, found := e.dupKeyValues[string(r.handleKey.newKeyValue.key)]; found {
				handle, err := tablecodec.DecodeRowKey(r.handleKey.newKeyValue.key)
				if err != nil {
					return errors.Trace(err)
				}
				err = e.updateDupRow(handle, newRows[i], e.OnDuplicate)
				if ignoreErr && kv.ErrKeyExists.Equal(err) {
					e.ctx.GetSessionVars().StmtCtx.AppendWarning(err)
					continue
				}
				if err != nil {
					return errors.Trace(err)
				}
				delete(e.dupKeyValues, string(r.handleKey.newKeyValue.key))
				newRows[i] = nil
				continue
			}
		}
		for _, uk := range r.uniqueKeys {
			if val, found := e.dupKeyValues[string(uk.newKeyValue.key)]; found {
				handle, err := tables.DecodeHandle(val)
				if err != nil {
					return errors.Trace(err)
				}
				err = e.updateDupRow(handle, newRows[i], e.OnDuplicate)
				if ignoreErr && kv.ErrKeyExists.Equal(err) {
					e.ctx.GetSessionVars().StmtCtx.AppendWarning(err)
					newRows[i] = nil
					break
				}
				if err != nil {
					return errors.Trace(err)
				}
				delete(e.dupKeyValues, string(uk.newKeyValue.key))
				newRows[i] = nil
				break
			}
		}
		// If row was checked with no duplicate keys,
		// we should do insert the row,
		// and key-values should be filled back to dupOldRowValues for the further row check,
		// due to there may be duplicate keys inside the insert statement.
		if newRows[i] != nil {
			newHandle, err := e.insertOneRow(newRows[i])
			if err != nil {
				return errors.Trace(err)
			}
			e.fillBackKeys(e.Table, r, newHandle)
		}
	}
	return nil
}

// Next implements Exec Next interface.
func (e *InsertExec) Next(ctx context.Context, chk *chunk.Chunk) error {
	chk.Reset()
	if e.finished {
		return nil
	}
	cols, err := e.getColumns(e.Table.Cols())
	if err != nil {
		return errors.Trace(err)
	}

	var rows []types.DatumRow
	if len(e.children) > 0 && e.children[0] != nil {
		rows, err = e.getRowsSelectChunk(ctx, cols)
	} else {
		rows, err = e.getRows(cols)
	}
	if err != nil {
		return errors.Trace(err)
	}

	return errors.Trace(e.exec(rows))
}

// Close implements the Executor Close interface.
func (e *InsertExec) Close() error {
	e.ctx.GetSessionVars().CurrInsertValues = nil
	if e.SelectExec != nil {
		return e.SelectExec.Close()
	}
	return nil
}

// Open implements the Executor Close interface.
func (e *InsertExec) Open(ctx context.Context) error {
	if e.SelectExec != nil {
		return e.SelectExec.Open(ctx)
	}
	return nil
}

// updateDupRow updates a duplicate row to a new row.
func (e *InsertExec) updateDupRow(handle int64, newRow types.DatumRow, onDuplicate []*expression.Assignment) (err error) {
	// Get the table record row from storage for update.
	oldValue, ok := e.dupOldRowValues[string(e.Table.RecordKey(handle))]
	if !ok {
		return errors.NotFoundf("can not be duplicated row, due to old row not found. handle %d", handle)
	}
	cols := e.Table.WritableCols()
	oldRow, oldRowMap, err := tables.DecodeRawRowData(e.ctx, e.Table.Meta(), handle, cols, oldValue)
	if err != nil {
		return errors.Trace(err)
	}
	// Fill write-only and write-reorg columns with originDefaultValue if not found in oldValue.
	for _, col := range cols {
		if col.State != model.StatePublic && oldRow[col.Offset].IsNull() {
			_, found := oldRowMap[col.ID]
			if !found {
				oldRow[col.Offset], err = table.GetColOriginDefaultValue(e.ctx, col.ToInfo())
				if err != nil {
					return errors.Trace(err)
				}
			}
		}
	}

	// Do update row.
	updatedRow, handleChanged, newHandle, err := e.doDupRowUpdate(handle, oldRow, newRow, onDuplicate)
	if err != nil {
		return errors.Trace(err)
	}
	return e.updateDupKeyValues(e.ctx, e.Table, handle, newHandle, handleChanged, updatedRow)
}

// doDupRowUpdate updates the duplicate row.
// TODO: Report rows affected.
func (e *InsertExec) doDupRowUpdate(handle int64, oldRow types.DatumRow, newRow types.DatumRow,
	cols []*expression.Assignment) (types.DatumRow, bool, int64, error) {
	assignFlag := make([]bool, len(e.Table.WritableCols()))
	// See http://dev.mysql.com/doc/refman/5.7/en/miscellaneous-functions.html#function_values
	e.ctx.GetSessionVars().CurrInsertValues = types.DatumRow(newRow)
	newData := make(types.DatumRow, len(oldRow))
	copy(newData, oldRow)
	for _, col := range cols {
		val, err1 := col.Expr.Eval(newData)
		if err1 != nil {
			return nil, false, 0, errors.Trace(err1)
		}
		newData[col.Col.Index] = val
		assignFlag[col.Col.Index] = true
	}
	_, handleChanged, newHandle, lastInsertID, err := updateRecord(e.ctx, handle, oldRow, newData, assignFlag, e.Table, true)
	if err != nil {
		return nil, false, 0, errors.Trace(err)
	}
	e.rowCount++
	if err := e.checkBatchLimit(); err != nil {
		return nil, false, 0, errors.Trace(err)
	}
	if lastInsertID != 0 {
		e.lastInsertID = lastInsertID
	}
	return newData, handleChanged, newHandle, nil
}
