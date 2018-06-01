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

	// For duplicate key update
	uniqueKeysInRows [][]keyWithDupError
	dupKeyValues     map[string][]byte
	dupOldRowValues  map[string][]byte
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

	// If `ON DUPLICATE KEY UPDATE` is specified, and no `IGNORE` keyword,
	// the to-be-insert rows will be check on duplicate keys and update to the new rows.
	if len(e.OnDuplicate) > 0 && !ignoreErr {
		err := e.batchUpdateDupRows(rows)
		if err != nil {
			return errors.Trace(err)
		}
	} else {
		if len(e.OnDuplicate) == 0 && ignoreErr {
			// If you use the IGNORE keyword, duplicate-key error that occurs while executing the INSERT statement are ignored.
			// For example, without IGNORE, a row that duplicates an existing UNIQUE index or PRIMARY KEY value in
			// the table causes a duplicate-key error and the statement is aborted. With IGNORE, the row is discarded and no error occurs.
			// However, if the `on duplicate update` is also specified, the duplicated row will be updated.
			// Using BatchGet in insert ignore to mark rows as duplicated before we add records to the table.
			var err error
			rows, err = e.batchMarkDupRows(rows)
			if err != nil {
				return errors.Trace(err)
			}
		}
		for _, row := range rows {
			// duplicate row will be marked as nil in batchMarkDupRows if
			// IgnoreErr is true. For IgnoreErr is false, it is a protection.
			if row == nil {
				continue
			}
			if err := e.checkBatchLimit(); err != nil {
				return errors.Trace(err)
			}
			if len(e.OnDuplicate) == 0 && !ignoreErr {
				e.ctx.Txn().SetOption(kv.PresumeKeyNotExists, nil)
			}
			h, err := e.Table.AddRecord(e.ctx, row, false)
			e.ctx.Txn().DelOption(kv.PresumeKeyNotExists)
			if err == nil {
				if !sessVars.ImportingData {
					e.ctx.StmtAddDirtyTableOP(DirtyTableAddRow, e.Table.Meta().ID, h, row)
				}
				e.rowCount++
				continue
			}
			if kv.ErrKeyExists.Equal(err) {
				// TODO: Use batch get to speed up `insert ignore on duplicate key update`.
				if len(e.OnDuplicate) > 0 && ignoreErr {
					data, err1 := e.Table.RowWithCols(e.ctx, h, e.Table.WritableCols())
					if err1 != nil {
						return errors.Trace(err1)
					}
					_, _, _, err = e.doDupRowUpdate(h, data, row, e.OnDuplicate)
					if kv.ErrKeyExists.Equal(err) {
						e.ctx.GetSessionVars().StmtCtx.AppendWarning(err)
						continue
					}
					if err != nil {
						return errors.Trace(err)
					}
					e.rowCount++
					continue
				}
			}
			return errors.Trace(err)
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

// initDupOldRowValue initializes dupOldRowValues which contain the to-be-updated rows from storage.
func (e *InsertExec) initDupOldRowValue(newRows []types.DatumRow) (err error) {
	e.dupOldRowValues = make(map[string][]byte, len(newRows))
	handles := make([]int64, 0, len(newRows))
	for _, keysInRow := range e.uniqueKeysInRows {
		for _, k := range keysInRow {
			if val, found := e.dupKeyValues[string(k.key)]; found {
				if k.isRecordKey {
					e.dupOldRowValues[string(k.key)] = val
				} else {
					var handle int64
					handle, err = e.decodeOldHandle(k, val)
					if err != nil {
						return errors.Trace(err)
					}
					handles = append(handles, handle)
				}
				break
			}
		}
	}
	valuesMap, err := e.batchGetOldValues(handles)
	if err != nil {
		return errors.Trace(err)
	}
	for k, v := range valuesMap {
		e.dupOldRowValues[k] = v
	}
	return nil
}

// updateDupRow updates a duplicate row to a new row.
func (e *InsertExec) updateDupRow(keys []keyWithDupError, k keyWithDupError, val []byte, newRow types.DatumRow, onDuplicate []*expression.Assignment) (err error) {
	oldHandle, err := e.decodeOldHandle(k, val)
	if err != nil {
		return errors.Trace(err)
	}

	// Get the table record row from storage for update.
	oldValue, ok := e.dupOldRowValues[string(e.Table.RecordKey(oldHandle))]
	if !ok {
		return errors.NotFoundf("can not be duplicated row, due to old row not found. handle %d", oldHandle)
	}
	cols := e.Table.WritableCols()
	oldRow, oldRowMap, err := tables.DecodeRawRowData(e.ctx, e.Table.Meta(), oldHandle, cols, oldValue)
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
	updatedRow, handleChanged, newHandle, err := e.doDupRowUpdate(oldHandle, oldRow, newRow, onDuplicate)
	if err != nil {
		return errors.Trace(err)
	}
	return e.updateDupKeyValues(keys, oldHandle, newHandle, handleChanged, updatedRow)
}

// updateDupKeyValues updates the dupKeyValues for further duplicate key check.
func (e *InsertExec) updateDupKeyValues(keys []keyWithDupError, oldHandle int64,
	newHandle int64, handleChanged bool, updatedRow types.DatumRow) error {
	// There is only one row per update.
	fillBackKeysInRows, err := e.getKeysNeedCheck([]types.DatumRow{updatedRow})
	if err != nil {
		return errors.Trace(err)
	}
	// Delete key-values belong to the old row.
	for _, del := range keys {
		delete(e.dupKeyValues, string(del.key))
	}
	// Fill back new key-values of the updated row.
	if handleChanged {
		delete(e.dupOldRowValues, string(e.Table.RecordKey(oldHandle)))
		e.fillBackKeys(fillBackKeysInRows[0], newHandle)
	} else {
		e.fillBackKeys(fillBackKeysInRows[0], oldHandle)
	}
	return nil
}

// batchUpdateDupRows updates multi-rows in batch if they are duplicate with rows in table.
func (e *InsertExec) batchUpdateDupRows(newRows []types.DatumRow) error {
	var err error
	e.uniqueKeysInRows, e.dupKeyValues, err = e.batchGetInsertKeys(newRows)
	if err != nil {
		return errors.Trace(err)
	}

	// Batch get the to-be-updated rows in storage.
	err = e.initDupOldRowValue(newRows)
	if err != nil {
		return errors.Trace(err)
	}

	for i, keysInRow := range e.uniqueKeysInRows {
		for _, k := range keysInRow {
			if val, found := e.dupKeyValues[string(k.key)]; found {
				err := e.updateDupRow(keysInRow, k, val, newRows[i], e.OnDuplicate)
				if err != nil {
					return errors.Trace(err)
				}
				// Clean up row for latest add record operation.
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
			e.fillBackKeys(keysInRow, newHandle)
		}
	}
	return nil
}

// fillBackKeys fills the updated key-value pair to the dupKeyValues for further check.
func (e *InsertExec) fillBackKeys(fillBackKeysInRow []keyWithDupError, handle int64) {
	if len(fillBackKeysInRow) == 0 {
		return
	}
	e.dupOldRowValues[string(e.Table.RecordKey(handle))] = fillBackKeysInRow[0].newRowValue
	for _, insert := range fillBackKeysInRow {
		if insert.isRecordKey {
			e.dupKeyValues[string(e.Table.RecordKey(handle))] = insert.newRowValue
		} else {
			e.dupKeyValues[string(insert.key)] = tables.EncodeHandle(handle)
		}
	}
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

// decodeOldHandle decode old handle by key-value pair.
// The key-value pair should only be a table record or a distinct index record.
// If the key is a record key, decode handle from the key, else decode handle from the value.
func (e *InsertExec) decodeOldHandle(k keyWithDupError, value []byte) (oldHandle int64, err error) {
	if k.isRecordKey {
		oldHandle, err = tablecodec.DecodeRowKey(k.key)
	} else {
		oldHandle, err = tables.DecodeHandle(value)
	}
	if err != nil {
		return 0, errors.Trace(err)
	}
	return oldHandle, nil
}
