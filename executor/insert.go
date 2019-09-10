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
	"encoding/hex"
	"fmt"

	"github.com/opentracing/opentracing-go"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/table/tables"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
)

// InsertExec represents an insert executor.
type InsertExec struct {
	*InsertValues
	OnDuplicate []*expression.Assignment
	Priority    mysql.PriorityEnum
}

func (e *InsertExec) exec(ctx context.Context, rows [][]types.Datum) error {
	// If tidb_batch_insert is ON and not in a transaction, we could use BatchInsert mode.
	sessVars := e.ctx.GetSessionVars()
	defer sessVars.CleanBuffers()
	ignoreErr := sessVars.StmtCtx.DupKeyAsWarning

	if !sessVars.LightningMode {
		txn, err := e.ctx.Txn(true)
		if err != nil {
			return err
		}
		sessVars.GetWriteStmtBufs().BufStore = kv.NewBufferStore(txn, kv.TempTxnMemBufCap)
	}

	e.ctx.GetSessionVars().StmtCtx.AddRecordRows(uint64(len(rows)))
	// If you use the IGNORE keyword, duplicate-key error that occurs while executing the INSERT statement are ignored.
	// For example, without IGNORE, a row that duplicates an existing UNIQUE index or PRIMARY KEY value in
	// the table causes a duplicate-key error and the statement is aborted. With IGNORE, the row is discarded and no error occurs.
	// However, if the `on duplicate update` is also specified, the duplicated row will be updated.
	// Using BatchGet in insert ignore to mark rows as duplicated before we add records to the table.
	// If `ON DUPLICATE KEY UPDATE` is specified, and no `IGNORE` keyword,
	// the to-be-insert rows will be check on duplicate keys and update to the new rows.
	if len(e.OnDuplicate) > 0 {
		err := e.batchUpdateDupRowsNew(ctx, rows)
		if err != nil {
			return err
		}
	} else if ignoreErr {
		err := e.batchCheckAndInsert(rows, e.addRecord)
		if err != nil {
			return err
		}
	} else {
		for _, row := range rows {
			if _, err := e.addRecord(row); err != nil {
				return err
			}
		}
	}
	return nil
}

func prefetchUniqueIndices(ctx context.Context, txn kv.Transaction, rows []toBeCheckedRow) (map[string][]byte, error) {
	nKeys := 0
	for _, r := range rows {
		if r.handleKey != nil {
			nKeys++
		}
		nKeys += len(r.uniqueKeys)
	}
	batchKeys := make([]kv.Key, 0, nKeys)
	for _, r := range rows {
		if r.handleKey != nil {
			batchKeys = append(batchKeys, r.handleKey.newKV.key)
		}
		for _, k := range r.uniqueKeys {
			batchKeys = append(batchKeys, k.newKV.key)
		}
	}
	return txn.BatchGet(batchKeys)
}

func prefetchConflictedOldRows(ctx context.Context, txn kv.Transaction, rows []toBeCheckedRow, values map[string][]byte) error {
	batchKeys := make([]kv.Key, 0, len(rows))
	for _, r := range rows {
		for _, uk := range r.uniqueKeys {
			if val, found := values[string(uk.newKV.key)]; found {
				handle, err := tables.DecodeHandle(val)
				if err != nil {
					return err
				}
				batchKeys = append(batchKeys, r.t.RecordKey(handle))
			}
		}
	}
	_, err := txn.BatchGet(batchKeys)
	return err
}

func prefetchDataCache(ctx context.Context, txn kv.Transaction, rows []toBeCheckedRow) error {
	if span := opentracing.SpanFromContext(ctx); span != nil && span.Tracer() != nil {
		span1 := span.Tracer().StartSpan("prefetchDataCache", opentracing.ChildOf(span.Context()))
		defer span1.Finish()
		ctx = opentracing.ContextWithSpan(ctx, span1)
	}
	values, err := prefetchUniqueIndices(ctx, txn, rows)
	if err != nil {
		return err
	}
	return prefetchConflictedOldRows(ctx, txn, rows, values)
}

// updateDupRowNew updates a duplicate row to a new row.
func (e *InsertExec) updateDupRowNew(ctx context.Context, txn kv.Transaction, row toBeCheckedRow, handle int64, onDuplicate []*expression.Assignment) error {
	oldRow, err := e.getOldRowNew(e.ctx, txn, row.t, handle, e.GenExprs)
	if err != nil {
		return err
	}

	_, _, _, err = e.doDupRowUpdate(handle, oldRow, row.row, e.OnDuplicate)
	if e.ctx.GetSessionVars().StmtCtx.DupKeyAsWarning && kv.ErrKeyExists.Equal(err) {
		e.ctx.GetSessionVars().StmtCtx.AppendWarning(err)
		return nil
	}
	return err
}

func (e *InsertExec) batchUpdateDupRowsNew(ctx context.Context, newRows [][]types.Datum) error {
	// Get keys need to be checked.
	toBeCheckedRows, err := e.getKeysNeedCheck(e.ctx, e.Table, newRows)
	if err != nil {
		return err
	}

	txn, err := e.ctx.Txn(true)
	if err != nil {
		return err
	}

	// Use BatchGet to fill cache.
	// It's an optimization and could be removed without affecting correctness.
	if err = prefetchDataCache(ctx, txn, toBeCheckedRows); err != nil {
		return err
	}

	for i, r := range toBeCheckedRows {
		if r.handleKey != nil {
			handle, err := tablecodec.DecodeRowKey(r.handleKey.newKV.key)
			if err != nil {
				return err
			}

			err = e.updateDupRowNew(ctx, txn, r, handle, e.OnDuplicate)
			if err == nil {
				continue
			}
			if !kv.IsErrNotFound(err) {
				return err
			}
		}

		for _, uk := range r.uniqueKeys {
			val, err := txn.Get(uk.newKV.key)
			if err != nil {
				if kv.IsErrNotFound(err) {
					continue
				}
				return err
			}
			handle, err := tables.DecodeHandle(val)
			if err != nil {
				return err
			}

			err = e.updateDupRowNew(ctx, txn, r, handle, e.OnDuplicate)
			if err != nil {
				if kv.IsErrNotFound(err) {
					// Data index inconsistent? A unique key provide the handle information, but the
					// handle points to nothing.
					logutil.Logger(ctx).Error("get old row failed when insert on dup",
						zap.String("uniqueKey", hex.EncodeToString(uk.newKV.key)),
						zap.Int64("handle", handle),
						zap.String("toBeInsertedRow", types.DatumsToStrNoErr(r.row)))
				}
				return err
			}

			newRows[i] = nil
			break
		}

		// If row was checked with no duplicate keys,
		// we should do insert the row,
		// and key-values should be filled back to dupOldRowValues for the further row check,
		// due to there may be duplicate keys inside the insert statement.
		if newRows[i] != nil {
			_, err := e.addRecord(newRows[i])
			if err != nil {
				return err
			}
		}
	}
	return nil
}

// batchUpdateDupRows updates multi-rows in batch if they are duplicate with rows in table.
func (e *InsertExec) batchUpdateDupRows(newRows [][]types.Datum) error {
	err := e.batchGetInsertKeys(e.ctx, e.Table, newRows)
	if err != nil {
		return err
	}

	// Batch get the to-be-updated rows in storage.
	err = e.initDupOldRowValue(e.ctx, e.Table, newRows)
	if err != nil {
		return err
	}

	for i, r := range e.toBeCheckedRows {
		if r.handleKey != nil {
			if _, found := e.dupKVs[string(r.handleKey.newKV.key)]; found {
				handle, err := tablecodec.DecodeRowKey(r.handleKey.newKV.key)
				if err != nil {
					return err
				}
				err = e.updateDupRow(r, handle, e.OnDuplicate)
				if err != nil {
					return err
				}
				continue
			}
		}
		for _, uk := range r.uniqueKeys {
			if val, found := e.dupKVs[string(uk.newKV.key)]; found {
				handle, err := tables.DecodeHandle(val)
				if err != nil {
					return err
				}
				err = e.updateDupRow(r, handle, e.OnDuplicate)
				if err != nil {
					return err
				}
				newRows[i] = nil
				break
			}
		}
		// If row was checked with no duplicate keys,
		// we should do insert the row,
		// and key-values should be filled back to dupOldRowValues for the further row check,
		// due to there may be duplicate keys inside the insert statement.
		if newRows[i] != nil {
			newHandle, err := e.addRecord(newRows[i])
			if err != nil {
				return err
			}
			e.fillBackKeys(e.Table, r, newHandle)
		}
	}
	return nil
}

// Next implements the Executor Next interface.
func (e *InsertExec) Next(ctx context.Context, req *chunk.Chunk) error {
	if span := opentracing.SpanFromContext(ctx); span != nil && span.Tracer() != nil {
		span1 := span.Tracer().StartSpan("insert.Next", opentracing.ChildOf(span.Context()))
		defer span1.Finish()
	}

	req.Reset()
	if len(e.children) > 0 && e.children[0] != nil {
		return e.insertRowsFromSelect(ctx, e.exec)
	}
	return e.insertRows(ctx, e.exec)
}

// Close implements the Executor Close interface.
func (e *InsertExec) Close() error {
	e.ctx.GetSessionVars().CurrInsertValues = chunk.Row{}
	e.setMessage()
	if e.SelectExec != nil {
		return e.SelectExec.Close()
	}
	return nil
}

// Open implements the Executor Open interface.
func (e *InsertExec) Open(ctx context.Context) error {
	if e.SelectExec != nil {
		return e.SelectExec.Open(ctx)
	}
	e.initEvalBuffer()
	return nil
}

// updateDupRow updates a duplicate row to a new row.
func (e *InsertExec) updateDupRow(row toBeCheckedRow, handle int64, onDuplicate []*expression.Assignment) error {
	oldRow, err := e.getOldRow(e.ctx, row.t, handle, e.GenExprs)
	if err != nil {
		logutil.Logger(context.Background()).Error("get old row failed when insert on dup", zap.Int64("handle", handle), zap.String("toBeInsertedRow", types.DatumsToStrNoErr(row.row)))
		return err
	}
	// Do update row.
	updatedRow, handleChanged, newHandle, err := e.doDupRowUpdate(handle, oldRow, row.row, onDuplicate)
	if e.ctx.GetSessionVars().StmtCtx.DupKeyAsWarning && kv.ErrKeyExists.Equal(err) {
		e.ctx.GetSessionVars().StmtCtx.AppendWarning(err)
		return nil
	}
	if err != nil {
		return err
	}
	return e.updateDupKeyValues(handle, newHandle, handleChanged, oldRow, updatedRow)
}

// doDupRowUpdate updates the duplicate row.
func (e *InsertExec) doDupRowUpdate(handle int64, oldRow []types.Datum, newRow []types.Datum,
	cols []*expression.Assignment) ([]types.Datum, bool, int64, error) {
	assignFlag := make([]bool, len(e.Table.WritableCols()))
	// See http://dev.mysql.com/doc/refman/5.7/en/miscellaneous-functions.html#function_values
	e.ctx.GetSessionVars().CurrInsertValues = chunk.MutRowFromDatums(newRow).ToRow()

	// NOTE: In order to execute the expression inside the column assignment,
	// we have to put the value of "oldRow" before "newRow" in "row4Update" to
	// be consistent with "Schema4OnDuplicate" in the "Insert" PhysicalPlan.
	row4Update := make([]types.Datum, 0, len(oldRow)+len(newRow))
	row4Update = append(row4Update, oldRow...)
	row4Update = append(row4Update, newRow...)

	// Update old row when the key is duplicated.
	for _, col := range cols {
		val, err1 := col.Expr.Eval(chunk.MutRowFromDatums(row4Update).ToRow())
		if err1 != nil {
			return nil, false, 0, err1
		}
		row4Update[col.Col.Index] = val
		assignFlag[col.Col.Index] = true
	}

	newData := row4Update[:len(oldRow)]
	_, handleChanged, newHandle, err := updateRecord(e.ctx, handle, oldRow, newData, assignFlag, e.Table, true)
	if err != nil {
		return nil, false, 0, err
	}
	return newData, handleChanged, newHandle, nil
}

// updateDupKeyValues updates the dupKeyValues for further duplicate key check.
func (e *InsertExec) updateDupKeyValues(oldHandle int64, newHandle int64,
	handleChanged bool, oldRow []types.Datum, updatedRow []types.Datum) error {
	// There is only one row per update.
	fillBackKeysInRows, err := e.getKeysNeedCheck(e.ctx, e.Table, [][]types.Datum{updatedRow})
	if err != nil {
		return err
	}
	// Delete old keys and fill back new key-values of the updated row.
	err = e.deleteDupKeys(e.ctx, e.Table, [][]types.Datum{oldRow})
	if err != nil {
		return err
	}

	if handleChanged {
		delete(e.dupOldRowValues, string(e.Table.RecordKey(oldHandle)))
		e.fillBackKeys(e.Table, fillBackKeysInRows[0], newHandle)
	} else {
		e.fillBackKeys(e.Table, fillBackKeysInRows[0], oldHandle)
	}
	return nil
}

// setMessage sets info message(ERR_INSERT_INFO) generated by INSERT statement
func (e *InsertExec) setMessage() {
	stmtCtx := e.ctx.GetSessionVars().StmtCtx
	numRecords := stmtCtx.RecordRows()
	if e.SelectExec != nil || numRecords > 1 {
		numWarnings := stmtCtx.WarningCount()
		var numDuplicates uint64
		if stmtCtx.DupKeyAsWarning {
			// if ignoreErr
			numDuplicates = numRecords - stmtCtx.CopiedRows()
		} else {
			if e.ctx.GetSessionVars().ClientCapability&mysql.ClientFoundRows > 0 {
				numDuplicates = stmtCtx.TouchedRows()
			} else {
				numDuplicates = stmtCtx.UpdatedRows()
			}
		}
		msg := fmt.Sprintf(mysql.MySQLErrName[mysql.ErrInsertInfo], numRecords, numDuplicates, numWarnings)
		stmtCtx.SetMessage(msg)
	}
}
