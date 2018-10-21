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
	"github.com/pingcap/errors"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/table/tables"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	log "github.com/sirupsen/logrus"
	"golang.org/x/net/context"
)

// CreateTableInsertExec represents an insert executor when creating table, it is basically similar to `InsertExec` except:
//  1. It is instantiated and executed at DDL owner server, during the execution of 'create table' DDL job
//  2. The 'on duplicate' behavior(ERROR/IGNORE/REPLACE) is specified by 'onDuplicate' option
// See 'https://dev.mysql.com/doc/refman/5.7/en/create-table-select.html' for more details
type CreateTableInsertExec struct {
	*InsertValues

	onDuplicate ast.OnDuplicateCreateTableSelectType

	finished bool
}

func (e *CreateTableInsertExec) exec(rows [][]types.Datum) error {
	// If tidb_batch_insert is ON and not in a transaction, we could use BatchInsert mode.
	sessVars := e.ctx.GetSessionVars()
	defer sessVars.CleanBuffers()

	e.rowCount = 0
	if !sessVars.LightningMode {
		sessVars.GetWriteStmtBufs().BufStore = kv.NewBufferStore(e.ctx.Txn(true), kv.TempTxnMemBufCap)
	}

	switch e.onDuplicate {
	case ast.OnDuplicateCreateTableSelectIgnore:
		err := e.batchCheckAndInsert(rows, e.addRecord)
		if err != nil {
			return errors.Trace(err)
		}
	case ast.OnDuplicateCreateTableSelectReplace:
		err := e.batchCheckAndReplaceInsert(rows)
		if err != nil {
			return errors.Trace(err)
		}
	case ast.OnDuplicateCreateTableSelectError:
		for _, row := range rows {
			if _, err := e.addRecord(row); err != nil {
				return errors.Trace(err)
			}
		}
	}
	e.finished = true
	return nil
}

// Next implements Exec Next interface.
func (e *CreateTableInsertExec) Next(ctx context.Context, chk *chunk.Chunk) error {
	chk.Reset()
	if e.finished {
		return nil
	}
	return errors.Trace(e.insertRowsFromSelect(ctx, e.exec))
}

// Close implements the Executor Close interface.
func (e *CreateTableInsertExec) Close() error {
	return e.SelectExec.Close()
}

// Open implements the Executor Close interface.
func (e *CreateTableInsertExec) Open(ctx context.Context) error {
	return e.SelectExec.Open(ctx)
}

// batchCheckAndReplaceInsert updates multi-rows in batch if they are duplicate with rows in table.
func (e *CreateTableInsertExec) batchCheckAndReplaceInsert(newRows [][]types.Datum) error {
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
			if _, found := e.dupKVs[string(r.handleKey.newKV.key)]; found {
				handle, err := tablecodec.DecodeRowKey(r.handleKey.newKV.key)
				if err != nil {
					return errors.Trace(err)
				}
				err = e.replaceDupRow(r, handle)
				if err != nil {
					return errors.Trace(err)
				}
				continue
			}
		}
		for _, uk := range r.uniqueKeys {
			if val, found := e.dupKVs[string(uk.newKV.key)]; found {
				handle, err := tables.DecodeHandle(val)
				if err != nil {
					return errors.Trace(err)
				}
				err = e.replaceDupRow(r, handle)
				if err != nil {
					return errors.Trace(err)
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
				return errors.Trace(err)
			}
			e.fillBackKeys(e.Table, r, newHandle)
		}
	}
	return nil
}

// replaceDupRow updates a duplicate row to a new row.
func (e *CreateTableInsertExec) replaceDupRow(row toBeCheckedRow, handle int64) (err error) {
	oldRow, err := e.getOldRow(e.ctx, e.Table, handle)
	if err != nil {
		log.Errorf("[insert on dup] handle is %d for the to-be-inserted row %s", handle, types.DatumsToStrNoErr(row.row))
		return errors.Trace(err)
	}

	// Do update row.
	updatedRow, handleChanged, newHandle, err := e.doDupRowReplace(handle, oldRow, row.row)
	if e.ctx.GetSessionVars().StmtCtx.DupKeyAsWarning && kv.ErrKeyExists.Equal(err) {
		e.ctx.GetSessionVars().StmtCtx.AppendWarning(err)
		return nil
	}
	if err != nil {
		return errors.Trace(err)
	}
	return e.updateDupKeyValues(handle, newHandle, handleChanged, oldRow, updatedRow)
}

// doDupRowReplace updates the duplicate row.
func (e *CreateTableInsertExec) doDupRowReplace(handle int64, oldRow []types.Datum, newRow []types.Datum) ([]types.Datum, bool, int64, error) {
	assignFlag := make([]bool, len(e.Table.WritableCols()))
	// See http://dev.mysql.com/doc/refman/5.7/en/miscellaneous-functions.html#function_values
	e.ctx.GetSessionVars().CurrInsertValues = chunk.MutRowFromDatums(newRow).ToRow()

	_, handleChanged, newHandle, err := updateRecord(e.ctx, handle, oldRow, newRow, assignFlag, e.Table, true)
	if err != nil {
		return nil, false, 0, errors.Trace(err)
	}
	return newRow, handleChanged, newHandle, nil
}

// updateDupKeyValues updates the dupKeyValues for further duplicate key check.
func (e *CreateTableInsertExec) updateDupKeyValues(oldHandle int64, newHandle int64,
	handleChanged bool, oldRow []types.Datum, updatedRow []types.Datum) error {
	// There is only one row per update.
	fillBackKeysInRows, err := e.getKeysNeedCheck(e.ctx, e.Table, [][]types.Datum{updatedRow})
	if err != nil {
		return errors.Trace(err)
	}
	// Delete old keys and fill back new key-values of the updated row.
	err = e.deleteDupKeys(e.ctx, e.Table, [][]types.Datum{oldRow})
	if err != nil {
		return errors.Trace(err)
	}
	if handleChanged {
		delete(e.dupOldRowValues, string(e.Table.RecordKey(oldHandle)))
		e.fillBackKeys(e.Table, fillBackKeysInRows[0], newHandle)
	} else {
		e.fillBackKeys(e.Table, fillBackKeysInRows[0], oldHandle)
	}
	return nil
}
