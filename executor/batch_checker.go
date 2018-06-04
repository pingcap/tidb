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
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/table/tables"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/types"
)

type keyValue struct {
	key   kv.Key
	value []byte
}

type keyValueWithDupInfo struct {
	newKeyValue keyValue
	oldKeyValue keyValue
	dupErr      error
}

type toBeCheckRow struct {
	rowValue   []byte
	handleKey  *keyValueWithDupInfo
	uniqueKeys []*keyValueWithDupInfo
}

type batchChecker struct {
	// For duplicate key update
	toBeCheckRows   []toBeCheckRow
	dupKeyValues    map[string][]byte
	dupOldRowValues map[string][]byte
}

// batchMarkDupRows marks rows with duplicate errors as nil.
// All duplicate rows were marked and appended as duplicate warnings
// to the statement context in batch.
func (b *batchChecker) batchMarkDupRows(ctx sessionctx.Context, t table.Table, rows []types.DatumRow) ([]types.DatumRow, error) {
	toBeCheckRows, values, err := b.batchGetInsertKeys(ctx, t, rows)
	if err != nil {
		return nil, errors.Trace(err)
	}
	// append warnings and get no duplicated error rows
	for i, r := range toBeCheckRows {
		if r.handleKey != nil {
			if _, found := values[string(r.handleKey.newKeyValue.key)]; found {
				rows[i] = nil
				ctx.GetSessionVars().StmtCtx.AppendWarning(r.handleKey.dupErr)
				continue
			}
		}
		for _, uk := range r.uniqueKeys {
			if _, found := values[string(uk.newKeyValue.key)]; found {
				// If duplicate keys were found in BatchGet, mark row = nil.
				rows[i] = nil
				ctx.GetSessionVars().StmtCtx.AppendWarning(uk.dupErr)
				break
			}
		}
		// If row was checked with no duplicate keys,
		// it should be add to values map for the further row check.
		// There may be duplicate keys inside the insert statement.
		if rows[i] != nil {
			if r.handleKey != nil {
				values[string(r.handleKey.newKeyValue.key)] = r.handleKey.newKeyValue.value
			}
			for _, uk := range r.uniqueKeys {
				values[string(uk.newKeyValue.key)] = []byte{}
			}
		}
	}
	// this statement was already been checked
	ctx.GetSessionVars().StmtCtx.BatchCheck = true
	return rows, nil
}

// batchGetOldValues gets the values of storage in batch.
func (b *batchChecker) batchGetOldValues(ctx sessionctx.Context, t table.Table, handles []int64) (map[string][]byte, error) {
	batchKeys := make([]kv.Key, 0, len(handles))
	for _, handle := range handles {
		batchKeys = append(batchKeys, t.RecordKey(handle))
	}
	values, err := kv.BatchGetValues(ctx.Txn(), batchKeys)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return values, nil
}

// encodeNewRow encodes a new row to value.
func (b *batchChecker) encodeNewRow(ctx sessionctx.Context, t table.Table, row types.DatumRow) ([]byte, error) {
	colIDs := make([]int64, 0, len(row))
	skimmedRow := make(types.DatumRow, 0, len(row))
	for _, col := range t.Cols() {
		if !tables.CanSkip(t.Meta(), col, row[col.Offset]) {
			colIDs = append(colIDs, col.ID)
			skimmedRow = append(skimmedRow, row[col.Offset])
		}
	}
	newRowValue, err := tablecodec.EncodeRow(ctx.GetSessionVars().StmtCtx, skimmedRow, colIDs, nil, nil)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return newRowValue, nil
}

// getKeysNeedCheck gets keys converted from to-be-insert rows to record keys and unique index keys,
// which need to be checked whether they are duplicate keys.
func (b *batchChecker) getKeysNeedCheck(ctx sessionctx.Context, t table.Table, rows []types.DatumRow) ([]toBeCheckRow, error) {
	nUnique := 0
	for _, v := range t.WritableIndices() {
		if v.Meta().Unique {
			nUnique++
		}
	}
	toBeCheckRows := make([]toBeCheckRow, 0, len(rows))

	var handleCol *table.Column
	// Get handle column if PK is handle.
	if t.Meta().PKIsHandle {
		for _, col := range t.Cols() {
			if col.IsPKHandleColumn(t.Meta()) {
				handleCol = col
				break
			}
		}
	}

	for _, row := range rows {
		var handleKey *keyValueWithDupInfo
		uniqueKeys := make([]*keyValueWithDupInfo, 0, nUnique)
		newRowValue, err := b.encodeNewRow(ctx, t, row)
		if err != nil {
			return nil, errors.Trace(err)
		}
		// Append record keys and errors.
		if t.Meta().PKIsHandle {
			handle := row[handleCol.Offset].GetInt64()
			handleKey = &keyValueWithDupInfo{
				newKeyValue: keyValue{
					key:   t.RecordKey(handle),
					value: newRowValue,
				},
				dupErr: kv.ErrKeyExists.FastGen("Duplicate entry '%d' for key 'PRIMARY'", handle),
			}
		}

		// append unique keys and errors
		for _, v := range t.WritableIndices() {
			if !v.Meta().Unique {
				continue
			}
			colVals, err1 := v.FetchValues(row, nil)
			if err1 != nil {
				return nil, errors.Trace(err1)
			}
			// Pass handle = 0 to GenIndexKey,
			// due to we only care about distinct key.
			key, distinct, err1 := v.GenIndexKey(ctx.GetSessionVars().StmtCtx,
				colVals, 0, nil)
			if err1 != nil {
				return nil, errors.Trace(err1)
			}
			// Skip the non-distinct keys.
			if !distinct {
				continue
			}
			colValStr, err1 := types.DatumsToString(colVals)
			if err1 != nil {
				return nil, errors.Trace(err1)
			}
			uniqueKeys = append(uniqueKeys, &keyValueWithDupInfo{
				newKeyValue: keyValue{
					key: key,
				},
				dupErr: kv.ErrKeyExists.FastGen("Duplicate entry '%s' for key '%s'",
					colValStr, v.Meta().Name),
			})
		}
		toBeCheckRows = append(toBeCheckRows, toBeCheckRow{
			rowValue:   newRowValue,
			handleKey:  handleKey,
			uniqueKeys: uniqueKeys,
		})
	}
	return toBeCheckRows, nil
}

// batchGetInsertKeys uses batch-get to fetch all key-value pairs to be checked for ignore or duplicate key update.
func (b *batchChecker) batchGetInsertKeys(ctx sessionctx.Context, t table.Table, newRows []types.DatumRow) ([]toBeCheckRow, map[string][]byte, error) {
	// Get keys need to be checked.
	toBeCheckRows, err := b.getKeysNeedCheck(ctx, t, newRows)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}

	// Batch get values.
	nKeys := 0
	for _, r := range toBeCheckRows {
		nKeys += len(r.uniqueKeys)
	}
	batchKeys := make([]kv.Key, 0, nKeys)
	for _, r := range toBeCheckRows {
		if r.handleKey != nil {
			batchKeys = append(batchKeys, r.handleKey.newKeyValue.key)
		}
		for _, k := range r.uniqueKeys {
			batchKeys = append(batchKeys, k.newKeyValue.key)
		}
	}
	values, err := kv.BatchGetValues(ctx.Txn(), batchKeys)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}
	return toBeCheckRows, values, nil
}

// initDupOldRowValue initializes dupOldRowValues which contain the to-be-updated rows from storage.
func (b *batchChecker) initDupOldRowValue(ctx sessionctx.Context, t table.Table, newRows []types.DatumRow) (err error) {
	b.dupOldRowValues = make(map[string][]byte, len(newRows))
	handles := make([]int64, 0, len(newRows))
	for _, r := range b.toBeCheckRows {
		if r.handleKey != nil {
			k := r.handleKey.newKeyValue.key
			if val, found := b.dupKeyValues[string(k)]; found {
				b.dupOldRowValues[string(k)] = val
			}
		}
		for _, uk := range r.uniqueKeys {
			if val, found := b.dupKeyValues[string(uk.newKeyValue.key)]; found {
				handle, err1 := tables.DecodeHandle(val)
				if err1 != nil {
					return errors.Trace(err1)
				}
				handles = append(handles, handle)
				break
			}
		}
	}
	valuesMap, err := b.batchGetOldValues(ctx, t, handles)
	if err != nil {
		return errors.Trace(err)
	}
	for k, v := range valuesMap {
		b.dupOldRowValues[k] = v
	}
	return nil
}

// updateDupKeyValues updates the dupKeyValues for further duplicate key check.
func (b *batchChecker) updateDupKeyValues(ctx sessionctx.Context, t table.Table, oldHandle int64,
	newHandle int64, handleChanged bool, updatedRow types.DatumRow) error {
	// There is only one row per update.
	fillBackKeysInRows, err := b.getKeysNeedCheck(ctx, t, []types.DatumRow{updatedRow})
	if err != nil {
		return errors.Trace(err)
	}
	// Fill back new key-values of the updated row.
	if handleChanged {
		delete(b.dupOldRowValues, string(t.RecordKey(oldHandle)))
		b.fillBackKeys(t, fillBackKeysInRows[0], newHandle)
	} else {
		b.fillBackKeys(t, fillBackKeysInRows[0], oldHandle)
	}
	return nil
}

// fillBackKeys fills the updated key-value pair to the dupKeyValues for further check.
func (b *batchChecker) fillBackKeys(t table.Table, row toBeCheckRow, handle int64) {
	if row.rowValue != nil {
		b.dupOldRowValues[string(t.RecordKey(handle))] = row.rowValue
	}
	if row.handleKey != nil {
		b.dupKeyValues[string(row.handleKey.newKeyValue.key)] = row.handleKey.newKeyValue.value
	}
	for _, uk := range row.uniqueKeys {
		b.dupKeyValues[string(uk.newKeyValue.key)] = tables.EncodeHandle(handle)
	}
}
