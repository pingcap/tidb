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
	"github.com/pingcap/tidb/model"
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
	newKV  keyValue
	dupErr error
}

type toBeCheckedRow struct {
	row        []types.Datum
	rowValue   []byte
	handleKey  *keyValueWithDupInfo
	uniqueKeys []*keyValueWithDupInfo
	// The table or partition this row belongs to.
	t table.Table
}

type batchChecker struct {
	// For duplicate key update
	toBeCheckedRows []toBeCheckedRow
	dupKVs          map[string][]byte
	dupOldRowValues map[string][]byte
}

// batchGetOldValues gets the values of storage in batch.
func (b *batchChecker) batchGetOldValues(ctx sessionctx.Context, batchKeys []kv.Key) error {
	values, err := kv.BatchGetValues(ctx.Txn(), batchKeys)
	if err != nil {
		return errors.Trace(err)
	}
	for k, v := range values {
		b.dupOldRowValues[k] = v
	}
	return nil
}

// encodeNewRow encodes a new row to value.
func (b *batchChecker) encodeNewRow(ctx sessionctx.Context, t table.Table, row []types.Datum) ([]byte, error) {
	colIDs := make([]int64, 0, len(row))
	skimmedRow := make([]types.Datum, 0, len(row))
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
func (b *batchChecker) getKeysNeedCheck(ctx sessionctx.Context, t table.Table, rows [][]types.Datum) ([]toBeCheckedRow, error) {
	nUnique := 0
	for _, v := range t.WritableIndices() {
		if v.Meta().Unique {
			nUnique++
		}
	}
	toBeCheckRows := make([]toBeCheckedRow, 0, len(rows))

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

	var err error
	for _, row := range rows {
		toBeCheckRows, err = b.getKeysNeedCheckOneRow(ctx, t, row, nUnique, handleCol, toBeCheckRows)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}
	return toBeCheckRows, nil
}

func (b *batchChecker) getKeysNeedCheckOneRow(ctx sessionctx.Context, t table.Table, row []types.Datum, nUnique int, handleCol *table.Column, result []toBeCheckedRow) ([]toBeCheckedRow, error) {
	var err error
	if p, ok := t.(table.PartitionedTable); ok {
		t, err = p.GetPartitionByRow(ctx, row)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}

	var handleKey *keyValueWithDupInfo
	uniqueKeys := make([]*keyValueWithDupInfo, 0, nUnique)
	newRowValue, err := b.encodeNewRow(ctx, t, row)
	if err != nil {
		return nil, errors.Trace(err)
	}
	// Append record keys and errors.
	if handleCol != nil {
		handle := row[handleCol.Offset].GetInt64()
		handleKey = &keyValueWithDupInfo{
			newKV: keyValue{
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
		colValStr, err1 := types.DatumsToString(colVals, false)
		if err1 != nil {
			return nil, errors.Trace(err1)
		}
		uniqueKeys = append(uniqueKeys, &keyValueWithDupInfo{
			newKV: keyValue{
				key: key,
			},
			dupErr: kv.ErrKeyExists.FastGen("Duplicate entry '%s' for key '%s'",
				colValStr, v.Meta().Name),
		})
	}
	result = append(result, toBeCheckedRow{
		row:        row,
		rowValue:   newRowValue,
		handleKey:  handleKey,
		uniqueKeys: uniqueKeys,
		t:          t,
	})
	return result, nil
}

// batchGetInsertKeys uses batch-get to fetch all key-value pairs to be checked for ignore or duplicate key update.
func (b *batchChecker) batchGetInsertKeys(ctx sessionctx.Context, t table.Table, newRows [][]types.Datum) (err error) {
	// Get keys need to be checked.
	b.toBeCheckedRows, err = b.getKeysNeedCheck(ctx, t, newRows)
	if err != nil {
		return errors.Trace(err)
	}

	// Batch get values.
	nKeys := 0
	for _, r := range b.toBeCheckedRows {
		if r.handleKey != nil {
			nKeys++
		}
		nKeys += len(r.uniqueKeys)
	}
	batchKeys := make([]kv.Key, 0, nKeys)
	for _, r := range b.toBeCheckedRows {
		if r.handleKey != nil {
			batchKeys = append(batchKeys, r.handleKey.newKV.key)
		}
		for _, k := range r.uniqueKeys {
			batchKeys = append(batchKeys, k.newKV.key)
		}
	}
	b.dupKVs, err = kv.BatchGetValues(ctx.Txn(), batchKeys)
	return errors.Trace(err)
}

func (b *batchChecker) initDupOldRowFromHandleKey() {
	for _, r := range b.toBeCheckedRows {
		if r.handleKey == nil {
			continue
		}
		k := r.handleKey.newKV.key
		if val, found := b.dupKVs[string(k)]; found {
			b.dupOldRowValues[string(k)] = val
		}
	}
}

func (b *batchChecker) initDupOldRowFromUniqueKey(ctx sessionctx.Context, newRows [][]types.Datum) error {
	batchKeys := make([]kv.Key, 0, len(newRows))
	for _, r := range b.toBeCheckedRows {
		for _, uk := range r.uniqueKeys {
			if val, found := b.dupKVs[string(uk.newKV.key)]; found {
				handle, err := tables.DecodeHandle(val)
				if err != nil {
					return errors.Trace(err)
				}
				batchKeys = append(batchKeys, r.t.RecordKey(handle))
			}
		}
	}
	return errors.Trace(b.batchGetOldValues(ctx, batchKeys))
}

// initDupOldRowValue initializes dupOldRowValues which contain the to-be-updated rows from storage.
func (b *batchChecker) initDupOldRowValue(ctx sessionctx.Context, t table.Table, newRows [][]types.Datum) error {
	b.dupOldRowValues = make(map[string][]byte, len(newRows))
	b.initDupOldRowFromHandleKey()
	return errors.Trace(b.initDupOldRowFromUniqueKey(ctx, newRows))
}

// fillBackKeys fills the updated key-value pair to the dupKeyValues for further check.
func (b *batchChecker) fillBackKeys(t table.Table, row toBeCheckedRow, handle int64) {
	if row.rowValue != nil {
		b.dupOldRowValues[string(t.RecordKey(handle))] = row.rowValue
	}
	if row.handleKey != nil {
		b.dupKVs[string(row.handleKey.newKV.key)] = row.handleKey.newKV.value
	}
	for _, uk := range row.uniqueKeys {
		b.dupKVs[string(uk.newKV.key)] = tables.EncodeHandle(handle)
	}
}

func (b *batchChecker) deleteDupKeys(row toBeCheckedRow) {
	if row.handleKey != nil {
		delete(b.dupKVs, string(row.handleKey.newKV.key))
	}
	for _, uk := range row.uniqueKeys {
		delete(b.dupKVs, string(uk.newKV.key))
	}
}

// getOldRow gets the table record row from storage for batch check.
// t could be a normal table or a partition, but it must not be a PartitionedTable.
func (b *batchChecker) getOldRow(ctx sessionctx.Context, t table.Table, handle int64) ([]types.Datum, error) {
	oldValue, ok := b.dupOldRowValues[string(t.RecordKey(handle))]
	if !ok {
		return nil, errors.NotFoundf("can not be duplicated row, due to old row not found. handle %d", handle)
	}
	cols := t.WritableCols()
	oldRow, oldRowMap, err := tables.DecodeRawRowData(ctx, t.Meta(), handle, cols, oldValue)
	if err != nil {
		return nil, errors.Trace(err)
	}
	// Fill write-only and write-reorg columns with originDefaultValue if not found in oldValue.
	for _, col := range cols {
		if col.State != model.StatePublic && oldRow[col.Offset].IsNull() {
			_, found := oldRowMap[col.ID]
			if !found {
				oldRow[col.Offset], err = table.GetColOriginDefaultValue(ctx, col.ToInfo())
				if err != nil {
					return nil, errors.Trace(err)
				}
			}
		}
	}
	return oldRow, nil
}
