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

	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/table/tables"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/stringutil"
)

type keyValue struct {
	key   kv.Key
	value []byte
}

type keyValueWithDupInfo struct {
	newKV        keyValue
	dupErr       error
	commonHandle bool
}

type toBeCheckedRow struct {
	row        []types.Datum
	rowValue   []byte
	handleKey  *keyValueWithDupInfo
	uniqueKeys []*keyValueWithDupInfo
	// t is the table or partition this row belongs to.
	t table.Table
}

// encodeNewRow encodes a new row to value.
func encodeNewRow(ctx sessionctx.Context, t table.Table, row []types.Datum) ([]byte, error) {
	colIDs := make([]int64, 0, len(row))
	skimmedRow := make([]types.Datum, 0, len(row))
	for _, col := range t.Cols() {
		if !tables.CanSkip(t.Meta(), col, &row[col.Offset]) {
			colIDs = append(colIDs, col.ID)
			skimmedRow = append(skimmedRow, row[col.Offset])
		}
	}
	sctx, rd := ctx.GetSessionVars().StmtCtx, &ctx.GetSessionVars().RowEncoder
	newRowValue, err := tablecodec.EncodeRow(sctx, skimmedRow, colIDs, nil, nil, rd)
	if err != nil {
		return nil, err
	}
	return newRowValue, nil
}

// getKeysNeedCheck gets keys converted from to-be-insert rows to record keys and unique index keys,
// which need to be checked whether they are duplicate keys.
func getKeysNeedCheck(ctx context.Context, sctx sessionctx.Context, t table.Table, rows [][]types.Datum) ([]toBeCheckedRow, error) {
	nUnique := 0
	for _, v := range t.WritableIndices() {
		if v.Meta().Unique {
			nUnique++
		}
	}
	toBeCheckRows := make([]toBeCheckedRow, 0, len(rows))

	var handleCols []*table.Column
	// Get handle column if PK is handle.
	if t.Meta().PKIsHandle {
		for _, col := range t.Cols() {
			if col.IsPKHandleColumn(t.Meta()) {
				handleCols = append(handleCols, col)
				break
			}
		}
	} else {
		handleCols = tables.TryGetCommonPkColumns(t)
	}

	var err error
	for _, row := range rows {
		toBeCheckRows, err = getKeysNeedCheckOneRow(sctx, t, row, nUnique, handleCols, toBeCheckRows)
		if err != nil {
			return nil, err
		}
	}
	return toBeCheckRows, nil
}

func getKeysNeedCheckOneRow(ctx sessionctx.Context, t table.Table, row []types.Datum, nUnique int, handleCols []*table.Column, result []toBeCheckedRow) ([]toBeCheckedRow, error) {
	var err error
	if p, ok := t.(table.PartitionedTable); ok {
		t, err = p.GetPartitionByRow(ctx, row)
		if err != nil {
			return nil, err
		}
	}

	uniqueKeys := make([]*keyValueWithDupInfo, 0, nUnique)
	newRowValue, err := encodeNewRow(ctx, t, row)
	if err != nil {
		return nil, err
	}
	// Append record keys and errors.
	var handle kv.Handle
	if t.Meta().IsCommonHandle {
		var err error
		handleOrdinals := make([]int, 0, len(handleCols))
		for _, col := range handleCols {
			handleOrdinals = append(handleOrdinals, col.Offset)
		}
		handle, err = kv.BuildHandleFromDatumRow(ctx.GetSessionVars().StmtCtx, row, handleOrdinals)
		if err != nil {
			return nil, err
		}
	} else if len(handleCols) > 0 {
		handle = kv.IntHandle(row[handleCols[0].Offset].GetInt64())
	}
	var handleKey *keyValueWithDupInfo
	if handle != nil {
		handleKey = &keyValueWithDupInfo{
			newKV: keyValue{
				key:   t.RecordKey(handle),
				value: newRowValue,
			},
			dupErr: kv.ErrKeyExists.FastGenByArgs(stringutil.MemoizeStr(handle.String), "PRIMARY"),
		}
	}

	// append unique keys and errors
	for _, v := range t.WritableIndices() {
		if !v.Meta().Unique {
			continue
		}
		if t.Meta().IsCommonHandle && v.Meta().Primary {
			continue
		}
		colVals, err1 := v.FetchValues(row, nil)
		if err1 != nil {
			return nil, err1
		}
		// Pass handle = 0 to GenIndexKey,
		// due to we only care about distinct key.
		key, distinct, err1 := v.GenIndexKey(ctx.GetSessionVars().StmtCtx,
			colVals, kv.IntHandle(0), nil)
		if err1 != nil {
			return nil, err1
		}
		// Skip the non-distinct keys.
		if !distinct {
			continue
		}
		colValStr, err1 := types.DatumsToString(colVals, false)
		if err1 != nil {
			return nil, err1
		}
		uniqueKeys = append(uniqueKeys, &keyValueWithDupInfo{
			newKV:        keyValue{key: key},
			dupErr:       kv.ErrKeyExists.FastGenByArgs(colValStr, v.Meta().Name),
			commonHandle: t.Meta().IsCommonHandle,
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

// getOldRow gets the table record row from storage for batch check.
// t could be a normal table or a partition, but it must not be a PartitionedTable.
func getOldRow(ctx context.Context, sctx sessionctx.Context, txn kv.Transaction, t table.Table, handle kv.Handle,
	genExprs []expression.Expression) ([]types.Datum, error) {
	oldValue, err := txn.Get(ctx, t.RecordKey(handle))
	if err != nil {
		return nil, err
	}

	cols := t.WritableCols()
	oldRow, oldRowMap, err := tables.DecodeRawRowData(sctx, t.Meta(), handle, cols, oldValue)
	if err != nil {
		return nil, err
	}
	// Fill write-only and write-reorg columns with originDefaultValue if not found in oldValue.
	gIdx := 0
	for _, col := range cols {
		if col.State != model.StatePublic && oldRow[col.Offset].IsNull() {
			_, found := oldRowMap[col.ID]
			if !found {
				oldRow[col.Offset], err = table.GetColOriginDefaultValue(sctx, col.ToInfo())
				if err != nil {
					return nil, err
				}
			}
		}
		if col.IsGenerated() {
			// only the virtual column needs fill back.
			if !col.GeneratedStored {
				val, err := genExprs[gIdx].Eval(chunk.MutRowFromDatums(oldRow).ToRow())
				if err != nil {
					return nil, err
				}
				oldRow[col.Offset], err = table.CastValue(sctx, val, col.ToInfo(), false, false)
				if err != nil {
					return nil, err
				}
			}
			gIdx++
		}
	}
	return oldRow, nil
}
