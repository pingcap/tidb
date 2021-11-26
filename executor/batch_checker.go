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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package executor

import (
	"context"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/errno"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/terror"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/table/tables"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/stringutil"
)

type keyValueWithDupInfo struct {
	newKey       kv.Key
	dupErr       error
	commonHandle bool
}

type toBeCheckedRow struct {
	row        []types.Datum
	handleKey  *keyValueWithDupInfo
	uniqueKeys []*keyValueWithDupInfo
	// t is the table or partition this row belongs to.
	t       table.Table
	ignored bool
}

// getKeysNeedCheck gets keys converted from to-be-insert rows to record keys and unique index keys,
// which need to be checked whether they are duplicate keys.
func getKeysNeedCheck(ctx context.Context, sctx sessionctx.Context, t table.Table, rows [][]types.Datum) ([]toBeCheckedRow, error) {
	nUnique := 0
	for _, v := range t.Indices() {
		if !tables.IsIndexWritable(v) {
			continue
		}
		if v.Meta().Unique {
			nUnique++
		}
	}
	toBeCheckRows := make([]toBeCheckedRow, 0, len(rows))

	var (
		tblHandleCols []*table.Column
		pkIdxInfo     *model.IndexInfo
	)
	// Get handle column if PK is handle.
	if t.Meta().PKIsHandle {
		for _, col := range t.Cols() {
			if col.IsPKHandleColumn(t.Meta()) {
				tblHandleCols = append(tblHandleCols, col)
				break
			}
		}
	} else if t.Meta().IsCommonHandle {
		pkIdxInfo = tables.FindPrimaryIndex(t.Meta())
		for _, idxCol := range pkIdxInfo.Columns {
			tblHandleCols = append(tblHandleCols, t.Cols()[idxCol.Offset])
		}
	}

	var err error
	for _, row := range rows {
		toBeCheckRows, err = getKeysNeedCheckOneRow(sctx, t, row, nUnique, tblHandleCols, pkIdxInfo, toBeCheckRows)
		if err != nil {
			return nil, err
		}
	}
	return toBeCheckRows, nil
}

func getKeysNeedCheckOneRow(ctx sessionctx.Context, t table.Table, row []types.Datum, nUnique int, handleCols []*table.Column,
	pkIdxInfo *model.IndexInfo, result []toBeCheckedRow) ([]toBeCheckedRow, error) {
	var err error
	if p, ok := t.(table.PartitionedTable); ok {
		t, err = p.GetPartitionByRow(ctx, row)
		if err != nil {
			if terr, ok := errors.Cause(err).(*terror.Error); ctx.GetSessionVars().StmtCtx.IgnoreNoPartition && ok && (terr.Code() == errno.ErrNoPartitionForGivenValue || terr.Code() == errno.ErrRowDoesNotMatchGivenPartitionSet) {
				ctx.GetSessionVars().StmtCtx.AppendWarning(err)
				result = append(result, toBeCheckedRow{ignored: true})
				return result, nil
			}
			return nil, err
		}
	}

	uniqueKeys := make([]*keyValueWithDupInfo, 0, nUnique)
	// Append record keys and errors.
	var handle kv.Handle
	if t.Meta().IsCommonHandle {
		var err error
		handle, err = buildHandleFromDatumRow(ctx.GetSessionVars().StmtCtx, row, handleCols, pkIdxInfo)
		if err != nil {
			return nil, err
		}
	} else if len(handleCols) > 0 {
		handle = kv.IntHandle(row[handleCols[0].Offset].GetInt64())
	}
	var handleKey *keyValueWithDupInfo
	if handle != nil {
		fn := func() string {
			var str string
			var err error
			if t.Meta().IsCommonHandle {
				data := make([]types.Datum, len(handleCols))
				for i, col := range handleCols {
					data[i] = row[col.Offset]
				}
				str, err = formatDataForDupError(data)
			} else {
				str, err = row[handleCols[0].Offset].ToString()
			}
			if err != nil {
				return kv.GetDuplicateErrorHandleString(handle)
			}
			return str
		}
		handleKey = &keyValueWithDupInfo{
			newKey: tablecodec.EncodeRecordKey(t.RecordPrefix(), handle),
			dupErr: kv.ErrKeyExists.FastGenByArgs(stringutil.MemoizeStr(fn), "PRIMARY"),
		}
	}

	// addChangingColTimes is used to fetch values while processing "modify/change column" operation.
	addChangingColTimes := 0
	// append unique keys and errors
	for _, v := range t.Indices() {
		if !tables.IsIndexWritable(v) {
			continue
		}
		if !v.Meta().Unique {
			continue
		}
		if t.Meta().IsCommonHandle && v.Meta().Primary {
			continue
		}
		if len(row) < len(t.WritableCols()) && addChangingColTimes == 0 {
			if col := tables.FindChangingCol(t.WritableCols(), v.Meta()); col != nil {
				row = append(row, row[col.DependencyColumnOffset])
				addChangingColTimes++
			}
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
		colValStr, err1 := formatDataForDupError(colVals)
		if err1 != nil {
			return nil, err1
		}
		uniqueKeys = append(uniqueKeys, &keyValueWithDupInfo{
			newKey:       key,
			dupErr:       kv.ErrKeyExists.FastGenByArgs(colValStr, v.Meta().Name),
			commonHandle: t.Meta().IsCommonHandle,
		})
	}
	if addChangingColTimes == 1 {
		row = row[:len(row)-1]
	}
	result = append(result, toBeCheckedRow{
		row:        row,
		handleKey:  handleKey,
		uniqueKeys: uniqueKeys,
		t:          t,
	})
	return result, nil
}

func buildHandleFromDatumRow(sctx *stmtctx.StatementContext, row []types.Datum, tblHandleCols []*table.Column, pkIdxInfo *model.IndexInfo) (kv.Handle, error) {
	pkDts := make([]types.Datum, 0, len(tblHandleCols))
	for i, col := range tblHandleCols {
		d := row[col.Offset]
		if pkIdxInfo != nil && len(pkIdxInfo.Columns) > 0 {
			tablecodec.TruncateIndexValue(&d, pkIdxInfo.Columns[i], col.ColumnInfo)
		}
		pkDts = append(pkDts, d)
	}
	handleBytes, err := codec.EncodeKey(sctx, nil, pkDts...)
	if err != nil {
		return nil, err
	}
	handle, err := kv.NewCommonHandle(handleBytes)
	if err != nil {
		return nil, err
	}
	return handle, nil
}

func formatDataForDupError(data []types.Datum) (string, error) {
	strs := make([]string, 0, len(data))
	for _, datum := range data {
		str, err := datum.ToString()
		if err != nil {
			return "", errors.Trace(err)
		}
		strs = append(strs, str)
	}
	return strings.Join(strs, "-"), nil
}

// getOldRow gets the table record row from storage for batch check.
// t could be a normal table or a partition, but it must not be a PartitionedTable.
func getOldRow(ctx context.Context, sctx sessionctx.Context, txn kv.Transaction, t table.Table, handle kv.Handle,
	genExprs []expression.Expression) ([]types.Datum, error) {
	oldValue, err := txn.Get(ctx, tablecodec.EncodeRecordKey(t.RecordPrefix(), handle))
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
