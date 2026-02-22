// Copyright 2015 PingCAP, Inc.
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

package tables

import (
	"context"
	"math"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/expression/exprctx"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/autoid"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/codec"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
)

// RowWithCols is used to get the corresponding column datum values with the given handle.
func RowWithCols(t table.Table, ctx sessionctx.Context, h kv.Handle, cols []*table.Column) ([]types.Datum, error) {
	// Get raw row data from kv.
	key := tablecodec.EncodeRecordKey(t.RecordPrefix(), h)
	txn, err := ctx.Txn(true)
	if err != nil {
		return nil, err
	}
	value, err := kv.GetValue(context.TODO(), txn, key)
	if err != nil {
		return nil, err
	}
	v, _, err := DecodeRawRowData(ctx.GetExprCtx(), t.Meta(), h, cols, value)
	if err != nil {
		return nil, err
	}
	return v, nil
}

func containFullColInHandle(meta *model.TableInfo, col *table.Column) (containFullCol bool, idxInHandle int) {
	pkIdx := FindPrimaryIndex(meta)
	for i, idxCol := range pkIdx.Columns {
		if meta.Columns[idxCol.Offset].ID == col.ID {
			idxInHandle = i
			containFullCol = idxCol.Length == types.UnspecifiedLength
			return
		}
	}
	return
}

// DecodeRawRowData decodes raw row data into a datum slice and a (columnID:columnValue) map.
func DecodeRawRowData(ctx expression.BuildContext, meta *model.TableInfo, h kv.Handle, cols []*table.Column,
	value []byte) ([]types.Datum, map[int64]types.Datum, error) {
	v := make([]types.Datum, len(cols))
	colTps := make(map[int64]*types.FieldType, len(cols))
	prefixCols := make(map[int64]struct{})
	for i, col := range cols {
		if col == nil {
			continue
		}
		if col.IsPKHandleColumn(meta) {
			if mysql.HasUnsignedFlag(col.GetFlag()) {
				v[i].SetUint64(uint64(h.IntValue()))
			} else {
				v[i].SetInt64(h.IntValue())
			}
			continue
		}
		if col.IsCommonHandleColumn(meta) && !types.NeedRestoredData(&col.FieldType) {
			if containFullCol, idxInHandle := containFullColInHandle(meta, col); containFullCol {
				dtBytes := h.EncodedCol(idxInHandle)
				_, dt, err := codec.DecodeOne(dtBytes)
				if err != nil {
					return nil, nil, err
				}
				dt, err = tablecodec.Unflatten(dt, &col.FieldType, ctx.GetEvalCtx().Location())
				if err != nil {
					return nil, nil, err
				}
				v[i] = dt
				continue
			}
			prefixCols[col.ID] = struct{}{}
		}
		colTps[col.ID] = &col.FieldType
	}
	rowMap, err := tablecodec.DecodeRowToDatumMap(value, colTps, ctx.GetEvalCtx().Location())
	if err != nil {
		return nil, rowMap, err
	}
	defaultVals := make([]types.Datum, len(cols))
	for i, col := range cols {
		if col == nil {
			continue
		}
		if col.IsPKHandleColumn(meta) || (col.IsCommonHandleColumn(meta) && !types.NeedRestoredData(&col.FieldType)) {
			if _, isPrefix := prefixCols[col.ID]; !isPrefix {
				continue
			}
		}
		ri, ok := rowMap[col.ID]
		if ok {
			v[i] = ri
			continue
		}
		if col.IsVirtualGenerated() {
			continue
		}
		if col.ChangeStateInfo != nil {
			v[i], _, err = GetChangingColVal(ctx, cols, col, rowMap, defaultVals)
		} else {
			v[i], err = GetColDefaultValue(ctx, col, defaultVals)
		}
		if err != nil {
			return nil, rowMap, err
		}
	}
	return v, rowMap, nil
}

// GetChangingColVal gets the changing column value when executing "modify/change column" statement.
// For statement like update-where, it will fetch the old row out and insert it into kv again.
// Since update statement can see the writable columns, it is responsible for the casting relative column / get the fault value here.
// old row : a-b-[nil]
// new row : a-b-[a'/default]
// Thus the writable new row is corresponding to Write-Only constraints.
func GetChangingColVal(ctx exprctx.BuildContext, cols []*table.Column, col *table.Column, rowMap map[int64]types.Datum, defaultVals []types.Datum) (_ types.Datum, isDefaultVal bool, err error) {
	relativeCol := cols[col.ChangeStateInfo.DependencyColumnOffset]
	idxColumnVal, ok := rowMap[relativeCol.ID]
	if ok {
		idxColumnVal, err = table.CastColumnValue(ctx, idxColumnVal, col.ColumnInfo, false, false)
		// TODO: Consider sql_mode and the error msg(encounter this error check whether to rollback).
		if err != nil {
			return idxColumnVal, false, errors.Trace(err)
		}
		return idxColumnVal, false, nil
	}

	idxColumnVal, err = GetColDefaultValue(ctx, col, defaultVals)
	if err != nil {
		return idxColumnVal, false, errors.Trace(err)
	}

	return idxColumnVal, true, nil
}


// IterRecords iterates records in the table and calls fn.
func IterRecords(t table.Table, ctx sessionctx.Context, cols []*table.Column,
	fn table.RecordIterFunc) error {
	prefix := t.RecordPrefix()
	txn, err := ctx.Txn(true)
	if err != nil {
		return err
	}

	startKey := tablecodec.EncodeRecordKey(t.RecordPrefix(), kv.IntHandle(math.MinInt64))
	it, err := txn.Iter(startKey, prefix.PrefixNext())
	if err != nil {
		return err
	}
	defer it.Close()

	if !it.Valid() {
		return nil
	}

	logutil.BgLogger().Debug("iterate records", zap.ByteString("startKey", startKey), zap.ByteString("key", it.Key()), zap.ByteString("value", it.Value()))

	colMap := make(map[int64]*types.FieldType, len(cols))
	for _, col := range cols {
		colMap[col.ID] = &col.FieldType
	}
	defaultVals := make([]types.Datum, len(cols))
	for it.Valid() && it.Key().HasPrefix(prefix) {
		// first kv pair is row lock information.
		// TODO: check valid lock
		// get row handle
		handle, err := tablecodec.DecodeRowKey(it.Key())
		if err != nil {
			return err
		}
		rowMap, err := tablecodec.DecodeRowToDatumMap(it.Value(), colMap, ctx.GetSessionVars().Location())
		if err != nil {
			return err
		}
		pkIds, decodeLoc := TryGetCommonPkColumnIds(t.Meta()), ctx.GetSessionVars().Location()
		data := make([]types.Datum, len(cols))
		for _, col := range cols {
			if col.IsPKHandleColumn(t.Meta()) {
				if mysql.HasUnsignedFlag(col.GetFlag()) {
					data[col.Offset].SetUint64(uint64(handle.IntValue()))
				} else {
					data[col.Offset].SetInt64(handle.IntValue())
				}
				continue
			} else if mysql.HasPriKeyFlag(col.GetFlag()) {
				data[col.Offset], err = tryDecodeColumnFromCommonHandle(col, handle, pkIds, decodeLoc)
				if err != nil {
					return err
				}
				continue
			}
			if _, ok := rowMap[col.ID]; ok {
				data[col.Offset] = rowMap[col.ID]
				continue
			}
			data[col.Offset], err = GetColDefaultValue(ctx.GetExprCtx(), col, defaultVals)
			if err != nil {
				return err
			}
		}
		more, err := fn(handle, data, cols)
		if !more || err != nil {
			return err
		}

		rk := tablecodec.EncodeRecordKey(t.RecordPrefix(), handle)
		err = kv.NextUntil(it, util.RowKeyPrefixFilter(rk))
		if err != nil {
			return err
		}
	}

	return nil
}

func tryDecodeColumnFromCommonHandle(col *table.Column, handle kv.Handle, pkIds []int64, decodeLoc *time.Location) (types.Datum, error) {
	for i, hid := range pkIds {
		if hid != col.ID {
			continue
		}
		_, d, err := codec.DecodeOne(handle.EncodedCol(i))
		if err != nil {
			return types.Datum{}, errors.Trace(err)
		}
		if d, err = tablecodec.Unflatten(d, &col.FieldType, decodeLoc); err != nil {
			return types.Datum{}, err
		}
		return d, nil
	}
	return types.Datum{}, nil
}

// GetColDefaultValue gets a column default value.
// The defaultVals is used to avoid calculating the default value multiple times.
func GetColDefaultValue(ctx exprctx.BuildContext, col *table.Column, defaultVals []types.Datum) (
	colVal types.Datum, err error) {
	if col.GetOriginDefaultValue() == nil && mysql.HasNotNullFlag(col.GetFlag()) {
		return colVal, errors.New("Miss column")
	}
	if defaultVals[col.Offset].IsNull() {
		colVal, err = table.GetColOriginDefaultValue(ctx, col.ToInfo())
		if err != nil {
			return colVal, err
		}
		defaultVals[col.Offset] = colVal
	} else {
		colVal = defaultVals[col.Offset]
	}

	return colVal, nil
}

// AllocHandle allocate a new handle.
// A statement could reserve some ID in the statement context, try those ones first.
func AllocHandle(ctx context.Context, mctx table.MutateContext, t table.Table) (kv.IntHandle,
	error) {
	if mctx != nil {
		if reserved, ok := mctx.GetReservedRowIDAlloc(); ok {
			// First try to alloc if the statement has reserved auto ID.
			if rowID, ok := reserved.Consume(); ok {
				return kv.IntHandle(rowID), nil
			}
		}
	}

	_, rowID, err := AllocHandleIDs(ctx, mctx, t, 1)
	return kv.IntHandle(rowID), err
}

// AllocHandleIDs allocates n handle ids (_tidb_rowid), and caches the range
// in the table.MutateContext.
func AllocHandleIDs(ctx context.Context, mctx table.MutateContext, t table.Table, n uint64) (int64, int64, error) {
	meta := t.Meta()
	base, maxID, err := t.Allocators(mctx).Get(autoid.RowIDAllocType).Alloc(ctx, n, 1, 1)
	if err != nil {
		return 0, 0, errors.Trace(err)
	}
	if meta.ShardRowIDBits > 0 {
		shardFmt := autoid.NewShardIDFormat(types.NewFieldType(mysql.TypeLonglong), meta.ShardRowIDBits, autoid.RowIDBitLength)
		// Use max record ShardRowIDBits to check overflow.
		if OverflowShardBits(maxID, meta.MaxShardRowIDBits, autoid.RowIDBitLength, true) {
			// If overflow, the rowID may be duplicated. For examples,
			// t.meta.ShardRowIDBits = 4
			// rowID = 0010111111111111111111111111111111111111111111111111111111111111
			// shard = 0100000000000000000000000000000000000000000000000000000000000000
			// will be duplicated with:
			// rowID = 0100111111111111111111111111111111111111111111111111111111111111
			// shard = 0010000000000000000000000000000000000000000000000000000000000000
			return 0, 0, autoid.ErrAutoincReadFailed
		}
		shard := mctx.GetRowIDShardGenerator().GetCurrentShard(int(n))
		base = shardFmt.Compose(shard, base)
		maxID = shardFmt.Compose(shard, maxID)
	}
	return base, maxID, nil
}

// OverflowShardBits checks whether the recordID overflow `1<<(typeBitsLength-shardRowIDBits-1) -1`.
func OverflowShardBits(recordID int64, shardRowIDBits uint64, typeBitsLength uint64, reservedSignBit bool) bool {
	var signBit uint64
	if reservedSignBit {
		signBit = 1
	}
	mask := (1<<shardRowIDBits - 1) << (typeBitsLength - shardRowIDBits - signBit)
	return recordID&int64(mask) > 0
}

// Allocators implements table.Table Allocators interface.
func (t *TableCommon) Allocators(ctx table.AllocatorContext) autoid.Allocators {
	if ctx == nil {
		return t.allocs
	}
	if alloc, ok := ctx.AlternativeAllocators(t.meta); ok {
		return alloc
	}
	return t.allocs
}

// Type implements table.Table Type interface.
func (t *TableCommon) Type() table.Type {
	return table.NormalTable
}

func (t *TableCommon) canSkip(col *table.Column, value *types.Datum) bool {
	return CanSkip(t.Meta(), col, value)
}

// CanSkip is for these cases, we can skip the columns in encoded row:
// 1. the column is included in primary key;
// 2. the column's default value is null, and the value equals to that but has no origin default;
// 3. the column is virtual generated.
func CanSkip(info *model.TableInfo, col *table.Column, value *types.Datum) bool {
	if col.IsPKHandleColumn(info) {
		return true
	}
	if col.IsCommonHandleColumn(info) {
		pkIdx := FindPrimaryIndex(info)
		for _, idxCol := range pkIdx.Columns {
			if info.Columns[idxCol.Offset].ID != col.ID {
				continue
			}
			canSkip := idxCol.Length == types.UnspecifiedLength
			canSkip = canSkip && !types.NeedRestoredData(&col.FieldType)
			return canSkip
		}
	}
	if col.GetDefaultValue() == nil && value.IsNull() && col.GetOriginDefaultValue() == nil {
		return true
	}
	if col.IsVirtualGenerated() {
		return true
	}
	return false
}

