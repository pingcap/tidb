package util

import (
	"github.com/pingcap/errors"

	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/rowcodec"
)

// BuildRowcodecColInfoForIndexColumns builds []rowcodec.ColInfo for the given index.
// The result can be used for decoding index key-values.
func BuildRowcodecColInfoForIndexColumns(idxInfo *model.IndexInfo, tblInfo *model.TableInfo) []rowcodec.ColInfo {
	colInfo := make([]rowcodec.ColInfo, 0, len(idxInfo.Columns))
	for _, idxCol := range idxInfo.Columns {
		col := tblInfo.Columns[idxCol.Offset]
		colInfo = append(colInfo, rowcodec.ColInfo{
			ID:         col.ID,
			IsPKHandle: tblInfo.PKIsHandle && mysql.HasPriKeyFlag(col.Flag),
			Ft:         rowcodec.FieldTypeFromModelColumn(col),
		})
	}
	return colInfo
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

// DecodeRawRowData decodes raw Row data into a datum slice and a (columnID:columnValue) map.
func DecodeRawRowData(ctx sessionctx.Context, meta *model.TableInfo, h kv.Handle, cols []*table.Column,
	value []byte) ([]types.Datum, map[int64]types.Datum, error) {
	v := make([]types.Datum, len(cols))
	colTps := make(map[int64]*types.FieldType, len(cols))
	prefixCols := make(map[int64]struct{})
	for i, col := range cols {
		if col == nil {
			continue
		}
		if col.IsPKHandleColumn(meta) {
			if mysql.HasUnsignedFlag(col.Flag) {
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
				dt, err = tablecodec.Unflatten(dt, &col.FieldType, ctx.GetSessionVars().Location())
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
	rowMap, err := tablecodec.DecodeRowToDatumMap(value, colTps, ctx.GetSessionVars().Location())
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
		if col.IsGenerated() && !col.GeneratedStored {
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

// FindPrimaryIndex uses to find primary index in tableInfo.
func FindPrimaryIndex(tblInfo *model.TableInfo) *model.IndexInfo {
	var pkIdx *model.IndexInfo
	for _, idx := range tblInfo.Indices {
		if idx.Primary {
			pkIdx = idx
			break
		}
	}
	return pkIdx
}

// GetColDefaultValue gets a column default value.
// The defaultVals is used to avoid calculating the default value multiple times.
func GetColDefaultValue(ctx sessionctx.Context, col *table.Column, defaultVals []types.Datum) (
	colVal types.Datum, err error) {
	if col.GetOriginDefaultValue() == nil && mysql.HasNotNullFlag(col.Flag) {
		return colVal, errors.New("Miss column")
	}
	if col.State != model.StatePublic {
		return colVal, nil
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

// GetChangingColVal gets the changing column value when executing "modify/change column" statement.
// For statement like update-where, it will fetch the old Row out and insert it into kv again.
// Since update statement can see the writable columns, it is responsible for the casting relative column / get the fault value here.
// old Row : a-b-[nil]
// new Row : a-b-[a'/default]
// Thus the writable new Row is corresponding to Write-Only constraints.
func GetChangingColVal(ctx sessionctx.Context, cols []*table.Column, col *table.Column, rowMap map[int64]types.Datum, defaultVals []types.Datum) (_ types.Datum, isDefaultVal bool, err error) {
	relativeCol := cols[col.ChangeStateInfo.DependencyColumnOffset]
	idxColumnVal, ok := rowMap[relativeCol.ID]
	if ok {
		idxColumnVal, err = table.CastValue(ctx, idxColumnVal, col.ColumnInfo, false, false)
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
