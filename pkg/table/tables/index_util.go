// Copyright 2016 PingCAP, Inc.
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
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/expression/exprstatic"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/sessionctx/stmtctx"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util"
	contextutil "github.com/pingcap/tidb/pkg/util/context"
	"github.com/pingcap/tidb/pkg/util/rowcodec"
)

// FindChangingCol finds the changing column in idxInfo.
func FindChangingCol(cols []*table.Column, idxInfo *model.IndexInfo) *table.Column {
	for _, ic := range idxInfo.Columns {
		if col := cols[ic.Offset]; col.ChangeStateInfo != nil {
			return col
		}
	}
	return nil
}

// IsIndexWritable check whether the index is writable.
func IsIndexWritable(idx table.Index) bool {
	s := idx.Meta().State
	if s != model.StateDeleteOnly && s != model.StateDeleteReorganization {
		return true
	}
	return false
}

// BuildRowcodecColInfoForIndexColumns builds []rowcodec.ColInfo for the given index.
// The result can be used for decoding index key-values.
func BuildRowcodecColInfoForIndexColumns(idxInfo *model.IndexInfo, tblInfo *model.TableInfo) []rowcodec.ColInfo {
	colInfo := make([]rowcodec.ColInfo, 0, len(idxInfo.Columns))
	for _, idxCol := range idxInfo.Columns {
		col := tblInfo.Columns[idxCol.Offset]
		ft := model.GetIdxChangingFieldType(idxCol, col).Clone()
		colInfo = append(colInfo, rowcodec.ColInfo{
			ID:         col.ID,
			IsPKHandle: tblInfo.PKIsHandle && mysql.HasPriKeyFlag(ft.GetFlag()),
			Ft:         ft,
		})
	}
	return colInfo
}

// BuildFieldTypesForIndexColumns builds the index columns field types.
func BuildFieldTypesForIndexColumns(idxInfo *model.IndexInfo, tblInfo *model.TableInfo) []*types.FieldType {
	tps := make([]*types.FieldType, 0, len(idxInfo.Columns))
	for _, idxCol := range idxInfo.Columns {
		col := tblInfo.Columns[idxCol.Offset]
		tps = append(tps, rowcodec.FieldTypeFromModelColumn(col))
	}
	return tps
}

// TryAppendCommonHandleRowcodecColInfos tries to append common handle columns to `colInfo`.
func TryAppendCommonHandleRowcodecColInfos(colInfo []rowcodec.ColInfo, tblInfo *model.TableInfo) []rowcodec.ColInfo {
	if !tblInfo.IsCommonHandle || tblInfo.CommonHandleVersion == 0 {
		return colInfo
	}
	if pkIdx := FindPrimaryIndex(tblInfo); pkIdx != nil {
		for _, idxCol := range pkIdx.Columns {
			col := tblInfo.Columns[idxCol.Offset]
			colInfo = append(colInfo, rowcodec.ColInfo{
				ID: col.ID,
				Ft: rowcodec.FieldTypeFromModelColumn(col),
			})
		}
	}
	return colInfo
}

// GenIndexValueFromIndex generate index value from index.
func GenIndexValueFromIndex(key []byte, value []byte, tblInfo *model.TableInfo, idxInfo *model.IndexInfo) ([]string, error) {
	idxColLen := len(idxInfo.Columns)
	colInfos := BuildRowcodecColInfoForIndexColumns(idxInfo, tblInfo)
	values, err := tablecodec.DecodeIndexKV(key, value, idxColLen, tablecodec.HandleNotNeeded, colInfos)
	if err != nil {
		return nil, errors.Trace(err)
	}
	valueStr := make([]string, 0, idxColLen)
	for i, val := range values[:idxColLen] {
		d, err := tablecodec.DecodeColumnValue(val, colInfos[i].Ft, time.Local)
		if err != nil {
			return nil, errors.Trace(err)
		}
		str, err := d.ToString()
		if err != nil {
			str = string(val)
		}
		if types.IsBinaryStr(colInfos[i].Ft) || types.IsTypeBit(colInfos[i].Ft) {
			str = util.FmtNonASCIIPrintableCharToHex(str, len(str), true)
		}
		valueStr = append(valueStr, str)
	}

	return valueStr, nil
}

// ExtractColumnsFromCondition returns the columns that are referenced in the index condition expression.
// If `includeColumnsReferencedByVirtualGeneratedColumns` is true, it will recursively extract the columns from the virtual generated columns.
// The returned columns might be duplicated.
func ExtractColumnsFromCondition(ctx expression.BuildContext, idxInfo *model.IndexInfo, tblInfo *model.TableInfo, includeColumnsReferencedByVirtualGeneratedColumns bool) ([]*model.IndexColumn, error) {
	if len(idxInfo.ConditionExprString) == 0 {
		return nil, nil
	}

	expr, err := expression.ParseSimpleExpr(ctx, idxInfo.ConditionExprString, expression.WithTableInfo("", tblInfo))
	if err != nil {
		return nil, err
	}
	return extractColumnsFromExpr(expr, tblInfo, includeColumnsReferencedByVirtualGeneratedColumns)
}

// DedupIndexColumns deduplicates the index columns based on their Offset.
func DedupIndexColumns(cols []*model.IndexColumn) []*model.IndexColumn {
	if len(cols) <= 1 {
		return cols
	}

	seen := make(map[int]struct{}, len(cols))
	result := make([]*model.IndexColumn, 0, len(cols))
	for _, col := range cols {
		if _, found := seen[col.Offset]; !found {
			seen[col.Offset] = struct{}{}
			result = append(result, col)
		}
	}
	return result
}

// extractColumnsFromExpr extracts the columns from the given expression.
// If `includeVirtualGeneratedColumn` is true, it will recursively extract the columns from the virtual generated columns.
// The returned columns might be duplicated.
func extractColumnsFromExpr(expr expression.Expression, tblInfo *model.TableInfo, includeVirtualGeneratedColumn bool) ([]*model.IndexColumn, error) {
	var neededCols []*model.IndexColumn
	cols := expression.ExtractColumns(expr)
	for _, col := range cols {
		if tblInfo.Columns[col.Index].IsVirtualGenerated() {
			if includeVirtualGeneratedColumn {
				depCols, err := extractColumnsFromExpr(col.VirtualExpr, tblInfo, includeVirtualGeneratedColumn)
				if err != nil {
					return nil, err
				}

				neededCols = append(neededCols, depCols...)
			}

			neededCols = append(neededCols, &model.IndexColumn{
				Name:   tblInfo.Columns[col.Index].Name,
				Offset: col.Index,
			})
		} else {
			neededCols = append(neededCols, &model.IndexColumn{
				Name:   tblInfo.Columns[col.Index].Name,
				Offset: col.Index,
			})
		}
	}

	return neededCols, nil
}

func init() {
	evalCtx := exprstatic.NewEvalContext(
		exprstatic.WithSQLMode(mysql.ModeNone),
		exprstatic.WithTypeFlags(types.DefaultStmtFlags),
		exprstatic.WithErrLevelMap(stmtctx.DefaultStmtErrLevels),
	)

	planCacheTracker := contextutil.NewPlanCacheTracker(contextutil.IgnoreWarn)

	indexConditionECtx = exprstatic.NewExprContext(
		exprstatic.WithEvalCtx(evalCtx),
		exprstatic.WithPlanCacheTracker(&planCacheTracker),
	)
}
