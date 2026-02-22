// Copyright 2024 PingCAP, Inc.
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

package logicalop

import (
	"fmt"
	"slices"

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/partidx"
	"github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/collate"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
)

func (ds *DataSource) buildTableGather() base.LogicalPlan {
	ts := LogicalTableScan{Source: ds, HandleCols: ds.HandleCols}.Init(ds.SCtx(), ds.QueryBlockOffset())
	ts.SetSchema(ds.Schema())
	sg := TiKVSingleGather{Source: ds, IsIndexGather: false}.Init(ds.SCtx(), ds.QueryBlockOffset())
	sg.SetSchema(ds.Schema())
	sg.SetChildren(ts)
	return sg
}

func (ds *DataSource) buildIndexGather(path *util.AccessPath) base.LogicalPlan {
	is := LogicalIndexScan{
		Source:         ds,
		IsDoubleRead:   false,
		Index:          path.Index,
		FullIdxCols:    path.FullIdxCols,
		FullIdxColLens: path.FullIdxColLens,
		IdxCols:        path.IdxCols,
		IdxColLens:     path.IdxColLens,
	}.Init(ds.SCtx(), ds.QueryBlockOffset())
	is.SetNoncacheableReason(path.NoncacheableReason)

	is.Columns = make([]*model.ColumnInfo, len(ds.Columns))
	copy(is.Columns, ds.Columns)
	is.SetSchema(ds.Schema())
	is.IdxCols, is.IdxColLens = util.IndexInfo2PrefixCols(is.Columns, is.Schema().Columns, is.Index)

	sg := TiKVSingleGather{
		Source:        ds,
		IsIndexGather: true,
		Index:         path.Index,
	}.Init(ds.SCtx(), ds.QueryBlockOffset())
	sg.SetNoncacheableReason(path.NoncacheableReason)
	sg.SetSchema(ds.Schema())
	sg.SetChildren(is)
	return sg
}

// Convert2Gathers builds logical TiKVSingleGathers from DataSource.
func (ds *DataSource) Convert2Gathers() (gathers []base.LogicalPlan) {
	tg := ds.buildTableGather()
	gathers = append(gathers, tg)
	for _, path := range ds.PossibleAccessPaths {
		if !path.IsIntHandlePath {
			path.IdxCols, path.IdxColLens, path.FullIdxCols, path.FullIdxColLens =
				util.IndexInfo2Cols(ds.Columns, ds.Schema().Columns, path.Index)
			// If index columns can cover all the needed columns, we can use a IndexGather + IndexScan.
			if ds.IsSingleScan(path.FullIdxCols, path.FullIdxColLens) {
				gathers = append(gathers, ds.buildIndexGather(path))
			}
			// TODO: If index columns can not cover the schema, use IndexLookUpGather.
		}
	}
	return gathers
}

func getPKIsHandleColFromSchema(cols []*model.ColumnInfo, schema *expression.Schema, pkIsHandle bool) *expression.Column {
	if !pkIsHandle {
		// If the PKIsHandle is false, return the ExtraHandleColumn.
		for i, col := range cols {
			if col.ID == model.ExtraHandleID {
				return schema.Columns[i]
			}
		}
		return nil
	}
	for i, col := range cols {
		if mysql.HasPriKeyFlag(col.GetFlag()) {
			return schema.Columns[i]
		}
	}
	return nil
}

// GetPKIsHandleCol gets the handle column if the PKIsHandle is true, otherwise, returns the ExtraHandleColumn.
func (ds *DataSource) GetPKIsHandleCol() *expression.Column {
	return getPKIsHandleColFromSchema(ds.Columns, ds.Schema(), ds.TableInfo.PKIsHandle)
}

// NewExtraHandleSchemaCol creates a new column for extra handle.
func (ds *DataSource) NewExtraHandleSchemaCol() *expression.Column {
	tp := types.NewFieldType(mysql.TypeLonglong)
	tp.SetFlag(mysql.NotNullFlag | mysql.PriKeyFlag)
	return &expression.Column{
		RetType:  tp,
		UniqueID: ds.SCtx().GetSessionVars().AllocPlanColumnID(),
		ID:       model.ExtraHandleID,
		OrigName: fmt.Sprintf("%v.%v.%v", ds.DBName, ds.TableInfo.Name, model.ExtraHandleName),
	}
}

// NewExtraCommitTSSchemaCol creates a new column for extra commit ts.
func (ds *DataSource) NewExtraCommitTSSchemaCol() *expression.Column {
	tp := types.NewFieldType(mysql.TypeLonglong)
	return &expression.Column{
		RetType:  tp,
		UniqueID: ds.SCtx().GetSessionVars().AllocPlanColumnID(),
		ID:       model.ExtraCommitTSID,
		OrigName: fmt.Sprintf("%v.%v.%v", ds.DBName, ds.TableInfo.Name, model.ExtraCommitTSName),
	}
}

func preferKeyColumnFromTable(dataSource *DataSource, originColumns []*expression.Column,
	originSchemaColumns []*model.ColumnInfo) (*expression.Column, *model.ColumnInfo) {
	var resultColumnInfo *model.ColumnInfo
	var resultColumn *expression.Column
	if dataSource.Table.Type().IsClusterTable() {
		// For cluster tables, ExtraHandleID is not valid as they are memory tables.
		// Use the first column from originColumns if available, otherwise fall back
		// to the first column from table metadata.
		if len(originColumns) > 0 {
			resultColumnInfo = originSchemaColumns[0]
			resultColumn = originColumns[0]
		} else {
			cols := dataSource.Table.Meta().Columns
			if len(cols) > 0 {
				col := cols[0]
				resultColumnInfo = col
				resultColumn = &expression.Column{
					RetType:  col.FieldType.Clone(),
					UniqueID: dataSource.SCtx().GetSessionVars().AllocPlanColumnID(),
					ID:       col.ID,
					OrigName: fmt.Sprintf("%v.%v.%v", dataSource.DBName, dataSource.TableInfo.Name, col.Name),
				}
			} else {
				// All cluster tables must have at least one column in their metadata.
				// If this is ever reached, it indicates a bug in table registration.
				logutil.BgLogger().Error("cluster table has no metadata columns",
					zap.String("db", dataSource.DBName.L),
					zap.String("table", dataSource.TableInfo.Name.L))
				resultColumn = dataSource.NewExtraHandleSchemaCol()
				resultColumnInfo = model.NewExtraHandleColInfo()
			}
		}
	} else {
		if dataSource.HandleCols != nil {
			resultColumn = dataSource.HandleCols.GetCol(0)
			resultColumnInfo = resultColumn.ToInfo()
		} else if dataSource.Table.Meta().PKIsHandle {
			// dataSource.HandleCols = nil doesn't mean datasource doesn't have a intPk handle.
			// since datasource.HandleCols will be cleared in the first columnPruner.
			resultColumn = dataSource.UnMutableHandleCols.GetCol(0)
			resultColumnInfo = resultColumn.ToInfo()
		} else {
			resultColumn = dataSource.NewExtraHandleSchemaCol()
			resultColumnInfo = model.NewExtraHandleColInfo()
		}
	}
	return resultColumn, resultColumnInfo
}

// IsSingleScan checks whether all the needed columns and conditions can be covered by the index.
func (ds *DataSource) IsSingleScan(indexColumns []*expression.Column, idxColLens []int) bool {
	if !ds.SCtx().GetSessionVars().OptPrefixIndexSingleScan || ds.ColsRequiringFullLen == nil {
		// ds.ColsRequiringFullLen is set at (*DataSource).PruneColumns. In some cases we don't reach (*DataSource).PruneColumns
		// and ds.ColsRequiringFullLen is nil, so we fall back to ds.isIndexCoveringColumns(ds.schema.Columns, indexColumns, idxColLens).
		return ds.IsIndexCoveringColumns(ds.Schema().Columns, indexColumns, idxColLens)
	}
	if !ds.IsIndexCoveringColumns(ds.ColsRequiringFullLen, indexColumns, idxColLens) {
		return false
	}
	for _, cond := range ds.AllConds {
		if !ds.IsIndexCoveringCondition(cond, indexColumns, idxColLens) {
			return false
		}
	}
	return true
}

// IsIndexCoveringColumns checks whether all the needed columns can be covered by the index.
func (ds *DataSource) IsIndexCoveringColumns(columns, indexColumns []*expression.Column, idxColLens []int) bool {
	for _, col := range columns {
		if !ds.indexCoveringColumn(col, indexColumns, idxColLens, false) {
			return false
		}
	}
	return true
}

// IsIndexCoveringCondition checks whether all the needed columns in the condition can be covered by the index.
func (ds *DataSource) IsIndexCoveringCondition(condition expression.Expression, indexColumns []*expression.Column, idxColLens []int) bool {
	switch v := condition.(type) {
	case *expression.Column:
		return ds.indexCoveringColumn(v, indexColumns, idxColLens, false)
	case *expression.ScalarFunction:
		// Even if the index only contains prefix `col`, the index can cover `col is null`.
		if v.FuncName.L == ast.IsNull {
			if col, ok := v.GetArgs()[0].(*expression.Column); ok {
				return ds.indexCoveringColumn(col, indexColumns, idxColLens, true)
			}
		}
		for _, arg := range v.GetArgs() {
			if !ds.IsIndexCoveringCondition(arg, indexColumns, idxColLens) {
				return false
			}
		}
		return true
	}
	return true
}

type handleCoverState uint8

const (
	stateNotCoveredByHandle handleCoverState = iota
	stateCoveredByIntHandle
	stateCoveredByCommonHandle
)

func (ds *DataSource) indexCoveringColumn(column *expression.Column, indexColumns []*expression.Column, idxColLens []int, ignoreLen bool) bool {
	handleCoveringState := ds.handleCoveringColumn(column, ignoreLen)
	// Original int pk can always cover the column.
	if handleCoveringState == stateCoveredByIntHandle {
		return true
	}
	evalCtx := ds.SCtx().GetExprCtx().GetEvalCtx()
	coveredByPlainIndex := isIndexColsCoveringCol(evalCtx, column, indexColumns, idxColLens, ignoreLen)
	if !coveredByPlainIndex && handleCoveringState != stateCoveredByCommonHandle {
		return false
	}
	isClusteredNewCollationIdx := collate.NewCollationEnabled() &&
		column.GetType(evalCtx).EvalType() == types.ETString &&
		!mysql.HasBinaryFlag(column.GetType(evalCtx).GetFlag())
	if !coveredByPlainIndex && handleCoveringState == stateCoveredByCommonHandle && isClusteredNewCollationIdx && ds.Table.Meta().CommonHandleVersion == 0 {
		return false
	}
	return true
}

// handleCoveringColumn checks if the column is covered by the primary key or extra handle columns.
func (ds *DataSource) handleCoveringColumn(column *expression.Column, ignoreLen bool) handleCoverState {
	if ds.TableInfo.PKIsHandle && mysql.HasPriKeyFlag(column.RetType.GetFlag()) {
		return stateCoveredByIntHandle
	}
	if column.ID == model.ExtraHandleID || column.ID == model.ExtraPhysTblID {
		return stateCoveredByIntHandle
	}
	evalCtx := ds.SCtx().GetExprCtx().GetEvalCtx()
	coveredByClusteredIndex := isIndexColsCoveringCol(evalCtx, column, ds.CommonHandleCols, ds.CommonHandleLens, ignoreLen)
	if coveredByClusteredIndex {
		return stateCoveredByCommonHandle
	}
	return stateNotCoveredByHandle
}

func isIndexColsCoveringCol(sctx expression.EvalContext, col *expression.Column, indexCols []*expression.Column, idxColLens []int, ignoreLen bool) bool {
	for i, indexCol := range indexCols {
		if indexCol == nil || !col.EqualByExprAndID(sctx, indexCol) {
			continue
		}
		if ignoreLen || idxColLens[i] == types.UnspecifiedLength || idxColLens[i] == col.RetType.GetFlen() {
			return true
		}
	}
	return false
}

// AppendTableCol appends a column to the original columns of the table before pruning,
// accessed through ds.TblCols and ds.TblColsByID.
func (ds *DataSource) AppendTableCol(col *expression.Column) {
	ds.TblCols = append(ds.TblCols, col)
	ds.TblColsByID[col.ID] = col
}

// CheckPartialIndexes checks and removes the partial indexes that cannot be used according to the pushed down conditions.
// It will go through each partial index to see whether it's condition constraints are all satisfied by the pushed down conditions.
// Detailed checking can be found in the comment of `CheckConstraints`.
// And we specially implement a `AlwaysMeetConstraints` function for IS NOT NULL constraint to make it suitable for plan cache.
// It's a special handler now, and it's not easy to extend to other constraints.
func (ds *DataSource) CheckPartialIndexes() {
	var columnNames types.NameSlice
	var removedPaths map[int64]struct{}
	partialIndexUsedHint, hasPartialIndex := false, false
	for _, path := range ds.PossibleAccessPaths {
		// If there is no condition expression, it is not a partial index.
		// So we skip it directly.
		if path.Index == nil || path.Index.ConditionExprString == "" {
			continue
		}
		hasPartialIndex = true
		if columnNames == nil {
			columnNames = make(types.NameSlice, 0, ds.schema.Len())
			for i := range ds.Schema().Columns {
				columnNames = append(columnNames, &types.FieldName{
					TblName: ds.TableInfo.Name,
					ColName: ds.Columns[i].Name,
				})
			}
		}
		// Convert the raw string expression to Expression.
		expr, err := expression.ParseSimpleExpr(ds.SCtx().GetExprCtx(), path.Index.ConditionExprString, expression.WithInputSchemaAndNames(ds.schema, columnNames, ds.TableInfo))
		cnfExprs := expression.SplitCNFItems(expr)
		if err != nil || !partidx.CheckConstraints(ds.SCtx(), cnfExprs, ds.PushedDownConds) {
			if removedPaths == nil {
				removedPaths = make(map[int64]struct{})
			}
			removedPaths[path.Index.ID] = struct{}{}
			continue
		}
		if path.Forced {
			partialIndexUsedHint = true
		}
		// A special handler for plan cache.
		// We only do it for single IS NOT NULL constraint now.
		if ds.SCtx().GetSessionVars().StmtCtx.UseCache() {
			if !partidx.AlwaysMeetConstraints(ds.SCtx(), cnfExprs, ds.PushedDownConds) {
				path.NoncacheableReason = "IndexScan of partial index is uncacheable"
			}
		}
	}
	// 1. No partial index,
	// 2. Or no partial index is removed and no partial index is used by hint.
	// In these cases, we don't need to do anything.
	if !hasPartialIndex || (len(removedPaths) == 0 && !partialIndexUsedHint) {
		return
	}
	checkIndex := func(path *util.AccessPath, checkForced bool) bool {
		isRemoved := false
		if path.Index != nil {
			_, isRemoved = removedPaths[path.Index.ID]
		}
		return isRemoved || (checkForced && !path.Forced)
	}
	if partialIndexUsedHint {
		ds.AllPossibleAccessPaths = slices.DeleteFunc(ds.AllPossibleAccessPaths, func(path *util.AccessPath) bool {
			return checkIndex(path, true)
		})
		ds.PossibleAccessPaths = slices.DeleteFunc(ds.PossibleAccessPaths, func(path *util.AccessPath) bool {
			return checkIndex(path, true)
		})
	} else {
		ds.AllPossibleAccessPaths = slices.DeleteFunc(ds.AllPossibleAccessPaths, func(path *util.AccessPath) bool {
			return checkIndex(path, false)
		})
		ds.PossibleAccessPaths = slices.DeleteFunc(ds.PossibleAccessPaths, func(path *util.AccessPath) bool {
			return checkIndex(path, false)
		})
	}
}
