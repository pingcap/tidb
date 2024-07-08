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

package core

import (
	"math"

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/cardinality"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/cost"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	"github.com/pingcap/tidb/pkg/planner/property"
	"github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/planner/util/debugtrace"
	"github.com/pingcap/tidb/pkg/planner/util/tablesampler"
	"github.com/pingcap/tidb/pkg/statistics"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/types"
	h "github.com/pingcap/tidb/pkg/util/hint"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/ranger"
	"go.uber.org/zap"
)

// DataSource represents a tableScan without condition push down.
type DataSource struct {
	logicalop.LogicalSchemaProducer

	AstIndexHints []*ast.IndexHint
	IndexHints    []h.HintedIndex
	table         table.Table
	TableInfo     *model.TableInfo
	Columns       []*model.ColumnInfo
	DBName        model.CIStr

	TableAsName *model.CIStr
	// IndexMergeHints are the hint for indexmerge.
	IndexMergeHints []h.HintedIndex
	// PushedDownConds are the conditions that will be pushed down to coprocessor.
	PushedDownConds []expression.Expression
	// AllConds contains all the filters on this table. For now it's maintained
	// in predicate push down and used in partition pruning/index merge.
	AllConds []expression.Expression

	StatisticTable *statistics.Table
	TableStats     *property.StatsInfo

	// PossibleAccessPaths stores all the possible access path for physical plan, including table scan.
	PossibleAccessPaths []*util.AccessPath

	// The data source may be a partition, rather than a real table.
	PartitionDefIdx *int
	PhysicalTableID int64
	PartitionNames  []model.CIStr

	// handleCol represents the handle column for the datasource, either the
	// int primary key column or extra handle column.
	// handleCol *expression.Column
	HandleCols          util.HandleCols
	UnMutableHandleCols util.HandleCols
	// TblCols contains the original columns of table before being pruned, and it
	// is used for estimating table scan cost.
	TblCols []*expression.Column
	// CommonHandleCols and CommonHandleLens save the info of primary key which is the clustered index.
	CommonHandleCols []*expression.Column
	CommonHandleLens []int
	// TblColHists contains the Histogram of all original table columns,
	// it is converted from StatisticTable, and used for IO/network cost estimating.
	TblColHists *statistics.HistColl
	// PreferStoreType means the DataSource is enforced to which storage.
	PreferStoreType int
	// PreferPartitions store the map, the key represents store type, the value represents the partition name list.
	PreferPartitions map[int][]model.CIStr
	SampleInfo       *tablesampler.TableSampleInfo
	IS               infoschema.InfoSchema
	// IsForUpdateRead should be true in either of the following situations
	// 1. use `inside insert`, `update`, `delete` or `select for update` statement
	// 2. isolation level is RC
	IsForUpdateRead bool

	// contain unique index and the first field is tidb_shard(),
	// such as (tidb_shard(a), a ...), the fields are more than 2
	ContainExprPrefixUk bool

	// ColsRequiringFullLen is the columns that must be fetched with full length.
	// It is used to decide whether single scan is enough when reading from an index.
	ColsRequiringFullLen []*expression.Column

	// AccessPathMinSelectivity is the minimal selectivity among the access paths.
	// It's calculated after we generated the access paths and estimated row count for them, and before entering findBestTask.
	// It considers CountAfterIndex for index paths and CountAfterAccess for table paths and index merge paths.
	AccessPathMinSelectivity float64
}

// ExtractCorrelatedCols implements LogicalPlan interface.
func (ds *DataSource) ExtractCorrelatedCols() []*expression.CorrelatedColumn {
	corCols := make([]*expression.CorrelatedColumn, 0, len(ds.PushedDownConds))
	for _, expr := range ds.PushedDownConds {
		corCols = append(corCols, expression.ExtractCorColumns(expr)...)
	}
	return corCols
}

// getTablePath finds the TablePath from a group of accessPaths.
func getTablePath(paths []*util.AccessPath) *util.AccessPath {
	for _, path := range paths {
		if path.IsTablePath() {
			return path
		}
	}
	return nil
}

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

	is.Columns = make([]*model.ColumnInfo, len(ds.Columns))
	copy(is.Columns, ds.Columns)
	is.SetSchema(ds.Schema())
	is.IdxCols, is.IdxColLens = expression.IndexInfo2PrefixCols(is.Columns, is.Schema().Columns, is.Index)

	sg := TiKVSingleGather{
		Source:        ds,
		IsIndexGather: true,
		Index:         path.Index,
	}.Init(ds.SCtx(), ds.QueryBlockOffset())
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
			path.FullIdxCols, path.FullIdxColLens = expression.IndexInfo2Cols(ds.Columns, ds.Schema().Columns, path.Index)
			path.IdxCols, path.IdxColLens = expression.IndexInfo2PrefixCols(ds.Columns, ds.Schema().Columns, path.Index)
			// If index columns can cover all of the needed columns, we can use a IndexGather + IndexScan.
			if ds.isSingleScan(path.FullIdxCols, path.FullIdxColLens) {
				gathers = append(gathers, ds.buildIndexGather(path))
			}
			// TODO: If index columns can not cover the schema, use IndexLookUpGather.
		}
	}
	return gathers
}

func detachCondAndBuildRangeForPath(
	sctx base.PlanContext,
	path *util.AccessPath,
	conds []expression.Expression,
	histColl *statistics.HistColl,
) error {
	if len(path.IdxCols) == 0 {
		path.TableFilters = conds
		return nil
	}
	res, err := ranger.DetachCondAndBuildRangeForIndex(sctx.GetRangerCtx(), conds, path.IdxCols, path.IdxColLens, sctx.GetSessionVars().RangeMaxSize)
	if err != nil {
		return err
	}
	path.Ranges = res.Ranges
	path.AccessConds = res.AccessConds
	path.TableFilters = res.RemainedConds
	path.EqCondCount = res.EqCondCount
	path.EqOrInCondCount = res.EqOrInCount
	path.IsDNFCond = res.IsDNFCond
	path.ConstCols = make([]bool, len(path.IdxCols))
	if res.ColumnValues != nil {
		for i := range path.ConstCols {
			path.ConstCols[i] = res.ColumnValues[i] != nil
		}
	}
	path.CountAfterAccess, err = cardinality.GetRowCountByIndexRanges(sctx, histColl, path.Index.ID, path.Ranges)
	return err
}

func (ds *DataSource) deriveCommonHandleTablePathStats(path *util.AccessPath, conds []expression.Expression, isIm bool) error {
	path.CountAfterAccess = float64(ds.StatisticTable.RealtimeCount)
	path.Ranges = ranger.FullNotNullRange()
	path.IdxCols, path.IdxColLens = expression.IndexInfo2PrefixCols(ds.Columns, ds.Schema().Columns, path.Index)
	path.FullIdxCols, path.FullIdxColLens = expression.IndexInfo2Cols(ds.Columns, ds.Schema().Columns, path.Index)
	if len(conds) == 0 {
		return nil
	}
	if err := detachCondAndBuildRangeForPath(ds.SCtx(), path, conds, ds.TableStats.HistColl); err != nil {
		return err
	}
	if path.EqOrInCondCount == len(path.AccessConds) {
		accesses, remained := path.SplitCorColAccessCondFromFilters(ds.SCtx(), path.EqOrInCondCount)
		path.AccessConds = append(path.AccessConds, accesses...)
		path.TableFilters = remained
		if len(accesses) > 0 && ds.StatisticTable.Pseudo {
			path.CountAfterAccess = cardinality.PseudoAvgCountPerValue(ds.StatisticTable)
		} else {
			selectivity := path.CountAfterAccess / float64(ds.StatisticTable.RealtimeCount)
			for i := range accesses {
				col := path.IdxCols[path.EqOrInCondCount+i]
				ndv := cardinality.EstimateColumnNDV(ds.StatisticTable, col.ID)
				ndv *= selectivity
				if ndv < 1 {
					ndv = 1.0
				}
				path.CountAfterAccess = path.CountAfterAccess / ndv
			}
		}
	}
	// If the `CountAfterAccess` is less than `stats.RowCount`, there must be some inconsistent stats info.
	// We prefer the `stats.RowCount` because it could use more stats info to calculate the selectivity.
	if path.CountAfterAccess < ds.StatsInfo().RowCount && !isIm {
		path.CountAfterAccess = math.Min(ds.StatsInfo().RowCount/cost.SelectionFactor, float64(ds.StatisticTable.RealtimeCount))
	}
	return nil
}

// deriveTablePathStats will fulfill the information that the AccessPath need.
// isIm indicates whether this function is called to generate the partial path for IndexMerge.
func (ds *DataSource) deriveTablePathStats(path *util.AccessPath, conds []expression.Expression, isIm bool) error {
	if ds.SCtx().GetSessionVars().StmtCtx.EnableOptimizerDebugTrace {
		debugtrace.EnterContextCommon(ds.SCtx())
		defer debugtrace.LeaveContextCommon(ds.SCtx())
	}
	if path.IsCommonHandlePath {
		return ds.deriveCommonHandleTablePathStats(path, conds, isIm)
	}
	var err error
	path.CountAfterAccess = float64(ds.StatisticTable.RealtimeCount)
	path.TableFilters = conds
	var pkCol *expression.Column
	isUnsigned := false
	if ds.TableInfo.PKIsHandle {
		if pkColInfo := ds.TableInfo.GetPkColInfo(); pkColInfo != nil {
			isUnsigned = mysql.HasUnsignedFlag(pkColInfo.GetFlag())
			pkCol = expression.ColInfo2Col(ds.Schema().Columns, pkColInfo)
		}
	} else {
		pkCol = ds.Schema().GetExtraHandleColumn()
	}
	if pkCol == nil {
		path.Ranges = ranger.FullIntRange(isUnsigned)
		return nil
	}

	path.Ranges = ranger.FullIntRange(isUnsigned)
	if len(conds) == 0 {
		return nil
	}
	// for cnf condition combination, c=1 and c=2 and (1 member of (a)),
	// c=1 and c=2 will derive invalid range represented by an access condition as constant of 0 (false).
	// later this constant of 0 will be built as empty range.
	path.AccessConds, path.TableFilters = ranger.DetachCondsForColumn(ds.SCtx().GetRangerCtx(), conds, pkCol)
	// If there's no access cond, we try to find that whether there's expression containing correlated column that
	// can be used to access data.
	corColInAccessConds := false
	if len(path.AccessConds) == 0 {
		for i, filter := range path.TableFilters {
			eqFunc, ok := filter.(*expression.ScalarFunction)
			if !ok || eqFunc.FuncName.L != ast.EQ {
				continue
			}
			lCol, lOk := eqFunc.GetArgs()[0].(*expression.Column)
			if lOk && lCol.Equal(ds.SCtx().GetExprCtx().GetEvalCtx(), pkCol) {
				_, rOk := eqFunc.GetArgs()[1].(*expression.CorrelatedColumn)
				if rOk {
					path.AccessConds = append(path.AccessConds, filter)
					path.TableFilters = append(path.TableFilters[:i], path.TableFilters[i+1:]...)
					corColInAccessConds = true
					break
				}
			}
			rCol, rOk := eqFunc.GetArgs()[1].(*expression.Column)
			if rOk && rCol.Equal(ds.SCtx().GetExprCtx().GetEvalCtx(), pkCol) {
				_, lOk := eqFunc.GetArgs()[0].(*expression.CorrelatedColumn)
				if lOk {
					path.AccessConds = append(path.AccessConds, filter)
					path.TableFilters = append(path.TableFilters[:i], path.TableFilters[i+1:]...)
					corColInAccessConds = true
					break
				}
			}
		}
	}
	if corColInAccessConds {
		path.CountAfterAccess = 1
		return nil
	}
	var remainedConds []expression.Expression
	path.Ranges, path.AccessConds, remainedConds, err = ranger.BuildTableRange(path.AccessConds, ds.SCtx().GetRangerCtx(), pkCol.RetType, ds.SCtx().GetSessionVars().RangeMaxSize)
	path.TableFilters = append(path.TableFilters, remainedConds...)
	if err != nil {
		return err
	}
	path.CountAfterAccess, err = cardinality.GetRowCountByIntColumnRanges(ds.SCtx(), &ds.StatisticTable.HistColl, pkCol.ID, path.Ranges)
	// If the `CountAfterAccess` is less than `stats.RowCount`, there must be some inconsistent stats info.
	// We prefer the `stats.RowCount` because it could use more stats info to calculate the selectivity.
	if path.CountAfterAccess < ds.StatsInfo().RowCount && !isIm {
		path.CountAfterAccess = math.Min(ds.StatsInfo().RowCount/cost.SelectionFactor, float64(ds.StatisticTable.RealtimeCount))
	}
	return err
}

func (ds *DataSource) fillIndexPath(path *util.AccessPath, conds []expression.Expression) error {
	if ds.SCtx().GetSessionVars().StmtCtx.EnableOptimizerDebugTrace {
		debugtrace.EnterContextCommon(ds.SCtx())
		defer debugtrace.LeaveContextCommon(ds.SCtx())
	}
	path.Ranges = ranger.FullRange()
	path.CountAfterAccess = float64(ds.StatisticTable.RealtimeCount)
	path.IdxCols, path.IdxColLens = expression.IndexInfo2PrefixCols(ds.Columns, ds.Schema().Columns, path.Index)
	path.FullIdxCols, path.FullIdxColLens = expression.IndexInfo2Cols(ds.Columns, ds.Schema().Columns, path.Index)
	if !path.Index.Unique && !path.Index.Primary && len(path.Index.Columns) == len(path.IdxCols) {
		handleCol := ds.getPKIsHandleCol()
		if handleCol != nil && !mysql.HasUnsignedFlag(handleCol.RetType.GetFlag()) {
			alreadyHandle := false
			for _, col := range path.IdxCols {
				if col.ID == model.ExtraHandleID || col.EqualColumn(handleCol) {
					alreadyHandle = true
				}
			}
			// Don't add one column twice to the index. May cause unexpected errors.
			if !alreadyHandle {
				path.IdxCols = append(path.IdxCols, handleCol)
				path.IdxColLens = append(path.IdxColLens, types.UnspecifiedLength)
				// Also updates the map that maps the index id to its prefix column ids.
				if len(ds.TableStats.HistColl.Idx2ColUniqueIDs[path.Index.ID]) == len(path.Index.Columns) {
					ds.TableStats.HistColl.Idx2ColUniqueIDs[path.Index.ID] = append(ds.TableStats.HistColl.Idx2ColUniqueIDs[path.Index.ID], handleCol.UniqueID)
				}
			}
		}
	}
	err := detachCondAndBuildRangeForPath(ds.SCtx(), path, conds, ds.TableStats.HistColl)
	return err
}

// deriveIndexPathStats will fulfill the information that the AccessPath need.
// conds is the conditions used to generate the DetachRangeResult for path.
// isIm indicates whether this function is called to generate the partial path for IndexMerge.
func (ds *DataSource) deriveIndexPathStats(path *util.AccessPath, _ []expression.Expression, isIm bool) {
	if ds.SCtx().GetSessionVars().StmtCtx.EnableOptimizerDebugTrace {
		debugtrace.EnterContextCommon(ds.SCtx())
		defer debugtrace.LeaveContextCommon(ds.SCtx())
	}
	if path.EqOrInCondCount == len(path.AccessConds) {
		accesses, remained := path.SplitCorColAccessCondFromFilters(ds.SCtx(), path.EqOrInCondCount)
		path.AccessConds = append(path.AccessConds, accesses...)
		path.TableFilters = remained
		if len(accesses) > 0 && ds.StatisticTable.Pseudo {
			path.CountAfterAccess = cardinality.PseudoAvgCountPerValue(ds.StatisticTable)
		} else {
			selectivity := path.CountAfterAccess / float64(ds.StatisticTable.RealtimeCount)
			for i := range accesses {
				col := path.IdxCols[path.EqOrInCondCount+i]
				ndv := cardinality.EstimateColumnNDV(ds.StatisticTable, col.ID)
				ndv *= selectivity
				if ndv < 1 {
					ndv = 1.0
				}
				path.CountAfterAccess = path.CountAfterAccess / ndv
			}
		}
	}
	var indexFilters []expression.Expression
	indexFilters, path.TableFilters = ds.splitIndexFilterConditions(path.TableFilters, path.FullIdxCols, path.FullIdxColLens)
	path.IndexFilters = append(path.IndexFilters, indexFilters...)
	// If the `CountAfterAccess` is less than `stats.RowCount`, there must be some inconsistent stats info.
	// We prefer the `stats.RowCount` because it could use more stats info to calculate the selectivity.
	if path.CountAfterAccess < ds.StatsInfo().RowCount && !isIm {
		path.CountAfterAccess = math.Min(ds.StatsInfo().RowCount/cost.SelectionFactor, float64(ds.StatisticTable.RealtimeCount))
	}
	if path.IndexFilters != nil {
		selectivity, _, err := cardinality.Selectivity(ds.SCtx(), ds.TableStats.HistColl, path.IndexFilters, nil)
		if err != nil {
			logutil.BgLogger().Debug("calculate selectivity failed, use selection factor", zap.Error(err))
			selectivity = cost.SelectionFactor
		}
		if isIm {
			path.CountAfterIndex = path.CountAfterAccess * selectivity
		} else {
			path.CountAfterIndex = math.Max(path.CountAfterAccess*selectivity, ds.StatsInfo().RowCount)
		}
	} else {
		path.CountAfterIndex = path.CountAfterAccess
	}
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

func (ds *DataSource) getPKIsHandleCol() *expression.Column {
	return getPKIsHandleColFromSchema(ds.Columns, ds.Schema(), ds.TableInfo.PKIsHandle)
}
