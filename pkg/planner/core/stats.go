// Copyright 2017 PingCAP, Inc.
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
	"fmt"
	"math"
	"slices"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/cardinality"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/cost"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	"github.com/pingcap/tidb/pkg/planner/core/stats"
	"github.com/pingcap/tidb/pkg/planner/property"
	"github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/planner/util/debugtrace"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/stmtctx"
	"github.com/pingcap/tidb/pkg/statistics"
	"github.com/pingcap/tidb/pkg/types"
	h "github.com/pingcap/tidb/pkg/util/hint"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/ranger"
	"go.uber.org/zap"
)

// RecursiveDeriveStats4Test is a exporter just for test.
func RecursiveDeriveStats4Test(p base.LogicalPlan) (*property.StatsInfo, bool, error) {
	return p.RecursiveDeriveStats(nil)
}

// GetStats4Test is a exporter just for test.
func GetStats4Test(p base.LogicalPlan) *property.StatsInfo {
	return p.StatsInfo()
}

func deriveStats4LogicalTableScan(lp base.LogicalPlan) (_ *property.StatsInfo, _ bool, err error) {
	ts := lp.(*logicalop.LogicalTableScan)
	initStats(ts.Source)
	ts.SetStats(deriveStatsByFilter(ts.Source, ts.AccessConds, nil))
	// ts.Handle could be nil if PK is Handle, and PK column has been pruned.
	// TODO: support clustered index.
	if ts.HandleCols != nil {
		// TODO: restrict mem usage of table ranges.
		ts.Ranges, _, _, err = ranger.BuildTableRange(ts.AccessConds, ts.SCtx().GetRangerCtx(), ts.HandleCols.GetCol(0).RetType, 0)
	} else {
		isUnsigned := false
		if ts.Source.TableInfo.PKIsHandle {
			if pkColInfo := ts.Source.TableInfo.GetPkColInfo(); pkColInfo != nil {
				isUnsigned = mysql.HasUnsignedFlag(pkColInfo.GetFlag())
			}
		}
		ts.Ranges = ranger.FullIntRange(isUnsigned)
	}
	if err != nil {
		return nil, false, err
	}
	return ts.StatsInfo(), true, nil
}

func deriveStats4LogicalIndexScan(lp base.LogicalPlan, selfSchema *expression.Schema) (*property.StatsInfo, bool, error) {
	is := lp.(*logicalop.LogicalIndexScan)
	initStats(is.Source)
	is.SetStats(deriveStatsByFilter(is.Source, is.AccessConds, nil))
	if len(is.AccessConds) == 0 {
		is.Ranges = ranger.FullRange()
	}
	is.IdxCols, is.IdxColLens = expression.IndexInfo2PrefixCols(is.Columns, selfSchema.Columns, is.Index)
	is.FullIdxCols, is.FullIdxColLens = expression.IndexInfo2Cols(is.Columns, selfSchema.Columns, is.Index)
	if !is.Index.Unique && !is.Index.Primary && len(is.Index.Columns) == len(is.IdxCols) {
		handleCol := is.GetPKIsHandleCol(selfSchema)
		if handleCol != nil && !mysql.HasUnsignedFlag(handleCol.RetType.GetFlag()) {
			is.IdxCols = append(is.IdxCols, handleCol)
			is.IdxColLens = append(is.IdxColLens, types.UnspecifiedLength)
		}
	}
	return is.StatsInfo(), true, nil
}

// deriveStats4DataSource initialize or derive the stats property for type of DataSource plan.
// It returns the stats, a bool value indicating whether the stats is changed and an error.
// The ds.stats represent the stats after applying all pushed down conditions
//
//	(include all predicates which directly infer the range or predicates that require additional selection execution).
//
// So the ds.stats.rowcount is the output rowcount of ds after applying all pushed down conditions,
//
//	it's not equal to the rowcount after applying only the access conditions and also not equal to the ds.TableStats.RowCount.
//
// The ds.TableStats.RowCount >= ds.stats.RowCount >= ds.stats.CountAfterAccess
func deriveStats4DataSource(lp base.LogicalPlan) (*property.StatsInfo, bool, error) {
	ds := lp.(*logicalop.DataSource)
	if ds.StatsInfo() != nil {
		return ds.StatsInfo(), false, nil
	}
	initStats(ds)
	if ds.StatsInfo() != nil {
		// Just reload the GroupNDVs.
		selectivity := ds.StatsInfo().RowCount / ds.TableStats.RowCount
		ds.SetStats(ds.TableStats.Scale(lp.SCtx().GetSessionVars(), selectivity))
		return ds.StatsInfo(), false, nil
	}
	if ds.SCtx().GetSessionVars().StmtCtx.EnableOptimizerDebugTrace {
		debugtrace.EnterContextCommon(ds.SCtx())
		defer debugtrace.LeaveContextCommon(ds.SCtx())
	}
	// two preprocess here.
	// 1: PushDownNot here can convert query 'not (a != 1)' to 'a = 1'.
	// 2: EliminateNoPrecisionCast here can convert query 'cast(c<int> as bigint) = 1' to 'c = 1' to leverage access range.
	exprCtx := ds.SCtx().GetExprCtx()
	for i, expr := range ds.PushedDownConds {
		ds.PushedDownConds[i] = expression.EliminateNoPrecisionLossCast(exprCtx, expr)
	}
	// Prune indexes based on WHERE and ordering columns (ORDER BY, MIN/MAX/FIRST_VALUE) if we have too many
	threshold := ds.SCtx().GetSessionVars().OptIndexPruneThreshold
	if len(ds.AllPossibleAccessPaths) > threshold && (len(ds.WhereColumns) > 0 || len(ds.OrderingColumns) > 0 || len(ds.JoinColumns) > 0) {
		ds.AllPossibleAccessPaths = pruneIndexesByWhereAndOrder(ds, ds.AllPossibleAccessPaths, ds.WhereColumns, ds.OrderingColumns, ds.JoinColumns, threshold)
		// Make a copy for PossibleAccessPaths to avoid sharing the same slice
		ds.PossibleAccessPaths = make([]*util.AccessPath, len(ds.AllPossibleAccessPaths))
		copy(ds.PossibleAccessPaths, ds.AllPossibleAccessPaths)
	}
	// Fill index paths for all paths (pruned or not)
	for _, path := range ds.AllPossibleAccessPaths {
		if path.IsTablePath() {
			continue
		}
		err := fillIndexPath(ds, path, ds.PushedDownConds)
		if err != nil {
			return nil, false, err
		}
	}
	// TODO: Can we move ds.deriveStatsByFilter after pruning by heuristics? In this way some computation can be avoided
	// when ds.PossibleAccessPaths are pruned.
	ds.SetStats(deriveStatsByFilter(ds, ds.PushedDownConds, ds.AllPossibleAccessPaths))
	// after heuristic pruning, the new path are stored into ds.PossibleAccessPaths.
	err := derivePathStatsAndTryHeuristics(ds)
	if err != nil {
		return nil, false, err
	}

	// index merge path is generated from all conditions from ds based on ds.PossibleAccessPath.
	// we should renew ds.PossibleAccessPath to AllPossibleAccessPath once a new DS is generated.
	if err := generateIndexMergePath(ds); err != nil {
		return nil, false, err
	}

	if ds.SCtx().GetSessionVars().StmtCtx.EnableOptimizerDebugTrace {
		debugTraceAccessPaths(ds.SCtx(), ds.PossibleAccessPaths)
	}
	indexForce := false
	ds.AccessPathMinSelectivity, indexForce = getGeneralAttributesFromPaths(ds.PossibleAccessPaths, float64(ds.TblColHists.RealtimeCount))
	if indexForce {
		ds.SCtx().GetSessionVars().StmtCtx.SetIndexForce()
	}

	return ds.StatsInfo(), true, nil
}

func fillIndexPath(ds *logicalop.DataSource, path *util.AccessPath, conds []expression.Expression) error {
	if ds.SCtx().GetSessionVars().StmtCtx.EnableOptimizerDebugTrace {
		debugtrace.EnterContextCommon(ds.SCtx())
		defer debugtrace.LeaveContextCommon(ds.SCtx())
	}
	path.Ranges = ranger.FullRange()
	path.CountAfterAccess = float64(ds.StatisticTable.RealtimeCount)
	path.MinCountAfterAccess = 0
	path.MaxCountAfterAccess = 0
	path.IdxCols, path.IdxColLens = expression.IndexInfo2PrefixCols(ds.Columns, ds.Schema().Columns, path.Index)
	path.FullIdxCols, path.FullIdxColLens = expression.IndexInfo2Cols(ds.Columns, ds.Schema().Columns, path.Index)
	if !path.Index.Unique && !path.Index.Primary && len(path.Index.Columns) == len(path.IdxCols) {
		handleCol := ds.GetPKIsHandleCol()
		if handleCol != nil && !mysql.HasUnsignedFlag(handleCol.RetType.GetFlag()) {
			alreadyHandle := false
			for _, col := range path.IdxCols {
				if col.ID == model.ExtraHandleID || col.EqualColumn(handleCol) {
					alreadyHandle = true
				}
			}
			// Don't add one column twice to the index. May cause unexpected errors.
			if !alreadyHandle {
				path.FullIdxCols = append(path.FullIdxCols, handleCol)
				path.FullIdxColLens = append(path.FullIdxColLens, types.UnspecifiedLength)
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

// adjustCountAfterAccess adjusts the CountAfterAccess when it's less than the estimated table row count.
func adjustCountAfterAccess(ds *logicalop.DataSource, path *util.AccessPath) {
	// If the `CountAfterAccess` is less than `stats.RowCount`, it means that paths were estimated using
	// different assumptions regarding individual or compound selectivity estimates.
	// We prefer the `stats.RowCount` to provide consistency in estimation across all paths.
	// Add an arbitrary tolerance factor to account for comparison with floating point
	if (path.CountAfterAccess + cost.ToleranceFactor) < ds.StatsInfo().RowCount {
		// Store the MinCountAfterAccess "before" adjusting the "CountAfterAccess". This can be used to differentiate
		// the "Min" estimate for each index/inthandle path when CountAfterAccess has been equalized.
		if path.MinCountAfterAccess > 0 {
			path.MinCountAfterAccess = min(path.MinCountAfterAccess, path.CountAfterAccess)
		} else {
			path.MinCountAfterAccess = path.CountAfterAccess
		}
		path.CountAfterAccess = min(ds.StatsInfo().RowCount/cost.SelectionFactor, float64(ds.StatisticTable.RealtimeCount))
		// Ensure MaxCountAfterAccess is updated to reflect that "after" result
		path.MaxCountAfterAccess = max(path.CountAfterAccess, path.MaxCountAfterAccess)
	}
}

// deriveIndexPathStats will fulfill the information that the AccessPath need.
// isIm indicates whether this function is called to generate the partial path for IndexMerge.
func deriveIndexPathStats(ds *logicalop.DataSource, path *util.AccessPath, _ []expression.Expression, isIm bool) {
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
	indexFilters, path.TableFilters = splitIndexFilterConditions(ds, path.TableFilters, path.FullIdxCols, path.FullIdxColLens)
	path.IndexFilters = append(path.IndexFilters, indexFilters...)
	if !isIm {
		// Check if we need to apply a lower bound to CountAfterAccess
		adjustCountAfterAccess(ds, path)
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

// deriveTablePathStats will fulfill the information that the AccessPath need.
// isIm indicates whether this function is called to generate the partial path for IndexMerge.
func deriveTablePathStats(ds *logicalop.DataSource, path *util.AccessPath, conds []expression.Expression, isIm bool) error {
	if ds.SCtx().GetSessionVars().StmtCtx.EnableOptimizerDebugTrace {
		debugtrace.EnterContextCommon(ds.SCtx())
		defer debugtrace.LeaveContextCommon(ds.SCtx())
	}
	if path.IsCommonHandlePath {
		return deriveCommonHandleTablePathStats(ds, path, conds, isIm)
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
					path.TableFilters = slices.Delete(path.TableFilters, i, i+1)
					corColInAccessConds = true
					break
				}
			}
			rCol, rOk := eqFunc.GetArgs()[1].(*expression.Column)
			if rOk && rCol.Equal(ds.SCtx().GetExprCtx().GetEvalCtx(), pkCol) {
				_, lOk := eqFunc.GetArgs()[0].(*expression.CorrelatedColumn)
				if lOk {
					path.AccessConds = append(path.AccessConds, filter)
					path.TableFilters = slices.Delete(path.TableFilters, i, i+1)
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
	var countEst statistics.RowEstimate
	countEst, err = cardinality.GetRowCountByIntColumnRanges(ds.SCtx(), &ds.StatisticTable.HistColl, pkCol.ID, path.Ranges)
	path.CountAfterAccess = countEst.Est
	if !isIm {
		// Check if we need to apply a lower bound to CountAfterAccess
		adjustCountAfterAccess(ds, path)
	}
	return err
}

func deriveCommonHandleTablePathStats(ds *logicalop.DataSource, path *util.AccessPath, conds []expression.Expression, isIm bool) error {
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
	if !isIm {
		// Check if we need to apply a lower bound to CountAfterAccess
		adjustCountAfterAccess(ds, path)
	}
	return nil
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
	path.MinAccessCondsForDNFCond = res.MinAccessCondsForDNFCond
	path.ConstCols = make([]bool, len(path.IdxCols))
	if res.ColumnValues != nil {
		for i := range path.ConstCols {
			path.ConstCols[i] = res.ColumnValues[i] != nil
		}
	}
	indexCols := path.IdxCols
	if len(indexCols) > len(path.Index.Columns) { // remove clustered primary key if it has been added to path.IdxCols
		indexCols = indexCols[0:len(path.Index.Columns)]
	}
	count, err := cardinality.GetRowCountByIndexRanges(sctx, histColl, path.Index.ID, path.Ranges, indexCols)
	path.CountAfterAccess, path.MinCountAfterAccess, path.MaxCountAfterAccess = count.Est, count.MinEst, count.MaxEst
	return err
}

func getGeneralAttributesFromPaths(paths []*util.AccessPath, totalRowCount float64) (float64, bool) {
	minSelectivity := 1.0
	indexForce := false
	for _, path := range paths {
		// For table path and index merge path, AccessPath.CountAfterIndex is not set and meaningless,
		// but we still consider their AccessPath.CountAfterAccess.
		if totalRowCount > 0 {
			if path.IsTablePath() || path.PartialIndexPaths != nil {
				minSelectivity = min(minSelectivity, path.CountAfterAccess/totalRowCount)
			} else {
				minSelectivity = min(minSelectivity, path.CountAfterIndex/totalRowCount)
			}
		}
		if !indexForce && path.Forced {
			indexForce = true
		}
	}
	return minSelectivity, indexForce
}

func getGroupNDVs(ds *logicalop.DataSource) []property.GroupNDV {
	colGroups := ds.AskedColumnGroup
	if len(ds.AskedColumnGroup) == 0 {
		return nil
	}
	tbl := ds.TableStats.HistColl
	ndvs := make([]property.GroupNDV, 0, len(colGroups))
	tbl.ForEachIndexImmutable(func(idxID int64, idx *statistics.Index) bool {
		colsLen := len(tbl.Idx2ColUniqueIDs[idxID])
		// tbl.Idx2ColUniqueIDs may only contain the prefix of index columns.
		// But it may exceeds the total index since the index would contain the handle column if it's not a unique index.
		// We append the handle at fillIndexPath.
		if colsLen < len(idx.Info.Columns) {
			return false
		} else if colsLen > len(idx.Info.Columns) {
			colsLen--
		}
		idxCols := make([]int64, colsLen)
		copy(idxCols, tbl.Idx2ColUniqueIDs[idxID])
		slices.Sort(idxCols)
		for _, g := range colGroups {
			// We only want those exact matches.
			if len(g) != colsLen {
				return false
			}
			match := true
			for i, col := range g {
				// Both slices are sorted according to UniqueID.
				if col.UniqueID != idxCols[i] {
					match = false
					break
				}
			}
			if match && idx.IsEssentialStatsLoaded() {
				ndv := property.GroupNDV{
					Cols: idxCols,
					NDV:  float64(idx.NDV),
				}
				ndvs = append(ndvs, ndv)
				return true
			}
		}
		return false
	})
	return ndvs
}

func initStats(ds *logicalop.DataSource) {
	if ds.StatisticTable == nil {
		ds.StatisticTable = stats.GetStatsTable(ds.SCtx(), ds.TableInfo, ds.PhysicalTableID)
	}
	tableStats := &property.StatsInfo{
		RowCount:     float64(ds.StatisticTable.RealtimeCount),
		ColNDVs:      make(map[int64]float64, ds.Schema().Len()),
		HistColl:     ds.StatisticTable.GenerateHistCollFromColumnInfo(ds.TableInfo, ds.TblCols),
		StatsVersion: ds.StatisticTable.Version,
	}
	if ds.StatisticTable.Pseudo {
		tableStats.StatsVersion = statistics.PseudoVersion
	}

	statsRecord := ds.SCtx().GetSessionVars().StmtCtx.GetUsedStatsInfo(true)
	name, tblInfo := stats.GetTblInfoForUsedStatsByPhysicalID(ds.SCtx(), ds.PhysicalTableID)
	statsRecord.RecordUsedInfo(ds.PhysicalTableID, &stmtctx.UsedStatsInfoForTable{
		Name:            name,
		TblInfo:         tblInfo,
		Version:         tableStats.StatsVersion,
		RealtimeCount:   tableStats.HistColl.RealtimeCount,
		ModifyCount:     tableStats.HistColl.ModifyCount,
		ColAndIdxStatus: ds.StatisticTable.ColAndIdxExistenceMap,
	})

	for _, col := range ds.Schema().Columns {
		tableStats.ColNDVs[col.UniqueID] = cardinality.EstimateColumnNDV(ds.StatisticTable, col.ID)
	}
	ds.TableStats = tableStats
	ds.TableStats.GroupNDVs = getGroupNDVs(ds)
	ds.TblColHists = ds.StatisticTable.ID2UniqueID(ds.TblCols)
	for _, col := range ds.TableInfo.Columns {
		if col.State != model.StatePublic {
			continue
		}
	}
}

func deriveStatsByFilter(ds *logicalop.DataSource, conds expression.CNFExprs, filledPaths []*util.AccessPath) *property.StatsInfo {
	if ds.SCtx().GetSessionVars().StmtCtx.EnableOptimizerDebugTrace {
		debugtrace.EnterContextCommon(ds.SCtx())
		defer debugtrace.LeaveContextCommon(ds.SCtx())
	}
	selectivity, _, err := cardinality.Selectivity(ds.SCtx(), ds.TableStats.HistColl, conds, filledPaths)
	if err != nil {
		logutil.BgLogger().Debug("something wrong happened, use the default selectivity", zap.Error(err))
		selectivity = cost.SelectionFactor
	}
	// TODO: remove NewHistCollBySelectivity later on.
	// if ds.SCtx().GetSessionVars().OptimizerSelectivityLevel >= 1 {
	// Only '0' is suggested, see https://docs.pingcap.com/zh/tidb/stable/system-variables#tidb_optimizer_selectivity_level.
	// stats.HistColl = stats.HistColl.NewHistCollBySelectivity(ds.SCtx(), nodes)
	// }
	return ds.TableStats.Scale(ds.SCtx().GetSessionVars(), selectivity)
}

// We bind logic of derivePathStats and tryHeuristics together. When some path matches the heuristic rule, we don't need
// to derive stats of subsequent paths. In this way we can save unnecessary computation of derivePathStats.
func derivePathStatsAndTryHeuristics(ds *logicalop.DataSource) error {
	if ds.SCtx().GetSessionVars().StmtCtx.EnableOptimizerDebugTrace {
		debugtrace.EnterContextCommon(ds.SCtx())
		defer debugtrace.LeaveContextCommon(ds.SCtx())
	}
	uniqueIdxsWithDoubleScan := make([]*util.AccessPath, 0, len(ds.AllPossibleAccessPaths))
	singleScanIdxs := make([]*util.AccessPath, 0, len(ds.AllPossibleAccessPaths))
	var (
		selected, uniqueBest, refinedBest *util.AccessPath
		isRefinedPath                     bool
	)
	// step1: if user prefer tiFlash store type, tiFlash path should always be built anyway ahead.
	var tiflashPath *util.AccessPath
	if ds.PreferStoreType&h.PreferTiFlash != 0 {
		for _, path := range ds.AllPossibleAccessPaths {
			if path.StoreType == kv.TiFlash {
				err := deriveTablePathStats(ds, path, ds.PushedDownConds, false)
				if err != nil {
					return err
				}
				path.IsSingleScan = true
				tiflashPath = path
				break
			}
		}
	}
	// step2: kv path should follow the heuristic rules.
	for _, path := range ds.AllPossibleAccessPaths {
		if path.IsTablePath() {
			err := deriveTablePathStats(ds, path, ds.PushedDownConds, false)
			if err != nil {
				return err
			}
			path.IsSingleScan = true
		} else {
			deriveIndexPathStats(ds, path, ds.PushedDownConds, false)
			if !path.IsSingleScan {
				path.IsSingleScan = ds.IsSingleScan(path.FullIdxCols, path.FullIdxColLens)
			}
		}
		// step: 3
		// Try some heuristic rules to select access path.
		// tiFlash path also have table-range-scan (range point like here) to be heuristic treated.
		if len(path.Ranges) == 0 {
			selected = path
			break
		}
		if path.OnlyPointRange(ds.SCtx().GetSessionVars().StmtCtx.TypeCtx()) {
			if path.IsTablePath() || path.Index.Unique {
				if path.IsSingleScan {
					selected = path
					break
				}
				uniqueIdxsWithDoubleScan = append(uniqueIdxsWithDoubleScan, path)
			}
		} else if path.IsSingleScan {
			singleScanIdxs = append(singleScanIdxs, path)
		}
	}
	if selected == nil && len(uniqueIdxsWithDoubleScan) > 0 {
		uniqueIdxAccessCols := make([]util.Col2Len, 0, len(uniqueIdxsWithDoubleScan))
		for _, uniqueIdx := range uniqueIdxsWithDoubleScan {
			uniqueIdxAccessCols = append(uniqueIdxAccessCols, uniqueIdx.GetCol2LenFromAccessConds(ds.SCtx()))
			// Find the unique index with the minimal number of ranges as `uniqueBest`.
			/*
				If the number of scan ranges are equal, choose the one with the least table predicates - meaning the unique index with the most index predicates.
				Because the most index predicates means that it is more likely to fetch 0 index rows.
				Example in the test "TestPointgetIndexChoosen".
			*/
			if uniqueBest == nil || len(uniqueIdx.Ranges) < len(uniqueBest.Ranges) ||
				(len(uniqueIdx.Ranges) == len(uniqueBest.Ranges) && len(uniqueIdx.TableFilters) < len(uniqueBest.TableFilters)) {
				uniqueBest = uniqueIdx
			}
		}
		// `uniqueBest` may not always be the best.
		// ```
		// create table t(a int, b int, c int, unique index idx_b(b), index idx_b_c(b, c));
		// select b, c from t where b = 5 and c > 10;
		// ```
		// In the case, `uniqueBest` is `idx_b`. However, `idx_b_c` is better than `idx_b`.
		// Hence, for each index in `singleScanIdxs`, we check whether it is better than some index in `uniqueIdxsWithDoubleScan`.
		// If yes, the index is a refined one. We find the refined index with the minimal number of ranges as `refineBest`.
		for _, singleScanIdx := range singleScanIdxs {
			col2Len := singleScanIdx.GetCol2LenFromAccessConds(ds.SCtx())
			for _, uniqueIdxCol2Len := range uniqueIdxAccessCols {
				accessResult, comparable1 := util.CompareCol2Len(col2Len, uniqueIdxCol2Len)
				if comparable1 && accessResult == 1 {
					if refinedBest == nil || len(singleScanIdx.Ranges) < len(refinedBest.Ranges) {
						refinedBest = singleScanIdx
					}
				}
			}
		}
		// `refineBest` may not always be better than `uniqueBest`.
		// ```
		// create table t(a int, b int, c int, d int, unique index idx_a(a), unique index idx_b_c(b, c), unique index idx_b_c_a_d(b, c, a, d));
		// select a, b, c from t where a = 1 and b = 2 and c in (1, 2, 3, 4, 5);
		// ```
		// In the case, `refinedBest` is `idx_b_c_a_d` and `uniqueBest` is `a`. `idx_b_c_a_d` needs to access five points while `idx_a`
		// only needs one point access and one table access.
		// Hence we should compare `len(refinedBest.Ranges)` and `2*len(uniqueBest.Ranges)` to select the better one.
		if refinedBest != nil && (uniqueBest == nil || len(refinedBest.Ranges) < 2*len(uniqueBest.Ranges)) {
			selected = refinedBest
			isRefinedPath = true
		} else {
			selected = uniqueBest
		}
	}
	// heuristic rule pruning other path should consider hint prefer.
	// If no hints and some path matches a heuristic rule, just remove other possible paths.
	if selected != nil {
		// heuristic rule pruning only affect current DS's PossibleAccessPaths, where physical plan will be generated.
		ds.PossibleAccessPaths[0] = selected
		ds.PossibleAccessPaths = ds.PossibleAccessPaths[:1]
		// if user wanna tiFlash read, while current heuristic choose a TiKV path. so we shouldn't prune tiFlash path.
		keep := ds.PreferStoreType&h.PreferTiFlash != 0 && selected.StoreType != kv.TiFlash
		if keep {
			// also keep tiflash path as well.
			ds.PossibleAccessPaths = append(ds.PossibleAccessPaths, tiflashPath)
			return nil
		}
		var tableName string
		if ds.TableAsName.O == "" {
			tableName = ds.TableInfo.Name.O
		} else {
			tableName = ds.TableAsName.O
		}
		var sb strings.Builder
		if selected.IsTablePath() {
			// TODO: primary key / handle / real name?
			fmt.Fprintf(&sb, "handle of %s is selected since the path only has point ranges", tableName)
		} else {
			if selected.Index.Unique {
				sb.WriteString("unique ")
			}
			sb.WriteString(fmt.Sprintf("index %s of %s is selected since the path", selected.Index.Name.O, tableName))
			if isRefinedPath {
				sb.WriteString(" only fetches limited number of rows")
			} else {
				sb.WriteString(" only has point ranges")
			}
			if selected.IsSingleScan {
				sb.WriteString(" with single scan")
			} else {
				sb.WriteString(" with double scan")
			}
		}
		if ds.SCtx().GetSessionVars().StmtCtx.InVerboseExplain {
			ds.SCtx().GetSessionVars().StmtCtx.AppendNote(errors.NewNoStackError(sb.String()))
		} else {
			ds.SCtx().GetSessionVars().StmtCtx.AppendExtraNote(errors.NewNoStackError(sb.String()))
		}
	}
	return nil
}

// loadTableStats loads the stats of the table and store it in the statement `UsedStatsInfo` if it didn't exist
func loadTableStats(ctx sessionctx.Context, tblInfo *model.TableInfo, pid int64) {
	statsRecord := ctx.GetSessionVars().StmtCtx.GetUsedStatsInfo(true)
	if statsRecord.GetUsedInfo(pid) != nil {
		return
	}

	pctx := ctx.GetPlanCtx()
	tableStats := stats.GetStatsTable(pctx, tblInfo, pid)

	name := tblInfo.Name.O
	partInfo := tblInfo.GetPartitionInfo()
	if partInfo != nil {
		for _, p := range partInfo.Definitions {
			if p.ID == pid {
				name += " " + p.Name.O
			}
		}
	}
	usedStats := &stmtctx.UsedStatsInfoForTable{
		Name:          name,
		TblInfo:       tblInfo,
		RealtimeCount: tableStats.HistColl.RealtimeCount,
		ModifyCount:   tableStats.HistColl.ModifyCount,
		Version:       tableStats.Version,
	}
	if tableStats.Pseudo {
		usedStats.Version = statistics.PseudoVersion
	}
	statsRecord.RecordUsedInfo(pid, usedStats)
}

// indexWithScore stores an access path along with its pre-calculated coverage information.
// This avoids re-iterating through index columns during sorting.
type indexWithScore struct {
	path                     *util.AccessPath
	whereCount               int
	joinCount                int
	orderingCount            int
	consecutiveWhereCount    int // Consecutive WHERE columns from start of index
	consecutiveJoinCount     int // Consecutive JOIN columns from start of index
	consecutiveOrderingCount int // Consecutive ordering columns from start of index
}

// pruneIndexesByWhereAndOrder prunes indexes based on their coverage of WHERE, join and ordering columns.
//
// NOTE: This is an APPROXIMATE pruning - to find "interesting" indexes, not the "best" index.
//
// This pruning is controlled by variable - tidb_opt_index_prune_threshold.
// Pruning occurs before many of the index fields are populated - to avoid the cost
// of building predicate ranges. Therefore, pruning is an approximation based upon
// column coverage only, not the actual predicate ranges or ordering requirements.
// The target of this pruning is for customers requiring a very large number of
// of indexes (>100) on a table, which causes excessive planning time.
func pruneIndexesByWhereAndOrder(ds *logicalop.DataSource, paths []*util.AccessPath, whereColumns, orderingColumns, joinColumns []*expression.Column, threshold int) []*util.AccessPath {
	if len(paths) <= 1 {
		return paths
	}

	// If there are no required columns, don't prune - just return all paths
	if len(whereColumns) == 0 && len(orderingColumns) == 0 && len(joinColumns) == 0 {
		return paths
	}

	totalWhereColumns := 0
	totalOrderingColumns := 0
	totalJoinColumns := 0

	// Build ID-based lookup maps for column matching
	whereColIDs := make(map[int64]struct{}, len(whereColumns))
	for _, col := range whereColumns {
		whereColIDs[col.ID] = struct{}{}
		totalWhereColumns++
	}
	orderingColIDs := make(map[int64]struct{}, len(orderingColumns))
	for _, col := range orderingColumns {
		// Skip if already in whereColIDs to avoid double-counting
		if _, exists := whereColIDs[col.ID]; !exists {
			orderingColIDs[col.ID] = struct{}{}
			totalOrderingColumns++
		}
	}
	joinColIDs := make(map[int64]struct{}, len(joinColumns))
	for _, col := range joinColumns {
		// Skip if already in whereColIDs to avoid double-counting
		if _, exists := whereColIDs[col.ID]; !exists {
			joinColIDs[col.ID] = struct{}{}
			totalJoinColumns++
		}
	}
	// Calculate total coverage as a single table index
	totalLocalRequiredCols := totalWhereColumns + totalOrderingColumns
	// Calculate total coverage as a join table index
	totalJoinRequiredCols := totalJoinColumns + totalWhereColumns
	maxConsecutiveWhere, maxConsecutiveJoin := 0, 0

	// maxIndexes allows a minimum number of indexes to be kept regardless of threshold.
	// max/minToKeep only calculate index plans to keep in addition to table plans.
	// This avoids extreme pruning when threshold is very low (or even zero).
	const maxIndexes = 10
	maxToKeep := max(maxIndexes, threshold)
	minToKeep := max(1, min(maxIndexes, threshold))
	// Prepare lists to hold indexes with different levels of coverage
	perfectCoveringIndexes := make([]indexWithScore, 0, maxIndexes) // Indexes covering all required columns
	preferredIndexes := make([]indexWithScore, 0, maxIndexes)       // "Next" best indexes with preferable coveraage
	tablePaths := make([]*util.AccessPath, 0, 1)                    // Usually just one table path

	lenPerfectCoveringIndexes := 0
	hasSingleScan := false
	for _, path := range paths {
		if path.IsTablePath() {
			tablePaths = append(tablePaths, path)
			continue
		}

		// If we have forced paths, we shouldn't prune any paths.
		// Hints processing should have already done the pruning.
		// It means we may have entered this function due to a low threshold.
		if path.Forced {
			return paths
		}

		// Count how many WHERE, join and ordering columns are covered by this index
		// Also track consecutive matches from the start (most valuable for index usage)
		coveredWhereCount := 0
		coveredOrderingCount := 0
		coveredJoinCount := 0
		consecutiveWhereCount := 0
		consecutiveOrderingCount := 0
		consecutiveJoinCount := 0

		skipThisIndex := false

		for i, idxCol := range path.FullIdxCols {
			idxColID := idxCol.ID

			// Check if this index column matches a WHERE column
			if totalWhereColumns > 0 {
				if _, found := whereColIDs[idxColID]; found {
					coveredWhereCount++
					// Track consecutive matches from start of index
					if i == consecutiveWhereCount {
						consecutiveWhereCount++
					}
					// Also increment consecutive join count if there has already been at least one
					// consecutive join match, and the current index column position matches the
					// sum of consecutive join and where matches.
					if consecutiveJoinCount > 0 && i == consecutiveJoinCount+consecutiveWhereCount {
						consecutiveJoinCount++
					}
					continue // Move to next index column - since a column can only appear once
				}
			}
			// Check if this index column matches a join column
			if totalJoinColumns > 0 {
				if _, found := joinColIDs[idxColID]; found {
					coveredJoinCount++
					// Track consecutive matches from start of index
					if i == consecutiveJoinCount+consecutiveWhereCount {
						consecutiveJoinCount++
					}
					continue // Move to next index column - since a column can only appear once
				}
			}

			// Check if this index column matches an ordering column (ORDER BY or MIN/MAX)
			if totalOrderingColumns > 0 {
				if _, found := orderingColIDs[idxColID]; found {
					coveredOrderingCount++
					// Track consecutive matches from where column matches
					if i == consecutiveOrderingCount+consecutiveWhereCount {
						consecutiveOrderingCount++
					}
					continue
				}
			}
			// If we have enough perfect covering indexes, and we've reached here then
			// the index columns are not a match. Don't continue with this index.
			if (i == 0 && lenPerfectCoveringIndexes >= minToKeep) ||
				((lenPerfectCoveringIndexes > maxToKeep || hasSingleScan) &&
					i < maxConsecutiveWhere && i < maxConsecutiveJoin) {
				skipThisIndex = true
				break
			}
		}

		if skipThisIndex {
			continue
		}
		// Calculate this plans totals
		totalLocalCovered := coveredWhereCount + coveredOrderingCount
		totalJoinCovered := coveredWhereCount + coveredJoinCount
		totalConsecutive := consecutiveWhereCount + consecutiveOrderingCount + consecutiveJoinCount

		if totalLocalCovered == totalLocalRequiredCols && totalJoinCovered == totalJoinRequiredCols {
			path.IsSingleScan = ds.IsSingleScan(path.FullIdxCols, path.FullIdxColLens)
		}
		// Perfect covering: covers ALL required columns AND has consecutive matches from start
		if ((totalLocalCovered == totalLocalRequiredCols && totalLocalRequiredCols > 0) ||
			(totalJoinCovered == totalJoinRequiredCols && totalJoinRequiredCols > 0)) &&
			totalConsecutive > 0 {
			perfectCoveringIndexes = append(perfectCoveringIndexes, indexWithScore{
				path:                     path,
				whereCount:               coveredWhereCount,
				orderingCount:            coveredOrderingCount,
				joinCount:                coveredJoinCount,
				consecutiveWhereCount:    consecutiveWhereCount,
				consecutiveJoinCount:     consecutiveJoinCount,
				consecutiveOrderingCount: consecutiveOrderingCount,
			})
			maxConsecutiveWhere = max(maxConsecutiveWhere, consecutiveWhereCount)
			maxConsecutiveJoin = max(maxConsecutiveJoin, consecutiveJoinCount)
			lenPerfectCoveringIndexes = len(perfectCoveringIndexes)
			if path.IsSingleScan {
				hasSingleScan = true
			}
			continue
		}

		// Preferred: has meaningful coverage
		hasGoodCoverage := false

		if lenPerfectCoveringIndexes >= minToKeep {
			if consecutiveWhereCount > maxConsecutiveWhere || consecutiveJoinCount > maxConsecutiveJoin {
				hasGoodCoverage = true
			}
			if consecutiveWhereCount == totalWhereColumns && totalWhereColumns > 1 {
				hasGoodCoverage = true
			}
			// consecutiveJoinCount can be greater than totalJoinColumns because
			// we include consecutive where and join columns - since both are available
			// for index join matching.
			if consecutiveJoinCount >= totalJoinColumns && totalJoinColumns > 0 {
				hasGoodCoverage = true
			}
			if consecutiveOrderingCount == totalOrderingColumns && totalOrderingColumns > 0 {
				hasGoodCoverage = true
			}
		} else {
			if totalConsecutive > 0 {
				hasGoodCoverage = true
			}
			if coveredWhereCount == totalWhereColumns && totalWhereColumns > 0 {
				hasGoodCoverage = true
			}
		}

		if hasGoodCoverage {
			preferredIndexes = append(preferredIndexes, indexWithScore{
				path:                     path,
				whereCount:               coveredWhereCount,
				orderingCount:            coveredOrderingCount,
				joinCount:                coveredJoinCount,
				consecutiveJoinCount:     consecutiveJoinCount,
				consecutiveWhereCount:    consecutiveWhereCount,
				consecutiveOrderingCount: consecutiveOrderingCount,
			})
		}
	}

	// Build result with priority: table paths, perfect covering, then preferred
	result := make([]*util.AccessPath, 0, maxIndexes)

	// CRITICAL: Always include table paths - this is mandatory for correctness
	result = append(result, tablePaths...)

	// Only sort perfect covering indexes if we have more than we can use
	// If we'll keep all of them, no need to sort
	if len(perfectCoveringIndexes) > maxToKeep {
		// Score perfectCoveringIndexes the same way as preferred, but include isSingleScan bonus
		slices.SortFunc(perfectCoveringIndexes, func(a, b indexWithScore) int {
			scoreA := calculateScoreFromCoverage(a, totalWhereColumns, totalJoinColumns, totalOrderingColumns, a.path.IsSingleScan)
			scoreB := calculateScoreFromCoverage(b, totalWhereColumns, totalJoinColumns, totalOrderingColumns, b.path.IsSingleScan)
			if scoreA != scoreB {
				return scoreB - scoreA
			}
			// Tie-breaker: shorter index first
			return len(a.path.Index.Columns) - len(b.path.Index.Columns)
		})
	}

	// Add perfect covering indexes (prefer these over preferred)
	// Add as many index paths (to the result containing table paths) as per maxToKeep.
	remaining := maxToKeep
	if remaining > 0 && len(perfectCoveringIndexes) > 0 {
		toAdd := min(remaining, len(perfectCoveringIndexes))
		for _, idxWithScore := range perfectCoveringIndexes[:toAdd] {
			result = append(result, idxWithScore.path)
		}
		remaining -= toAdd
	}

	// Add preferred indexes if we still have room (extract paths from indexWithScore)
	if remaining > 0 && len(preferredIndexes) > 0 {
		// Only sort if we have more preferred indexes than remaining slots
		// If we'll keep all of them, no need to sort
		if len(preferredIndexes) > remaining {
			slices.SortFunc(preferredIndexes, func(a, b indexWithScore) int {
				// Calculate scores using pre-computed coverage info
				scoreA := calculateScoreFromCoverage(a, totalWhereColumns, totalJoinColumns, totalOrderingColumns, false)
				scoreB := calculateScoreFromCoverage(b, totalWhereColumns, totalJoinColumns, totalOrderingColumns, false)
				// Higher score is better, so reverse the comparison
				if scoreA != scoreB {
					return scoreB - scoreA
				}
				// If same score, prefer shorter index
				return len(a.path.Index.Columns) - len(b.path.Index.Columns)
			})
		}
		toAdd := min(remaining, len(preferredIndexes))
		for _, idxWithScore := range preferredIndexes[:toAdd] {
			result = append(result, idxWithScore.path)
		}
	}

	// Safety check: if we ended up with nothing, return the original paths
	// This prevents accidentally pruning everything
	if len(result) == 0 {
		return paths
	}

	// Additional safety: if we only have table paths and no indexes at all, keep original
	// This might happen if no indexes match, but we should still consider all indexes
	if len(result) == len(tablePaths) && len(perfectCoveringIndexes) == 0 && len(preferredIndexes) == 0 {
		// We pruned ALL indexes - this is probably too aggressive, keep original
		return paths
	}

	return result
}

// calculateScoreFromCoverage calculates a ranking score using already-computed coverage information.
// This avoids re-iterating through index columns.
func calculateScoreFromCoverage(info indexWithScore, totalWhereColumns, totalJoinColumns, totalOrderingCols int, isSingleScan bool) int {
	score := 0

	// Score for WHERE column coverage
	score += info.whereCount * 10

	// Bonus for consecutive WHERE columns from start (critical for index usage)
	// Consecutive columns are much more valuable than scattered matches
	// Index on (a,b,c,d) with WHERE a=1 AND b=2 AND c=3 can use first 3 columns
	// But with WHERE a=1 AND d=4, can only use first 1 column
	score += info.consecutiveWhereCount * 10

	// Bonus if the index is covering all WHERE columns
	if info.whereCount == totalWhereColumns {
		score += 10
	}

	// Bonus for consecutive WHERE columns from start (critical for index usage)
	// Consecutive columns are much more valuable than scattered matches
	// Index on (a,b,c,d) with WHERE a=1 AND b=2 AND c=3 can use first 3 columns
	// But with WHERE a=1 AND d=4, can only use first 1 column
	score += info.consecutiveJoinCount * 10

	// Bonus if the index is covering all WHERE columns
	if info.joinCount > 0 && info.joinCount == totalJoinColumns {
		score += 10
	}

	// NOTE: For ordering, we cannot guarantee that the presence of ordering
	// columns will always lead to a plan that doesn't need sort.
	// So we only give a bonus for consecutive ordering columns
	if info.consecutiveOrderingCount == totalOrderingCols {
		score += 10
	}
	// Bonus for single-scan
	if isSingleScan {
		score += 20
	}

	return score
}
