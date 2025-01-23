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
	"strconv"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/cardinality"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/cost"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	"github.com/pingcap/tidb/pkg/planner/property"
	"github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/planner/util/debugtrace"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/stmtctx"
	"github.com/pingcap/tidb/pkg/statistics"
	"github.com/pingcap/tidb/pkg/table"
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
	// PushDownNot here can convert query 'not (a != 1)' to 'a = 1'.
	exprCtx := ts.SCtx().GetExprCtx()
	for i, expr := range ts.AccessConds {
		// TODO The expressions may be shared by TableScan and several IndexScans, there would be redundant
		// `PushDownNot` function call in multiple `DeriveStats` then.
		ts.AccessConds[i] = expression.PushDownNot(exprCtx, expr)
	}
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
	exprCtx := is.SCtx().GetExprCtx()
	for i, expr := range is.AccessConds {
		is.AccessConds[i] = expression.PushDownNot(exprCtx, expr)
	}
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

func deriveStats4DataSource(lp base.LogicalPlan) (*property.StatsInfo, bool, error) {
	ds := lp.(*logicalop.DataSource)
	if ds.StatsInfo() != nil {
		return ds.StatsInfo(), false, nil
	}
	initStats(ds)
	if ds.StatsInfo() != nil {
		// Just reload the GroupNDVs.
		selectivity := ds.StatsInfo().RowCount / ds.TableStats.RowCount
		ds.SetStats(ds.TableStats.Scale(selectivity))
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
		ds.PushedDownConds[i] = expression.PushDownNot(exprCtx, expr)
		ds.PushedDownConds[i] = expression.EliminateNoPrecisionLossCast(exprCtx, ds.PushedDownConds[i])
	}
	for _, path := range ds.PossibleAccessPaths {
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
	ds.SetStats(deriveStatsByFilter(ds, ds.PushedDownConds, ds.PossibleAccessPaths))
	err := derivePathStatsAndTryHeuristics(ds)
	if err != nil {
		return nil, false, err
	}

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
	// If the `CountAfterAccess` is less than `stats.RowCount`, there must be some inconsistent stats info.
	// We prefer the `stats.RowCount` because it could use more stats info to calculate the selectivity.
	if path.CountAfterAccess < ds.StatsInfo().RowCount && !isIm {
		path.CountAfterAccess = math.Min(ds.StatsInfo().RowCount/cost.SelectionFactor, float64(ds.StatisticTable.RealtimeCount))
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
	path.CountAfterAccess, err = cardinality.GetRowCountByIndexRanges(sctx, histColl, path.Index.ID, path.Ranges)
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
			if match {
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

// getTblInfoForUsedStatsByPhysicalID get table name, partition name and HintedTable that will be used to record used stats.
func getTblInfoForUsedStatsByPhysicalID(sctx base.PlanContext, id int64) (fullName string, tblInfo *model.TableInfo) {
	fullName = "tableID " + strconv.FormatInt(id, 10)

	is := domain.GetDomain(sctx).InfoSchema()
	var tbl table.Table
	var partDef *model.PartitionDefinition

	tbl, partDef = infoschema.FindTableByTblOrPartID(is, id)
	if tbl == nil || tbl.Meta() == nil {
		return
	}
	tblInfo = tbl.Meta()
	fullName = tblInfo.Name.O
	if partDef != nil {
		fullName += " " + partDef.Name.O
	} else if pi := tblInfo.GetPartitionInfo(); pi != nil && len(pi.Definitions) > 0 {
		fullName += " global"
	}
	return
}

func initStats(ds *logicalop.DataSource) {
	if ds.StatisticTable == nil {
		ds.StatisticTable = getStatsTable(ds.SCtx(), ds.TableInfo, ds.PhysicalTableID)
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
	name, tblInfo := getTblInfoForUsedStatsByPhysicalID(ds.SCtx(), ds.PhysicalTableID)
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
	return ds.TableStats.Scale(selectivity)
}

// We bind logic of derivePathStats and tryHeuristics together. When some path matches the heuristic rule, we don't need
// to derive stats of subsequent paths. In this way we can save unnecessary computation of derivePathStats.
func derivePathStatsAndTryHeuristics(ds *logicalop.DataSource) error {
	if ds.SCtx().GetSessionVars().StmtCtx.EnableOptimizerDebugTrace {
		debugtrace.EnterContextCommon(ds.SCtx())
		defer debugtrace.LeaveContextCommon(ds.SCtx())
	}
	uniqueIdxsWithDoubleScan := make([]*util.AccessPath, 0, len(ds.PossibleAccessPaths))
	singleScanIdxs := make([]*util.AccessPath, 0, len(ds.PossibleAccessPaths))
	var (
		selected, uniqueBest, refinedBest *util.AccessPath
		isRefinedPath                     bool
	)
	// step1: if user prefer tiFlash store type, tiFlash path should always be built anyway ahead.
	var tiflashPath *util.AccessPath
	if ds.PreferStoreType&h.PreferTiFlash != 0 {
		for _, path := range ds.PossibleAccessPaths {
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
	for _, path := range ds.PossibleAccessPaths {
		if path.IsTablePath() {
			err := deriveTablePathStats(ds, path, ds.PushedDownConds, false)
			if err != nil {
				return err
			}
			path.IsSingleScan = true
		} else {
			deriveIndexPathStats(ds, path, ds.PushedDownConds, false)
			path.IsSingleScan = isSingleScan(ds, path.FullIdxCols, path.FullIdxColLens)
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
	tableStats := getStatsTable(pctx, tblInfo, pid)

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
