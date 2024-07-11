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
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/planner/cardinality"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/cost"
	"github.com/pingcap/tidb/pkg/planner/property"
	"github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/planner/util/debugtrace"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/stmtctx"
	"github.com/pingcap/tidb/pkg/statistics"
	"github.com/pingcap/tidb/pkg/statistics/asyncload"
	"github.com/pingcap/tidb/pkg/table"
	h "github.com/pingcap/tidb/pkg/util/hint"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
)

func (p *basePhysicalPlan) StatsCount() float64 {
	return p.StatsInfo().RowCount
}

func getFakeStats(schema *expression.Schema) *property.StatsInfo {
	profile := &property.StatsInfo{
		RowCount: 1,
		ColNDVs:  make(map[int64]float64, schema.Len()),
	}
	for _, col := range schema.Columns {
		profile.ColNDVs[col.UniqueID] = 1
	}
	return profile
}

// RecursiveDeriveStats4Test is a exporter just for test.
func RecursiveDeriveStats4Test(p base.LogicalPlan) (*property.StatsInfo, error) {
	return p.RecursiveDeriveStats(nil)
}

// GetStats4Test is a exporter just for test.
func GetStats4Test(p base.LogicalPlan) *property.StatsInfo {
	return p.StatsInfo()
}

func (ds *DataSource) getGroupNDVs(colGroups [][]*expression.Column) []property.GroupNDV {
	if colGroups == nil {
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

func (ds *DataSource) initStats(colGroups [][]*expression.Column) {
	if ds.TableStats != nil {
		// Reload GroupNDVs since colGroups may have changed.
		ds.TableStats.GroupNDVs = ds.getGroupNDVs(colGroups)
		return
	}
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
	ds.TableStats.GroupNDVs = ds.getGroupNDVs(colGroups)
	ds.TblColHists = ds.StatisticTable.ID2UniqueID(ds.TblCols)
	for _, col := range ds.TableInfo.Columns {
		if col.State != model.StatePublic {
			continue
		}
		// If we enable lite stats init or we just found out the meta info of the column is missed, we need to register columns for async load.
		_, isLoadNeeded, _ := ds.StatisticTable.ColumnIsLoadNeeded(col.ID, false)
		if isLoadNeeded {
			asyncload.AsyncLoadHistogramNeededItems.Insert(model.TableItemID{
				TableID:          ds.TableInfo.ID,
				ID:               col.ID,
				IsIndex:          false,
				IsSyncLoadFailed: ds.SCtx().GetSessionVars().StmtCtx.StatsLoad.Timeout > 0,
			}, false)
		}
	}
}

func (ds *DataSource) deriveStatsByFilter(conds expression.CNFExprs, filledPaths []*util.AccessPath) *property.StatsInfo {
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
func (ds *DataSource) derivePathStatsAndTryHeuristics() error {
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
				err := ds.deriveTablePathStats(path, ds.PushedDownConds, false)
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
			err := ds.deriveTablePathStats(path, ds.PushedDownConds, false)
			if err != nil {
				return err
			}
			path.IsSingleScan = true
		} else {
			ds.deriveIndexPathStats(path, ds.PushedDownConds, false)
			path.IsSingleScan = ds.isSingleScan(path.FullIdxCols, path.FullIdxColLens)
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

// DeriveStats implement LogicalPlan DeriveStats interface.
func (ds *DataSource) DeriveStats(_ []*property.StatsInfo, _ *expression.Schema, _ []*expression.Schema, colGroups [][]*expression.Column) (*property.StatsInfo, error) {
	if ds.StatsInfo() != nil && len(colGroups) == 0 {
		return ds.StatsInfo(), nil
	}
	ds.initStats(colGroups)
	if ds.StatsInfo() != nil {
		// Just reload the GroupNDVs.
		selectivity := ds.StatsInfo().RowCount / ds.TableStats.RowCount
		ds.SetStats(ds.TableStats.Scale(selectivity))
		return ds.StatsInfo(), nil
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
		err := ds.fillIndexPath(path, ds.PushedDownConds)
		if err != nil {
			return nil, err
		}
	}
	// TODO: Can we move ds.deriveStatsByFilter after pruning by heuristics? In this way some computation can be avoided
	// when ds.PossibleAccessPaths are pruned.
	ds.SetStats(ds.deriveStatsByFilter(ds.PushedDownConds, ds.PossibleAccessPaths))
	err := ds.derivePathStatsAndTryHeuristics()
	if err != nil {
		return nil, err
	}

	if err := ds.generateIndexMergePath(); err != nil {
		return nil, err
	}

	if ds.SCtx().GetSessionVars().StmtCtx.EnableOptimizerDebugTrace {
		debugTraceAccessPaths(ds.SCtx(), ds.PossibleAccessPaths)
	}
	ds.AccessPathMinSelectivity = getMinSelectivityFromPaths(ds.PossibleAccessPaths, float64(ds.TblColHists.RealtimeCount))

	return ds.StatsInfo(), nil
}

func getMinSelectivityFromPaths(paths []*util.AccessPath, totalRowCount float64) float64 {
	minSelectivity := 1.0
	if totalRowCount <= 0 {
		return minSelectivity
	}
	for _, path := range paths {
		// For table path and index merge path, AccessPath.CountAfterIndex is not set and meaningless,
		// but we still consider their AccessPath.CountAfterAccess.
		if path.IsTablePath() || path.PartialIndexPaths != nil {
			minSelectivity = min(minSelectivity, path.CountAfterAccess/totalRowCount)
			continue
		}
		minSelectivity = min(minSelectivity, path.CountAfterIndex/totalRowCount)
	}
	return minSelectivity
}

func deriveLimitStats(childProfile *property.StatsInfo, limitCount float64) *property.StatsInfo {
	stats := &property.StatsInfo{
		RowCount: math.Min(limitCount, childProfile.RowCount),
		ColNDVs:  make(map[int64]float64, len(childProfile.ColNDVs)),
	}
	for id, c := range childProfile.ColNDVs {
		stats.ColNDVs[id] = math.Min(c, stats.RowCount)
	}
	return stats
}

func (p *LogicalJoin) getGroupNDVs(colGroups [][]*expression.Column, childStats []*property.StatsInfo) []property.GroupNDV {
	outerIdx := int(-1)
	if p.JoinType == LeftOuterJoin || p.JoinType == LeftOuterSemiJoin || p.JoinType == AntiLeftOuterSemiJoin {
		outerIdx = 0
	} else if p.JoinType == RightOuterJoin {
		outerIdx = 1
	}
	if outerIdx >= 0 && len(colGroups) > 0 {
		return childStats[outerIdx].GroupNDVs
	}
	return nil
}

// DeriveStats implement LogicalPlan DeriveStats interface.
// If the type of join is SemiJoin, the selectivity of it will be same as selection's.
// If the type of join is LeftOuterSemiJoin, it will not add or remove any row. The last column is a boolean value, whose NDV should be two.
// If the type of join is inner/outer join, the output of join(s, t) should be N(s) * N(t) / (V(s.key) * V(t.key)) * Min(s.key, t.key).
// N(s) stands for the number of rows in relation s. V(s.key) means the NDV of join key in s.
// This is a quite simple strategy: We assume every bucket of relation which will participate join has the same number of rows, and apply cross join for
// every matched bucket.
func (p *LogicalJoin) DeriveStats(childStats []*property.StatsInfo, selfSchema *expression.Schema, childSchema []*expression.Schema, colGroups [][]*expression.Column) (*property.StatsInfo, error) {
	if p.StatsInfo() != nil {
		// Reload GroupNDVs since colGroups may have changed.
		p.StatsInfo().GroupNDVs = p.getGroupNDVs(colGroups, childStats)
		return p.StatsInfo(), nil
	}
	leftProfile, rightProfile := childStats[0], childStats[1]
	leftJoinKeys, rightJoinKeys, _, _ := p.GetJoinKeys()
	p.EqualCondOutCnt = cardinality.EstimateFullJoinRowCount(p.SCtx(),
		0 == len(p.EqualConditions),
		leftProfile, rightProfile,
		leftJoinKeys, rightJoinKeys,
		childSchema[0], childSchema[1],
		nil, nil)
	if p.JoinType == SemiJoin || p.JoinType == AntiSemiJoin {
		p.SetStats(&property.StatsInfo{
			RowCount: leftProfile.RowCount * cost.SelectionFactor,
			ColNDVs:  make(map[int64]float64, len(leftProfile.ColNDVs)),
		})
		for id, c := range leftProfile.ColNDVs {
			p.StatsInfo().ColNDVs[id] = c * cost.SelectionFactor
		}
		return p.StatsInfo(), nil
	}
	if p.JoinType == LeftOuterSemiJoin || p.JoinType == AntiLeftOuterSemiJoin {
		p.SetStats(&property.StatsInfo{
			RowCount: leftProfile.RowCount,
			ColNDVs:  make(map[int64]float64, selfSchema.Len()),
		})
		for id, c := range leftProfile.ColNDVs {
			p.StatsInfo().ColNDVs[id] = c
		}
		p.StatsInfo().ColNDVs[selfSchema.Columns[selfSchema.Len()-1].UniqueID] = 2.0
		p.StatsInfo().GroupNDVs = p.getGroupNDVs(colGroups, childStats)
		return p.StatsInfo(), nil
	}
	count := p.EqualCondOutCnt
	if p.JoinType == LeftOuterJoin {
		count = math.Max(count, leftProfile.RowCount)
	} else if p.JoinType == RightOuterJoin {
		count = math.Max(count, rightProfile.RowCount)
	}
	colNDVs := make(map[int64]float64, selfSchema.Len())
	for id, c := range leftProfile.ColNDVs {
		colNDVs[id] = math.Min(c, count)
	}
	for id, c := range rightProfile.ColNDVs {
		colNDVs[id] = math.Min(c, count)
	}
	p.SetStats(&property.StatsInfo{
		RowCount: count,
		ColNDVs:  colNDVs,
	})
	p.StatsInfo().GroupNDVs = p.getGroupNDVs(colGroups, childStats)
	return p.StatsInfo(), nil
}

// ExtractColGroups implements LogicalPlan ExtractColGroups interface.
func (p *LogicalJoin) ExtractColGroups(colGroups [][]*expression.Column) [][]*expression.Column {
	leftJoinKeys, rightJoinKeys, _, _ := p.GetJoinKeys()
	extracted := make([][]*expression.Column, 0, 2+len(colGroups))
	if len(leftJoinKeys) > 1 && (p.JoinType == InnerJoin || p.JoinType == LeftOuterJoin || p.JoinType == RightOuterJoin) {
		extracted = append(extracted, expression.SortColumns(leftJoinKeys), expression.SortColumns(rightJoinKeys))
	}
	var outerSchema *expression.Schema
	if p.JoinType == LeftOuterJoin || p.JoinType == LeftOuterSemiJoin || p.JoinType == AntiLeftOuterSemiJoin {
		outerSchema = p.Children()[0].Schema()
	} else if p.JoinType == RightOuterJoin {
		outerSchema = p.Children()[1].Schema()
	}
	if len(colGroups) == 0 || outerSchema == nil {
		return extracted
	}
	_, offsets := outerSchema.ExtractColGroups(colGroups)
	if len(offsets) == 0 {
		return extracted
	}
	for _, offset := range offsets {
		extracted = append(extracted, colGroups[offset])
	}
	return extracted
}

func (la *LogicalApply) getGroupNDVs(colGroups [][]*expression.Column, childStats []*property.StatsInfo) []property.GroupNDV {
	if len(colGroups) > 0 && (la.JoinType == LeftOuterSemiJoin || la.JoinType == AntiLeftOuterSemiJoin || la.JoinType == LeftOuterJoin) {
		return childStats[0].GroupNDVs
	}
	return nil
}

// DeriveStats implement LogicalPlan DeriveStats interface.
func (la *LogicalApply) DeriveStats(childStats []*property.StatsInfo, selfSchema *expression.Schema, childSchema []*expression.Schema, colGroups [][]*expression.Column) (*property.StatsInfo, error) {
	if la.StatsInfo() != nil {
		// Reload GroupNDVs since colGroups may have changed.
		la.StatsInfo().GroupNDVs = la.getGroupNDVs(colGroups, childStats)
		return la.StatsInfo(), nil
	}
	leftProfile := childStats[0]
	la.SetStats(&property.StatsInfo{
		RowCount: leftProfile.RowCount,
		ColNDVs:  make(map[int64]float64, selfSchema.Len()),
	})
	for id, c := range leftProfile.ColNDVs {
		la.StatsInfo().ColNDVs[id] = c
	}
	if la.JoinType == LeftOuterSemiJoin || la.JoinType == AntiLeftOuterSemiJoin {
		la.StatsInfo().ColNDVs[selfSchema.Columns[selfSchema.Len()-1].UniqueID] = 2.0
	} else {
		for i := childSchema[0].Len(); i < selfSchema.Len(); i++ {
			la.StatsInfo().ColNDVs[selfSchema.Columns[i].UniqueID] = leftProfile.RowCount
		}
	}
	la.StatsInfo().GroupNDVs = la.getGroupNDVs(colGroups, childStats)
	return la.StatsInfo(), nil
}

// ExtractColGroups implements LogicalPlan ExtractColGroups interface.
func (la *LogicalApply) ExtractColGroups(colGroups [][]*expression.Column) [][]*expression.Column {
	var outerSchema *expression.Schema
	// Apply doesn't have RightOuterJoin.
	if la.JoinType == LeftOuterJoin || la.JoinType == LeftOuterSemiJoin || la.JoinType == AntiLeftOuterSemiJoin {
		outerSchema = la.Children()[0].Schema()
	}
	if len(colGroups) == 0 || outerSchema == nil {
		return nil
	}
	_, offsets := outerSchema.ExtractColGroups(colGroups)
	if len(offsets) == 0 {
		return nil
	}
	extracted := make([][]*expression.Column, len(offsets))
	for i, offset := range offsets {
		extracted[i] = colGroups[offset]
	}
	return extracted
}

// Exists and MaxOneRow produce at most one row, so we set the RowCount of stats one.
func getSingletonStats(schema *expression.Schema) *property.StatsInfo {
	ret := &property.StatsInfo{
		RowCount: 1.0,
		ColNDVs:  make(map[int64]float64, schema.Len()),
	}
	for _, col := range schema.Columns {
		ret.ColNDVs[col.UniqueID] = 1
	}
	return ret
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
