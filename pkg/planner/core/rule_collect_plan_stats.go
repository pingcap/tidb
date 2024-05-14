// Copyright 2021 PingCAP, Inc.
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
	"context"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/util/optimizetrace"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/statistics"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
)

type collectPredicateColumnsPoint struct{}

func (collectPredicateColumnsPoint) optimize(_ context.Context, plan base.LogicalPlan, _ *optimizetrace.LogicalOptimizeOp) (base.LogicalPlan, bool, error) {
	planChanged := false
	if plan.SCtx().GetSessionVars().InRestrictedSQL {
		return plan, planChanged, nil
	}
	predicateNeeded := variable.EnableColumnTracking.Load()
	syncWait := plan.SCtx().GetSessionVars().StatsLoadSyncWait.Load()
	histNeeded := syncWait > 0
	predicateColumns, histNeededColumns, visitedPhysTblIDs := CollectColumnStatsUsage(plan, predicateNeeded, histNeeded)
	if len(predicateColumns) > 0 {
		plan.SCtx().UpdateColStatsUsage(predicateColumns)
	}

	// Prepare the table metadata to avoid repeatedly fetching from the infoSchema below, and trigger extra sync/async
	// stats loading for the determinate mode.
	is := plan.SCtx().GetInfoSchema().(infoschema.InfoSchema)
	tblID2Tbl := make(map[int64]table.Table)
	visitedPhysTblIDs.ForEach(func(physicalTblID int) {
		tbl, _ := infoschema.FindTableByTblOrPartID(is, int64(physicalTblID))
		if tbl == nil {
			return
		}
		tblID2Tbl[int64(physicalTblID)] = tbl
	})

	// collect needed virtual columns from already needed columns
	// Note that we use the dependingVirtualCols only to collect needed index stats, but not to trigger stats loading on
	// the virtual columns themselves. It's because virtual columns themselves don't have statistics, while expression
	// indexes, which are indexes on virtual columns, have statistics. We don't waste the resource here now.
	dependingVirtualCols := CollectDependingVirtualCols(tblID2Tbl, histNeededColumns)

	histNeededIndices := collectSyncIndices(plan.SCtx(), append(histNeededColumns, dependingVirtualCols...), tblID2Tbl)
	histNeededItems := collectHistNeededItems(histNeededColumns, histNeededIndices)
	if histNeeded && len(histNeededItems) > 0 {
		err := RequestLoadStats(plan.SCtx(), histNeededItems, syncWait)
		return plan, planChanged, err
	}
	return plan, planChanged, nil
}

func (collectPredicateColumnsPoint) name() string {
	return "collect_predicate_columns_point"
}

type syncWaitStatsLoadPoint struct{}

func (syncWaitStatsLoadPoint) optimize(_ context.Context, plan base.LogicalPlan, _ *optimizetrace.LogicalOptimizeOp) (base.LogicalPlan, bool, error) {
	planChanged := false
	if plan.SCtx().GetSessionVars().InRestrictedSQL {
		return plan, planChanged, nil
	}
	if plan.SCtx().GetSessionVars().StmtCtx.IsSyncStatsFailed {
		return plan, planChanged, nil
	}
	err := SyncWaitStatsLoad(plan)
	return plan, planChanged, err
}

func (syncWaitStatsLoadPoint) name() string {
	return "sync_wait_stats_load_point"
}

// RequestLoadStats send load column/index stats requests to stats handle
func RequestLoadStats(ctx base.PlanContext, neededHistItems []model.StatsLoadItem, syncWait int64) error {
	maxExecutionTime := ctx.GetSessionVars().GetMaxExecutionTime()
	if maxExecutionTime > 0 && maxExecutionTime < uint64(syncWait) {
		syncWait = int64(maxExecutionTime)
	}
	failpoint.Inject("assertSyncWaitFailed", func(val failpoint.Value) {
		if val.(bool) {
			if syncWait != 1 {
				panic("syncWait should be 1(ms)")
			}
		}
	})
	var timeout = time.Duration(syncWait * time.Millisecond.Nanoseconds())
	stmtCtx := ctx.GetSessionVars().StmtCtx
	err := domain.GetDomain(ctx).StatsHandle().SendLoadRequests(stmtCtx, neededHistItems, timeout)
	if err != nil {
		stmtCtx.IsSyncStatsFailed = true
		if variable.StatsLoadPseudoTimeout.Load() {
			logutil.BgLogger().Warn("RequestLoadStats failed", zap.Error(err))
			stmtCtx.AppendWarning(err)
			return nil
		}
		logutil.BgLogger().Warn("RequestLoadStats failed", zap.Error(err))
		return err
	}
	return nil
}

// SyncWaitStatsLoad sync-wait for stats load until timeout
func SyncWaitStatsLoad(plan base.LogicalPlan) error {
	stmtCtx := plan.SCtx().GetSessionVars().StmtCtx
	if len(stmtCtx.StatsLoad.NeededItems) <= 0 {
		return nil
	}
	err := domain.GetDomain(plan.SCtx()).StatsHandle().SyncWaitStatsLoad(stmtCtx)
	if err != nil {
		stmtCtx.IsSyncStatsFailed = true
		if variable.StatsLoadPseudoTimeout.Load() {
			logutil.BgLogger().Warn("SyncWaitStatsLoad failed", zap.Error(err))
			stmtCtx.AppendWarning(err)
			return nil
		}
		logutil.BgLogger().Error("SyncWaitStatsLoad failed", zap.Error(err))
		return err
	}
	return nil
}

// CollectDependingVirtualCols collects the virtual columns that depend on the needed columns, and returns them in a new slice.
//
// Why do we need this?
// It's mainly for stats sync loading.
// Currently, virtual columns themselves don't have statistics. But expression indexes, which are indexes on virtual
// columns, have statistics. We need to collect needed virtual columns, then needed expression index stats can be
// collected for sync loading.
// In normal cases, if a virtual column can be used, which means related statistics may be needed, the corresponding
// expressions in the query must have already been replaced with the virtual column before here. So we just need to treat
// them like normal columns in stats sync loading, which means we just extract the Column from the expressions, the
// virtual columns we want will be there.
// However, in some cases (the mv index case now), the expressions are not replaced with the virtual columns before here.
// Instead, we match the expression in the query against the expression behind the virtual columns after here when
// building the access paths. This means we are unable to known what virtual columns will be needed by just extracting
// the Column from the expressions here. So we need to manually collect the virtual columns that may be needed.
//
// Note 1: As long as a virtual column depends on the needed columns, it will be collected. This could collect some virtual
// columns that are not actually needed.
// It's OK because that's how sync loading is expected. Sync loading only needs to ensure all actually needed stats are
// triggered to be loaded. Other logic of sync loading also works like this.
// If we want to collect only the virtual columns that are actually needed, we need to make the checking logic here exactly
// the same as the logic for generating the access paths, which will make the logic here very complicated.
//
// Note 2: Only direct dependencies are considered here.
// If a virtual column depends on another virtual column, and the latter depends on the needed columns, then the former
// will not be collected.
// For example: create table t(a int, b int, c int as (a+b), d int as (c+1)); If a is needed, then c will be collected,
// but d will not be collected.
// It's because currently it's impossible that statistics related to indirectly depending columns are actually needed.
// If we need to check indirect dependency some day, we can easily extend the logic here.
func CollectDependingVirtualCols(tblID2Tbl map[int64]table.Table, neededItems []model.StatsLoadItem) []model.StatsLoadItem {
	generatedCols := make([]model.StatsLoadItem, 0)

	// group the neededItems by table id
	tblID2neededColIDs := make(map[int64][]int64, len(tblID2Tbl))
	for _, item := range neededItems {
		if item.IsIndex {
			continue
		}
		tblID2neededColIDs[item.TableID] = append(tblID2neededColIDs[item.TableID], item.ID)
	}

	// process them by table id
	for tblID, colIDs := range tblID2neededColIDs {
		tbl := tblID2Tbl[tblID]
		if tbl == nil {
			continue
		}
		// collect the needed columns on this table into a set for faster lookup
		colNameSet := make(map[string]struct{}, len(colIDs))
		for _, colID := range colIDs {
			name := tbl.Meta().FindColumnNameByID(colID)
			if name == "" {
				continue
			}
			colNameSet[name] = struct{}{}
		}
		// iterate columns in this table, and collect the virtual columns that depend on the needed columns
		for _, col := range tbl.Cols() {
			// only handles virtual columns
			if !col.IsVirtualGenerated() {
				continue
			}
			// If this column is already needed, then skip it.
			if _, ok := colNameSet[col.Name.L]; ok {
				continue
			}
			// If there exists a needed column that is depended on by this virtual column,
			// then we think this virtual column is needed.
			for depCol := range col.Dependences {
				if _, ok := colNameSet[depCol]; ok {
					generatedCols = append(generatedCols, model.StatsLoadItem{TableItemID: model.TableItemID{TableID: tblID, ID: col.ID, IsIndex: false}, FullLoad: true})
					break
				}
			}
		}
	}
	return generatedCols
}

// collectSyncIndices will collect the indices which includes following conditions:
// 1. the indices contained the any one of histNeededColumns, eg: histNeededColumns contained A,B columns, and idx_a is
// composed up by A column, then we thought the idx_a should be collected
// 2. The stats condition of idx_a can't meet IsFullLoad, which means its stats was evicted previously
func collectSyncIndices(ctx base.PlanContext,
	histNeededColumns []model.StatsLoadItem,
	tblID2Tbl map[int64]table.Table,
) map[model.TableItemID]struct{} {
	histNeededIndices := make(map[model.TableItemID]struct{})
	stats := domain.GetDomain(ctx).StatsHandle()
	for _, column := range histNeededColumns {
		if column.IsIndex {
			continue
		}
		tbl := tblID2Tbl[column.TableID]
		if tbl == nil {
			continue
		}
		colName := tbl.Meta().FindColumnNameByID(column.ID)
		if colName == "" {
			continue
		}
		for _, idx := range tbl.Indices() {
			if idx.Meta().State != model.StatePublic {
				continue
			}
			idxCol := idx.Meta().FindColumnByName(colName)
			idxID := idx.Meta().ID
			if idxCol != nil {
				tblStats := stats.GetTableStats(tbl.Meta())
				if tblStats == nil || tblStats.Pseudo {
					continue
				}
				_, loadNeeded := tblStats.IndexIsLoadNeeded(idxID)
				if !loadNeeded {
					continue
				}
				histNeededIndices[model.TableItemID{TableID: column.TableID, ID: idxID, IsIndex: true}] = struct{}{}
			}
		}
	}
	return histNeededIndices
}

func collectHistNeededItems(histNeededColumns []model.StatsLoadItem, histNeededIndices map[model.TableItemID]struct{}) (histNeededItems []model.StatsLoadItem) {
	histNeededItems = make([]model.StatsLoadItem, 0, len(histNeededColumns)+len(histNeededIndices))
	for idx := range histNeededIndices {
		histNeededItems = append(histNeededItems, model.StatsLoadItem{TableItemID: idx, FullLoad: true})
	}
	histNeededItems = append(histNeededItems, histNeededColumns...)
	return
}

func recordTableRuntimeStats(sctx base.PlanContext, tbls map[int64]struct{}) {
	tblStats := sctx.GetSessionVars().StmtCtx.TableStats
	if tblStats == nil {
		tblStats = map[int64]any{}
	}
	for tblID := range tbls {
		tblJSONStats, skip, err := recordSingleTableRuntimeStats(sctx, tblID)
		if err != nil {
			logutil.BgLogger().Warn("record table json stats failed", zap.Int64("tblID", tblID), zap.Error(err))
		}
		if tblJSONStats == nil && !skip {
			logutil.BgLogger().Warn("record table json stats failed due to empty", zap.Int64("tblID", tblID))
		}
		tblStats[tblID] = tblJSONStats
	}
	sctx.GetSessionVars().StmtCtx.TableStats = tblStats
}

func recordSingleTableRuntimeStats(sctx base.PlanContext, tblID int64) (stats *statistics.Table, skip bool, err error) {
	dom := domain.GetDomain(sctx)
	statsHandle := dom.StatsHandle()
	is := sctx.GetDomainInfoSchema().(infoschema.InfoSchema)
	tbl, ok := is.TableByID(tblID)
	if !ok {
		return nil, false, nil
	}
	tableInfo := tbl.Meta()
	stats = statsHandle.GetTableStats(tableInfo)
	// Skip the warning if the table is a temporary table because the temporary table doesn't have stats.
	skip = tableInfo.TempTableType != model.TempTableNone
	return stats, skip, nil
}
