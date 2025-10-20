// Copyright 2025 PingCAP, Inc.
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

package stats

import (
	"strconv"

	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	core_metrics "github.com/pingcap/tidb/pkg/planner/core/metrics"
	"github.com/pingcap/tidb/pkg/planner/util/debugtrace"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/stmtctx"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/statistics"
	"github.com/pingcap/tidb/pkg/statistics/handle"
	"github.com/pingcap/tidb/pkg/util/intest"
)

// GetTblInfoForUsedStatsByPhysicalID get table name, partition name and HintedTable
// that will be used to record used stats.
func GetTblInfoForUsedStatsByPhysicalID(sctx base.PlanContext, id int64) (
	fullName string, tblInfo *model.TableInfo) {
	fullName = "tableID " + strconv.FormatInt(id, 10)

	is := sctx.GetLatestISWithoutSessExt()
	tbl, partDef := infoschema.FindTableByTblOrPartID(is.(infoschema.InfoSchema), id)
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

// GetStatsTable gets statistics information for a table specified by "tableID".
// A pseudo statistics table is returned in any of the following scenario:
// 1. tidb-server started and statistics handle has not been initialized.
// 2. table row count from statistics is zero.
// 3. statistics is outdated.
// Note: please also update getLatestVersionFromStatsTable() when logic in this function changes.
func GetStatsTable(ctx base.PlanContext, tblInfo *model.TableInfo, pid int64) *statistics.Table {
	var statsHandle *handle.Handle
	dom := domain.GetDomain(ctx)
	if dom != nil {
		statsHandle = dom.StatsHandle()
	}
	var usePartitionStats, countIs0, pseudoStatsForUninitialized, pseudoStatsForOutdated bool
	var statsTbl *statistics.Table
	if ctx.GetSessionVars().StmtCtx.EnableOptimizerDebugTrace {
		debugtrace.EnterContextCommon(ctx)
		defer func() {
			debugTraceGetStatsTbl(ctx,
				tblInfo,
				pid,
				statsHandle == nil,
				usePartitionStats,
				countIs0,
				pseudoStatsForUninitialized,
				pseudoStatsForOutdated,
				statsTbl,
			)
			debugtrace.LeaveContextCommon(ctx)
		}()
	}
	// 1. tidb-server started and statistics handle has not been initialized.
	if statsHandle == nil {
		return statistics.PseudoTable(tblInfo, false, true)
	}

	if pid == tblInfo.ID || ctx.GetSessionVars().StmtCtx.UseDynamicPartitionPrune() {
		statsTbl = statsHandle.GetPhysicalTableStats(tblInfo.ID, tblInfo)
	} else {
		usePartitionStats = true
		statsTbl = statsHandle.GetPhysicalTableStats(pid, tblInfo)
	}
	intest.Assert(statsTbl.ColAndIdxExistenceMap != nil, "The existence checking map must not be nil.")

	allowPseudoTblTriggerLoading := false
	// In OptObjectiveDeterminate mode, we need to ignore the real-time stats.
	// To achieve this, we copy the statsTbl and reset the real-time stats fields (set ModifyCount to 0 and set
	// RealtimeCount to the row count from the ANALYZE, which is fetched from loaded stats in GetAnalyzeRowCount()).
	if ctx.GetSessionVars().GetOptObjective() == vardef.OptObjectiveDeterminate {
		analyzeCount := max(int64(statsTbl.GetAnalyzeRowCount()), 0)
		// If the two fields are already the values we want, we don't need to modify it, and also we don't need to copy.
		if statsTbl.RealtimeCount != analyzeCount || statsTbl.ModifyCount != 0 {
			// Here is a case that we need specially care about:
			// The original stats table from the stats cache is not a pseudo table, but the analyze row count is 0 (probably
			// because of no col/idx stats are loaded), which will makes it a pseudo table according to the rule 2 below.
			// Normally, a pseudo table won't trigger stats loading since we assume it means "no stats available", but
			// in such case, we need it able to trigger stats loading.
			// That's why we use the special allowPseudoTblTriggerLoading flag here.
			if !statsTbl.Pseudo && statsTbl.RealtimeCount > 0 && analyzeCount == 0 {
				allowPseudoTblTriggerLoading = true
			}
			// Copy it so we can modify the ModifyCount and the RealtimeCount safely.
			statsTbl = statsTbl.CopyAs(statistics.MetaOnly)
			statsTbl.RealtimeCount = analyzeCount
			statsTbl.ModifyCount = 0
		}
	}

	// 2. table row count from statistics is zero.
	if statsTbl.RealtimeCount == 0 {
		countIs0 = true
		core_metrics.PseudoEstimationNotAvailable.Inc()
		return statistics.PseudoTable(tblInfo, allowPseudoTblTriggerLoading, true)
	}

	// 3. statistics is uninitialized or outdated.
	pseudoStatsForUninitialized = !statsTbl.IsInitialized()
	pseudoStatsForOutdated = ctx.GetSessionVars().GetEnablePseudoForOutdatedStats() && statsTbl.IsOutdated()
	if pseudoStatsForUninitialized || pseudoStatsForOutdated {
		tbl := *statsTbl
		tbl.Pseudo = true
		statsTbl = &tbl
		if pseudoStatsForUninitialized {
			core_metrics.PseudoEstimationNotAvailable.Inc()
		} else {
			core_metrics.PseudoEstimationOutdate.Inc()
		}
	}

	return statsTbl
}

// LoadTableStats loads the stats of the table and store it in the statement `UsedStatsInfo` if it didn't exist
func LoadTableStats(ctx sessionctx.Context, tblInfo *model.TableInfo, pid int64) {
	statsRecord := ctx.GetSessionVars().StmtCtx.GetUsedStatsInfo(true)
	if statsRecord.GetUsedInfo(pid) != nil {
		return
	}

	pctx := ctx.GetPlanCtx()
	tableStats := GetStatsTable(pctx, tblInfo, pid)

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
