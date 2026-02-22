// Copyright 2023 PingCAP, Inc.
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

package autoanalyze

import (
	"context"
	"fmt"
	"math/rand"
	"strings"
	"time"

	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/meta/metadef"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/sysproctrack"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/statistics"
	"github.com/pingcap/tidb/pkg/statistics/handle/autoanalyze/exec"
	"github.com/pingcap/tidb/pkg/statistics/handle/lockstats"
	statslogutil "github.com/pingcap/tidb/pkg/statistics/handle/logutil"
	statsutil "github.com/pingcap/tidb/pkg/statistics/handle/util"
	statstypes "github.com/pingcap/tidb/pkg/statistics/handle/types"
	"github.com/pingcap/tidb/pkg/util/sqlescape"
	"github.com/pingcap/tidb/pkg/util/timeutil"
	"go.uber.org/zap"
)

func RandomPickOneTableAndTryAutoAnalyze(
	sctx sessionctx.Context,
	statsHandle statstypes.StatsHandle,
	sysProcTracker sysproctrack.Tracker,
	autoAnalyzeRatio float64,
	pruneMode variable.PartitionPruneMode,
	start, end time.Time,
) bool {
	is := sctx.GetLatestInfoSchema().(infoschema.InfoSchema)
	dbs := infoschema.AllSchemaNames(is)
	// Shuffle the database and table slice to randomize the order of analyzing tables.
	rd := rand.New(rand.NewSource(time.Now().UnixNano())) // #nosec G404
	rd.Shuffle(len(dbs), func(i, j int) {
		dbs[i], dbs[j] = dbs[j], dbs[i]
	})
	// Query locked tables once to minimize overhead.
	// Outdated lock info is acceptable as we verify table lock status pre-analysis.
	lockedTables, err := lockstats.QueryLockedTables(statsutil.StatsCtx, sctx)
	if err != nil {
		statslogutil.StatsLogger().Warn(
			"check table lock failed",
			zap.Error(err),
		)
		return false
	}

	for _, db := range dbs {
		// Ignore the memory and system database.
		if metadef.IsMemOrSysDB(strings.ToLower(db)) {
			continue
		}

		tbls, err := is.SchemaTableInfos(context.Background(), ast.NewCIStr(db))
		terror.Log(err)
		// We shuffle dbs and tbls so that the order of iterating tables is random. If the order is fixed and the auto
		// analyze job of one table fails for some reason, it may always analyze the same table and fail again and again
		// when the HandleAutoAnalyze is triggered. Randomizing the order can avoid the problem.
		// TODO: Design a priority queue to place the table which needs analyze most in the front.
		rd.Shuffle(len(tbls), func(i, j int) {
			tbls[i], tbls[j] = tbls[j], tbls[i]
		})

		// We need to check every partition of every table to see if it needs to be analyzed.
		for _, tblInfo := range tbls {
			// Sometimes the tables are too many. Auto-analyze will take too much time on it.
			// so we need to check the available time.
			if !timeutil.WithinDayTimePeriod(start, end, time.Now()) {
				return false
			}
			// If table locked, skip analyze all partitions of the table.
			// FIXME: This check is not accurate, because other nodes may change the table lock status at any time.
			if _, ok := lockedTables[tblInfo.ID]; ok {
				continue
			}

			if tblInfo.IsView() {
				continue
			}

			pi := tblInfo.GetPartitionInfo()
			// No partitions, analyze the whole table.
			if pi == nil {
				statsTbl, found := statsHandle.GetNonPseudoPhysicalTableStats(tblInfo.ID)
				if !found {
					continue
				}

				sql := "analyze table %n.%n"
				analyzed := tryAutoAnalyzeTable(sctx, statsHandle, sysProcTracker, tblInfo, statsTbl, autoAnalyzeRatio, sql, db, tblInfo.Name.O)
				if analyzed {
					// analyze one table at a time to let it get the freshest parameters.
					// others will be analyzed next round which is just 3s later.
					return true
				}
				continue
			}
			// Only analyze the partition that has not been locked.
			partitionDefs := make([]model.PartitionDefinition, 0, len(pi.Definitions))
			for _, def := range pi.Definitions {
				if _, ok := lockedTables[def.ID]; !ok {
					partitionDefs = append(partitionDefs, def)
				}
			}
			partitionStats := getPartitionStats(statsHandle, partitionDefs)
			if pruneMode == variable.Dynamic {
				analyzed := tryAutoAnalyzePartitionTableInDynamicMode(
					sctx,
					statsHandle,
					sysProcTracker,
					tblInfo,
					partitionDefs,
					partitionStats,
					db,
					autoAnalyzeRatio,
				)
				if analyzed {
					return true
				}
				continue
			}
			for _, def := range partitionDefs {
				sql := "analyze table %n.%n partition %n"
				statsTbl := partitionStats[def.ID]
				analyzed := tryAutoAnalyzeTable(sctx, statsHandle, sysProcTracker, tblInfo, statsTbl, autoAnalyzeRatio, sql, db, tblInfo.Name.O, def.Name.O)
				if analyzed {
					return true
				}
			}
		}
	}

	return false
}

func getPartitionStats(
	statsHandle statstypes.StatsHandle,
	defs []model.PartitionDefinition,
) map[int64]*statistics.Table {
	partitionStats := make(map[int64]*statistics.Table, len(defs))

	for _, def := range defs {
		stats, found := statsHandle.GetNonPseudoPhysicalTableStats(def.ID)
		if found {
			partitionStats[def.ID] = stats
		}
	}

	return partitionStats
}

// Determine whether the table and index require analysis.
func tryAutoAnalyzeTable(
	sctx sessionctx.Context,
	statsHandle statstypes.StatsHandle,
	sysProcTracker sysproctrack.Tracker,
	tblInfo *model.TableInfo,
	statsTbl *statistics.Table,
	ratio float64,
	sql string,
	params ...any,
) bool {
	// 1. If the statistics are either not loaded or are classified as pseudo, there is no need for analyze
	//    Pseudo statistics can be created by the optimizer, so we need to double check it.
	// 2. If the table is too small, we don't want to waste time to analyze it.
	//    Leave the opportunity to other bigger tables.
	if statsTbl == nil || statsTbl.Pseudo || statsTbl.RealtimeCount < statistics.AutoAnalyzeMinCnt {
		return false
	}

	// Check if the table needs to analyze.
	if needAnalyze, reason := NeedAnalyzeTable(
		statsTbl,
		ratio,
	); needAnalyze {
		escaped, err := sqlescape.EscapeSQL(sql, params...)
		if err != nil {
			return false
		}
		statslogutil.StatsLogger().Info(
			"auto analyze triggered",
			zap.String("sql", escaped),
			zap.String("reason", reason),
		)

		tableStatsVer := sctx.GetSessionVars().AnalyzeVersion
		statistics.CheckAnalyzeVerOnTable(statsTbl, &tableStatsVer)
		exec.AutoAnalyze(sctx, statsHandle, sysProcTracker, tableStatsVer, sql, params...)

		return true
	}

	// Whether the table needs to analyze or not, we need to check the indices of the table.
	for _, idx := range tblInfo.Indices {
		if idxStats := statsTbl.GetIdx(idx.ID); idxStats == nil && !statsTbl.ColAndIdxExistenceMap.HasAnalyzed(idx.ID, true) && idx.State == model.StatePublic {
			// Columnar index doesn't need stats yet.
			if idx.IsColumnarIndex() {
				continue
			}
			sqlWithIdx := sql + " index %n"
			paramsWithIdx := append(params, idx.Name.O)
			escaped, err := sqlescape.EscapeSQL(sqlWithIdx, paramsWithIdx...)
			if err != nil {
				return false
			}

			statslogutil.StatsLogger().Info(
				"auto analyze for unanalyzed indexes",
				zap.String("sql", escaped),
			)
			tableStatsVer := sctx.GetSessionVars().AnalyzeVersion
			statistics.CheckAnalyzeVerOnTable(statsTbl, &tableStatsVer)
			exec.AutoAnalyze(sctx, statsHandle, sysProcTracker, tableStatsVer, sqlWithIdx, paramsWithIdx...)
			return true
		}
	}
	return false
}

// NeedAnalyzeTable checks if we need to analyze the table:
//  1. If the table has never been analyzed, we need to analyze it.
//  2. If the table had been analyzed before, we need to analyze it when
//     "tbl.ModifyCount/tbl.Count > autoAnalyzeRatio" and the current time is
//     between `start` and `end`.
//
// Exposed for test.
func NeedAnalyzeTable(tbl *statistics.Table, autoAnalyzeRatio float64) (bool, string) {
	analyzed := tbl.IsAnalyzed()
	if !analyzed {
		return true, "table unanalyzed"
	}
	// Auto analyze is disabled.
	if autoAnalyzeRatio == 0 {
		return false, ""
	}
	// No need to analyze it.
	tblCnt := float64(tbl.RealtimeCount)
	if histCnt := tbl.GetAnalyzeRowCount(); histCnt > 0 {
		tblCnt = histCnt
	}
	if float64(tbl.ModifyCount)/tblCnt <= autoAnalyzeRatio {
		return false, ""
	}
	return true, fmt.Sprintf("too many modifications(%v/%v>%v)", tbl.ModifyCount, tblCnt, autoAnalyzeRatio)
}

// It is very similar to tryAutoAnalyzeTable, but it commits the analyze job in batch for partitions.
func tryAutoAnalyzePartitionTableInDynamicMode(
	sctx sessionctx.Context,
	statsHandle statstypes.StatsHandle,
	sysProcTracker sysproctrack.Tracker,
	tblInfo *model.TableInfo,
	partitionDefs []model.PartitionDefinition,
	partitionStats map[int64]*statistics.Table,
	db string,
	ratio float64,
) bool {
	tableStatsVer := sctx.GetSessionVars().AnalyzeVersion
	analyzePartitionBatchSize := int(vardef.AutoAnalyzePartitionBatchSize.Load())
	needAnalyzePartitionNames := make([]any, 0, len(partitionDefs))

	for _, def := range partitionDefs {
		partitionStats := partitionStats[def.ID]
		// 1. If the statistics are either not loaded or are classified as pseudo, there is no need for analyze.
		//	  Pseudo statistics can be created by the optimizer, so we need to double check it.
		// 2. If the table is too small, we don't want to waste time to analyze it.
		//    Leave the opportunity to other bigger tables.
		if partitionStats == nil || partitionStats.Pseudo || partitionStats.RealtimeCount < statistics.AutoAnalyzeMinCnt {
			continue
		}
		if needAnalyze, reason := NeedAnalyzeTable(
			partitionStats,
			ratio,
		); needAnalyze {
			needAnalyzePartitionNames = append(needAnalyzePartitionNames, def.Name.O)
			statslogutil.StatsLogger().Info(
				"need to auto analyze",
				zap.String("database", db),
				zap.String("table", tblInfo.Name.String()),
				zap.String("partition", def.Name.O),
				zap.String("reason", reason),
			)
			statistics.CheckAnalyzeVerOnTable(partitionStats, &tableStatsVer)
		}
	}

	getSQL := func(prefix, suffix string, numPartitions int) string {
		var sqlBuilder strings.Builder
		sqlBuilder.WriteString(prefix)
		for i := range numPartitions {
			if i != 0 {
				sqlBuilder.WriteString(",")
			}
			sqlBuilder.WriteString(" %n")
		}
		sqlBuilder.WriteString(suffix)
		return sqlBuilder.String()
	}

	if len(needAnalyzePartitionNames) > 0 {
		statslogutil.StatsLogger().Info("start to auto analyze",
			zap.String("database", db),
			zap.String("table", tblInfo.Name.String()),
			zap.Any("partitions", needAnalyzePartitionNames),
			zap.Int("analyze partition batch size", analyzePartitionBatchSize),
		)

		statsTbl := statsHandle.GetPhysicalTableStats(tblInfo.ID, tblInfo)
		statistics.CheckAnalyzeVerOnTable(statsTbl, &tableStatsVer)
		for i := 0; i < len(needAnalyzePartitionNames); i += analyzePartitionBatchSize {
			start := i
			end := min(start+analyzePartitionBatchSize, len(needAnalyzePartitionNames))

			// Do batch analyze for partitions.
			sql := getSQL("analyze table %n.%n partition", "", end-start)
			params := append([]any{db, tblInfo.Name.O}, needAnalyzePartitionNames[start:end]...)

			statslogutil.StatsLogger().Info(
				"auto analyze triggered",
				zap.String("database", db),
				zap.String("table", tblInfo.Name.String()),
				zap.Any("partitions", needAnalyzePartitionNames[start:end]),
			)
			exec.AutoAnalyze(sctx, statsHandle, sysProcTracker, tableStatsVer, sql, params...)
		}

		return true
	}
	// Check if any index of the table needs to analyze.
	for _, idx := range tblInfo.Indices {
		if idx.State != model.StatePublic || statsutil.IsSpecialGlobalIndex(idx, tblInfo) {
			continue
		}
		// Columnar index doesn't need stats yet.
		if idx.IsColumnarIndex() {
			continue
		}
		// Collect all the partition names that need to analyze.
		for _, def := range partitionDefs {
			partitionStats := partitionStats[def.ID]
			// 1. If the statistics are either not loaded or are classified as pseudo, there is no need for analyze.
			//    Pseudo statistics can be created by the optimizer, so we need to double check it.
			if partitionStats == nil || partitionStats.Pseudo {
				continue
			}
			// 2. If the index is not analyzed, we need to analyze it.
			if !partitionStats.ColAndIdxExistenceMap.HasAnalyzed(idx.ID, true) {
				needAnalyzePartitionNames = append(needAnalyzePartitionNames, def.Name.O)
				statistics.CheckAnalyzeVerOnTable(partitionStats, &tableStatsVer)
			}
		}
		if len(needAnalyzePartitionNames) > 0 {
			statsTbl := statsHandle.GetPhysicalTableStats(tblInfo.ID, tblInfo)
			statistics.CheckAnalyzeVerOnTable(statsTbl, &tableStatsVer)

			for i := 0; i < len(needAnalyzePartitionNames); i += analyzePartitionBatchSize {
				start := i
				end := min(start+analyzePartitionBatchSize, len(needAnalyzePartitionNames))

				sql := getSQL("analyze table %n.%n partition", " index %n", end-start)
				params := append([]any{db, tblInfo.Name.O}, needAnalyzePartitionNames[start:end]...)
				params = append(params, idx.Name.O)
				statslogutil.StatsLogger().Info("auto analyze for unanalyzed",
					zap.String("database", db),
					zap.String("table", tblInfo.Name.String()),
					zap.String("index", idx.Name.String()),
					zap.Any("partitions", needAnalyzePartitionNames[start:end]),
				)
				exec.AutoAnalyze(sctx, statsHandle, sysProcTracker, tableStatsVer, sql, params...)
			}

			return true
		}
	}

	return false
}
