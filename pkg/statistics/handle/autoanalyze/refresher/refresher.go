// Copyright 2024 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package refresher

import (
	"strings"
	"time"

	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/sysproctrack"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/statistics"
	"github.com/pingcap/tidb/pkg/statistics/handle/autoanalyze/exec"
	"github.com/pingcap/tidb/pkg/statistics/handle/autoanalyze/priorityqueue"
	"github.com/pingcap/tidb/pkg/statistics/handle/lockstats"
	statslogutil "github.com/pingcap/tidb/pkg/statistics/handle/logutil"
	statstypes "github.com/pingcap/tidb/pkg/statistics/handle/types"
	statsutil "github.com/pingcap/tidb/pkg/statistics/handle/util"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/timeutil"
	"github.com/tikv/client-go/v2/oracle"
	"go.uber.org/zap"
)

const (
	// unanalyzedTableDefaultChangePercentage is the default change percentage of unanalyzed table.
	unanalyzedTableDefaultChangePercentage = 1
	// unanalyzedTableDefaultLastUpdateDuration is the default last update duration of unanalyzed table.
	unanalyzedTableDefaultLastUpdateDuration = -30 * time.Minute
)

// Refresher provides methods to refresh stats info.
// NOTE: Refresher is not thread-safe.
type Refresher struct {
	statsHandle    statstypes.StatsHandle
	sysProcTracker sysproctrack.Tracker
	// This will be refreshed every time we rebuild the priority queue.
	autoAnalysisTimeWindow

	// Jobs is the priority queue of analysis jobs.
	// Exported for testing purposes.
	Jobs *priorityqueue.AnalysisPriorityQueue
}

// NewRefresher creates a new Refresher and starts the goroutine.
func NewRefresher(
	statsHandle statstypes.StatsHandle,
	sysProcTracker sysproctrack.Tracker,
) *Refresher {
	r := &Refresher{
		statsHandle:    statsHandle,
		sysProcTracker: sysProcTracker,
		Jobs:           priorityqueue.NewAnalysisPriorityQueue(),
	}

	return r
}

// PickOneTableAndAnalyzeByPriority picks one table and analyzes it by priority.
func (r *Refresher) PickOneTableAndAnalyzeByPriority() bool {
	if !r.autoAnalysisTimeWindow.isWithinTimeWindow(time.Now()) {
		return false
	}

	se, err := r.statsHandle.SPool().Get()
	if err != nil {
		statslogutil.StatsLogger().Error(
			"Get session context failed",
			zap.Error(err),
		)
		return false
	}
	defer r.statsHandle.SPool().Put(se)
	sctx := se.(sessionctx.Context)
	// Pick the table with the highest weight.
	for r.Jobs.Len() > 0 {
		job := r.Jobs.Pop()
		if valid, failReason := job.IsValidToAnalyze(
			sctx,
		); !valid {
			statslogutil.SingletonStatsSamplerLogger().Info(
				"Table is not ready to analyze",
				zap.String("failReason", failReason),
				zap.Stringer("job", job),
			)
			continue
		}
		statslogutil.StatsLogger().Info(
			"Auto analyze triggered",
			zap.Stringer("job", job),
		)
		err = job.Analyze(
			r.statsHandle,
			r.sysProcTracker,
		)
		if err != nil {
			statslogutil.StatsLogger().Error(
				"Execute auto analyze job failed",
				zap.Stringer("job", job),
				zap.Error(err),
			)
		}
		// Only analyze one table each time.
		return true
	}
	statslogutil.SingletonStatsSamplerLogger().Info(
		"No table to analyze",
	)
	return false
}

// RebuildTableAnalysisJobQueue rebuilds the priority queue of analysis jobs.
func (r *Refresher) RebuildTableAnalysisJobQueue() error {
	// Reset the priority queue.
	r.Jobs = priorityqueue.NewAnalysisPriorityQueue()

	if err := statsutil.CallWithSCtx(
		r.statsHandle.SPool(),
		func(sctx sessionctx.Context) error {
			parameters := exec.GetAutoAnalyzeParameters(sctx)
			autoAnalyzeRatio := exec.ParseAutoAnalyzeRatio(parameters[variable.TiDBAutoAnalyzeRatio])
			// Get the available time period for auto analyze and check if the current time is in the period.
			start, end, err := exec.ParseAutoAnalysisWindow(
				parameters[variable.TiDBAutoAnalyzeStartTime],
				parameters[variable.TiDBAutoAnalyzeEndTime],
			)
			if err != nil {
				statslogutil.StatsLogger().Error(
					"parse auto analyze period failed",
					zap.Error(err),
				)
				return err
			}
			// We will check it again when we try to execute the job.
			// So store the time window for later use.
			r.autoAnalysisTimeWindow = autoAnalysisTimeWindow{
				start: start,
				end:   end,
			}
			if !r.autoAnalysisTimeWindow.isWithinTimeWindow(time.Now()) {
				return nil
			}
			calculator := priorityqueue.NewPriorityCalculator()
			pruneMode := variable.PartitionPruneMode(sctx.GetSessionVars().PartitionPruneMode.Load())
			is := sctx.GetDomainInfoSchema().(infoschema.InfoSchema)
			// Query locked tables once to minimize overhead.
			// Outdated lock info is acceptable as we verify table lock status pre-analysis.
			lockedTables, err := lockstats.QueryLockedTables(sctx)
			if err != nil {
				return err
			}
			// Get current timestamp from the session context.
			currentTs, err := getStartTs(sctx)
			if err != nil {
				return err
			}

			dbs := infoschema.AllSchemaNames(is)
			for _, db := range dbs {
				// Sometimes the tables are too many. Auto-analyze will take too much time on it.
				// so we need to check the available time.
				if !r.autoAnalysisTimeWindow.isWithinTimeWindow(time.Now()) {
					return nil
				}
				// Ignore the memory and system database.
				if util.IsMemOrSysDB(strings.ToLower(db)) {
					continue
				}

				tbls := is.SchemaTables(model.NewCIStr(db))
				// We need to check every partition of every table to see if it needs to be analyzed.
				for _, tbl := range tbls {
					// If table locked, skip analyze all partitions of the table.
					if _, ok := lockedTables[tbl.Meta().ID]; ok {
						continue
					}

					tblInfo := tbl.Meta()
					if tblInfo.IsView() {
						continue
					}
					pi := tblInfo.GetPartitionInfo()
					pushJobFunc := func(job priorityqueue.AnalysisJob) {
						if job == nil {
							return
						}
						// Calculate the weight of the job.
						weight := calculator.CalculateWeight(job)
						// We apply a penalty to larger tables, which can potentially result in a negative weight.
						// To prevent this, we filter out any negative weights. Under normal circumstances, table sizes should not be negative.
						if weight <= 0 {
							statslogutil.SingletonStatsSamplerLogger().Warn(
								"Table gets a negative weight",
								zap.Float64("weight", weight),
								zap.Stringer("job", job),
							)
						}
						job.SetWeight(weight)
						// Push the job onto the queue.
						r.Jobs.Push(job)
					}
					// No partitions, analyze the whole table.
					if pi == nil {
						job := CreateTableAnalysisJob(
							sctx,
							db,
							tblInfo,
							r.statsHandle.GetTableStatsForAutoAnalyze(tblInfo),
							autoAnalyzeRatio,
							currentTs,
						)
						pushJobFunc(job)
						// Skip the rest of the loop.
						continue
					}

					// Only analyze the partition that has not been locked.
					partitionDefs := make([]model.PartitionDefinition, 0, len(pi.Definitions))
					for _, def := range pi.Definitions {
						if _, ok := lockedTables[def.ID]; !ok {
							partitionDefs = append(partitionDefs, def)
						}
					}
					partitionStats := getPartitionStats(r.statsHandle, tblInfo, partitionDefs)
					// If the prune mode is static, we need to analyze every partition as a separate table.
					if pruneMode == variable.Static {
						for pIDAndName, stats := range partitionStats {
							job := CreateStaticPartitionAnalysisJob(
								sctx,
								db,
								tblInfo,
								pIDAndName.ID,
								pIDAndName.Name,
								stats,
								autoAnalyzeRatio,
								currentTs,
							)
							pushJobFunc(job)
						}
					} else {
						job := createTableAnalysisJobForPartitions(
							sctx,
							db,
							tblInfo,
							r.statsHandle.GetPartitionStatsForAutoAnalyze(tblInfo, tblInfo.ID),
							partitionStats,
							autoAnalyzeRatio,
							currentTs,
						)
						pushJobFunc(job)
					}
				}
			}

			return nil
		},
		statsutil.FlagWrapTxn,
	); err != nil {
		return err
	}

	return nil
}

// CreateTableAnalysisJob creates a TableAnalysisJob for the physical table.
func CreateTableAnalysisJob(
	sctx sessionctx.Context,
	tableSchema string,
	tblInfo *model.TableInfo,
	tblStats *statistics.Table,
	autoAnalyzeRatio float64,
	currentTs uint64,
) priorityqueue.AnalysisJob {
	if !isEligibleForAnalysis(tblStats) {
		return nil
	}

	tableStatsVer := sctx.GetSessionVars().AnalyzeVersion
	statistics.CheckAnalyzeVerOnTable(tblStats, &tableStatsVer)

	changePercentage := CalculateChangePercentage(tblStats, autoAnalyzeRatio)
	tableSize := calculateTableSize(tblInfo, tblStats)
	lastAnalysisDuration := GetTableLastAnalyzeDuration(tblStats, currentTs)
	indexes := CheckIndexesNeedAnalyze(tblInfo, tblStats)

	// No need to analyze.
	// We perform a separate check because users may set the auto analyze ratio to 0,
	// yet still wish to analyze newly added indexes and tables that have not been analyzed.
	if changePercentage == 0 && len(indexes) == 0 {
		return nil
	}

	job := priorityqueue.NewNonPartitionedTableAnalysisJob(
		tableSchema,
		tblInfo.Name.O,
		tblInfo.ID,
		indexes,
		tableStatsVer,
		changePercentage,
		tableSize,
		lastAnalysisDuration,
	)

	return job
}

// CreateStaticPartitionAnalysisJob creates a TableAnalysisJob for the static partition.
func CreateStaticPartitionAnalysisJob(
	sctx sessionctx.Context,
	tableSchema string,
	globalTblInfo *model.TableInfo,
	partitionID int64,
	partitionName string,
	partitionStats *statistics.Table,
	autoAnalyzeRatio float64,
	currentTs uint64,
) priorityqueue.AnalysisJob {
	if !isEligibleForAnalysis(partitionStats) {
		return nil
	}

	tableStatsVer := sctx.GetSessionVars().AnalyzeVersion
	statistics.CheckAnalyzeVerOnTable(partitionStats, &tableStatsVer)

	changePercentage := CalculateChangePercentage(partitionStats, autoAnalyzeRatio)
	tableSize := calculateTableSize(globalTblInfo, partitionStats)
	lastAnalysisDuration := GetTableLastAnalyzeDuration(partitionStats, currentTs)
	indexes := CheckIndexesNeedAnalyze(globalTblInfo, partitionStats)

	// No need to analyze.
	// We perform a separate check because users may set the auto analyze ratio to 0,
	// yet still wish to analyze newly added indexes and tables that have not been analyzed.
	if changePercentage == 0 && len(indexes) == 0 {
		return nil
	}

	job := priorityqueue.NewStaticPartitionTableAnalysisJob(
		tableSchema,
		globalTblInfo.Name.O,
		globalTblInfo.ID,
		partitionName,
		partitionID,
		indexes,
		tableStatsVer,
		changePercentage,
		tableSize,
		lastAnalysisDuration,
	)

	return job
}

// CalculateChangePercentage calculates the change percentage of the table
// based on the change count and the analysis count.
func CalculateChangePercentage(
	tblStats *statistics.Table,
	autoAnalyzeRatio float64,
) float64 {
	if !tblStats.IsAnalyzed() {
		return unanalyzedTableDefaultChangePercentage
	}

	// Auto analyze based on the change percentage is disabled.
	// However, this check should not affect the analysis of indexes,
	// as index analysis is still needed for query performance.
	if autoAnalyzeRatio == 0 {
		return 0
	}

	tblCnt := float64(tblStats.RealtimeCount)
	if histCnt := tblStats.GetAnalyzeRowCount(); histCnt > 0 {
		tblCnt = histCnt
	}
	res := float64(tblStats.ModifyCount) / tblCnt
	if res > autoAnalyzeRatio {
		return res
	}

	return 0
}

func calculateTableSize(
	tblInfo *model.TableInfo,
	tblStats *statistics.Table,
) float64 {
	tblCnt := float64(tblStats.RealtimeCount)
	// TODO: Ignore unanalyzable columns.
	colCnt := float64(len(tblInfo.Columns))

	return tblCnt * colCnt
}

// GetTableLastAnalyzeDuration gets the duration since the last analysis of the table.
func GetTableLastAnalyzeDuration(
	tblStats *statistics.Table,
	currentTs uint64,
) time.Duration {
	lastTime := findLastAnalyzeTime(tblStats, currentTs)
	currentTime := oracle.GetTimeFromTS(currentTs)

	// Calculate the duration since last analyze.
	return currentTime.Sub(lastTime)
}

// findLastAnalyzeTime finds the last analyze time of the table.
// It uses `LastUpdateVersion` to find the last analyze time.
// The `LastUpdateVersion` is the version of the transaction that updates the statistics.
// It always not null(default 0), so we can use it to find the last analyze time.
func findLastAnalyzeTime(
	tblStats *statistics.Table,
	currentTs uint64,
) time.Time {
	// Table is not analyzed, compose a fake version.
	if !tblStats.IsAnalyzed() {
		phy := oracle.GetTimeFromTS(currentTs)
		return phy.Add(unanalyzedTableDefaultLastUpdateDuration)
	}
	return oracle.GetTimeFromTS(tblStats.LastAnalyzeVersion)
}

// CheckIndexesNeedAnalyze checks if the indexes of the table need to be analyzed.
func CheckIndexesNeedAnalyze(
	tblInfo *model.TableInfo,
	tblStats *statistics.Table,
) []string {
	// If table is not analyzed, we need to analyze whole table.
	// So we don't need to check indexes.
	if !tblStats.IsAnalyzed() {
		return nil
	}

	indexes := make([]string, 0, len(tblInfo.Indices))
	// Check if missing index stats.
	for _, idx := range tblInfo.Indices {
		if _, ok := tblStats.Indices[idx.ID]; !ok && !tblStats.ColAndIdxExistenceMap.HasAnalyzed(idx.ID, true) && idx.State == model.StatePublic {
			indexes = append(indexes, idx.Name.O)
		}
	}

	return indexes
}

func createTableAnalysisJobForPartitions(
	sctx sessionctx.Context,
	tableSchema string,
	tblInfo *model.TableInfo,
	tblStats *statistics.Table,
	partitionStats map[PartitionIDAndName]*statistics.Table,
	autoAnalyzeRatio float64,
	currentTs uint64,
) priorityqueue.AnalysisJob {
	if !isEligibleForAnalysis(tblStats) {
		return nil
	}

	// TODO: figure out how to check the table stats version correctly for partitioned tables.
	tableStatsVer := sctx.GetSessionVars().AnalyzeVersion
	statistics.CheckAnalyzeVerOnTable(tblStats, &tableStatsVer)

	averageChangePercentage, avgSize, minLastAnalyzeDuration, partitionNames := CalculateIndicatorsForPartitions(
		tblInfo,
		partitionStats,
		autoAnalyzeRatio,
		currentTs,
	)
	partitionIndexes := CheckNewlyAddedIndexesNeedAnalyzeForPartitionedTable(
		tblInfo,
		partitionStats,
	)
	// No need to analyze.
	// We perform a separate check because users may set the auto analyze ratio to 0,
	// yet still wish to analyze newly added indexes and tables that have not been analyzed.
	if len(partitionNames) == 0 && len(partitionIndexes) == 0 {
		return nil
	}

	job := priorityqueue.NewDynamicPartitionedTableAnalysisJob(
		tableSchema,
		tblInfo.Name.O,
		tblInfo.ID,
		partitionNames,
		partitionIndexes,
		tableStatsVer,
		averageChangePercentage,
		avgSize,
		minLastAnalyzeDuration,
	)

	return job
}

// CalculateIndicatorsForPartitions calculates the average change percentage,
// average size and average last analyze duration for the partitions that meet the threshold.
// Change percentage is the ratio of the number of modified rows to the total number of rows.
// Size is the product of the number of rows and the number of columns.
// Last analyze duration is the duration since the last analyze.
func CalculateIndicatorsForPartitions(
	tblInfo *model.TableInfo,
	partitionStats map[PartitionIDAndName]*statistics.Table,
	autoAnalyzeRatio float64,
	currentTs uint64,
) (
	avgChange float64,
	avgSize float64,
	avgLastAnalyzeDuration time.Duration,
	partitionNames []string,
) {
	totalChangePercent := 0.0
	totalSize := 0.0
	count := 0.0
	partitionNames = make([]string, 0, len(partitionStats))
	cols := float64(len(tblInfo.Columns))
	totalLastAnalyzeDuration := time.Duration(0)

	for pIDAndName, tblStats := range partitionStats {
		changePercent := CalculateChangePercentage(tblStats, autoAnalyzeRatio)
		// Skip partition analysis if it doesn't meet the threshold, stats are not yet loaded,
		// or the auto analyze ratio is set to 0 by the user.
		if changePercent == 0 {
			continue
		}

		totalChangePercent += changePercent
		// size = count * cols
		totalSize += float64(tblStats.RealtimeCount) * cols
		lastAnalyzeDuration := GetTableLastAnalyzeDuration(tblStats, currentTs)
		totalLastAnalyzeDuration += lastAnalyzeDuration
		partitionNames = append(partitionNames, pIDAndName.Name)
		count++
	}
	if len(partitionNames) == 0 {
		return 0, 0, 0, partitionNames
	}

	avgChange = totalChangePercent / count
	avgSize = totalSize / count
	avgLastAnalyzeDuration = totalLastAnalyzeDuration / time.Duration(count)

	return avgChange, avgSize, avgLastAnalyzeDuration, partitionNames
}

// CheckNewlyAddedIndexesNeedAnalyzeForPartitionedTable checks if the indexes of the partitioned table need to be analyzed.
// It returns a map from index name to the names of the partitions that need to be analyzed.
// NOTE: This is only for newly added indexes.
func CheckNewlyAddedIndexesNeedAnalyzeForPartitionedTable(
	tblInfo *model.TableInfo,
	partitionStats map[PartitionIDAndName]*statistics.Table,
) map[string][]string {
	partitionIndexes := make(map[string][]string, len(tblInfo.Indices))

	for _, idx := range tblInfo.Indices {
		// No need to analyze the index if it's not public.
		if idx.State != model.StatePublic {
			continue
		}

		// Find all the partitions that need to analyze this index.
		names := make([]string, 0, len(partitionStats))
		for pIDAndName, tblStats := range partitionStats {
			if _, ok := tblStats.Indices[idx.ID]; !ok && !tblStats.ColAndIdxExistenceMap.HasAnalyzed(idx.ID, true) {
				names = append(names, pIDAndName.Name)
			}
		}

		if len(names) > 0 {
			partitionIndexes[idx.Name.O] = names
		}
	}

	return partitionIndexes
}

func getStartTs(sctx sessionctx.Context) (uint64, error) {
	txn, err := sctx.Txn(true)
	if err != nil {
		return 0, err
	}
	return txn.StartTS(), nil
}

// PartitionIDAndName is a struct that contains the ID and name of a partition.
// Exported for testing purposes. Do not use it in other packages.
type PartitionIDAndName struct {
	Name string
	ID   int64
}

func getPartitionStats(
	statsHandle statstypes.StatsHandle,
	tblInfo *model.TableInfo,
	defs []model.PartitionDefinition,
) map[PartitionIDAndName]*statistics.Table {
	partitionStats := make(map[PartitionIDAndName]*statistics.Table, len(defs))

	for _, def := range defs {
		stats := statsHandle.GetPartitionStatsForAutoAnalyze(tblInfo, def.ID)
		// Ignore the partition if it's not ready to analyze.
		if !isEligibleForAnalysis(stats) {
			continue
		}
		d := PartitionIDAndName{
			ID:   def.ID,
			Name: def.Name.O,
		}
		partitionStats[d] = stats
	}

	return partitionStats
}

func isEligibleForAnalysis(
	tblStats *statistics.Table,
) bool {
	// 1. If the statistics are either not loaded or are classified as pseudo, there is no need for analyze.
	//	  Pseudo statistics can be created by the optimizer, so we need to double check it.
	// 2. If the table is too small, we don't want to waste time to analyze it.
	//    Leave the opportunity to other bigger tables.
	if tblStats == nil || tblStats.Pseudo || tblStats.RealtimeCount < exec.AutoAnalyzeMinCnt {
		return false
	}

	return true
}

// autoAnalysisTimeWindow is a struct that contains the start and end time of the auto analyze time window.
type autoAnalysisTimeWindow struct {
	start time.Time
	end   time.Time
}

// isWithinTimeWindow checks if the current time is within the time window.
// If the auto analyze time window is not set or the current time is not in the window, return false.
func (a autoAnalysisTimeWindow) isWithinTimeWindow(currentTime time.Time) bool {
	if a.start == (time.Time{}) || a.end == (time.Time{}) {
		return false
	}
	return timeutil.WithinDayTimePeriod(a.start, a.end, currentTime)
}
