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
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/statistics"
	"github.com/pingcap/tidb/pkg/statistics/handle/autoanalyze/exec"
	"github.com/pingcap/tidb/pkg/statistics/handle/autoanalyze/priorityqueue"
	"github.com/pingcap/tidb/pkg/statistics/handle/lockstats"
	statslogutil "github.com/pingcap/tidb/pkg/statistics/handle/logutil"
	statstypes "github.com/pingcap/tidb/pkg/statistics/handle/types"
	statsutil "github.com/pingcap/tidb/pkg/statistics/handle/util"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/tikv/client-go/v2/oracle"
	"go.uber.org/zap"
)

// Refresher provides methods to refresh stats info.
// NOTE: Refresher is not thread-safe.
type Refresher struct {
	statsHandle    statstypes.StatsHandle
	sysProcTracker sessionctx.SysProcTracker

	jobs *priorityqueue.AnalysisPriorityQueue
}

// NewRefresher creates a new Refresher and starts the goroutine.
func NewRefresher(
	statsHandle statstypes.StatsHandle,
	sysProcTracker sessionctx.SysProcTracker,
) (*Refresher, error) {
	r := &Refresher{
		statsHandle:    statsHandle,
		sysProcTracker: sysProcTracker,
		jobs:           priorityqueue.NewAnalysisPriorityQueue(),
	}

	return r, nil
}

func (r *Refresher) pickOneTableAndAnalyzeByPriority() {
	se, err := r.statsHandle.SPool().Get()
	if err != nil {
		statslogutil.StatsLogger().Error(
			"Get session context failed",
			zap.Error(err),
		)
		return
	}
	defer r.statsHandle.SPool().Put(se)
	sctx := se.(sessionctx.Context)
	// Pick the table with the highest weight.
	for r.jobs.Len() > 0 {
		job := r.jobs.Pop()
		if valid, failReason := job.IsValidToAnalyze(
			sctx,
		); !valid {
			statslogutil.StatsLogger().Info(
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
		err = job.Execute(
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
		return
	}
}

func (r *Refresher) rebuildTableAnalysisJobQueue() error {
	// Reset the priority queue.
	r.jobs = priorityqueue.NewAnalysisPriorityQueue()

	if err := statsutil.CallWithSCtx(
		r.statsHandle.SPool(),
		func(sctx sessionctx.Context) error {
			parameters := exec.GetAutoAnalyzeParameters(sctx)
			autoAnalyzeRatio := exec.ParseAutoAnalyzeRatio(parameters[variable.TiDBAutoAnalyzeRatio])
			calculator := priorityqueue.NewPriorityCalculator(autoAnalyzeRatio)
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
					pushJobFunc := func(job *priorityqueue.TableAnalysisJob) {
						if job == nil {
							return
						}
						// Calculate the weight of the job.
						job.Weight = calculator.CalculateWeight(job)
						if job.Weight == 0 {
							return
						}
						// Push the job onto the queue.
						r.jobs.Push(job)
					}
					// No partitions, analyze the whole table.
					if pi == nil {
						job := createTableAnalysisJob(
							sctx,
							db,
							tblInfo,
							r.statsHandle.GetPartitionStats(tblInfo, tblInfo.ID),
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
						for _, def := range pi.Definitions {
							job := createTableAnalysisJob(
								sctx,
								db,
								tblInfo,
								partitionStats[def.ID],
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
							r.statsHandle.GetPartitionStats(tblInfo, tblInfo.ID),
							partitionDefs,
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

func createTableAnalysisJob(
	sctx sessionctx.Context,
	tableSchema string,
	tblInfo *model.TableInfo,
	tblStats *statistics.Table,
	autoAnalyzeRatio float64,
	currentTs uint64,
) *priorityqueue.TableAnalysisJob {
	tableStatsVer := sctx.GetSessionVars().AnalyzeVersion
	statistics.CheckAnalyzeVerOnTable(tblStats, &tableStatsVer)

	changePercentage := calculateChangePercentage(tblStats, autoAnalyzeRatio)
	tableSize := calculateTableSize(tblInfo, tblStats)
	lastAnalysisDuration := getTableLastAnalyzeDuration(tblStats, currentTs)
	indexes := checkIndexesNeedAnalyze(tblInfo, tblStats)

	job := &priorityqueue.TableAnalysisJob{
		TableID:              tblInfo.ID,
		TableSchema:          tableSchema,
		TableName:            tblInfo.Name.O,
		TableStatsVer:        tableStatsVer,
		ChangePercentage:     changePercentage,
		TableSize:            tableSize,
		LastAnalysisDuration: lastAnalysisDuration,
		Indexes:              indexes,
	}

	return job
}

func calculateChangePercentage(
	tblStats *statistics.Table,
	autoAnalyzeRatio float64,
) float64 {
	// If the stats are not loaded, we don't need to analyze it.
	// If the table is too small, we don't want to waste time to analyze it.
	// Leave the opportunity to other bigger tables.
	if tblStats.Pseudo || tblStats.RealtimeCount < exec.AutoAnalyzeMinCnt {
		return 0
	}

	if !exec.TableAnalyzed(tblStats) {
		return 1
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

func getTableLastAnalyzeDuration(
	tblStats *statistics.Table,
	currentTs uint64,
) time.Duration {
	// Calculate the duration since last analyze.
	versionTs := tblStats.Version
	currentTime := oracle.GetTimeFromTS(currentTs)
	versionTime := oracle.GetTimeFromTS(versionTs)

	return time.Duration(currentTime.Sub(versionTime).Seconds())
}

func checkIndexesNeedAnalyze(
	tblInfo *model.TableInfo,
	tblStats *statistics.Table,
) []string {
	// If table is not analyzed, we need to analyze whole table.
	// So we don't need to check indexes.
	if !exec.TableAnalyzed(tblStats) {
		return nil
	}

	indexes := make([]string, 0, len(tblInfo.Indices))
	// Check if missing index stats.
	for _, idx := range tblInfo.Indices {
		if _, ok := tblStats.Indices[idx.ID]; !ok && idx.State == model.StatePublic {
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
	defs []model.PartitionDefinition,
	partitionStats map[int64]*statistics.Table,
	autoAnalyzeRatio float64,
	currentTs uint64,
) *priorityqueue.TableAnalysisJob {
	// TODO: figure out how to check the table stats version correctly for partitioned tables.
	tableStatsVer := sctx.GetSessionVars().AnalyzeVersion
	statistics.CheckAnalyzeVerOnTable(tblStats, &tableStatsVer)

	averageChangePercentage, avgSize, minLastAnalyzeDuration, partitionNames := calculateIndicatorsForPartitions(
		tblInfo,
		partitionStats,
		defs,
		autoAnalyzeRatio,
		currentTs,
	)
	partitionIndexes := checkNewlyAddedIndexesNeedAnalyzeForPartitionedTable(
		tblInfo,
		defs,
		partitionStats,
	)
	// No need to analyze.
	if len(partitionNames) == 0 && len(partitionIndexes) == 0 {
		return nil
	}

	job := &priorityqueue.TableAnalysisJob{
		TableID:              tblInfo.ID,
		TableSchema:          tableSchema,
		TableName:            tblInfo.Name.O,
		TableStatsVer:        tableStatsVer,
		ChangePercentage:     averageChangePercentage,
		TableSize:            avgSize,
		LastAnalysisDuration: minLastAnalyzeDuration,
		Partitions:           partitionNames,
		PartitionIndexes:     partitionIndexes,
	}

	return job
}

// calculateIndicatorsForPartitions calculates the average change percentage,
// average size and average last analyze duration for the partitions that meet the threshold.
// Change percentage is the ratio of the number of modified rows to the total number of rows.
// Size is the product of the number of rows and the number of columns.
// Last analyze duration is the duration since the last analyze.
func calculateIndicatorsForPartitions(
	tblInfo *model.TableInfo,
	partitionStats map[int64]*statistics.Table,
	defs []model.PartitionDefinition,
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
	partitionNames = make([]string, 0, len(defs))
	cols := float64(len(tblInfo.Columns))
	totalLastAnalyzeDuration := time.Duration(0)

	for _, def := range defs {
		tblStats := partitionStats[def.ID]
		changePercent := calculateChangePercentage(tblStats, autoAnalyzeRatio)
		// No need to analyze the partition because it doesn't meet the threshold or stats are not loaded yet.
		if changePercent == 0 {
			continue
		}

		totalChangePercent += changePercent
		// size = count * cols
		totalSize += float64(tblStats.RealtimeCount) * cols
		lastAnalyzeDuration := getTableLastAnalyzeDuration(tblStats, currentTs)
		totalLastAnalyzeDuration += lastAnalyzeDuration
		partitionNames = append(partitionNames, def.Name.O)
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

// checkNewlyAddedIndexesNeedAnalyzeForPartitionedTable checks if the indexes of the partitioned table need to be analyzed.
// It returns a map from index name to the names of the partitions that need to be analyzed.
// NOTE: This is only for newly added indexes.
func checkNewlyAddedIndexesNeedAnalyzeForPartitionedTable(
	tblInfo *model.TableInfo,
	defs []model.PartitionDefinition,
	partitionStats map[int64]*statistics.Table,
) map[string][]string {
	partitionIndexes := make(map[string][]string, len(tblInfo.Indices))

	for _, idx := range tblInfo.Indices {
		// No need to analyze the index if it's not public.
		if idx.State != model.StatePublic {
			continue
		}

		// Find all the partitions that need to analyze this index.
		names := make([]string, 0, len(defs))
		for _, def := range defs {
			tblStats := partitionStats[def.ID]
			if _, ok := tblStats.Indices[idx.ID]; !ok {
				names = append(names, def.Name.O)
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

func getPartitionStats(
	statsHandle statstypes.StatsHandle,
	tblInfo *model.TableInfo,
	defs []model.PartitionDefinition,
) map[int64]*statistics.Table {
	partitionStats := make(map[int64]*statistics.Table, len(defs))

	for _, def := range defs {
		// TODO: use GetPartitionStatsForAutoAnalyze to save memory.
		partitionStats[def.ID] = statsHandle.GetPartitionStats(tblInfo, def.ID)
	}

	return partitionStats
}
