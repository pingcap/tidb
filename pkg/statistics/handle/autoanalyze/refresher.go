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
	"container/heap"
	"strings"
	"sync"
	"time"

	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/statistics"
	"github.com/pingcap/tidb/pkg/statistics/handle/autoanalyze/analyzequeue"
	"github.com/pingcap/tidb/pkg/statistics/handle/lockstats"
	statslogutil "github.com/pingcap/tidb/pkg/statistics/handle/logutil"
	statstypes "github.com/pingcap/tidb/pkg/statistics/handle/types"
	statsutil "github.com/pingcap/tidb/pkg/statistics/handle/util"
	"github.com/pingcap/tidb/pkg/util"
	"go.uber.org/zap"
)

const (
	// Refresh the jobs queue every 10 minutes.
	refreshInterval = 10 * time.Minute
)

// Refresher represents a struct with a goroutine that periodically rebuilds the jobs queue.
type Refresher struct {
	statsHandle    statstypes.StatsHandle
	sysProcTracker sessionctx.SysProcTracker

	ticker *time.Ticker
	stop   chan struct{}
	jobs   *analyzequeue.AnalysisQueue
	// Used to protect the jobs queue.
	mu sync.RWMutex
}

// NewRefresher creates a new Refresher and starts the goroutine.
func NewRefresher(
	statsHandle statstypes.StatsHandle,
	sysProcTracker sessionctx.SysProcTracker,
) (*Refresher, error) {
	r := &Refresher{
		stop:           make(chan struct{}),
		ticker:         time.NewTicker(refreshInterval),
		statsHandle:    statsHandle,
		sysProcTracker: sysProcTracker,
	}

	// Initialize the jobs queue.
	if err := r.buildTableAnalysisJobQueue(); err != nil {
		return nil, err
	}

	go r.runRefresher()

	return r, nil
}

// runRefresher is the goroutine function that periodically rebuilds the jobs queue.
func (r *Refresher) runRefresher() {
	for {
		select {
		case <-r.ticker.C:
			err := r.buildTableAnalysisJobQueue()
			if err != nil {
				statslogutil.StatsLogger().Error(
					"build table analysis job queue failed",
					zap.Error(err),
				)
			}
		case <-r.stop:
			return
		}
	}
}

// Stop stops the refresher goroutine.
func (r *Refresher) Stop() {
	r.ticker.Stop()
	close(r.stop)
}

//nolint:unused
func (r *Refresher) pickOneTableForAnalysisByPriority() {
	r.mu.RLock()
	// Pick the table with the highest weight.
	for r.jobs.Len() > 0 {
		job := heap.Pop(r.jobs).(*analyzequeue.TableAnalysisJob)
		se, err := r.statsHandle.SPool().Get()
		if err != nil {
			statslogutil.StatsLogger().Error(
				"get session context failed",
				zap.Error(err),
			)
			continue
		}
		sctx := se.(sessionctx.Context)
		if !job.IsValidToAnalyze(
			sctx,
		) {
			statslogutil.StatsLogger().Info(
				"table is not ready to analyze",
				zap.Any("job", job),
			)
		}
		statslogutil.StatsLogger().Info(
			"auto analyze triggered",
			zap.Any("job", job),
		)
		err = job.Execute(
			r.statsHandle,
			r.sysProcTracker,
		)
		if err != nil {
			statslogutil.StatsLogger().Error(
				"execute auto analyze job failed",
				zap.Error(err),
			)
		}
	}
	r.mu.RUnlock()
}

func (r *Refresher) buildTableAnalysisJobQueue() error {
	pq := make(analyzequeue.AnalysisQueue, 0)
	heap.Init(&pq)

	err := statsutil.CallWithSCtx(
		r.statsHandle.SPool(),
		func(sctx sessionctx.Context) error {
			parameters := getAutoAnalyzeParameters(sctx)
			autoAnalyzeRatio := parseAutoAnalyzeRatio(parameters[variable.TiDBAutoAnalyzeRatio])
			// Instantiate a weight calculator.
			calculator := analyzequeue.NewPriorityCalculator(autoAnalyzeRatio)
			pruneMode := variable.PartitionPruneMode(sctx.GetSessionVars().PartitionPruneMode.Load())
			is := sctx.GetDomainInfoSchema().(infoschema.InfoSchema)

			// Query locked tables once to minimize overhead.
			// Outdated lock info is acceptable as we verify table lock status pre-analysis.
			lockedTables, err := lockstats.QueryLockedTables(sctx)
			if err != nil {
				return err
			}

			dbs := is.AllSchemaNames()
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
					pushJob := func(job *analyzequeue.TableAnalysisJob) {
						if job == nil {
							return
						}
						// Calculate the weight of the job.
						job.Weight = calculator.CalculateWeight(job)
						if job.Weight == 0 {
							return
						}
						// Push the job onto the queue.
						heap.Push(&pq, job)
					}
					// No partitions or prune mode is static, analyze the whole table.
					if pi == nil {
						job := createTableAnalysisJob(
							sctx,
							db,
							tblInfo,
							r.statsHandle.GetPartitionStats(tblInfo, tblInfo.ID),
							autoAnalyzeRatio,
						)
						pushJob(job)
					} else if pruneMode == variable.Static {
						// Only analyze the partition that has not been locked.
						partitionDefs := make([]model.PartitionDefinition, 0, len(pi.Definitions))
						for _, def := range pi.Definitions {
							if _, ok := lockedTables[def.ID]; !ok {
								partitionDefs = append(partitionDefs, def)
							}
						}
						partitionStats := getPartitionStats(r.statsHandle, tblInfo, partitionDefs)
						for _, def := range pi.Definitions {
							job := createTableAnalysisJob(
								sctx,
								db,
								tblInfo,
								partitionStats[def.ID],
								autoAnalyzeRatio,
							)
							pushJob(job)
						}
					} else {
						// Only analyze the partition that has not been locked.
						partitionDefs := make([]model.PartitionDefinition, 0, len(pi.Definitions))
						for _, def := range pi.Definitions {
							if _, ok := lockedTables[def.ID]; !ok {
								partitionDefs = append(partitionDefs, def)
							}
						}
						partitionStats := getPartitionStats(r.statsHandle, tblInfo, partitionDefs)
						job := createTableAnalysisJobForPartitionedTable(
							sctx,
							r.statsHandle,
							db,
							tblInfo,
							partitionDefs,
							partitionStats,
							autoAnalyzeRatio,
						)
						pushJob(job)
					}
				}
			}

			return nil
		},
	)
	if err != nil {
		return err
	}

	// Update the jobs queue.
	r.mu.Lock()
	r.jobs = &pq
	r.mu.Unlock()

	return nil
}

func createTableAnalysisJob(
	sctx sessionctx.Context,
	dbName string,
	tblInfo *model.TableInfo,
	tblStats *statistics.Table,
	autoAnalyzeRatio float64,
) *analyzequeue.TableAnalysisJob {
	tableStatsVer := sctx.GetSessionVars().AnalyzeVersion
	statistics.CheckAnalyzeVerOnTable(tblStats, &tableStatsVer)

	changePercentage := calculateChangePercentage(tblStats, autoAnalyzeRatio)
	indexes := checkIndexesNeedAnalyze(tblInfo, tblStats)

	job := &analyzequeue.TableAnalysisJob{
		TableID:       tblInfo.ID,
		DBName:        dbName,
		TableName:     tblInfo.Name.O,
		TableStatsVer: tableStatsVer,

		ChangePercentage: changePercentage,
		Indexes:          indexes,
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
	if tblStats.Pseudo || tblStats.RealtimeCount < AutoAnalyzeMinCnt {
		return 0
	}

	if !TableAnalyzed(tblStats) {
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

func checkIndexesNeedAnalyze(
	tblInfo *model.TableInfo,
	tblStats *statistics.Table,
) []string {
	// If table is not analyzed, we need to analyze whole table.
	// So we don't need to check indexes.
	if !TableAnalyzed(tblStats) {
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

func createTableAnalysisJobForPartitionedTable(
	sctx sessionctx.Context,
	statsHandle statstypes.StatsHandle,
	dbName string,
	tblInfo *model.TableInfo,
	defs []model.PartitionDefinition,
	partitionStats map[int64]*statistics.Table,
	autoAnalyzeRatio float64,
) *analyzequeue.TableAnalysisJob {
	tableStatsVer := sctx.GetSessionVars().AnalyzeVersion
	// TODO: this is different from the original implementation.
	tblStats := statsHandle.GetTableStats(tblInfo)
	statistics.CheckAnalyzeVerOnTable(tblStats, &tableStatsVer)

	averageChangePercentage, partitionNames := calculateAvgChangeForPartitions(
		partitionStats,
		defs,
		autoAnalyzeRatio,
	)
	partitionIndexes := checkIndexesNeedAnalyzeForPartitionedTable(
		tblInfo,
		defs,
		partitionStats,
	)

	job := &analyzequeue.TableAnalysisJob{
		TableID:       tblInfo.ID,
		DBName:        dbName,
		TableName:     tblInfo.Name.O,
		TableStatsVer: tableStatsVer,
		Partitions:    partitionNames,

		ChangePercentage: averageChangePercentage,
		PartitionIndexes: partitionIndexes,
	}

	return job
}

func calculateAvgChangeForPartitions(
	partitionStats map[int64]*statistics.Table,
	defs []model.PartitionDefinition,
	autoAnalyzeRatio float64,
) (float64, []string) {
	var totalChangePercent float64
	var count int
	partitionNames := make([]string, 0, len(defs))

	for _, def := range defs {
		tblStats := partitionStats[def.ID]
		changePercent := calculateChangePercentage(tblStats, autoAnalyzeRatio)
		if changePercent == 0 {
			continue
		}

		totalChangePercent += changePercent
		partitionNames = append(partitionNames, def.Name.O)
		count++
	}

	avgChange := totalChangePercent / float64(count)
	return avgChange, partitionNames
}

func checkIndexesNeedAnalyzeForPartitionedTable(
	tblInfo *model.TableInfo,
	defs []model.PartitionDefinition,
	partitionStats map[int64]*statistics.Table,
) map[string][]string {
	partitionIndexes := make(map[string][]string, len(tblInfo.Indices))

	for _, idx := range tblInfo.Indices {
		if idx.State != model.StatePublic {
			continue
		}

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

func getPartitionStats(
	statsHandle statstypes.StatsHandle,
	tblInfo *model.TableInfo,
	defs []model.PartitionDefinition,
) map[int64]*statistics.Table {
	partitionStats := make(map[int64]*statistics.Table, len(defs))

	for _, def := range defs {
		partitionStats[def.ID] = statsHandle.GetPartitionStats(tblInfo, def.ID)
	}

	return partitionStats
}
