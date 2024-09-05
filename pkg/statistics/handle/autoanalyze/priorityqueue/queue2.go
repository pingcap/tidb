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

package priorityqueue

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/pingcap/tidb/pkg/ddl/notifier"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/statistics"
	"github.com/pingcap/tidb/pkg/statistics/handle/autoanalyze/exec"
	"github.com/pingcap/tidb/pkg/statistics/handle/autoanalyze/internal/heap"
	statslogutil "github.com/pingcap/tidb/pkg/statistics/handle/logutil"
	statstypes "github.com/pingcap/tidb/pkg/statistics/handle/types"
	statsutil "github.com/pingcap/tidb/pkg/statistics/handle/util"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

// AnalysisPriorityQueueV2 is a priority queue for TableAnalysisJobs.
type AnalysisPriorityQueueV2 struct {
	inner                  *heap.Heap[int64, AnalysisJob]
	statsHandle            statstypes.StatsHandle
	calculator             *PriorityCalculator
	autoAnalysisTimeWindow atomic.Value // stores *autoAnalysisTimeWindow

	ctx    context.Context
	cancel context.CancelFunc
	wg     util.WaitGroupWrapper
	// lastDMLUpdateFetchTimestamp is the timestamp of the last DML update fetch.
	lastDMLUpdateFetchTimestamp atomic.Uint64

	// initialized is a flag to check if the queue is initialized.
	initialized atomic.Bool
}

// NewAnalysisPriorityQueue2 creates a new AnalysisPriorityQueue2.
func NewAnalysisPriorityQueue2(handle statstypes.StatsHandle) *AnalysisPriorityQueueV2 {
	ctx, cancel := context.WithCancel(context.Background())

	return &AnalysisPriorityQueueV2{
		statsHandle: handle,
		calculator:  NewPriorityCalculator(),
		ctx:         ctx,
		cancel:      cancel,
	}
}

// IsInitialized checks if the priority queue is initialized.
func (pq *AnalysisPriorityQueueV2) IsInitialized() bool {
	return pq.initialized.Load()
}

// Initialize initializes the priority queue.
func (pq *AnalysisPriorityQueueV2) Initialize() error {
	start := time.Now()
	defer func() {
		statslogutil.StatsLogger().Info("priority queue initialized", zap.Duration("duration", time.Since(start)))
	}()
	if pq.initialized.Load() {
		return nil
	}

	keyFunc := func(job AnalysisJob) (int64, error) {
		return job.GetTableID(), nil
	}
	lessFunc := func(a, b AnalysisJob) bool {
		return a.GetWeight() > b.GetWeight()
	}

	pq.inner = heap.NewHeap(keyFunc, lessFunc)

	if err := pq.init(); err != nil {
		pq.Close()
		return err
	}

	pq.wg.Run(pq.run)
	pq.initialized.Store(true)
	return nil
}

func (pq *AnalysisPriorityQueueV2) init() error {
	return statsutil.CallWithSCtx(pq.statsHandle.SPool(), func(sctx sessionctx.Context) error {
		parameters := exec.GetAutoAnalyzeParameters(sctx)
		err := pq.setAutoAnalysisTimeWindow(parameters)
		if err != nil {
			return err
		}
		if !pq.IsWithinTimeWindow() {
			return nil
		}
		timeWindow := pq.autoAnalysisTimeWindow.Load().(*AutoAnalysisTimeWindow)

		return FetchAllTablesAndBuildAnalysisJobs(sctx, parameters, *timeWindow, pq.statsHandle, pq.Push)
	}, statsutil.FlagWrapTxn)
}

func (pq *AnalysisPriorityQueueV2) run() {
	dmlFetchInterval := time.NewTicker(time.Minute * 1)
	defer dmlFetchInterval.Stop()
	timeRefreshInterval := time.NewTicker(time.Minute * 10)
	defer timeRefreshInterval.Stop()

	for {
		select {
		case <-pq.ctx.Done():
			return
		case <-dmlFetchInterval.C:
			statslogutil.StatsLogger().Info("fetching dml update")
			pq.fetchDMLUpdate()
		case <-timeRefreshInterval.C:
			statslogutil.StatsLogger().Info("refreshing time")
			pq.refreshTime()
		}
	}
}

func (pq *AnalysisPriorityQueueV2) fetchDMLUpdate() {
	if err := statsutil.CallWithSCtx(pq.statsHandle.SPool(), func(sctx sessionctx.Context) error {
		parameters := exec.GetAutoAnalyzeParameters(sctx)
		if err := pq.setAutoAnalysisTimeWindow(parameters); err != nil {
			return err
		}
		if !pq.IsWithinTimeWindow() {
			return nil
		}
		start := time.Now()
		defer func() {
			statslogutil.StatsLogger().Info("dml update processed", zap.Duration("duration", time.Since(start)))
		}()
		values := pq.statsHandle.Values()
		lastFetchTimestamp := pq.lastDMLUpdateFetchTimestamp.Load()
		var newMaxVersion uint64

		for _, value := range values {
			if value.Version > lastFetchTimestamp {
				// Handle the table stats
				pq.processTableStats(value)
			}
			newMaxVersion = max(newMaxVersion, value.Version)
		}

		// Only update if we've seen a newer version
		if newMaxVersion > lastFetchTimestamp {
			statslogutil.StatsLogger().Info("updating last fetch timestamp", zap.Uint64("new_max_version", newMaxVersion))
			pq.lastDMLUpdateFetchTimestamp.Store(newMaxVersion)
		}
		return nil
	}); err != nil {
		statslogutil.StatsLogger().Error("failed to fetch dml update", zap.Error(err))
	}
}

func (pq *AnalysisPriorityQueueV2) processTableStats(stats *statistics.Table) {
	if !stats.IsEligibleForAnalysis() {
		return
	}

	if err := statsutil.CallWithSCtx(pq.statsHandle.SPool(), func(sctx sessionctx.Context) error {
		parameters := exec.GetAutoAnalyzeParameters(sctx)
		autoAnalyzeRatio := exec.ParseAutoAnalyzeRatio(parameters[variable.TiDBAutoAnalyzeRatio])
		// Get current timestamp from the session context.
		currentTs, err := getStartTs(sctx)
		if err != nil {
			return err
		}
		jobFactory := NewAnalysisJobFactory(sctx, autoAnalyzeRatio, currentTs)
		changePercent := jobFactory.CalculateChangePercentage(stats)
		if changePercent == 0 {
			return nil
		}

		var job AnalysisJob
		pruneMode := variable.PartitionPruneMode(sctx.GetSessionVars().PartitionPruneMode.Load())

		is := sctx.GetDomainInfoSchema().(infoschema.InfoSchema)

		job, ok, _ := pq.inner.GetByKey(stats.PhysicalID)
		if !ok {
			job = pq.tryCreateJob(is, stats, pruneMode, jobFactory)
		} else {
			job = pq.tryUpdateJob(is, stats, job, jobFactory)
		}

		job.SetWeight(pq.calculator.CalculateWeight(job))
		return pq.inner.Add(job)
	}, statsutil.FlagWrapTxn); err != nil {
		statslogutil.StatsLogger().Error("failed to create table analysis job", zap.Error(err))
	}
}

func (pq *AnalysisPriorityQueueV2) tryCreateJob(
	is infoschema.InfoSchema,
	stats *statistics.Table,
	pruneMode variable.PartitionPruneMode,
	jobFactory *AnalysisJobFactory,
) (job AnalysisJob) {
	tableInfo, ok := pq.statsHandle.TableInfoByID(is, stats.PhysicalID)
	tableMeta := tableInfo.Meta()
	if !ok {
		statslogutil.StatsLogger().Warn("table info not found for table id", zap.Int64("table_id", stats.PhysicalID))
		return nil
	}
	schemaName, ok := is.SchemaNameByTableID(tableMeta.ID)
	if !ok {
		statslogutil.StatsLogger().Warn("schema name not found for table id", zap.Int64("table_id", stats.PhysicalID))
		return nil
	}
	partitionedTable := tableMeta.GetPartitionInfo()
	if partitionedTable == nil {
		job = jobFactory.CreateNonPartitionedTableAnalysisJob(
			schemaName.O,
			tableMeta,
			stats,
		)
	} else {
		partitionDefs := partitionedTable.Definitions
		if pruneMode == variable.Static {
			var partitionDef model.PartitionDefinition
			for _, def := range partitionDefs {
				if def.ID == stats.PhysicalID {
					partitionDef = def
					break
				}
			}
			job = jobFactory.CreateStaticPartitionAnalysisJob(
				schemaName.O,
				tableMeta,
				partitionDef.ID,
				partitionDef.Name.O,
				stats,
			)
		} else {
			partitionStats := GetPartitionStats(pq.statsHandle, tableMeta, partitionDefs)
			job = jobFactory.CreateDynamicPartitionedTableAnalysisJob(
				schemaName.O,
				tableMeta,
				stats,
				partitionStats,
			)
		}
	}
	return job
}

func (pq *AnalysisPriorityQueueV2) tryUpdateJob(
	is infoschema.InfoSchema,
	stats *statistics.Table,
	oldJob AnalysisJob,
	jobFactory *AnalysisJobFactory,
) (newJob AnalysisJob) {
	indicators := oldJob.GetIndicators()
	// For dynamic partitioned table, there is no way to only update the partition that has been changed.
	if IsDynamicPartitionedTableAnalysisJob(oldJob) {
		tableInfo, ok := pq.statsHandle.TableInfoByID(is, stats.PhysicalID)
		tableMeta := tableInfo.Meta()
		if !ok {
			statslogutil.StatsLogger().Warn("table info not found for table id", zap.Int64("table_id", stats.PhysicalID))
			return
		}
		partitionedTable := tableMeta.GetPartitionInfo()
		partitionDefs := partitionedTable.Definitions
		partitionStats := GetPartitionStats(pq.statsHandle, tableMeta, partitionDefs)
		schemaName, ok := is.SchemaNameByTableID(tableMeta.ID)
		if !ok {
			statslogutil.StatsLogger().Warn("schema not found for table id", zap.Int64("table_id", stats.PhysicalID))
			return
		}
		return jobFactory.CreateDynamicPartitionedTableAnalysisJob(
			schemaName.O,
			tableMeta,
			stats,
			partitionStats,
		)
	}
	indicators.ChangePercentage = jobFactory.CalculateChangePercentage(stats)
	indicators.TableSize = jobFactory.CalculateTableSize(stats)
	oldJob.SetIndicators(indicators)
	return oldJob
}

func (pq *AnalysisPriorityQueueV2) refreshTime() {
	if !pq.IsWithinTimeWindow() {
		return
	}
	start := time.Now()
	defer func() {
		statslogutil.StatsLogger().Info("time refreshed", zap.Duration("duration", time.Since(start)))
	}()
	if err := statsutil.CallWithSCtx(pq.statsHandle.SPool(), func(sctx sessionctx.Context) error {
		jobs := pq.inner.List()
		for _, job := range jobs {
			indicators := job.GetIndicators()
			currentTs, err := getStartTs(sctx)
			if err != nil {
				return err
			}

			jobFactory := NewAnalysisJobFactory(sctx, 0, currentTs)
			tableStats, ok := pq.statsHandle.Get(job.GetTableID())
			if !ok {
				// TODO: Handle this case.
				continue
			}
			indicators.LastAnalysisDuration = jobFactory.GetTableLastAnalyzeDuration(tableStats)
			job.SetIndicators(indicators)
			job.SetWeight(pq.calculator.CalculateWeight(job))
			if err := pq.inner.Add(job); err != nil {
				statslogutil.StatsLogger().Error("failed to add job to priority queue", zap.Error(err))
			}
		}
		return nil
	}, statsutil.FlagWrapTxn); err != nil {
		statslogutil.StatsLogger().Error("failed to refresh time", zap.Error(err))
	}
}

func (*AnalysisPriorityQueueV2) handleDDLEvent(_ context.Context, _ sessionctx.Context, _ notifier.SchemaChangeEvent) {
	// TODO: Handle the ddl event.
	// Only care about the add index event.
}

// Push pushes a job into the priority queue.
func (pq *AnalysisPriorityQueueV2) Push(job AnalysisJob) error {
	return pq.inner.Add(job)
}

// Pop pops a job from the priority queue.
func (pq *AnalysisPriorityQueueV2) Pop() (AnalysisJob, error) {
	return pq.inner.Pop()
}

// IsEmpty checks whether the priority queue is empty.
func (pq *AnalysisPriorityQueueV2) IsEmpty() bool {
	return pq.inner.IsEmpty()
}

// Close closes the priority queue.
func (pq *AnalysisPriorityQueueV2) Close() {
	pq.cancel()
	pq.wg.Wait()
	pq.inner.Close()
}

func (pq *AnalysisPriorityQueueV2) setAutoAnalysisTimeWindow(
	parameters map[string]string,
) error {
	start, end, err := exec.ParseAutoAnalysisWindow(
		parameters[variable.TiDBAutoAnalyzeStartTime],
		parameters[variable.TiDBAutoAnalyzeEndTime],
	)
	if err != nil {
		return errors.Wrap(err, "parse auto analyze period failed")
	}
	timeWindow := NewAutoAnalysisTimeWindow(start, end)
	pq.autoAnalysisTimeWindow.Store(&timeWindow)
	return nil
}

// IsWithinTimeWindow checks if the current time is within the auto analyze time window.
func (pq *AnalysisPriorityQueueV2) IsWithinTimeWindow() bool {
	window := pq.autoAnalysisTimeWindow.Load().(*AutoAnalysisTimeWindow)
	return window.IsWithinTimeWindow(time.Now())
}
