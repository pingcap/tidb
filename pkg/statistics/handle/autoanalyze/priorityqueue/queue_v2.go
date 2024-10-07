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

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/ddl/notifier"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/statistics/handle/autoanalyze/exec"
	"github.com/pingcap/tidb/pkg/statistics/handle/autoanalyze/internal/heap"
	statslogutil "github.com/pingcap/tidb/pkg/statistics/handle/logutil"
	statstypes "github.com/pingcap/tidb/pkg/statistics/handle/types"
	statsutil "github.com/pingcap/tidb/pkg/statistics/handle/util"
	"github.com/pingcap/tidb/pkg/util"
	"go.uber.org/zap"
)

const (
	lastAnalysisDurationRefreshInterval = time.Minute * 10
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

	// initialized is a flag to check if the queue is initialized.
	initialized atomic.Bool
}

// NewAnalysisPriorityQueueV2 creates a new AnalysisPriorityQueue2.
func NewAnalysisPriorityQueueV2(handle statstypes.StatsHandle) *AnalysisPriorityQueueV2 {
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
	if pq.initialized.Load() {
		statslogutil.StatsLogger().Warn("Priority queue already initialized")
		return nil
	}

	start := time.Now()
	defer func() {
		statslogutil.StatsLogger().Info("Priority queue initialized", zap.Duration("duration", time.Since(start)))
	}()

	keyFunc := func(job AnalysisJob) (int64, error) {
		return job.GetTableID(), nil
	}
	// We want the job with the highest weight to be at the top of the priority queue.
	lessFunc := func(a, b AnalysisJob) bool {
		return a.GetWeight() > b.GetWeight()
	}
	pq.inner = heap.NewHeap(keyFunc, lessFunc)
	if err := pq.init(); err != nil {
		pq.Close()
		return errors.Trace(err)
	}

	// Start a goroutine to maintain the priority queue.
	pq.wg.Run(pq.run)
	pq.initialized.Store(true)
	return nil
}

// init initializes the priority queue and adds jobs to it.
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

		err = FetchAllTablesAndBuildAnalysisJobs(
			sctx,
			parameters,
			*timeWindow,
			pq.statsHandle,
			pq.Push,
		)
		if err != nil {
			return errors.Trace(err)
		}
		return nil
	}, statsutil.FlagWrapTxn)
}

// run maintains the priority queue.
func (pq *AnalysisPriorityQueueV2) run() {
	defer func() {
		if r := recover(); r != nil {
			statslogutil.StatsLogger().Error("Priority queue panicked", zap.Any("recover", r), zap.Stack("stack"))
		}
	}()

	timeRefreshInterval := time.NewTicker(lastAnalysisDurationRefreshInterval)
	defer timeRefreshInterval.Stop()

	for {
		select {
		case <-pq.ctx.Done():
			statslogutil.StatsLogger().Info("Priority queue stopped")
			return
		case <-timeRefreshInterval.C:
			statslogutil.StatsLogger().Info("Start to refresh last analysis durations of jobs")
			pq.RefreshLastAnalysisDuration()
		}
	}
}

// RefreshLastAnalysisDuration refreshes the last analysis duration of all jobs in the priority queue.
func (pq *AnalysisPriorityQueueV2) RefreshLastAnalysisDuration() {
	if err := statsutil.CallWithSCtx(pq.statsHandle.SPool(), func(sctx sessionctx.Context) error {
		parameters := exec.GetAutoAnalyzeParameters(sctx)
		if err := pq.setAutoAnalysisTimeWindow(parameters); err != nil {
			return errors.Trace(err)
		}
		if !pq.IsWithinTimeWindow() {
			statslogutil.StatsLogger().Debug("Not within the auto analyze time window, skip refreshing last analysis duration")
			return nil
		}
		start := time.Now()
		defer func() {
			statslogutil.StatsLogger().Info("Last analysis duration refreshed", zap.Duration("duration", time.Since(start)))
		}()
		jobs := pq.inner.List()
		for _, job := range jobs {
			indicators := job.GetIndicators()
			currentTs, err := getStartTs(sctx)
			if err != nil {
				return errors.Trace(err)
			}

			jobFactory := NewAnalysisJobFactory(sctx, 0, currentTs)
			tableStats, ok := pq.statsHandle.Get(job.GetTableID())
			if !ok {
				statslogutil.StatsLogger().Warn("Table stats not found during refreshing last analysis duration",
					zap.Int64("tableID", job.GetTableID()),
					zap.String("job", job.String()),
				)
				err := pq.inner.Delete(job)
				if err != nil {
					statslogutil.StatsLogger().Error("Failed to delete job from priority queue",
						zap.Error(err),
						zap.String("job", job.String()),
					)
				}
			}
			indicators.LastAnalysisDuration = jobFactory.GetTableLastAnalyzeDuration(tableStats)
			job.SetIndicators(indicators)
			job.SetWeight(pq.calculator.CalculateWeight(job))
			if err := pq.inner.Update(job); err != nil {
				statslogutil.StatsLogger().Error("Failed to add job to priority queue",
					zap.Error(err),
					zap.String("job", job.String()),
				)
			}
		}
		return nil
	}, statsutil.FlagWrapTxn); err != nil {
		statslogutil.StatsLogger().Error("Failed to refresh last analysis duration", zap.Error(err))
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

// Close closes the priority queue.
func (pq *AnalysisPriorityQueueV2) Close() {
	if !pq.initialized.Load() {
		return
	}

	pq.cancel()
	pq.wg.Wait()
	pq.inner.Close()
}
