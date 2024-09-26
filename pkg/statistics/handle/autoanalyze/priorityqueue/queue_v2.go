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
	if pq.initialized.Load() {
		statslogutil.StatsLogger().Warn("priority queue already initialized")
		return nil
	}

	start := time.Now()
	defer func() {
		statslogutil.StatsLogger().Info("priority queue initialized", zap.Duration("duration", time.Since(start)))
	}()

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

		// TODO: Add the jobs to the priority queue.
		return nil
	}, statsutil.FlagWrapTxn)
}

// run maintains the priority queue.
func (pq *AnalysisPriorityQueueV2) run() {
	timeRefreshInterval := time.NewTicker(lastAnalysisDurationRefreshInterval)
	defer timeRefreshInterval.Stop()

	for {
		select {
		case <-pq.ctx.Done():
			return
		case <-timeRefreshInterval.C:
			statslogutil.StatsLogger().Info("start to refresh last analysis durations of jobs")
			pq.refreshLastAnalysisDuration()
		}
	}
}

// refreshLastAnalysisDuration refreshes the last analysis duration of all jobs in the priority queue.
func (pq *AnalysisPriorityQueueV2) refreshLastAnalysisDuration() {
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
	pq.cancel()
	pq.wg.Wait()
	pq.inner.Close()
}

func getStartTs(sctx sessionctx.Context) (uint64, error) {
	txn, err := sctx.Txn(true)
	if err != nil {
		return 0, err
	}
	return txn.StartTS(), nil
}
