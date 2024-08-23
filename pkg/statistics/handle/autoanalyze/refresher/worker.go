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
	"context"
	"sync"

	"github.com/pingcap/tidb/pkg/sessionctx/sysproctrack"
	"github.com/pingcap/tidb/pkg/statistics/handle/autoanalyze/priorityqueue"
	statslogutil "github.com/pingcap/tidb/pkg/statistics/handle/logutil"
	statstypes "github.com/pingcap/tidb/pkg/statistics/handle/types"
	"go.uber.org/zap"
)

type worker struct {
	statsHandle    statstypes.StatsHandle
	sysProcTracker sysproctrack.Tracker
	wg             sync.WaitGroup
	jobChan        chan priorityqueue.AnalysisJob
	ctx            context.Context
	cancel         context.CancelFunc
	runningJobs    map[int64]struct{}
	runningJobsMu  sync.Mutex
	maxConcurrency int
}

func newWorker(statsHandle statstypes.StatsHandle, sysProcTracker sysproctrack.Tracker, maxConcurrency int) *worker {
	ctx, cancel := context.WithCancel(context.Background())
	w := &worker{
		statsHandle:    statsHandle,
		sysProcTracker: sysProcTracker,
		jobChan:        make(chan priorityqueue.AnalysisJob, maxConcurrency),
		ctx:            ctx,
		cancel:         cancel,
		runningJobs:    make(map[int64]struct{}),
		maxConcurrency: maxConcurrency,
	}
	w.wg.Add(1)
	go w.run()
	return w
}

// UpdateConcurrency updates the maximum concurrency for the worker
func (w *worker) UpdateConcurrency(newConcurrency int) {
	w.runningJobsMu.Lock()
	defer w.runningJobsMu.Unlock()

	if newConcurrency == w.maxConcurrency {
		return
	}

	// Create a new job channel with the updated capacity
	newJobChan := make(chan priorityqueue.AnalysisJob, newConcurrency)

	// Move existing jobs to the new channel
	close(w.jobChan)
	for job := range w.jobChan {
		newJobChan <- job
	}

	w.jobChan = newJobChan
	w.maxConcurrency = newConcurrency
}

func (w *worker) run() {
	defer w.wg.Done()
	for {
		select {
		case <-w.ctx.Done():
			return
		case job := <-w.jobChan:
			w.runningJobsMu.Lock()
			w.runningJobs[job.GetTableID()] = struct{}{}
			w.runningJobsMu.Unlock()

			if err := job.Analyze(w.statsHandle, w.sysProcTracker); err != nil {
				statslogutil.StatsLogger().Error(
					"Auto analyze job execution failed",
					zap.Stringer("job", job),
					zap.Error(err),
				)
			}

			w.runningJobsMu.Lock()
			delete(w.runningJobs, job.GetTableID())
			w.runningJobsMu.Unlock()
		}
	}
}

func (w *worker) SubmitJob(job priorityqueue.AnalysisJob) bool {
	w.runningJobsMu.Lock()
	defer w.runningJobsMu.Unlock()

	if len(w.runningJobs) >= w.maxConcurrency {
		statslogutil.StatsLogger().Warn("Worker at maximum capacity, job discarded", zap.Stringer("job", job))
		return false
	}

	select {
	case w.jobChan <- job:
		return true
	default:
		statslogutil.StatsLogger().Warn("Worker job channel is full, job discarded", zap.Stringer("job", job))
		return false
	}
}

func (w *worker) GetRunningJobs() map[int64]struct{} {
	w.runningJobsMu.Lock()
	defer w.runningJobsMu.Unlock()
	runningJobs := make(map[int64]struct{}, len(w.runningJobs))
	for id := range w.runningJobs {
		runningJobs[id] = struct{}{}
	}
	return runningJobs
}

func (w *worker) Stop() {
	w.cancel()
	close(w.jobChan)
	w.wg.Wait()
}
