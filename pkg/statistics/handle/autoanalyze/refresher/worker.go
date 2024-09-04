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
	"sync"
	"time"

	"github.com/pingcap/tidb/pkg/sessionctx/sysproctrack"
	"github.com/pingcap/tidb/pkg/statistics/handle/autoanalyze/priorityqueue"
	statslogutil "github.com/pingcap/tidb/pkg/statistics/handle/logutil"
	statstypes "github.com/pingcap/tidb/pkg/statistics/handle/types"
	"github.com/pingcap/tidb/pkg/util"
	"go.uber.org/zap"
)

// worker manages the execution of analysis jobs.
// Fields are ordered to represent the mutex protection clearly.
//
//nolint:fieldalignment
type worker struct {
	statsHandle    statstypes.StatsHandle
	sysProcTracker sysproctrack.Tracker
	wg             util.WaitGroupWrapper

	mu sync.Mutex
	// mu is used to protect the following fields.
	runningJobs    map[int64]struct{}
	maxConcurrency int
}

// NewWorker creates a new worker.
func NewWorker(statsHandle statstypes.StatsHandle, sysProcTracker sysproctrack.Tracker, maxConcurrency int) *worker {
	w := &worker{
		statsHandle:    statsHandle,
		sysProcTracker: sysProcTracker,
		runningJobs:    make(map[int64]struct{}),
		maxConcurrency: maxConcurrency,
	}
	return w
}

// UpdateConcurrency updates the maximum concurrency for the worker
func (w *worker) UpdateConcurrency(newConcurrency int) {
	w.mu.Lock()
	defer w.mu.Unlock()
	statslogutil.StatsLogger().Info(
		"Update concurrency",
		zap.Int("newConcurrency", newConcurrency),
		zap.Int("oldConcurrency", w.maxConcurrency),
	)
	w.maxConcurrency = newConcurrency
}

// SubmitJob submits a job to the worker.
// It returns false if the job is not submitted due to concurrency limit.
func (w *worker) SubmitJob(job priorityqueue.AnalysisJob) bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	if len(w.runningJobs) >= w.maxConcurrency {
		statslogutil.StatsLogger().Warn("Worker at maximum capacity, job discarded", zap.Stringer("job", job))
		return false
	}
	w.runningJobs[job.GetTableID()] = struct{}{}

	w.wg.RunWithRecover(
		func() {
			w.processJob(job)
		},
		func(r any) {
			if r != nil {
				statslogutil.StatsLogger().Error("Auto analyze job execution failed", zap.Any("recover", r), zap.Stack("stack"))
			}
		},
	)
	statslogutil.StatsLogger().Info("Job submitted", zap.Stringer("job", job))
	return true
}

func (w *worker) processJob(job priorityqueue.AnalysisJob) {
	defer func() {
		w.mu.Lock()
		defer w.mu.Unlock()
		delete(w.runningJobs, job.GetTableID())
	}()

	if err := job.Analyze(w.statsHandle, w.sysProcTracker); err != nil {
		statslogutil.StatsLogger().Error(
			"Auto analyze job execution failed",
			zap.Stringer("job", job),
			zap.Error(err),
		)
	}
}

// GetRunningJobs returns the running jobs.
func (w *worker) GetRunningJobs() map[int64]struct{} {
	w.mu.Lock()
	defer w.mu.Unlock()
	runningJobs := make(map[int64]struct{}, len(w.runningJobs))
	for id := range w.runningJobs {
		runningJobs[id] = struct{}{}
	}
	return runningJobs
}

// GetMaxConcurrency returns the maximum concurrency for the worker.
func (w *worker) GetMaxConcurrency() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.maxConcurrency
}

// Stop stops the worker.
func (w *worker) Stop() {
	w.wg.Wait()
}

// WaitAutoAnalyzeFinishedForTest waits for all running auto-analyze jobs to finish.
// Only used for test.
func (w *worker) WaitAutoAnalyzeFinishedForTest() {
	done := make(chan struct{})
	go func() {
		for {
			w.mu.Lock()
			if len(w.runningJobs) == 0 {
				w.mu.Unlock()
				close(done)
				return
			}
			w.mu.Unlock()
			time.Sleep(time.Millisecond * 100)
		}
	}()

	<-done
}
