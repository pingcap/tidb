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
	"sync/atomic"

	"github.com/pingcap/tidb/pkg/sessionctx/sysproctrack"
	"github.com/pingcap/tidb/pkg/statistics/handle/autoanalyze/priorityqueue"
	statslogutil "github.com/pingcap/tidb/pkg/statistics/handle/logutil"
	statstypes "github.com/pingcap/tidb/pkg/statistics/handle/types"
	"github.com/pingcap/tidb/pkg/util"
	"go.uber.org/zap"
)

type worker struct {
	statsHandle    statstypes.StatsHandle
	sysProcTracker sysproctrack.Tracker
	jobChan        chan priorityqueue.AnalysisJob
	exitChan       chan struct{}
	wg             util.WaitGroupWrapper
	jobCnt         atomic.Int64
}

func newWorker(statsHandle statstypes.StatsHandle, sysProcTracker sysproctrack.Tracker, jobChan chan priorityqueue.AnalysisJob, concurrency int) *worker {
	result := &worker{
		statsHandle:    statsHandle,
		sysProcTracker: sysProcTracker,
		jobChan:        jobChan,
		exitChan:       make(chan struct{}),
	}
	result.start(concurrency)
	return result
}

func (w *worker) start(concurrency int) {
	for i := 0; i < concurrency; i++ {
		w.wg.Run(w.task)
	}
}

func (w *worker) task() {
	for {
		select {
		case job := <-w.jobChan:
			statslogutil.StatsLogger().Info(
				"Auto analyze triggered",
				zap.Stringer("job", job),
			)
			err := job.Analyze(
				w.statsHandle,
				w.sysProcTracker,
			)
			if err != nil {
				statslogutil.StatsLogger().Error(
					"Execute auto analyze job failed",
					zap.Stringer("job", job),
					zap.Error(err),
				)
			}
			w.jobCnt.Add(1)
		case <-w.exitChan:
			return
		}
	}
}

func (w *worker) close() {
	close(w.exitChan)
	w.wg.Wait()
}

func (w *worker) CompletedJobsCnt() int64 {
	return w.jobCnt.Load()
}
