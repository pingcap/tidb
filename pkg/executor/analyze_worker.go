// Copyright 2022 PingCAP, Inc.
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

package executor

import (
	"context"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/statistics"
	"github.com/pingcap/tidb/pkg/statistics/handle"
	"github.com/pingcap/tidb/pkg/statistics/handle/util"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/sqlkiller"
	"go.uber.org/zap"
)

type analyzeSaveStatsWorker struct {
	resultsCh <-chan *statistics.AnalyzeResults
	errCh     chan<- error
	killer    *sqlkiller.SQLKiller
}

func newAnalyzeSaveStatsWorker(
	resultsCh <-chan *statistics.AnalyzeResults,
	errCh chan<- error,
	killer *sqlkiller.SQLKiller) *analyzeSaveStatsWorker {
	worker := &analyzeSaveStatsWorker{
		resultsCh: resultsCh,
		errCh:     errCh,
		killer:    killer,
	}
	return worker
}

func (worker *analyzeSaveStatsWorker) run(ctx context.Context, statsHandle *handle.Handle, analyzeSnapshot bool) {
	defer func() {
		failpoint.InjectCall("analyzeSaveStatsWorkerDone")
	}()
	var current *statistics.AnalyzeResults
	var runErr error
	defer func() {
		if r := recover(); r != nil {
			logutil.BgLogger().Error("analyze save stats worker panicked", zap.Any("recover", r), zap.Stack("stack"))
			runErr = getAnalyzePanicErr(r)
		}
		if runErr == nil {
			return
		}
		// Keep consuming after failure until the producer closes the channel.
		// Otherwise every save worker can exit while the result handler is
		// blocked sending, before it can collect these errors.
		worker.errCh <- runErr
		if current != nil {
			finishJobWithLog(statsHandle, current.Job, runErr)
			current.DestroyAndPutToPool()
		}
		for results := range worker.resultsCh {
			finishJobWithLog(statsHandle, results.Job, runErr)
			results.DestroyAndPutToPool()
		}
	}()
	for results := range worker.resultsCh {
		current = results
		failpoint.Inject("analyzeSaveStatsWorkerPanic", nil)
		if runErr = worker.killer.HandleSignal(); runErr != nil {
			return
		}
		runErr = statsHandle.SaveAnalyzeResultToStorage(results, analyzeSnapshot, util.StatsMetaHistorySourceAnalyze)
		if runErr != nil {
			logutil.Logger(ctx).Error("save table stats to storage failed", zap.Error(runErr))
			return
		}
		finishJobWithLog(statsHandle, results.Job, nil)
		current = nil
		results.DestroyAndPutToPool()
	}
}
