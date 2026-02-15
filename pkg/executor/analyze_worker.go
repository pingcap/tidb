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
	"github.com/pingcap/tidb/pkg/util/dbterror/exeerrors"
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
	errReported := false
	defer func() {
		if r := recover(); r != nil {
			logutil.BgLogger().Error("analyze save stats worker panicked", zap.Any("recover", r), zap.Stack("stack"))
			if !errReported {
				worker.errCh <- getAnalyzePanicErr(r)
				errReported = true
			}
		}
	}()
	for results := range worker.resultsCh {
		mockKill := false
		failpoint.Inject("mockAnalyzeSaveWorkerKill", func(val failpoint.Value) {
			if val.(bool) {
				mockKill = true
			}
		})
		if mockKill {
			err := exeerrors.ErrQueryInterrupted.GenWithStackByArgs()
			finishJobWithLog(statsHandle, results.Job, err)
			results.DestroyAndPutToPool()
			if !errReported {
				worker.errCh <- err
				errReported = true
			}
			return
		}
		if err := worker.killer.HandleSignal(); err != nil {
			finishJobWithLog(statsHandle, results.Job, err)
			results.DestroyAndPutToPool()
			if !errReported {
				worker.errCh <- err
				errReported = true
			}
			return
		}
		err := statsHandle.SaveAnalyzeResultToStorage(results, analyzeSnapshot, util.StatsMetaHistorySourceAnalyze)
		if err != nil {
			logutil.Logger(ctx).Warn("save table stats to storage failed", zap.Error(err))
			finishJobWithLog(statsHandle, results.Job, err)
			if !errReported {
				worker.errCh <- err
				errReported = true
			}
			// Keep draining results to avoid blocking analyze workers after a save failure.
		} else {
			finishJobWithLog(statsHandle, results.Job, nil)
		}
		results.DestroyAndPutToPool()
	}
}
