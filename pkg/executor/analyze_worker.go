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

	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/statistics"
	"github.com/pingcap/tidb/pkg/statistics/handle/util"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/sqlkiller"
	"go.uber.org/zap"
)

type analyzeSaveStatsWorker struct {
	resultsCh <-chan *statistics.AnalyzeResults
	sctx      sessionctx.Context
	errCh     chan<- error
	killer    *sqlkiller.SQLKiller
}

func newAnalyzeSaveStatsWorker(
	resultsCh <-chan *statistics.AnalyzeResults,
	sctx sessionctx.Context,
	errCh chan<- error,
	killer *sqlkiller.SQLKiller) *analyzeSaveStatsWorker {
	worker := &analyzeSaveStatsWorker{
		resultsCh: resultsCh,
		sctx:      sctx,
		errCh:     errCh,
		killer:    killer,
	}
	return worker
}

func (worker *analyzeSaveStatsWorker) run(ctx context.Context, analyzeSnapshot bool) {
	defer func() {
		if r := recover(); r != nil {
			logutil.BgLogger().Error("analyze save stats worker panicked", zap.Any("recover", r), zap.Stack("stack"))
			worker.errCh <- getAnalyzePanicErr(r)
		}
	}()
	for results := range worker.resultsCh {
		if err := worker.killer.HandleSignal(); err != nil {
			worker.errCh <- err
			return
		}
		statsHandle := domain.GetDomain(worker.sctx).StatsHandle()
		err := statsHandle.SaveTableStatsToStorage(results, analyzeSnapshot, util.StatsMetaHistorySourceAnalyze)
		if err != nil {
			logutil.Logger(ctx).Error("save table stats to storage failed", zap.Error(err))
			finishJobWithLog(worker.sctx, results.Job, err)
			worker.errCh <- err
		} else {
			finishJobWithLog(worker.sctx, results.Job, nil)
		}
		results.DestroyAndPutToPool()
		if err != nil {
			return
		}
	}
}
