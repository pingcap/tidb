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
	"strings"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/statistics"
	"github.com/pingcap/tidb/statistics/handle"
	"github.com/pingcap/tidb/util/logutil"
	pderr "github.com/tikv/pd/client/errs"
	"go.uber.org/zap"
)

var persistAnalyzeTableStats = handle.SaveTableStatsToStorage
var recordAnalyzeHistoricalStats = recordHistoricalStats

const analyzeSaveStatsTransientRetryCount = 4
const analyzeSaveStatsTransientRetryBackoff = 100

func isAnalyzeSaveTransientError(err error) bool {
	if err == nil {
		return false
	}
	errText := strings.ToLower(err.Error())
	if pderr.ErrClientGetTSOTimeout.Equal(err) || strings.Contains(errText, "get tso timeout") {
		return true
	}
	if pderr.ErrClientGetTSO.Equal(err) ||
		strings.Contains(errText, pderr.NotLeaderErr) ||
		strings.Contains(errText, pderr.MismatchLeaderErr) ||
		strings.Contains(errText, "leader changed") {
		return true
	}
	return false
}

func saveAnalyzeTableStatsWithRetry(ctx context.Context, sctx sessionctx.Context, results *statistics.AnalyzeResults, analyzeSnapshot bool, killed *uint32) error {
	var firstTransientErr error
	for attempt := 1; attempt <= analyzeSaveStatsTransientRetryCount; attempt++ {
		if err := checkAnalyzeSaveInterrupted(ctx, killed); err != nil {
			return err
		}
		err := persistAnalyzeTableStats(sctx, results, analyzeSnapshot)
		if err == nil {
			return nil
		}
		if !isAnalyzeSaveTransientError(err) {
			return err
		}
		if firstTransientErr == nil {
			firstTransientErr = err
		}
		if attempt == analyzeSaveStatsTransientRetryCount {
			return err
		}
		logutil.Logger(ctx).Warn(
			"save table stats to storage hit transient PD/TSO error, retrying",
			zap.Int("attempt", attempt),
			zap.Error(firstTransientErr),
		)
		if err := waitAnalyzeSaveRetry(ctx, killed, time.Duration(attempt)*analyzeSaveStatsTransientRetryBackoff*time.Millisecond); err != nil {
			return err
		}
	}
	return firstTransientErr
}

func checkAnalyzeSaveInterrupted(ctx context.Context, killed *uint32) error {
	if killed != nil && atomic.LoadUint32(killed) == 1 {
		return errors.Trace(ErrQueryInterrupted)
	}
	select {
	case <-ctx.Done():
		return errors.Trace(ctx.Err())
	default:
		return nil
	}
}

func waitAnalyzeSaveRetry(ctx context.Context, killed *uint32, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return errors.Trace(ctx.Err())
		case <-ticker.C:
			if killed != nil && atomic.LoadUint32(killed) == 1 {
				return errors.Trace(ErrQueryInterrupted)
			}
		case <-timer.C:
			return checkAnalyzeSaveInterrupted(ctx, killed)
		}
	}
}

type analyzeSaveStatsWorker struct {
	resultsCh <-chan *statistics.AnalyzeResults
	sctx      sessionctx.Context
	errCh     chan<- error
	killed    *uint32
}

func newAnalyzeSaveStatsWorker(
	resultsCh <-chan *statistics.AnalyzeResults,
	sctx sessionctx.Context,
	errCh chan<- error,
	killed *uint32) *analyzeSaveStatsWorker {
	worker := &analyzeSaveStatsWorker{
		resultsCh: resultsCh,
		sctx:      sctx,
		errCh:     errCh,
		killed:    killed,
	}
	return worker
}

func (worker *analyzeSaveStatsWorker) run(ctx context.Context, analyzeSnapshot bool) {
	reportedErr := false
	reportErr := func(err error) {
		if !reportedErr {
			worker.errCh <- err
			reportedErr = true
		}
	}
	defer func() {
		if r := recover(); r != nil {
			logutil.BgLogger().Error("analyze save stats worker panicked", zap.Any("recover", r), zap.Stack("stack"))
			reportErr(getAnalyzePanicErr(r))
		}
	}()
	interrupted := false
	var persistedErr error
	for results := range worker.resultsCh {
		if atomic.LoadUint32(worker.killed) == 1 {
			finishJobWithLog(worker.sctx, results.Job, ErrQueryInterrupted)
			if !interrupted {
				reportErr(errors.Trace(ErrQueryInterrupted))
				interrupted = true
			}
			continue
		}
		if persistedErr != nil {
			finishJobWithLog(worker.sctx, results.Job, persistedErr)
			continue
		}
		err := saveAnalyzeTableStatsWithRetry(ctx, worker.sctx, results, analyzeSnapshot, worker.killed)
		if err != nil {
			logutil.Logger(ctx).Error("save table stats to storage failed", zap.Error(err))
			finishJobWithLog(worker.sctx, results.Job, err)
			reportErr(err)
			persistedErr = err
		} else {
			finishJobWithLog(worker.sctx, results.Job, nil)
			// Dump stats to historical storage.
			if err := recordAnalyzeHistoricalStats(worker.sctx, results.TableID.TableID); err != nil {
				logutil.BgLogger().Error("record historical stats failed", zap.Error(err))
			}
		}
		invalidInfoSchemaStatCache(results.TableID.GetStatisticsID())
	}
}
