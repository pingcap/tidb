// Copyright 2019 PingCAP, Inc.
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

package expensivequery

import (
	"sync/atomic"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// Handle is the handler for expensive query.
type Handle struct {
	exitCh chan struct{}
	sm     atomic.Value
}

// NewExpensiveQueryHandle builds a new expensive query handler.
func NewExpensiveQueryHandle(exitCh chan struct{}) *Handle {
	return &Handle{exitCh: exitCh}
}

// SetSessionManager sets the SessionManager which is used to fetching the info
// of all active sessions.
func (eqh *Handle) SetSessionManager(sm util.SessionManager) *Handle {
	eqh.sm.Store(sm)
	return eqh
}

// Run starts a expensive query checker goroutine at the start time of the server.
func (eqh *Handle) Run() {
	threshold := atomic.LoadUint64(&variable.ExpensiveQueryTimeThreshold)
	txnThreshold := atomic.LoadUint64(&variable.ExpensiveTxnTimeThreshold)
	ongoingTxnDurationHistogramInternal := metrics.OngoingTxnDurationHistogram.WithLabelValues(metrics.LblInternal)
	ongoingTxnDurationHistogramGeneral := metrics.OngoingTxnDurationHistogram.WithLabelValues(metrics.LblGeneral)
	lastMetricTime := time.Time{}
	// use 100ms as tickInterval temply, may use given interval or use defined variable later
	tickInterval := time.Millisecond * time.Duration(100)
	ticker := time.NewTicker(tickInterval)
	defer ticker.Stop()
	sm := eqh.sm.Load().(util.SessionManager)
	for {
		select {
		case <-ticker.C:
			needMetrics := false
			if now := time.Now(); now.Sub(lastMetricTime) > 15*time.Second {
				// Because the reporting interval of metrics is generally 15 seconds.
				needMetrics = true
				lastMetricTime = now
			}
			processInfo := sm.ShowProcessList()
			for _, info := range processInfo {
				if info.CurTxnStartTS != 0 {
					txnCostTime := time.Since(info.CurTxnCreateTime)
					if txnCostTime >= time.Second*time.Duration(txnThreshold) {
						if needMetrics {
							if info.StmtCtx.InRestrictedSQL {
								ongoingTxnDurationHistogramInternal.Observe(txnCostTime.Seconds())
							} else {
								ongoingTxnDurationHistogramGeneral.Observe(txnCostTime.Seconds())
							}
						}
						if time.Since(info.ExpensiveTxnLogTime) > 10*time.Minute && log.GetLevel() <= zapcore.WarnLevel {
							logExpensiveQuery(txnCostTime, info, "expensive_txn")
							info.ExpensiveTxnLogTime = time.Now()
						}
					}
				}
				if len(info.Info) == 0 {
					continue
				}
				costTime := time.Since(info.Time)
				if time.Since(info.ExpensiveLogTime) > 60*time.Second && costTime >= time.Second*time.Duration(threshold) && log.GetLevel() <= zapcore.WarnLevel {
					logExpensiveQuery(costTime, info, "expensive_query")
					info.ExpensiveLogTime = time.Now()
				}
				if info.MaxExecutionTime > 0 && costTime > time.Duration(info.MaxExecutionTime)*time.Millisecond {
					logutil.BgLogger().Warn("execution timeout, kill it", zap.Duration("costTime", costTime),
						zap.Duration("maxExecutionTime", time.Duration(info.MaxExecutionTime)*time.Millisecond), zap.String("processInfo", info.String()))
					sm.Kill(info.ID, true, true)
				}
				if info.ID == sm.GetAutoAnalyzeProcID() {
					maxAutoAnalyzeTime := variable.MaxAutoAnalyzeTime.Load()
					if maxAutoAnalyzeTime > 0 && costTime > time.Duration(maxAutoAnalyzeTime)*time.Second {
						logutil.BgLogger().Warn("auto analyze timeout, kill it", zap.Duration("costTime", costTime),
							zap.Duration("maxAutoAnalyzeTime", time.Duration(maxAutoAnalyzeTime)*time.Second), zap.String("processInfo", info.String()))
						sm.Kill(info.ID, true, false)
					}
				}
			}
			threshold = atomic.LoadUint64(&variable.ExpensiveQueryTimeThreshold)
			txnThreshold = atomic.LoadUint64(&variable.ExpensiveTxnTimeThreshold)
		case <-eqh.exitCh:
			return
		}
	}
}

// LogOnQueryExceedMemQuota prints a log when memory usage of connID is out of memory quota.
func (eqh *Handle) LogOnQueryExceedMemQuota(connID uint64) {
	if log.GetLevel() > zapcore.WarnLevel {
		return
	}
	// The out-of-memory SQL may be the internal SQL which is executed during
	// the bootstrap phase, and the `sm` is not set at this phase. This is
	// unlikely to happen except for testing. Thus we do not need to log
	// detailed message for it.
	v := eqh.sm.Load()
	if v == nil {
		logutil.BgLogger().Info("expensive_query during bootstrap phase", zap.Uint64("conn", connID))
		return
	}
	sm := v.(util.SessionManager)
	info, ok := sm.GetProcessInfo(connID)
	if !ok {
		return
	}
	logExpensiveQuery(time.Since(info.Time), info, "memory exceeds quota")
}

// logExpensiveQuery logs the queries which exceed the time threshold or memory threshold.
func logExpensiveQuery(costTime time.Duration, info *util.ProcessInfo, msg string) {
	fields := util.GenLogFields(costTime, info, true)
	if fields == nil {
		return
	}
	logutil.BgLogger().Warn(msg, fields...)
}
