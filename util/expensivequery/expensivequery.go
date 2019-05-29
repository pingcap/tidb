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
// See the License for the specific language governing permissions and
// limitations under the License.

package expensivequery

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// Handle is the handler for expensive query.
type Handle struct {
	mu      sync.RWMutex
	exitCh  chan struct{}
	queries map[uint32]InterruptableQuery
}

// NewExpensiveQueryHandle builds a new expensive query handler.
func NewExpensiveQueryHandle(exitCh chan struct{}) *Handle {
	return &Handle{
		exitCh:  exitCh,
		queries: make(map[uint32]InterruptableQuery),
	}
}

// Run starts a expensive query checker goroutine at the start time of the server.
func (eqh *Handle) Run(sm util.SessionManager) {
	threshold := atomic.LoadUint64(&variable.ExpensiveQueryTimeThreshold)
	curInterval := time.Second * time.Duration(threshold)
	ticker := time.NewTicker(curInterval / 2)
	for {
		select {
		case <-ticker.C:
			eqh.checkAll()
			if log.GetLevel() > zapcore.WarnLevel || sm == nil {
				continue
			}
			processInfo := sm.ShowProcessList()
			for _, info := range processInfo {
				if len(info.Info) == 0 || info.ExceedExpensiveTimeThresh {
					continue
				}
				if costTime := time.Since(info.Time); costTime >= curInterval {
					logExpensiveQuery(costTime, info)
					info.ExceedExpensiveTimeThresh = true
				}
			}
			threshold = atomic.LoadUint64(&variable.ExpensiveQueryTimeThreshold)
			if newInterval := time.Second * time.Duration(threshold); curInterval != newInterval {
				curInterval = newInterval
				ticker.Stop()
				ticker = time.NewTicker(curInterval / 2)
			}
		case <-eqh.exitCh:
			return
		}
	}
}

// logExpensiveQuery logs the queries which exceed the time threshold or memory threshold.
func logExpensiveQuery(costTime time.Duration, info *util.ProcessInfo) {
	logFields := make([]zap.Field, 0, 20)
	logFields = append(logFields, zap.String("cost_time", strconv.FormatFloat(costTime.Seconds(), 'f', -1, 64)+"s"))
	execDetail := info.StmtCtx.GetExecDetails()
	logFields = append(logFields, execDetail.ToZapFields()...)
	if copTaskInfo := info.StmtCtx.CopTasksDetails(); copTaskInfo != nil {
		logFields = append(logFields, copTaskInfo.ToZapFields()...)
	}
	if statsInfo := info.StatsInfo(info.Plan); len(statsInfo) > 0 {
		var buf strings.Builder
		firstComma := false
		vStr := ""
		for k, v := range statsInfo {
			if v == 0 {
				vStr = "pseudo"
			} else {
				vStr = strconv.FormatUint(v, 10)
			}
			if firstComma {
				buf.WriteString("," + k + ":" + vStr)
			} else {
				buf.WriteString(k + ":" + vStr)
				firstComma = true
			}
		}
		logFields = append(logFields, zap.String("stats", buf.String()))
	}
	if info.ID != 0 {
		logFields = append(logFields, zap.Uint64("conn_id", info.ID))
	}
	if len(info.User) > 0 {
		logFields = append(logFields, zap.String("user", info.User))
	}
	if len(info.DB) > 0 {
		logFields = append(logFields, zap.String("database", info.DB))
	}
	var tableIDs, indexIDs string
	if len(info.StmtCtx.TableIDs) > 0 {
		tableIDs = strings.Replace(fmt.Sprintf("%v", info.StmtCtx.TableIDs), " ", ",", -1)
		logFields = append(logFields, zap.String("table_ids", tableIDs))
	}
	if len(info.StmtCtx.IndexIDs) > 0 {
		indexIDs = strings.Replace(fmt.Sprintf("%v", info.StmtCtx.IndexIDs), " ", ",", -1)
		logFields = append(logFields, zap.String("index_ids", indexIDs))
	}
	logFields = append(logFields, zap.Uint64("txn_start_ts", info.CurTxnStartTS))
	if memTracker := info.StmtCtx.MemTracker; memTracker != nil {
		logFields = append(logFields, zap.String("mem_max", memTracker.BytesToString(memTracker.MaxConsumed())))
	}

	const logSQLLen = 1024 * 8
	sql := info.Info
	if len(sql) > logSQLLen {
		sql = fmt.Sprintf("%s len(%d)", sql[:logSQLLen], len(sql))
	}
	logFields = append(logFields, zap.String("sql", sql))

	logutil.Logger(context.Background()).Warn("expensive_query", logFields...)
}
