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
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	rpprof "runtime/pprof"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/disk"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/memory"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// Handle is the handler for expensive query.
type Handle struct {
	exitCh chan struct{}
	sm     atomic.Value

	record memoryUsageAlarmRecord
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
	// use 100ms as tickInterval temply, may use given interval or use defined variable later
	tickInterval := time.Millisecond * time.Duration(100)
	ticker := time.NewTicker(tickInterval)
	defer ticker.Stop()
	sm := eqh.sm.Load().(util.SessionManager)
	eqh.initMemoryUsageAlarmRecord()
	for {
		select {
		case <-ticker.C:
			processInfo := sm.ShowProcessList()
			for _, info := range processInfo {
				if len(info.Info) == 0 {
					continue
				}
				costTime := time.Since(info.Time)
				if !info.ExceedExpensiveTimeThresh && costTime >= time.Second*time.Duration(threshold) && log.GetLevel() <= zapcore.WarnLevel {
					logExpensiveQuery(costTime, info)
					info.ExceedExpensiveTimeThresh = true
				}

				if info.MaxExecutionTime > 0 && costTime > time.Duration(info.MaxExecutionTime)*time.Millisecond {
					sm.Kill(info.ID, true)
				}
			}
			threshold = atomic.LoadUint64(&variable.ExpensiveQueryTimeThreshold)
			if eqh.record.err == nil {
				eqh.oomKillerAlert()
			}
		case <-eqh.exitCh:
			return
		}
	}
}

type memoryUsageAlarmRecord struct {
	err               error
	useInstanceMemory bool // useInstanceMemory indicate whether use instance memory or system memory to alarm oom risk.
	serverMemoryQuota uint64
	lastRecordTime    time.Time

	tmpDir              string
	lastLogFileName     []string
	lastProfileFileName [][]string // heap, goroutine
}

func (eqh *Handle) initMemoryUsageAlarmRecord() {
	if config.GetGlobalConfig().Performance.ServerMemoryQuota != 0 {
		eqh.record.serverMemoryQuota = config.GetGlobalConfig().Performance.ServerMemoryQuota
		eqh.record.useInstanceMemory = true
	} else {
		eqh.record.serverMemoryQuota, eqh.record.err = memory.MemTotal()
		if eqh.record.err != nil {
			logutil.BgLogger().Error("Get system total memory fail.", zap.Error(eqh.record.err))
			return
		}
		eqh.record.useInstanceMemory = false
	}
	eqh.record.lastRecordTime = time.Time{}
	eqh.record.tmpDir = config.GetGlobalConfig().TempStoragePath
	if alert := config.GetGlobalConfig().Performance.MemoryUsageAlarmRatio; alert == 0 || alert == 1 {
		eqh.record.err = errors.New("close memory usage alarm recorder")
	}
	eqh.record.lastProfileFileName = make([][]string, 2)
}

// If Performance.ServerMemoryQuota is set, use instance memory usage and ServerMemoryQuota * 80% to check oom risk.
// If Performance.ServerMemoryQuota is not set, use system memory usage and total memory * 80% to check oom risk.
func (eqh *Handle) oomKillerAlert() {
	var memoryUsage uint64
	instanceStats := &runtime.MemStats{}
	if eqh.record.useInstanceMemory {
		runtime.ReadMemStats(instanceStats)
		memoryUsage = instanceStats.HeapAlloc
	} else {
		memoryUsage, eqh.record.err = memory.MemUsed()
		if eqh.record.err != nil {
			logutil.BgLogger().Error("Get system usage memory fail.", zap.Error(eqh.record.err))
			return
		}
	}

	// If we use instance memory usage to check oom risk, we also need use NextGC compared with the quota.
	// Maybe the instance will oom but the HeapAlloc isn't updated after golang GC.
	// Go's GC don't take the system memory limit into consideration, that means:
	//
	// 1. You have 16G physical memory;
	// 2. You have 9G live objects;
	// 3. Go will run GC when memory usage reaches 9G * (1 + GOGC / 100) = 18G, OOM.
	if float64(memoryUsage) > float64(eqh.record.serverMemoryQuota)*config.GetGlobalConfig().Performance.MemoryUsageAlarmRatio ||
		(eqh.record.useInstanceMemory && instanceStats.NextGC > eqh.record.serverMemoryQuota) {
		// At least ten seconds between two recordings that memory usage is less than threshold (default 80% system memory).
		// If the memory is still exceeded, only records once.
		if time.Since(eqh.record.lastRecordTime) > 10*time.Second {
			eqh.oomRecord(memoryUsage)
		}
		eqh.record.lastRecordTime = time.Now()
	}
}

func (eqh *Handle) oomRecord(memUsage uint64) {
	if eqh.record.useInstanceMemory {
		logutil.BgLogger().Warn("The TiDB instance now takes a lot of memory, has the risk of OOM",
			zap.Any("tidbMemUsage", memUsage),
			zap.Any("serverMemoryQuota", eqh.record.serverMemoryQuota),
		)
	} else {
		logutil.BgLogger().Warn("The os system now takes a lot of memory, has the risk of OOM",
			zap.Any("osMemUsage", memUsage),
			zap.Any("osMemoryTotal", eqh.record.serverMemoryQuota),
		)
	}

	if eqh.record.err = disk.CheckAndInitTempDir(); eqh.record.err != nil {
		return
	}
	eqh.oomRecordSQL()
	eqh.oomRecordProfile()

	tryRemove := func(filename []string) {
		// Keep the last 5 files
		if len(filename) < 6 {
			return
		}
		err := os.Remove(filename[0])
		if err != nil {
			logutil.BgLogger().Error("Remove temp files failed.", zap.Error(err))
		}
		filename = filename[1:]
	}
	tryRemove(eqh.record.lastLogFileName)
	for i := range eqh.record.lastProfileFileName {
		tryRemove(eqh.record.lastProfileFileName[i])
	}
}

func (eqh *Handle) oomRecordSQL() {
	sm := eqh.sm.Load().(util.SessionManager)
	processInfo := sm.ShowProcessList()
	pinfo := make([]*util.ProcessInfo, 0, len(processInfo))
	for _, info := range processInfo {
		if len(info.Info) != 0 {
			pinfo = append(pinfo, info)
		}
	}
	now := time.Now()

	fileName := filepath.Join(config.GetGlobalConfig().TempStoragePath, "oom_sql"+time.Now().Format(time.RFC3339))
	eqh.record.lastLogFileName = append(eqh.record.lastLogFileName, fileName)
	f, err := os.Create(fileName)
	if err != nil {
		logutil.BgLogger().Error("Create oom record file fail.", zap.Error(err))
		return
	}
	defer func() {
		err := f.Close()
		if err != nil {
			logutil.BgLogger().Error("Close oom record file fail.", zap.Error(err))
		}
	}()
	printTop10 := func(cmp func(i, j int) bool) {
		sort.Slice(pinfo, cmp)
		list := pinfo
		if len(list) > 10 {
			list = list[:10]
		}
		var buf strings.Builder
		for i, info := range list {
			buf.WriteString(fmt.Sprintf("SQL %v: \n", i))
			fields := genLogFields(now.Sub(info.Time), info)
			for _, field := range fields {
				switch field.Type {
				case zapcore.StringType:
					buf.WriteString(fmt.Sprintf("%v: %v", field.Key, field.String))
				case zapcore.Uint8Type, zapcore.Uint16Type, zapcore.Uint32Type, zapcore.Uint64Type:
					buf.WriteString(fmt.Sprintf("%v: %v", field.Key, uint64(field.Integer)))
				case zapcore.Int8Type, zapcore.Int16Type, zapcore.Int32Type, zapcore.Int64Type:
					buf.WriteString(fmt.Sprintf("%v: %v", field.Key, field.Integer))
				}
				buf.WriteString("\n")
			}
		}
		_, err = f.WriteString(buf.String())
	}

	_, err = f.WriteString("Top 10 memory usage of SQL for OOM analyze\n")
	printTop10(func(i, j int) bool {
		return pinfo[i].StmtCtx.MemTracker.MaxConsumed() > pinfo[j].StmtCtx.MemTracker.MaxConsumed()
	})

	_, err = f.WriteString("Top 10 time usage of SQL for OOM analyze\n")
	printTop10(func(i, j int) bool {
		return pinfo[i].Time.Before(pinfo[j].Time)
	})

	logutil.BgLogger().Info("Get oom sql successfully.", zap.Any("SQLs file path:", eqh.record.lastLogFileName))
}

func (eqh *Handle) oomRecordProfile() {
	items := []struct {
		name  string
		gc    int
		debug int
	}{
		{name: "heap", gc: 1},
		{name: "goroutine", debug: 2},
	}
	for i, item := range items {
		fileName := filepath.Join(eqh.record.tmpDir, item.name+time.Now().Format(time.RFC3339))
		eqh.record.lastProfileFileName[i] = append(eqh.record.lastProfileFileName[i], fileName)
		f, err := os.Create(fileName)
		if err != nil {
			logutil.BgLogger().Error(fmt.Sprintf("Create %v profile file fail.", item.name), zap.Error(err))
			return
		}
		defer func() {
			err := f.Close()
			if err != nil {
				logutil.BgLogger().Error(fmt.Sprintf("Close %v profile file fail.", item.name), zap.Error(err))
			}
		}()
		if item.gc > 0 {
			runtime.GC()
		}
		p := rpprof.Lookup(item.name)
		err = p.WriteTo(f, item.debug)
		if err != nil {
			logutil.BgLogger().Error(fmt.Sprintf("Write %v profile file fail.", item.name), zap.Error(err))
			return
		}
		logutil.BgLogger().Info(fmt.Sprintf("Get %v profile successfully.", item.name), zap.Any("Profile file path:", fileName))
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
		logutil.BgLogger().Info("expensive_query during bootstrap phase", zap.Uint64("conn_id", connID))
		return
	}
	sm := v.(util.SessionManager)
	info, ok := sm.GetProcessInfo(connID)
	if !ok {
		return
	}
	logExpensiveQuery(time.Since(info.Time), info)
}

func genLogFields(costTime time.Duration, info *util.ProcessInfo) []zap.Field {
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
	var tableIDs, indexNames string
	if len(info.StmtCtx.TableIDs) > 0 {
		tableIDs = strings.Replace(fmt.Sprintf("%v", info.StmtCtx.TableIDs), " ", ",", -1)
		logFields = append(logFields, zap.String("table_ids", tableIDs))
	}
	if len(info.StmtCtx.IndexNames) > 0 {
		indexNames = strings.Replace(fmt.Sprintf("%v", info.StmtCtx.IndexNames), " ", ",", -1)
		logFields = append(logFields, zap.String("index_names", indexNames))
	}
	logFields = append(logFields, zap.Uint64("txn_start_ts", info.CurTxnStartTS))
	if memTracker := info.StmtCtx.MemTracker; memTracker != nil {
		logFields = append(logFields, zap.String("mem_max", fmt.Sprintf("%d Bytes (%v)", memTracker.MaxConsumed(), memTracker.BytesToString(memTracker.MaxConsumed()))))
	}

	const logSQLLen = 1024 * 8
	var sql string
	if len(info.Info) > 0 {
		sql = info.Info
	}
	if len(sql) > logSQLLen {
		sql = fmt.Sprintf("%s len(%d)", sql[:logSQLLen], len(sql))
	}
	logFields = append(logFields, zap.String("sql", sql))
	return logFields
}

// logExpensiveQuery logs the queries which exceed the time threshold or memory threshold.
func logExpensiveQuery(costTime time.Duration, info *util.ProcessInfo) {
	logutil.BgLogger().Warn("expensive_query", genLogFields(costTime, info)...)
}
