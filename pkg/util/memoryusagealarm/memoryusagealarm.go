// Copyright 2020 PingCAP, Inc.
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

package memoryusagealarm

import (
	"cmp"
	"fmt"
	"os"
	"path/filepath"
	rpprof "runtime/pprof"
	"slices"
	"strings"
	"sync/atomic"
	"time"

	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/disk"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/memory"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// Handle is the handler for expensive query.
type Handle struct {
	exitCh chan struct{}
	sm     atomic.Pointer[util.SessionManager]
}

// NewMemoryUsageAlarmHandle builds a memory usage alarm handler.
func NewMemoryUsageAlarmHandle(exitCh chan struct{}) *Handle {
	return &Handle{exitCh: exitCh}
}

// SetSessionManager sets the SessionManager which is used to fetching the info
// of all active sessions.
func (eqh *Handle) SetSessionManager(sm util.SessionManager) *Handle {
	eqh.sm.Store(&sm)
	return eqh
}

// Run starts a memory usage alarm goroutine at the start time of the server.
func (eqh *Handle) Run() {
	// use 100ms as tickInterval temply, may use given interval or use defined variable later
	tickInterval := time.Millisecond * time.Duration(100)
	ticker := time.NewTicker(tickInterval)
	defer ticker.Stop()
	sm := eqh.sm.Load()
	record := &memoryUsageAlarm{}
	for {
		select {
		case <-ticker.C:
			record.alarm4ExcessiveMemUsage(*sm)
		case <-eqh.exitCh:
			return
		}
	}
}

type memoryUsageAlarm struct {
	lastCheckTime                 time.Time
	lastUpdateVariableTime        time.Time
	err                           error
	baseRecordDir                 string
	lastRecordDirName             []string
	lastRecordMemUsed             uint64
	memoryUsageAlarmRatio         float64
	memoryUsageAlarmKeepRecordNum int64
	serverMemoryLimit             uint64
	isServerMemoryLimitSet        bool
	initialized                   bool
}

func (record *memoryUsageAlarm) updateVariable() {
	if time.Since(record.lastUpdateVariableTime) < 60*time.Second {
		return
	}
	record.memoryUsageAlarmRatio = variable.MemoryUsageAlarmRatio.Load()
	record.memoryUsageAlarmKeepRecordNum = variable.MemoryUsageAlarmKeepRecordNum.Load()
	record.serverMemoryLimit = memory.ServerMemoryLimit.Load()
	if record.serverMemoryLimit != 0 {
		record.isServerMemoryLimitSet = true
	} else {
		record.serverMemoryLimit, record.err = memory.MemTotal()
		if record.err != nil {
			logutil.BgLogger().Error("get system total memory fail", zap.Error(record.err))
			return
		}
		record.isServerMemoryLimitSet = false
	}
	record.lastUpdateVariableTime = time.Now()
}

func (record *memoryUsageAlarm) initMemoryUsageAlarmRecord() {
	record.lastCheckTime = time.Time{}
	record.lastUpdateVariableTime = time.Time{}
	record.updateVariable()
	tidbLogDir, _ := filepath.Split(config.GetGlobalConfig().Log.File.Filename)
	record.baseRecordDir = filepath.Join(tidbLogDir, "oom_record")
	if record.err = disk.CheckAndCreateDir(record.baseRecordDir); record.err != nil {
		return
	}
	// Read last records
	recordDirs, err := os.ReadDir(record.baseRecordDir)
	if err != nil {
		record.err = err
		return
	}
	for _, dir := range recordDirs {
		name := filepath.Join(record.baseRecordDir, dir.Name())
		if strings.Contains(dir.Name(), "record") {
			record.lastRecordDirName = append(record.lastRecordDirName, name)
		}
	}
	record.initialized = true
}

// If Performance.ServerMemoryQuota is set, use `ServerMemoryQuota * MemoryUsageAlarmRatio` to check oom risk.
// If Performance.ServerMemoryQuota is not set, use `system total memory size * MemoryUsageAlarmRatio` to check oom risk.
func (record *memoryUsageAlarm) alarm4ExcessiveMemUsage(sm util.SessionManager) {
	if !record.initialized {
		record.initMemoryUsageAlarmRecord()
		if record.err != nil {
			return
		}
	} else {
		record.updateVariable()
	}
	if record.memoryUsageAlarmRatio <= 0.0 || record.memoryUsageAlarmRatio >= 1.0 {
		return
	}
	var memoryUsage uint64
	instanceStats := memory.ReadMemStats()
	if record.isServerMemoryLimitSet {
		memoryUsage = instanceStats.HeapAlloc
	} else {
		memoryUsage, record.err = memory.MemUsed()
		if record.err != nil {
			logutil.BgLogger().Error("get system memory usage fail", zap.Error(record.err))
			return
		}
	}

	// TODO: Consider NextGC to record SQLs.
	if needRecord, reason := record.needRecord(memoryUsage); needRecord {
		record.lastCheckTime = time.Now()
		record.lastRecordMemUsed = memoryUsage
		record.doRecord(memoryUsage, instanceStats.HeapAlloc, sm, reason)
		record.tryRemoveRedundantRecords()
	}
}

// AlarmReason implements alarm reason.
type AlarmReason uint

const (
	// GrowTooFast is the reason that memory increasing too fast.
	GrowTooFast AlarmReason = iota
	// ExceedAlarmRatio is the reason that memory used exceed threshold.
	ExceedAlarmRatio
	// NoReason means no alarm
	NoReason
)

func (reason AlarmReason) String() string {
	return [...]string{"memory usage grows too fast", "memory usage exceeds alarm ratio", "no reason"}[reason]
}

func (record *memoryUsageAlarm) needRecord(memoryUsage uint64) (bool, AlarmReason) {
	// At least 60 seconds between two recordings that memory usage is less than threshold (default 70% system memory).
	// If the memory is still exceeded, only records once.
	// If the memory used ratio recorded this time is 0.1 higher than last time, we will force record this time.
	if float64(memoryUsage) <= float64(record.serverMemoryLimit)*record.memoryUsageAlarmRatio {
		return false, NoReason
	}

	interval := time.Since(record.lastCheckTime)
	memDiff := int64(memoryUsage) - int64(record.lastRecordMemUsed)
	if interval > 60*time.Second {
		return true, ExceedAlarmRatio
	}
	if float64(memDiff) > 0.1*float64(record.serverMemoryLimit) {
		return true, GrowTooFast
	}
	return false, NoReason
}

func (record *memoryUsageAlarm) doRecord(memUsage uint64, instanceMemoryUsage uint64, sm util.SessionManager, alarmReason AlarmReason) {
	fields := make([]zap.Field, 0, 6)
	fields = append(fields, zap.Bool("is tidb_server_memory_limit set", record.isServerMemoryLimitSet))
	if record.isServerMemoryLimitSet {
		fields = append(fields, zap.Any("tidb_server_memory_limit", record.serverMemoryLimit))
		fields = append(fields, zap.Any("tidb-server memory usage", memUsage))
	} else {
		fields = append(fields, zap.Any("system memory total", record.serverMemoryLimit))
		fields = append(fields, zap.Any("system memory usage", memUsage))
		fields = append(fields, zap.Any("tidb-server memory usage", instanceMemoryUsage))
	}
	fields = append(fields, zap.Any("memory-usage-alarm-ratio", record.memoryUsageAlarmRatio))
	fields = append(fields, zap.Any("record path", record.baseRecordDir))
	logutil.BgLogger().Warn(fmt.Sprintf("tidb-server has the risk of OOM because of %s. Running SQLs and heap profile will be recorded in record path", alarmReason.String()), fields...)
	recordDir := filepath.Join(record.baseRecordDir, "record"+record.lastCheckTime.Format(time.RFC3339))
	if record.err = disk.CheckAndCreateDir(recordDir); record.err != nil {
		return
	}
	record.lastRecordDirName = append(record.lastRecordDirName, recordDir)
	if record.err = record.recordSQL(sm, recordDir); record.err != nil {
		return
	}
	if record.err = record.recordProfile(recordDir); record.err != nil {
		return
	}
}

func (record *memoryUsageAlarm) tryRemoveRedundantRecords() {
	filename := &record.lastRecordDirName
	for len(*filename) > int(record.memoryUsageAlarmKeepRecordNum) {
		err := os.RemoveAll((*filename)[0])
		if err != nil {
			logutil.BgLogger().Error("remove temp files failed", zap.Error(err))
		}
		*filename = (*filename)[1:]
	}
}

func getPlanString(info *util.ProcessInfo) string {
	var buf strings.Builder
	rows := info.PlanExplainRows
	buf.WriteString(fmt.Sprintf("|%v|%v|%v|%v|%v|", "id", "estRows", "task", "access object", "operator info"))
	for _, row := range rows {
		buf.WriteString(fmt.Sprintf("\n|%v|%v|%v|%v|%v|", row[0], row[1], row[2], row[3], row[4]))
	}
	return buf.String()
}

func (record *memoryUsageAlarm) printTop10SqlInfo(pinfo []*util.ProcessInfo, f *os.File) {
	if _, err := f.WriteString("The 10 SQLs with the most memory usage for OOM analysis\n"); err != nil {
		logutil.BgLogger().Error("write top 10 memory sql info fail", zap.Error(err))
	}
	memBuf := record.getTop10SqlInfoByMemoryUsage(pinfo)
	if _, err := f.WriteString(memBuf.String()); err != nil {
		logutil.BgLogger().Error("write top 10 memory sql info fail", zap.Error(err))
	}
	if _, err := f.WriteString("The 10 SQLs with the most time usage for OOM analysis\n"); err != nil {
		logutil.BgLogger().Error("write top 10 time cost sql info fail", zap.Error(err))
	}
	costBuf := record.getTop10SqlInfoByCostTime(pinfo)
	if _, err := f.WriteString(costBuf.String()); err != nil {
		logutil.BgLogger().Error("write top 10 time cost sql info fail", zap.Error(err))
	}
}

func (record *memoryUsageAlarm) getTop10SqlInfo(cmp func(i, j *util.ProcessInfo) int, pinfo []*util.ProcessInfo) strings.Builder {
	slices.SortFunc(pinfo, cmp)
	list := pinfo
	var buf strings.Builder
	oomAction := variable.OOMAction.Load()
	serverMemoryLimit := memory.ServerMemoryLimit.Load()
	for i, totalCnt := 0, 10; i < len(list) && totalCnt > 0; i++ {
		info := list[i]
		buf.WriteString(fmt.Sprintf("SQL %v: \n", i))
		fields := util.GenLogFields(record.lastCheckTime.Sub(info.Time), info, false)
		if fields == nil {
			continue
		}
		fields = append(fields, zap.String("tidb_mem_oom_action", oomAction))
		fields = append(fields, zap.Uint64("tidb_server_memory_limit", serverMemoryLimit))
		fields = append(fields, zap.Int64("tidb_mem_quota_query", info.OOMAlarmVariablesInfo.SessionMemQuotaQuery))
		fields = append(fields, zap.Int("tidb_analyze_version", info.OOMAlarmVariablesInfo.SessionAnalyzeVersion))
		fields = append(fields, zap.Bool("tidb_enable_rate_limit_action", info.OOMAlarmVariablesInfo.SessionEnabledRateLimitAction))
		fields = append(fields, zap.String("current_analyze_plan", getPlanString(info)))
		for _, field := range fields {
			switch field.Type {
			case zapcore.StringType:
				fmt.Fprintf(&buf, "%v: %v", field.Key, field.String)
			case zapcore.Uint8Type, zapcore.Uint16Type, zapcore.Uint32Type, zapcore.Uint64Type:
				fmt.Fprintf(&buf, "%v: %v", field.Key, uint64(field.Integer))
			case zapcore.Int8Type, zapcore.Int16Type, zapcore.Int32Type, zapcore.Int64Type:
				fmt.Fprintf(&buf, "%v: %v", field.Key, field.Integer)
			case zapcore.BoolType:
				fmt.Fprintf(&buf, "%v: %v", field.Key, field.Integer == 1)
			}
			buf.WriteString("\n")
		}
		totalCnt--
	}
	buf.WriteString("\n")
	return buf
}

func (record *memoryUsageAlarm) getTop10SqlInfoByMemoryUsage(pinfo []*util.ProcessInfo) strings.Builder {
	return record.getTop10SqlInfo(func(i, j *util.ProcessInfo) int {
		return cmp.Compare(j.MemTracker.MaxConsumed(), i.MemTracker.MaxConsumed())
	}, pinfo)
}

func (record *memoryUsageAlarm) getTop10SqlInfoByCostTime(pinfo []*util.ProcessInfo) strings.Builder {
	return record.getTop10SqlInfo(func(i, j *util.ProcessInfo) int {
		return i.Time.Compare(j.Time)
	}, pinfo)
}

func (record *memoryUsageAlarm) recordSQL(sm util.SessionManager, recordDir string) error {
	processInfo := sm.ShowProcessList()
	pinfo := make([]*util.ProcessInfo, 0, len(processInfo))
	for _, info := range processInfo {
		if len(info.Info) != 0 {
			pinfo = append(pinfo, info)
		}
	}
	fileName := filepath.Join(recordDir, "running_sql")
	f, err := os.Create(fileName)
	if err != nil {
		logutil.BgLogger().Error("create oom record file fail", zap.Error(err))
		return err
	}
	defer func() {
		err := f.Close()
		if err != nil {
			logutil.BgLogger().Error("close oom record file fail", zap.Error(err))
		}
	}()
	record.printTop10SqlInfo(pinfo, f)
	return nil
}

type item struct {
	Name  string
	Debug int
}

func (*memoryUsageAlarm) recordProfile(recordDir string) error {
	items := []item{
		{Name: "heap"},
		{Name: "goroutine", Debug: 2},
	}
	for _, item := range items {
		if err := write(item, recordDir); err != nil {
			return err
		}
	}
	return nil
}

func write(item item, recordDir string) error {
	fileName := filepath.Join(recordDir, item.Name)
	f, err := os.Create(fileName)
	if err != nil {
		logutil.BgLogger().Error(fmt.Sprintf("create %v profile file fail", item.Name), zap.Error(err))
		return err
	}
	p := rpprof.Lookup(item.Name)
	err = p.WriteTo(f, item.Debug)
	if err != nil {
		logutil.BgLogger().Error(fmt.Sprintf("write %v profile file fail", item.Name), zap.Error(err))
		return err
	}

	//nolint: revive
	defer func() {
		err := f.Close()
		if err != nil {
			logutil.BgLogger().Error(fmt.Sprintf("close %v profile file fail", item.Name), zap.Error(err))
		}
	}()
	return nil
}
