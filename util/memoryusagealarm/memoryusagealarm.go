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
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	rpprof "runtime/pprof"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/disk"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/memory"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"golang.org/x/exp/slices"
)

// Handle is the handler for expensive query.
type Handle struct {
	exitCh chan struct{}
	sm     atomic.Value
}

// NewMemoryUsageAlarmHandle builds a memory usage alarm handler.
func NewMemoryUsageAlarmHandle(exitCh chan struct{}) *Handle {
	return &Handle{exitCh: exitCh}
}

// SetSessionManager sets the SessionManager which is used to fetching the info
// of all active sessions.
func (eqh *Handle) SetSessionManager(sm util.SessionManager) *Handle {
	eqh.sm.Store(sm)
	return eqh
}

// Run starts a memory usage alarm goroutine at the start time of the server.
func (eqh *Handle) Run() {
	// use 100ms as tickInterval temply, may use given interval or use defined variable later
	tickInterval := time.Millisecond * time.Duration(100)
	ticker := time.NewTicker(tickInterval)
	defer ticker.Stop()
	sm := eqh.sm.Load().(util.SessionManager)
	record := &memoryUsageAlarm{}
	for {
		select {
		case <-ticker.C:
			record.alarm4ExcessiveMemUsage(sm)
		case <-eqh.exitCh:
			return
		}
	}
}

type memoryUsageAlarm struct {
	lastCheckTime                 time.Time
	err                           error
	baseRecordDir                 string
	lastRecordDirName             []string
	lastRecordMemUsed             uint64
	memoryUsageAlarmRatio         float64
	memoryUsageAlarmKeepRecordNum int64
	serverMemoryQuota             uint64
	isServerMemoryQuotaSet        bool
	initialized                   bool
}

func (record *memoryUsageAlarm) updateVariable() {
	record.memoryUsageAlarmRatio = variable.MemoryUsageAlarmRatio.Load()
	record.memoryUsageAlarmKeepRecordNum = variable.MemoryUsageAlarmKeepRecordNum.Load()
}

func (record *memoryUsageAlarm) initMemoryUsageAlarmRecord() {
	record.memoryUsageAlarmRatio = variable.MemoryUsageAlarmRatio.Load()
	record.memoryUsageAlarmKeepRecordNum = variable.MemoryUsageAlarmKeepRecordNum.Load()
	if quota := config.GetGlobalConfig().Performance.ServerMemoryQuota; quota != 0 {
		record.serverMemoryQuota = quota
		record.isServerMemoryQuotaSet = true
	} else {
		record.serverMemoryQuota, record.err = memory.MemTotal()
		if record.err != nil {
			logutil.BgLogger().Error("get system total memory fail", zap.Error(record.err))
			return
		}
		record.isServerMemoryQuotaSet = false
	}
	record.lastCheckTime = time.Time{}
	record.lastRecordMemUsed = uint64(float64(record.serverMemoryQuota) * record.memoryUsageAlarmRatio)
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
	instanceStats := &runtime.MemStats{}
	runtime.ReadMemStats(instanceStats)
	if record.isServerMemoryQuotaSet {
		memoryUsage = instanceStats.HeapAlloc
	} else {
		memoryUsage, record.err = memory.MemUsed()
		if record.err != nil {
			logutil.BgLogger().Error("get system memory usage fail", zap.Error(record.err))
			return
		}
	}

	// TODO: Consider NextGC to record SQLs.
	if record.needRecord(memoryUsage) {
		record.lastCheckTime = time.Now()
		record.lastRecordMemUsed = memoryUsage
		record.doRecord(memoryUsage, instanceStats.HeapAlloc, sm)
		record.tryRemoveNoNeedRecords()
	}
}

func (record *memoryUsageAlarm) needRecord(memoryUsage uint64) bool {
	// At least 60 seconds between two recordings that memory usage is less than threshold (default 70% system memory).
	// If the memory is still exceeded, only records once.
	// If the memory used ratio recorded this time is 0.1 higher than last time, we will force record this time.
	if float64(memoryUsage) <= float64(record.serverMemoryQuota)*record.memoryUsageAlarmRatio {
		return false
	}

	interval := time.Since(record.lastCheckTime)
	memDiff := int64(memoryUsage) - int64(record.lastRecordMemUsed)
	if interval > 5*time.Second {
		logutil.BgLogger().Warn("Record Memory Information.", zap.String("record time", time.Now().String()), zap.String("last record time", record.lastCheckTime.String()))
		return true
	}
	if float64(memDiff) > 0.1*float64(record.serverMemoryQuota) {
		logutil.BgLogger().Warn("Record Memory Information.", zap.String("record memory", strconv.FormatUint(memoryUsage, 10)), zap.String("last record memory", strconv.FormatUint(record.lastRecordMemUsed, 10)))
		return true
	}
	return false
}

func (record *memoryUsageAlarm) doRecord(memUsage uint64, instanceMemoryUsage uint64, sm util.SessionManager) {
	fields := make([]zap.Field, 0, 6)
	fields = append(fields, zap.Bool("is server-memory-quota set", record.isServerMemoryQuotaSet))
	if record.isServerMemoryQuotaSet {
		fields = append(fields, zap.Any("server-memory-quota", record.serverMemoryQuota))
		fields = append(fields, zap.Any("tidb-server memory usage", memUsage))
	} else {
		fields = append(fields, zap.Any("system memory total", record.serverMemoryQuota))
		fields = append(fields, zap.Any("system memory usage", memUsage))
		fields = append(fields, zap.Any("tidb-server memory usage", instanceMemoryUsage))
	}
	fields = append(fields, zap.Any("memory-usage-alarm-ratio", record.memoryUsageAlarmRatio))
	fields = append(fields, zap.Any("record path", record.baseRecordDir))
	logutil.BgLogger().Warn("tidb-server has the risk of OOM. Running SQLs and heap profile will be recorded in record path", fields...)
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

func (record *memoryUsageAlarm) tryRemoveNoNeedRecords() {
	filename := &record.lastRecordDirName
	for len(*filename) > int(record.memoryUsageAlarmKeepRecordNum) {
		err := os.RemoveAll((*filename)[0])
		if err != nil {
			logutil.BgLogger().Error("remove temp files failed", zap.Error(err))
		}
		*filename = (*filename)[1:]
	}
}

func getRelevantSystemVariableBuf() string {
	var buf strings.Builder
	buf.WriteString("System variables : \n")
	buf.WriteString(fmt.Sprintf("oom-action: %v \n", config.GetGlobalConfig().OOMAction))
	buf.WriteString(fmt.Sprintf("mem-quota-query : %v \n", config.GetGlobalConfig().MemQuotaQuery))
	buf.WriteString(fmt.Sprintf("server-memory-quota : %v \n", config.GetGlobalConfig().Performance.ServerMemoryQuota))
	buf.WriteString("\n")
	return buf.String()
}

func getCurrentAnalyzePlan(info *util.ProcessInfo) string {
	var buf strings.Builder
	rows := info.CurrentAnalyzeRows(info.Plan, info.RuntimeStatsColl)
	buf.WriteString(fmt.Sprintf("|%v|%v|%v|%v|%v|%v|%v|%v|%v|", "id", "estRows", "actRows", "task", "access object", "execution info", "operator info", "memory", "disk"))
	for _, row := range rows {
		buf.WriteString(fmt.Sprintf("\n|%v|%v|%v|%v|%v|%v|%v|%v|%v|", row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8]))
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

func (record *memoryUsageAlarm) getTop10SqlInfo(cmp func(i, j *util.ProcessInfo) bool, pinfo []*util.ProcessInfo) strings.Builder {
	slices.SortFunc(pinfo, cmp)
	list := pinfo
	if len(list) > 10 {
		list = list[:10]
	}
	var buf strings.Builder
	for i, info := range list {
		buf.WriteString(fmt.Sprintf("SQL %v: \n", i))
		fields := util.GenLogFields(record.lastCheckTime.Sub(info.Time), info, false, true)
		fields = append(fields, info.OomAlarmVariablesInfo...)
		fields = append(fields, zap.String("current_analyze_plan", getCurrentAnalyzePlan(info)))
		for _, field := range fields {
			switch field.Type {
			case zapcore.StringType:
				buf.WriteString(fmt.Sprintf("%v: %v", field.Key, field.String))
			case zapcore.Uint8Type, zapcore.Uint16Type, zapcore.Uint32Type, zapcore.Uint64Type:
				buf.WriteString(fmt.Sprintf("%v: %v", field.Key, uint64(field.Integer)))
			case zapcore.Int8Type, zapcore.Int16Type, zapcore.Int32Type, zapcore.Int64Type:
				buf.WriteString(fmt.Sprintf("%v: %v", field.Key, field.Integer))
			case zapcore.BoolType:
				buf.WriteString(fmt.Sprintf("%v: %v", field.Key, field.Integer == 1))
			}
			buf.WriteString("\n")
		}
	}
	buf.WriteString("\n")
	return buf
}

func (record *memoryUsageAlarm) getTop10SqlInfoByMemoryUsage(pinfo []*util.ProcessInfo) strings.Builder {
	return record.getTop10SqlInfo(func(i, j *util.ProcessInfo) bool {
		return i.StmtCtx.MemTracker.MaxConsumed() > j.StmtCtx.MemTracker.MaxConsumed()
	}, pinfo)
}

func (record *memoryUsageAlarm) getTop10SqlInfoByCostTime(pinfo []*util.ProcessInfo) strings.Builder {
	return record.getTop10SqlInfo(func(i, j *util.ProcessInfo) bool {
		return i.Time.Before(j.Time)
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
	if _, err = f.WriteString(getRelevantSystemVariableBuf()); err != nil {
		logutil.BgLogger().Error("write oom record file fail", zap.Error(err))
	}
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
