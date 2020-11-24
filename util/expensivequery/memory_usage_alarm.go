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
// See the License for the specific language governing permissions and
// limitations under the License.

package expensivequery

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"runtime"
	rpprof "runtime/pprof"
	"sort"
	"strings"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/disk"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/memory"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

type memoryUsageAlarm struct {
	err                    error
	isServerMemoryQuotaSet bool
	serverMemoryQuota      uint64
	lastCheckTime          time.Time

	tmpDir              string
	lastLogFileName     []string
	lastProfileFileName [][]string // heap, goroutine
}

func initMemoryUsageAlarmRecord() (record *memoryUsageAlarm) {
	record = &memoryUsageAlarm{}
	if alert := config.GetGlobalConfig().Performance.MemoryUsageAlarmRatio; alert == 0 || alert == 1 {
		record.err = errors.New("close memory usage alarm recorder")
		return
	}
	if quota := config.GetGlobalConfig().Performance.ServerMemoryQuota; quota != 0 {
		record.serverMemoryQuota = quota
		record.isServerMemoryQuotaSet = true
	} else {
		// TODO: Get the memory info in container directly.
		record.serverMemoryQuota, record.err = memory.MemTotal()
		if record.err != nil {
			logutil.BgLogger().Error("get system total memory fail", zap.Error(record.err))
			return
		}
		record.isServerMemoryQuotaSet = false
	}
	record.lastCheckTime = time.Time{}
	record.tmpDir = filepath.Join(config.GetGlobalConfig().TempStoragePath, "record")
	record.lastProfileFileName = make([][]string, 2)
	// Read last records
	files, err := ioutil.ReadDir(record.tmpDir)
	if err != nil {
		return record
	}
	for _, f := range files {
		name := filepath.Join(record.tmpDir, f.Name())
		if strings.Contains(f.Name(), "running_sql") {
			record.lastLogFileName = append(record.lastLogFileName, name)
		}
		if strings.Contains(f.Name(), "heap") {
			record.lastProfileFileName[0] = append(record.lastProfileFileName[0], name)
		}
		if strings.Contains(f.Name(), "goroutine") {
			record.lastProfileFileName[1] = append(record.lastProfileFileName[1], name)
		}
	}

	return record
}

// If Performance.ServerMemoryQuota is set, use `ServerMemoryQuota * MemoryUsageAlarmRatio` to check oom risk.
// If Performance.ServerMemoryQuota is not set, use `system total memory size * MemoryUsageAlarmRatio` to check oom risk.
func (record *memoryUsageAlarm) alarm4ExcessiveMemUsage(sm util.SessionManager) {
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
	if float64(memoryUsage) > float64(record.serverMemoryQuota)*config.GetGlobalConfig().Performance.MemoryUsageAlarmRatio {
		// At least ten seconds between two recordings that memory usage is less than threshold (default 80% system memory).
		// If the memory is still exceeded, only records once.
		interval := time.Since(record.lastCheckTime)
		record.lastCheckTime = time.Now()
		if interval > 10*time.Second {
			record.doRecord(memoryUsage, instanceStats.HeapAlloc, sm)
		}
	}
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
	fields = append(fields, zap.Any("memory-usage-alarm-ratio", config.GetGlobalConfig().Performance.MemoryUsageAlarmRatio))
	fields = append(fields, zap.Any("record path", record.tmpDir))

	logutil.BgLogger().Warn("tidb-server has the risk of OOM. Running SQLs and heap profile will be recorded in record path", fields...)

	if record.err = disk.CheckAndInitTempDir(); record.err != nil {
		return
	}
	record.recordSQL(sm)
	record.recordProfile()

	tryRemove := func(filename *[]string) {
		// Keep the last 5 files
		for len(*filename) > 5 {
			err := os.Remove((*filename)[0])
			if err != nil {
				logutil.BgLogger().Error("remove temp files failed", zap.Error(err))
				return
			}
			*filename = (*filename)[1:]
		}
	}
	tryRemove(&record.lastLogFileName)
	for i := range record.lastProfileFileName {
		tryRemove(&record.lastProfileFileName[i])
	}
}

func (record *memoryUsageAlarm) recordSQL(sm util.SessionManager) {
	processInfo := sm.ShowProcessList()
	pinfo := make([]*util.ProcessInfo, 0, len(processInfo))
	for _, info := range processInfo {
		if len(info.Info) != 0 {
			pinfo = append(pinfo, info)
		}
	}

	fileName := filepath.Join(record.tmpDir, "running_sql"+record.lastCheckTime.Format(time.RFC3339))
	record.lastLogFileName = append(record.lastLogFileName, fileName)
	f, err := os.Create(fileName)
	if err != nil {
		logutil.BgLogger().Error("create oom record file fail", zap.Error(err))
		return
	}
	defer func() {
		err := f.Close()
		if err != nil {
			logutil.BgLogger().Error("close oom record file fail", zap.Error(err))
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
			fields := genLogFields(record.lastCheckTime.Sub(info.Time), info)
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
		buf.WriteString("\n")
		_, err = f.WriteString(buf.String())
	}

	_, err = f.WriteString("The 10 SQLs with the most memory usage for OOM analysis\n")
	printTop10(func(i, j int) bool {
		return pinfo[i].StmtCtx.MemTracker.MaxConsumed() > pinfo[j].StmtCtx.MemTracker.MaxConsumed()
	})

	_, err = f.WriteString("The 10 SQLs with the most time usage for OOM analysis\n")
	printTop10(func(i, j int) bool {
		return pinfo[i].Time.Before(pinfo[j].Time)
	})
}

func (record *memoryUsageAlarm) recordProfile() {
	items := []struct {
		name  string
		debug int
	}{
		{name: "heap"},
		{name: "goroutine", debug: 2},
	}
	for i, item := range items {
		fileName := filepath.Join(record.tmpDir, item.name+record.lastCheckTime.Format(time.RFC3339))
		record.lastProfileFileName[i] = append(record.lastProfileFileName[i], fileName)
		f, err := os.Create(fileName)
		if err != nil {
			logutil.BgLogger().Error(fmt.Sprintf("create %v profile file fail", item.name), zap.Error(err))
			return
		}
		defer func() {
			err := f.Close()
			if err != nil {
				logutil.BgLogger().Error(fmt.Sprintf("close %v profile file fail", item.name), zap.Error(err))
			}
		}()
		p := rpprof.Lookup(item.name)
		err = p.WriteTo(f, item.debug)
		if err != nil {
			logutil.BgLogger().Error(fmt.Sprintf("write %v profile file fail", item.name), zap.Error(err))
			return
		}
	}
}
