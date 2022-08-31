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

package expensivequery

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	rpprof "runtime/pprof"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/disk"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/memory"
	"github.com/shirou/gopsutil/v3/process"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"golang.org/x/exp/slices"
)

type s3Config struct {
	accessKey             string
	secretKey             string
	endPoint              string
	regionName            string
	bucketName            string
	disableSSL            bool
	s3ForcePathStyle      bool
	enableUploadOOMRecord bool
	timeoutSeconds        int
}

type memoryUsageAlarm struct {
	lastCheckTime                         time.Time
	err                                   error
	tmpDir                                string
	lastProfileFileName                   [][]string
	lastSystemInfoFileName                []string
	lastLogFileName                       []string
	s3Conf                                s3Config
	memoryUsageAlarmIntervalSeconds       uint64
	autoGcRatio                           float64
	memoryUsageAlarmRatio                 float64
	serverMemoryQuota                     uint64
	memoryUsageAlarmDesensitizationEnable bool
	isServerMemoryQuotaSet                bool
	initialized                           bool
	memoryUsageAlarmTruncationEnable      bool
}

func (record *memoryUsageAlarm) initS3Config() {
	record.s3Conf.accessKey = config.GetGlobalConfig().S3.AccessKey
	record.s3Conf.secretKey = config.GetGlobalConfig().S3.SecretKey
	record.s3Conf.endPoint = config.GetGlobalConfig().S3.EndPoint
	record.s3Conf.regionName = config.GetGlobalConfig().S3.RegionName
	record.s3Conf.bucketName = config.GetGlobalConfig().S3.BucketName
	record.s3Conf.disableSSL = config.GetGlobalConfig().S3.DisableSSL
	record.s3Conf.s3ForcePathStyle = config.GetGlobalConfig().S3.S3ForcePathStyle
	record.s3Conf.enableUploadOOMRecord = config.GetGlobalConfig().S3.EnableUploadOOMRecord
	record.s3Conf.timeoutSeconds = config.GetGlobalConfig().S3.TimeoutSeconds
}

func (record *memoryUsageAlarm) uploadFileToS3(filename string, uploader *s3manager.Uploader, timeout time.Duration) {
	file, err := os.OpenFile(filename, os.O_RDONLY, 0600)
	if err != nil {
		logutil.BgLogger().Error("open record file fail", zap.Error(err))
		return
	}
	ctx, cancel := context.WithTimeout(aws.BackgroundContext(), timeout)
	if _, err = uploader.UploadWithContext(ctx, &s3manager.UploadInput{
		Bucket: aws.String(record.s3Conf.bucketName),
		Key:    aws.String(filename),
		Body:   file,
	}); err != nil {
		cancel()
		logutil.BgLogger().Error("upload to s3 fail", zap.Error(err))
		return
	}
	defer func() {
		cancel()
		if err := file.Close(); err != nil {
			logutil.BgLogger().Error("close record file fail", zap.Error(err))
		}
	}()
}

func (record *memoryUsageAlarm) createSessionAndUploadFilesToS3(filenames []string) {
	if record.initialized {
		sess, err := session.NewSession(&aws.Config{
			Credentials:      credentials.NewStaticCredentials(record.s3Conf.accessKey, record.s3Conf.secretKey, ""),
			Endpoint:         aws.String(record.s3Conf.endPoint),
			Region:           aws.String(record.s3Conf.regionName),
			DisableSSL:       aws.Bool(record.s3Conf.disableSSL),
			S3ForcePathStyle: aws.Bool(record.s3Conf.s3ForcePathStyle),
		})
		if err != nil {
			logutil.BgLogger().Error("create s3 new session fail", zap.Error(err))
			return
		}
		uploader := s3manager.NewUploader(sess)
		for _, filename := range filenames {
			go record.uploadFileToS3(filename, uploader, time.Second*time.Duration(record.s3Conf.timeoutSeconds))
		}
	}
}

func (record *memoryUsageAlarm) initMemoryUsageAlarmRecord() {
	if recordToS3 := config.GetGlobalConfig().S3.EnableUploadOOMRecord; recordToS3 {
		record.initS3Config()
	}
	record.memoryUsageAlarmRatio = variable.MemoryUsageAlarmRatio.Load()
	record.memoryUsageAlarmDesensitizationEnable = variable.MemoryUsageAlarmDesensitizationEnable.Load()
	record.memoryUsageAlarmTruncationEnable = variable.MemoryUsageAlarmTruncationEnable.Load()
	record.autoGcRatio = variable.AutoGcMemoryRatio.Load()
	record.memoryUsageAlarmIntervalSeconds = variable.MemoryUsageAlarmIntervalSeconds.Load()
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
	record.tmpDir = filepath.Join(config.GetGlobalConfig().TempStoragePath, "record")
	if record.err = disk.CheckAndCreateDir(record.tmpDir); record.err != nil {
		return
	}
	record.lastProfileFileName = make([][]string, 2)
	// Read last records
	files, err := os.ReadDir(record.tmpDir)
	if err != nil {
		record.err = err
		return
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
		if strings.Contains(f.Name(), "system_info") {
			record.lastSystemInfoFileName = append(record.lastSystemInfoFileName, name)
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
	if float64(memoryUsage) > float64(record.serverMemoryQuota)*record.memoryUsageAlarmRatio {
		// At least ten seconds between two recordings that memory usage is less than threshold (default 70% system memory).
		// If the memory is still exceeded, only records once.

		if float64(memoryUsage) > float64(record.serverMemoryQuota)*record.autoGcRatio {
			// if memory usage is more than threshold (default 80% system memory), doing GC actively to avoid OOM risk.
			// revive:disable:call-to-gc
			runtime.GC()
		}
		interval := time.Since(record.lastCheckTime)
		if interval > time.Duration(record.memoryUsageAlarmIntervalSeconds)*time.Second {
			record.lastCheckTime = time.Now()
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
	fields = append(fields, zap.Any("memory-usage-alarm-ratio", record.memoryUsageAlarmRatio))
	fields = append(fields, zap.Any("record path", record.tmpDir))

	logutil.BgLogger().Warn("tidb-server has the risk of OOM. Running SQLs and heap profile will be recorded in record path", fields...)

	if record.err = disk.CheckAndCreateDir(record.tmpDir); record.err != nil {
		return
	}

	var recordSQLFile string
	var recordSystemInfoFile string
	var recordProfileFiles []string
	if recordSQLFile, record.err = record.recordSQL(sm); record.err != nil {
		return
	}
	if recordProfileFiles, record.err = record.recordProfile(); record.err != nil {
		return
	}
	if recordSystemInfoFile, record.err = record.recordSystemInfoFile(); record.err != nil {
		return
	}
	recordFiles := make([]string, 0, len(recordProfileFiles)+2)
	recordFiles = append(recordFiles, recordSQLFile)
	recordFiles = append(recordFiles, recordProfileFiles...)
	recordFiles = append(recordFiles, recordSystemInfoFile)
	if record.s3Conf.enableUploadOOMRecord {
		record.createSessionAndUploadFilesToS3(recordFiles)
	}

	tryRemove := func(filename *[]string) {
		// Keep the last 5 files
		for len(*filename) > 5 {
			err := os.Remove((*filename)[0])
			if err != nil {
				logutil.BgLogger().Error("remove temp files failed", zap.Error(err))
			}
			*filename = (*filename)[1:]
		}
	}
	tryRemove(&record.lastLogFileName)
	tryRemove(&record.lastSystemInfoFileName)
	for i := range record.lastProfileFileName {
		tryRemove(&record.lastProfileFileName[i])
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
	buf.WriteString(fmt.Sprintf("|%v|%v|%v|%v|%v|%v|%v|%v|%v|\n", "id", "estRows", "actRows", "task", "access object", "execution info", "operator info", "memory", "disk"))
	for _, row := range rows {
		buf.WriteString(fmt.Sprintf("|%v|%v|%v|%v|%v|%v|%v|%v|%v|\n", row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8]))
	}
	return buf.String()
}

func (record *memoryUsageAlarm) recordSQL(sm util.SessionManager) (string, error) {
	processInfo := sm.ShowProcessList()
	pinfo := make([]*util.ProcessInfo, 0, len(processInfo))
	for _, info := range processInfo {
		if len(info.Info) != 0 {
			pinfo = append(pinfo, info)
		}
	}

	fileName := filepath.Join(record.tmpDir, "running_sql"+record.lastCheckTime.Format(time.RFC3339))
	println(fileName)
	record.lastLogFileName = append(record.lastLogFileName, fileName)
	f, err := os.Create(fileName)
	if err != nil {
		logutil.BgLogger().Error("create oom record file fail", zap.Error(err))
		return "", err
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

	printTop10 := func(cmp func(i, j *util.ProcessInfo) bool) {
		slices.SortFunc(pinfo, cmp)
		list := pinfo
		if len(list) > 10 {
			list = list[:10]
		}
		var buf strings.Builder
		for i, info := range list {
			buf.WriteString(fmt.Sprintf("SQL %v: \n", i))
			fields := genLogFields(record.lastCheckTime.Sub(info.Time), info, record.memoryUsageAlarmTruncationEnable, record.memoryUsageAlarmDesensitizationEnable)
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
		if _, err = f.WriteString(buf.String()); err != nil {
			logutil.BgLogger().Error("write oom record file fail", zap.Error(err))
		}
	}

	_, err = f.WriteString("The 10 SQLs with the most memory usage for OOM analysis\n")
	printTop10(func(i, j *util.ProcessInfo) bool {
		return i.StmtCtx.MemTracker.MaxConsumed() > j.StmtCtx.MemTracker.MaxConsumed()
	})

	_, err = f.WriteString("The 10 SQLs with the most time usage for OOM analysis\n")
	printTop10(func(i, j *util.ProcessInfo) bool {
		return i.Time.Before(j.Time)
	})
	return fileName, nil
}

type item struct {
	Name  string
	Debug int
}

func (record *memoryUsageAlarm) recordProfile() ([]string, error) {
	items := []item{
		{Name: "heap"},
		{Name: "goroutine", Debug: 2},
	}
	profileFilenames := make([]string, 0, len(items))
	for i, item := range items {
		fileName, err := record.write(i, item)
		if err != nil {
			return profileFilenames, err
		}
		profileFilenames = append(profileFilenames, fileName)
	}
	return profileFilenames, nil
}

func (record *memoryUsageAlarm) write(i int, item item) (string, error) {
	fileName := filepath.Join(record.tmpDir, item.Name+record.lastCheckTime.Format(time.RFC3339))
	record.lastProfileFileName[i] = append(record.lastProfileFileName[i], fileName)
	f, err := os.Create(fileName)
	if err != nil {
		logutil.BgLogger().Error(fmt.Sprintf("create %v profile file fail", item.Name), zap.Error(err))
		return fileName, err
	}

	p := rpprof.Lookup(item.Name)
	err = p.WriteTo(f, item.Debug)
	if err != nil {
		logutil.BgLogger().Error(fmt.Sprintf("write %v profile file fail", item.Name), zap.Error(err))
		return fileName, err
	}

	//nolint: revive
	defer func() {
		err := f.Close()
		if err != nil {
			logutil.BgLogger().Error(fmt.Sprintf("close %v profile file fail", item.Name), zap.Error(err))
		}
	}()
	return fileName, nil
}

func (record *memoryUsageAlarm) recordSystemInfoFile() (string, error) {
	procs, _ := process.Processes()
	pInfos := make([]*process.Process, 0, len(procs))
	for _, proc := range procs {
		if _, err := proc.MemoryInfo(); err == nil {
			pInfos = append(pInfos, proc)
		}
	}
	fileName := filepath.Join(record.tmpDir, "system_info"+record.lastCheckTime.Format(time.RFC3339))
	record.lastSystemInfoFileName = append(record.lastSystemInfoFileName, fileName)
	f, err := os.Create(fileName)
	if err != nil {
		logutil.BgLogger().Error("create oom record file fail", zap.Error(err))
		return "", err
	}
	defer func() {
		err := f.Close()
		if err != nil {
			logutil.BgLogger().Error("close oom record file fail", zap.Error(err))
		}
	}()
	slices.SortFunc(pInfos, func(i, j *process.Process) bool {
		memoryLeft, errLeft := i.MemoryInfo()
		memoryRight, errRight := j.MemoryInfo()
		if errLeft != nil {
			logutil.BgLogger().Error("get system thread memory info failed.", zap.Error(errLeft))
			return false
		}
		if errRight != nil {
			logutil.BgLogger().Error("get system thread memory info failed.", zap.Error(errRight))
			return false
		}
		return memoryLeft.RSS > memoryRight.RSS
	})
	if len(pInfos) > 10 {
		pInfos = pInfos[:10]
	}
	var buf strings.Builder
	var name string
	var memInfo *process.MemoryInfoStat
	for _, pInfo := range pInfos {
		if name, err = pInfo.Name(); err != nil {
			return fileName, err
		}
		if memInfo, err = pInfo.MemoryInfo(); err != nil {
			return fileName, err
		}
		buf.WriteString(fmt.Sprintf("%v: %v", "pid", pInfo.Pid) + "\n")
		buf.WriteString(fmt.Sprintf("%v: %v", "name", name) + "\n")
		buf.WriteString(fmt.Sprintf("%v: %v", "memroy", memInfo.String()) + "\n")
		buf.WriteString("\n")
	}
	if _, err = f.WriteString("The 10 system threads with the most memory usage for OOM analysis\n"); err != nil {
		logutil.BgLogger().Error("write oom record file fail", zap.Error(err))
		return fileName, err
	}

	if _, err = f.WriteString(buf.String()); err != nil {
		logutil.BgLogger().Error("write oom record file fail", zap.Error(err))
		return fileName, err
	}
	return fileName, nil
}
