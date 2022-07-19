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
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/disk"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/memory"
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
}

type memoryUsageAlarm struct {
	err                    error
	initialized            bool
	isServerMemoryQuotaSet bool
	serverMemoryQuota      uint64
	memoryUsageAlarmRatio  float64
	lastCheckTime          time.Time

	tmpDir              string
	lastLogFileName     []string
	lastProfileFileName [][]string // heap, goroutine

	s3Conf s3Config
}

func (record *memoryUsageAlarm) initS3Config() {
	if recordToS3 := config.GetGlobalConfig().S3.EnableUploadOOMRecord; recordToS3 {
		record.s3Conf.accessKey = config.GetGlobalConfig().S3.AccessKey
		record.s3Conf.secretKey = config.GetGlobalConfig().S3.SecretKey
		record.s3Conf.endPoint = config.GetGlobalConfig().S3.EndPoint
		record.s3Conf.bucketName = config.GetGlobalConfig().S3.BucketName
		record.s3Conf.disableSSL = config.GetGlobalConfig().S3.DisableSSL
		record.s3Conf.s3ForcePathStyle = config.GetGlobalConfig().S3.S3ForcePathStyle
		record.s3Conf.enableUploadOOMRecord = config.GetGlobalConfig().S3.EnableUploadOOMRecord
	}
}

func (record *memoryUsageAlarm) uploadFileToS3(filenames []string) {
	if record.s3Conf.enableUploadOOMRecord && record.initialized && time.Since(record.lastCheckTime) > 600*time.Second {
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
			file, err := os.OpenFile(filename, os.O_RDONLY, 0600)
			if err != nil {
				logutil.BgLogger().Error("open record file fail", zap.Error(err))
				return
			}

			if _, err = uploader.Upload(&s3manager.UploadInput{
				Bucket: aws.String(record.s3Conf.bucketName),
				Key:    aws.String(filename),
				Body:   file,
			}); err != nil {
				logutil.BgLogger().Error("upload to s3 fail", zap.Error(err))
				return
			}

			//nolint: revive
			defer func() {
				if err := file.Close(); err != nil {
					logutil.BgLogger().Error("close record file fail", zap.Error(err))
					return
				}
			}()
		}
	}
}

func (record *memoryUsageAlarm) initMemoryUsageAlarmRecord() {
	record.initS3Config()
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
	}
	record.initialized = true
}

// If Performance.ServerMemoryQuota is set, use `ServerMemoryQuota * MemoryUsageAlarmRatio` to check oom risk.
// If Performance.ServerMemoryQuota is not set, use `system total memory size * MemoryUsageAlarmRatio` to check oom risk.
func (record *memoryUsageAlarm) alarm4ExcessiveMemUsage(sm util.SessionManager) {
	if record.memoryUsageAlarmRatio <= 0.0 || record.memoryUsageAlarmRatio >= 1.0 {
		return
	}
	if !record.initialized {
		record.initMemoryUsageAlarmRecord()
		if record.err != nil {
			return
		}
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
		// At least ten seconds between two recordings that memory usage is less than threshold (default 80% system memory).
		// If the memory is still exceeded, only records once.
		interval := time.Since(record.lastCheckTime)
		if interval > 10*time.Second {
			record.doRecord(memoryUsage, instanceStats.HeapAlloc, sm)
		}
		record.lastCheckTime = time.Now()
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

	recordSQLFile := record.recordSQL(sm)
	recordProfileFiles := record.recordProfile()

	if recordSQLFile != "" && len(recordProfileFiles) != 0 {
		recordFiles := make([]string, 0, len(recordProfileFiles)+1)
		recordFiles = append(recordFiles, recordSQLFile)
		recordFiles = append(recordFiles, recordProfileFiles...)
		record.uploadFileToS3(recordFiles)
	} else {
		logutil.BgLogger().Error("get record file names fail")
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
	for i := range record.lastProfileFileName {
		tryRemove(&record.lastProfileFileName[i])
	}
}

func (record *memoryUsageAlarm) recordSQL(sm util.SessionManager) string {
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
		return ""
	}
	defer func() {
		err := f.Close()
		if err != nil {
			logutil.BgLogger().Error("close oom record file fail", zap.Error(err))
		}
	}()
	printTop10 := func(cmp func(i, j *util.ProcessInfo) bool) {
		slices.SortFunc(pinfo, cmp)
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
	printTop10(func(i, j *util.ProcessInfo) bool {
		return i.StmtCtx.MemTracker.MaxConsumed() > j.StmtCtx.MemTracker.MaxConsumed()
	})

	_, err = f.WriteString("The 10 SQLs with the most time usage for OOM analysis\n")
	printTop10(func(i, j *util.ProcessInfo) bool {
		return i.Time.Before(j.Time)
	})
	return fileName
}

func (record *memoryUsageAlarm) recordProfile() []string {
	items := []struct {
		name  string
		debug int
	}{
		{name: "heap"},
		{name: "goroutine", debug: 2},
	}
	profileFileNames := make([]string, len(items))
	for i, item := range items {
		fileName := filepath.Join(record.tmpDir, item.name+record.lastCheckTime.Format(time.RFC3339))
		profileFileNames = append(profileFileNames, fileName)
		record.lastProfileFileName[i] = append(record.lastProfileFileName[i], fileName)
		f, err := os.Create(fileName)
		if err != nil {
			logutil.BgLogger().Error(fmt.Sprintf("create %v profile file fail", item.name), zap.Error(err))
			return profileFileNames
		}
		//nolint: revive
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
			return profileFileNames
		}
	}
	return profileFileNames
}
