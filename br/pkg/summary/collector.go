// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package summary

import (
	"fmt"
	"math"
	"strings"
	"sync"
	"time"

	"github.com/docker/go-units"
	"github.com/pingcap/log"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

const (
	// BackupUnit tells summary in backup
	BackupUnit = "backup"
	// RestoreUnit tells summary in restore
	RestoreUnit = "restore"

	// TotalKV is a field we collect during backup/restore
	TotalKV = "total kv"
	// TotalBytes is a field we collect during backup/restore
	TotalBytes = "total bytes"
	// BackupDataSize is a field we collect after backup finish
	BackupDataSize = "backup data size(after compressed)"
	// RestoreDataSize is a field we collection after restore finish
	RestoreDataSize = "restore data size(after compressed)"
)

// LogCollector collects infos into summary log.
type LogCollector interface {
	SetUnit(unit string)

	CollectSuccessUnit(name string, unitCount int, arg interface{})

	CollectFailureUnit(name string, reason error)

	CollectDuration(name string, t time.Duration)

	CollectInt(name string, t int)

	CollectUInt(name string, t uint64)

	SetSuccessStatus(success bool)

	CollectToStr(name string) string

	Summary(name string)
}

type logFunc func(msg string, fields ...zap.Field)

var collector LogCollector = NewLogCollector(log.Info)

// InitCollector initilize global collector instance.
func InitCollector( // revive:disable-line:flag-parameter
	hasLogFile bool,
) {
	logF := log.L().Info
	if hasLogFile {
		conf := new(log.Config)
		// Always duplicate summary to stdout.
		logger, _, err := log.InitLogger(conf)
		if err == nil {
			logF = func(msg string, fields ...zap.Field) {
				logger.Info(msg, fields...)
				log.Info(msg, fields...)
			}
		}
	}
	collector = NewLogCollector(logF)
}

type logCollector struct {
	mu               sync.Mutex
	unit             string
	successUnitCount int
	failureUnitCount int
	successCosts     map[string]time.Duration
	successData      map[string]uint64
	failureReasons   map[string]error
	durations        map[string]time.Duration
	ints             map[string]int
	uints            map[string]uint64
	successStatus    bool
	startTime        time.Time

	log logFunc
}

// NewLogCollector returns a new LogCollector.
func NewLogCollector(log logFunc) LogCollector {
	return &logCollector{
		successUnitCount: 0,
		failureUnitCount: 0,
		successCosts:     make(map[string]time.Duration),
		successData:      make(map[string]uint64),
		failureReasons:   make(map[string]error),
		durations:        make(map[string]time.Duration),
		ints:             make(map[string]int),
		uints:            make(map[string]uint64),
		log:              log,
		startTime:        time.Now(),
	}
}

func (tc *logCollector) SetUnit(unit string) {
	tc.mu.Lock()
	defer tc.mu.Unlock()
	tc.unit = unit
}

func (tc *logCollector) CollectSuccessUnit(name string, unitCount int, arg interface{}) {
	tc.mu.Lock()
	defer tc.mu.Unlock()

	switch v := arg.(type) {
	case time.Duration:
		tc.successUnitCount += unitCount
		tc.successCosts[name] += v
	case uint64:
		tc.successData[name] += v
	}
}

func (tc *logCollector) CollectFailureUnit(name string, reason error) {
	tc.mu.Lock()
	defer tc.mu.Unlock()
	if _, ok := tc.failureReasons[name]; !ok {
		tc.failureReasons[name] = reason
		tc.failureUnitCount++
	}
}

func (tc *logCollector) CollectDuration(name string, t time.Duration) {
	tc.mu.Lock()
	defer tc.mu.Unlock()
	tc.durations[name] += t
}

func (tc *logCollector) CollectInt(name string, t int) {
	tc.mu.Lock()
	defer tc.mu.Unlock()
	tc.ints[name] += t
}

func (tc *logCollector) CollectUInt(name string, t uint64) {
	tc.mu.Lock()
	defer tc.mu.Unlock()
	tc.uints[name] += t
}

func (tc *logCollector) SetSuccessStatus(success bool) {
	tc.mu.Lock()
	defer tc.mu.Unlock()
	tc.successStatus = success
}

func logKeyFor(key string) string {
	return strings.ReplaceAll(key, " ", "-")
}

func (tc *logCollector) summaryDetail(needClean bool) []zap.Field {
	tc.mu.Lock()
	defer func() {
		if needClean {
			tc.durations = make(map[string]time.Duration)
			tc.ints = make(map[string]int)
			tc.successCosts = make(map[string]time.Duration)
			tc.failureReasons = make(map[string]error)
			tc.ints = make(map[string]int)
			tc.uints = make(map[string]uint64)
		}
		tc.mu.Unlock()
	}()

	logFields := make([]zap.Field, 0, len(tc.durations)+len(tc.ints)+3)
	logFields = append(logFields,
		zap.Int("total-ranges", tc.failureUnitCount+tc.successUnitCount),
		zap.Int("ranges-succeed", tc.successUnitCount),
		zap.Int("ranges-failed", tc.failureUnitCount),
	)

	for key, val := range tc.durations {
		logFields = append(logFields, zap.Duration(logKeyFor(key), val))
	}
	for key, val := range tc.ints {
		logFields = append(logFields, zap.Int(logKeyFor(key), val))
	}
	for key, val := range tc.uints {
		logFields = append(logFields, zap.Uint64(logKeyFor(key), val))
	}

	if len(tc.failureReasons) != 0 || !tc.successStatus {
		for unitName, reason := range tc.failureReasons {
			logFields = append(logFields, zap.String("unit-name", unitName), zap.Error(reason))
		}
		return logFields
	}

	totalDureTime := time.Since(tc.startTime)
	logFields = append(logFields, zap.Duration("total-take", totalDureTime))
	for name, data := range tc.successData {
		if name == TotalBytes {
			logFields = append(logFields,
				zap.String("total-kv-size", units.HumanSize(float64(data))),
				zap.String("average-speed", units.HumanSize(float64(data)/totalDureTime.Seconds())+"/s"))
			continue
		}
		if name == BackupDataSize {
			if tc.failureUnitCount+tc.successUnitCount == 0 {
				logFields = append(logFields, zap.String("Result", "Nothing to bakcup"))
			} else {
				logFields = append(logFields,
					zap.String(BackupDataSize, units.HumanSize(float64(data))))
			}
			continue
		}
		if name == RestoreDataSize {
			if tc.failureUnitCount+tc.successUnitCount == 0 {
				logFields = append(logFields, zap.String("Result", "Nothing to restore"))
			} else {
				logFields = append(logFields,
					zap.String(RestoreDataSize, units.HumanSize(float64(data))))
			}
			continue
		}
		logFields = append(logFields, zap.Uint64(logKeyFor(name), data))
	}

	return logFields
}

func (tc *logCollector) Summary(name string) {
	logFields := tc.summaryDetail(true)
	tc.log(name+" success summary", logFields...)
}

func field2String(field zapcore.Field) string {
	switch field.Type {
	case zapcore.StringType:
		return field.String
	case zapcore.Int64Type, zapcore.Int32Type, zapcore.Uint32Type, zapcore.Uint64Type:
		return fmt.Sprintf("%v", field.Integer)
	case zapcore.Float64Type:
		return fmt.Sprintf("%v", math.Float64frombits(uint64(field.Integer)))
	case zapcore.StringerType:
		return field.Interface.(fmt.Stringer).String()
	case zapcore.DurationType:
		return ((time.Duration)(field.Integer)).String()
	default:
		return ""
	}
}

func (tc *logCollector) CollectToStr(cmdName string) string {
	var logFields []zap.Field

	logFields = append(logFields, zap.String(cmdName, "success"))
	summaryDetail := tc.summaryDetail(false)
	logFields = append(logFields, summaryDetail...)

	var outstr string
	for _, f := range logFields {
		k := f.Key
		v := field2String(f)
		if len(k) > 0 && len(v) > 0 {
			outstr = outstr + "[\"" + k + "\"=" + v + "]"
		}
	}
	return outstr
}

// SetLogCollector allow pass LogCollector outside.
func SetLogCollector(l LogCollector) {
	collector = l
}
