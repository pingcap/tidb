// Copyright 2016 PingCAP, Inc.
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

package util

import (
	"runtime"
	"strings"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/parser"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
)

const (
	// DefaultMaxRetries indicates the max retry count.
	DefaultMaxRetries = 30
	// RetryInterval indicates retry interval.
	RetryInterval uint64 = 500
	// GCTimeFormat is the format that gc_worker used to store times.
	GCTimeFormat = "20060102-15:04:05 -0700"
)

// RunWithRetry will run the f with backoff and retry.
// retryCnt: Max retry count
// backoff: When run f failed, it will sleep backoff * triedCount time.Millisecond.
// Function f should have two return value. The first one is an bool which indicate if the err if retryable.
// The second is if the f meet any error.
func RunWithRetry(retryCnt int, backoff uint64, f func() (bool, error)) (err error) {
	for i := 1; i <= retryCnt; i++ {
		var retryAble bool
		retryAble, err = f()
		if err == nil || !retryAble {
			return errors.Trace(err)
		}
		sleepTime := time.Duration(backoff*uint64(i)) * time.Millisecond
		time.Sleep(sleepTime)
	}
	return errors.Trace(err)
}

// GetStack gets the stacktrace.
func GetStack() []byte {
	const size = 4096
	buf := make([]byte, size)
	stackSize := runtime.Stack(buf, false)
	buf = buf[:stackSize]
	return buf
}

// WithRecovery wraps goroutine startup call with force recovery.
// it will dump current goroutine stack into log if catch any recover result.
//   exec:      execute logic function.
//   recoverFn: handler will be called after recover and before dump stack, passing `nil` means noop.
func WithRecovery(exec func(), recoverFn func(r interface{})) {
	defer func() {
		r := recover()
		if recoverFn != nil {
			recoverFn(r)
		}
		if r != nil {
			logutil.BgLogger().Error("panic in the recoverable goroutine",
				zap.Reflect("r", r),
				zap.Stack("stack trace"))
		}
	}()
	exec()
}

// CompatibleParseGCTime parses a string with `GCTimeFormat` and returns a time.Time. If `value` can't be parsed as that
// format, truncate to last space and try again. This function is only useful when loading times that saved by
// gc_worker. We have changed the format that gc_worker saves time (removed the last field), but when loading times it
// should be compatible with the old format.
func CompatibleParseGCTime(value string) (time.Time, error) {
	t, err := time.Parse(GCTimeFormat, value)

	if err != nil {
		// Remove the last field that separated by space
		parts := strings.Split(value, " ")
		prefix := strings.Join(parts[:len(parts)-1], " ")
		t, err = time.Parse(GCTimeFormat, prefix)
	}

	if err != nil {
		err = errors.Errorf("string \"%v\" doesn't has a prefix that matches format \"%v\"", value, GCTimeFormat)
	}
	return t, err
}

const (
	// syntaxErrorPrefix is the common prefix for SQL syntax error in TiDB.
	syntaxErrorPrefix = "You have an error in your SQL syntax; check the manual that corresponds to your TiDB version for the right syntax to use"
)

// SyntaxError converts parser error to TiDB's syntax error.
func SyntaxError(err error) error {
	if err == nil {
		return nil
	}
	logutil.BgLogger().Error("syntax error", zap.Error(err))

	// If the error is already a terror with stack, pass it through.
	if errors.HasStack(err) {
		cause := errors.Cause(err)
		if _, ok := cause.(*terror.Error); ok {
			return err
		}
	}

	return parser.ErrParse.GenWithStackByArgs(syntaxErrorPrefix, err.Error())
}

// SyntaxWarn converts parser warn to TiDB's syntax warn.
func SyntaxWarn(err error) error {
	if err == nil {
		return nil
	}
	return parser.ErrParse.GenWithStackByArgs(syntaxErrorPrefix, err.Error())
}

const (
	// InformationSchemaName is the `INFORMATION_SCHEMA` database name.
	InformationSchemaName = "INFORMATION_SCHEMA"
	// InformationSchemaLowerName is the `INFORMATION_SCHEMA` database lower name.
	InformationSchemaLowerName = "information_schema"
	// PerformanceSchemaName is the `PERFORMANCE_SCHEMA` database name.
	PerformanceSchemaName = "PERFORMANCE_SCHEMA"
	// PerformanceSchemaLowerName is the `PERFORMANCE_SCHEMA` database lower name.
	PerformanceSchemaLowerName = "performance_schema"
)

// IsMemOrSysDB uses to check whether dbLowerName is memory database or system database.
func IsMemOrSysDB(dbLowerName string) bool {
	switch dbLowerName {
	case InformationSchemaLowerName, PerformanceSchemaLowerName, mysql.SystemDB:
		return true
	}
	return false
}

// ParseLocationLabels parses `LocationLabels` from a given string.
func ParseLocationLabels(locationLabels string) (labels []*metapb.StoreLabel) {
	locationLabels = strings.Trim(locationLabels, " \t")
	if len(locationLabels) == 0 {
		return
	}
	for _, kvPair := range strings.Split(locationLabels, ",") {
		if len(kvPair) == 0 {
			continue
		}
		v := strings.Split(kvPair, "=")
		if len(v) != 2 {
			logutil.BgLogger().Warn("Ignore invalid locationLabels", zap.String("labels", locationLabels))
			labels = make([]*metapb.StoreLabel, 0)
			return
		}
		labels = append(labels, &metapb.StoreLabel{
			Key:   strings.Trim(v[0], " \t"),
			Value: strings.Trim(v[1], " \t"),
		})
	}
	return
}
