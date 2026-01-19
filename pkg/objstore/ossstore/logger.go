// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package ossstore

import (
	"github.com/aliyun/alibabacloud-oss-go-sdk-v2/oss"
	"github.com/pingcap/log"
	tidblogutil "github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
)

type logPrinter struct {
	logger *zap.Logger
}

func newLogPrinter(extraFields ...zap.Field) *logPrinter {
	return &logPrinter{
		logger: log.L().WithOptions(zap.AddCallerSkip(1)).With(extraFields...),
	}
}

// Print implements oss.LogPrinter interface.
func (l *logPrinter) Print(args ...any) {
	var (
		levelStr, msg string
		invalid       bool
	)
	if len(args) != 2 {
		invalid = true
	} else {
		var ok bool
		if levelStr, ok = args[0].(string); !ok {
			invalid = true
		}
		if msg, ok = args[1].(string); !ok {
			invalid = true
		}
	}
	if invalid {
		l.logger.Warn("invalid log from OSS", zap.Any("args", args))
		return
	}

	// see this for the definition for level string:
	// https://github.com/aliyun/alibabacloud-oss-go-sdk-v2/blob/df6754d5101e9eb62a428823a9f7c59612f7de37/oss/logger.go#L31
	// OSS SDK is so badly written 😑
	var logFn func(msg string, fields ...zap.Field)
	switch levelStr {
	case "ERROR ":
		logFn = l.logger.Error
	case "WARNING ":
		logFn = l.logger.Warn
	case "INFO ":
		logFn = l.logger.Info
	case "DEBUG ":
		logFn = l.logger.Debug
	}
	if logFn != nil {
		logFn(msg)
	}
}

func getOSSLogLevel() int {
	// on oss.LogInfo level, OSS SDK will log invocation start/end, which is
	// quite noisy. So we map one level higher.
	switch tidblogutil.BgLogger().Level() {
	case zap.ErrorLevel:
		return oss.LogError
	case zap.WarnLevel, zap.InfoLevel:
		return oss.LogWarn
	case zap.DebugLevel:
		return oss.LogDebug
	}
	return oss.LogOff
}
