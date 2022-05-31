// Copyright 2017 PingCAP, Inc.
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

package logutil

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"runtime/trace"
	"time"

	gzap "github.com/grpc-ecosystem/go-grpc-middleware/logging/zap"
	"github.com/opentracing/opentracing-go"
	tlog "github.com/opentracing/opentracing-go/log"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

const (
	// DefaultLogMaxSize is the default size of log files.
	DefaultLogMaxSize = 300 // MB
	// DefaultLogFormat is the default format of the log.
	DefaultLogFormat = "text"
	// DefaultSlowThreshold is the default slow log threshold in millisecond.
	DefaultSlowThreshold = 300
	// DefaultQueryLogMaxLen is the default max length of the query in the log.
	DefaultQueryLogMaxLen = 4096
	// DefaultRecordPlanInSlowLog is the default value for whether enable log query plan in the slow log.
	DefaultRecordPlanInSlowLog = 1
	// DefaultTiDBEnableSlowLog enables TiDB to log slow queries.
	DefaultTiDBEnableSlowLog = true
)

// EmptyFileLogConfig is an empty FileLogConfig.
var EmptyFileLogConfig = FileLogConfig{}

// FileLogConfig serializes file log related config in toml/json.
type FileLogConfig struct {
	log.FileLogConfig
}

// NewFileLogConfig creates a FileLogConfig.
func NewFileLogConfig(maxSize uint) FileLogConfig {
	return FileLogConfig{FileLogConfig: log.FileLogConfig{
		MaxSize: int(maxSize),
	},
	}
}

// LogConfig serializes log related config in toml/json.
type LogConfig struct {
	log.Config

	// SlowQueryFile filename, default to File log config on empty.
	SlowQueryFile string
}

// NewLogConfig creates a LogConfig.
func NewLogConfig(level, format, slowQueryFile string, fileCfg FileLogConfig, disableTimestamp bool, opts ...func(*log.Config)) *LogConfig {
	c := &LogConfig{
		Config: log.Config{
			Level:            level,
			Format:           format,
			DisableTimestamp: disableTimestamp,
			File:             fileCfg.FileLogConfig,
		},
		SlowQueryFile: slowQueryFile,
	}
	for _, opt := range opts {
		opt(&c.Config)
	}
	return c
}

const (
	// SlowLogTimeFormat is the time format for slow log.
	SlowLogTimeFormat = time.RFC3339Nano
	// OldSlowLogTimeFormat is the first version of the the time format for slow log, This is use for compatibility.
	OldSlowLogTimeFormat = "2006-01-02-15:04:05.999999999 -0700"
)

// SlowQueryLogger is used to log slow query, InitLogger will modify it according to config file.
var SlowQueryLogger = log.L()

// InitLogger initializes a logger with cfg.
func InitLogger(cfg *LogConfig) error {
	gl, props, err := log.InitLogger(&cfg.Config, zap.AddStacktrace(zapcore.FatalLevel))
	if err != nil {
		return errors.Trace(err)
	}
	log.ReplaceGlobals(gl, props)

	// init dedicated logger for slow query log
	SlowQueryLogger, _, err = newSlowQueryLogger(cfg)
	if err != nil {
		return errors.Trace(err)
	}

	_, _, err = initGRPCLogger(cfg)
	if err != nil {
		return errors.Trace(err)
	}

	return nil
}

func initGRPCLogger(cfg *LogConfig) (*zap.Logger, *log.ZapProperties, error) {
	// Copy Config struct by assignment.
	config := cfg.Config
	var l *zap.Logger
	var err error
	var prop *log.ZapProperties
	if len(os.Getenv("GRPC_DEBUG")) > 0 {
		config.Level = "debug"
		l, prop, err = log.InitLogger(&config, zap.AddStacktrace(zapcore.FatalLevel))
		if err != nil {
			return nil, nil, errors.Trace(err)
		}
		gzap.ReplaceGrpcLoggerV2WithVerbosity(l, 999)
	} else {
		config.Level = "error"
		l, prop, err = log.InitLogger(&config, zap.AddStacktrace(zapcore.FatalLevel))
		if err != nil {
			return nil, nil, errors.Trace(err)
		}
		gzap.ReplaceGrpcLoggerV2(l)
	}

	return l, prop, nil
}

// ReplaceLogger replace global logger instance with given log config.
func ReplaceLogger(cfg *LogConfig) error {
	gl, props, err := log.InitLogger(&cfg.Config, zap.AddStacktrace(zapcore.FatalLevel))
	if err != nil {
		return errors.Trace(err)
	}
	log.ReplaceGlobals(gl, props)

	cfgJSON, err := json.Marshal(&cfg.Config)
	if err != nil {
		return errors.Trace(err)
	}

	SlowQueryLogger, _, err = newSlowQueryLogger(cfg)
	if err != nil {
		return errors.Trace(err)
	}

	log.S().Infof("replaced global logger with config: %s", string(cfgJSON))

	return nil
}

// SetLevel sets the zap logger's level.
func SetLevel(level string) error {
	l := zap.NewAtomicLevel()
	if err := l.UnmarshalText([]byte(level)); err != nil {
		return errors.Trace(err)
	}
	log.SetLevel(l.Level())
	return nil
}

type ctxLogKeyType struct{}

// CtxLogKey indicates the context key for logger
// public for test usage.
var CtxLogKey = ctxLogKeyType{}

// Logger gets a contextual logger from current context.
// contextual logger will output common fields from context.
func Logger(ctx context.Context) *zap.Logger {
	if ctxlogger, ok := ctx.Value(CtxLogKey).(*zap.Logger); ok {
		return ctxlogger
	}
	return log.L()
}

// BgLogger is alias of `logutil.BgLogger()`
func BgLogger() *zap.Logger {
	return log.L()
}

// WithConnID attaches connId to context.
func WithConnID(ctx context.Context, connID uint64) context.Context {
	var logger *zap.Logger
	if ctxLogger, ok := ctx.Value(CtxLogKey).(*zap.Logger); ok {
		logger = ctxLogger
	} else {
		logger = log.L()
	}
	return context.WithValue(ctx, CtxLogKey, logger.With(zap.Uint64("conn", connID)))
}

// WithTraceLogger attaches trace identifier to context
func WithTraceLogger(ctx context.Context, connID uint64) context.Context {
	var logger *zap.Logger
	if ctxLogger, ok := ctx.Value(CtxLogKey).(*zap.Logger); ok {
		logger = ctxLogger
	} else {
		logger = log.L()
	}
	return context.WithValue(ctx, CtxLogKey, wrapTraceLogger(ctx, connID, logger))
}

func wrapTraceLogger(ctx context.Context, connID uint64, logger *zap.Logger) *zap.Logger {
	return logger.WithOptions(zap.WrapCore(func(core zapcore.Core) zapcore.Core {
		tl := &traceLog{ctx: ctx}
		// cfg.Format == "", never return error
		enc, _ := log.NewTextEncoder(&log.Config{})
		traceCore := log.NewTextCore(enc, tl, tl).With([]zapcore.Field{zap.Uint64("conn", connID)})
		return zapcore.NewTee(traceCore, core)
	}))
}

type traceLog struct {
	ctx context.Context
}

func (t *traceLog) Enabled(_ zapcore.Level) bool {
	return true
}

func (t *traceLog) Write(p []byte) (n int, err error) {
	trace.Log(t.ctx, "log", string(p))
	return len(p), nil
}

func (t *traceLog) Sync() error {
	return nil
}

// WithKeyValue attaches key/value to context.
func WithKeyValue(ctx context.Context, key, value string) context.Context {
	var logger *zap.Logger
	if ctxLogger, ok := ctx.Value(CtxLogKey).(*zap.Logger); ok {
		logger = ctxLogger
	} else {
		logger = log.L()
	}
	return context.WithValue(ctx, CtxLogKey, logger.With(zap.String(key, value)))
}

// TraceEventKey presents the TraceEventKey in span log.
const TraceEventKey = "event"

// Event records event in current tracing span.
func Event(ctx context.Context, event string) {
	if span := opentracing.SpanFromContext(ctx); span != nil && span.Tracer() != nil {
		span.LogFields(tlog.String(TraceEventKey, event))
	}
}

// Eventf records event in current tracing span with format support.
func Eventf(ctx context.Context, format string, args ...interface{}) {
	if span := opentracing.SpanFromContext(ctx); span != nil && span.Tracer() != nil {
		span.LogFields(tlog.String(TraceEventKey, fmt.Sprintf(format, args...)))
	}
}

// SetTag sets tag kv-pair in current tracing span
func SetTag(ctx context.Context, key string, value interface{}) {
	if span := opentracing.SpanFromContext(ctx); span != nil && span.Tracer() != nil {
		span.SetTag(key, value)
	}
}
