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
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/tikv/client-go/v2/tikv"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"golang.org/x/net/http/httpproxy"
)

const (
	// DefaultLogMaxSize is the default size of log files.
	DefaultLogMaxSize = 300 // MB
	// DefaultLogFormat is the default format of the log.
	DefaultLogFormat = "text"
	// DefaultSlowThreshold is the default slow log threshold in millisecond.
	DefaultSlowThreshold = 300
	// DefaultSlowTxnThreshold is the default slow txn log threshold in ms.
	DefaultSlowTxnThreshold = 0
	// DefaultQueryLogMaxLen is the default max length of the query in the log.
	DefaultQueryLogMaxLen = 4096
	// DefaultRecordPlanInSlowLog is the default value for whether enable log query plan in the slow log.
	DefaultRecordPlanInSlowLog = 1
	// DefaultTiDBEnableSlowLog enables TiDB to log slow queries.
	DefaultTiDBEnableSlowLog = true
)

const (
	// LogFieldCategory is the field name for log category
	LogFieldCategory = "category"
	// LogFieldConn is the field name for connection id in log
	LogFieldConn = "conn"
	// LogFieldSessionAlias is the field name for session_alias in log
	LogFieldSessionAlias = "session_alias"
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

	// GeneralLogFile filenanme, default to File log config on empty.
	GeneralLogFile string
}

// NewLogConfig creates a LogConfig.
func NewLogConfig(level, format, slowQueryFile string, generalLogFile string, fileCfg FileLogConfig, disableTimestamp bool, opts ...func(*log.Config)) *LogConfig {
	c := &LogConfig{
		Config: log.Config{
			Level:            level,
			Format:           format,
			DisableTimestamp: disableTimestamp,
			File:             fileCfg.FileLogConfig,
		},
		SlowQueryFile:  slowQueryFile,
		GeneralLogFile: generalLogFile,
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

	// GRPCDebugEnvName is the environment variable name for GRPC_DEBUG.
	GRPCDebugEnvName = "GRPC_DEBUG"
)

// SlowQueryLogger is used to log slow query, InitLogger will modify it according to config file.
var SlowQueryLogger = log.L()

// GeneralLogger is used to log general log, InitLogger will modify it according to config file.
var GeneralLogger = log.L()

// InitLogger initializes a logger with cfg.
func InitLogger(cfg *LogConfig, opts ...zap.Option) error {
	opts = append(opts, zap.AddStacktrace(zapcore.FatalLevel))
	gl, props, err := log.InitLogger(&cfg.Config, opts...)
	if err != nil {
		return errors.Trace(err)
	}
	log.ReplaceGlobals(gl, props)

	// init dedicated logger for slow query log
	SlowQueryLogger, _, err = newSlowQueryLogger(cfg)
	if err != nil {
		return errors.Trace(err)
	}

	// init dedicated logger for general log
	GeneralLogger, _, err = newGeneralLogger(cfg)
	if err != nil {
		return errors.Trace(err)
	}

	initGRPCLogger(gl)
	tikv.SetLogContextKey(CtxLogKey)
	return nil
}

func initGRPCLogger(gl *zap.Logger) {
	level := zapcore.ErrorLevel
	verbosity := 0
	if len(os.Getenv(GRPCDebugEnvName)) > 0 {
		verbosity = 999
		level = zapcore.DebugLevel
	}

	newgl := gl.WithOptions(zap.WrapCore(func(core zapcore.Core) zapcore.Core {
		oldcore, ok := core.(*log.TextIOCore)
		if !ok {
			return oldcore
		}
		newcore := oldcore.Clone()
		leveler := zap.NewAtomicLevel()
		leveler.SetLevel(level)
		newcore.LevelEnabler = leveler
		return newcore
	}))
	gzap.ReplaceGrpcLoggerV2WithVerbosity(newgl, verbosity)
}

// ReplaceLogger replace global logger instance with given log config.
func ReplaceLogger(cfg *LogConfig, opts ...zap.Option) error {
	opts = append(opts, zap.AddStacktrace(zapcore.FatalLevel))
	gl, props, err := log.InitLogger(&cfg.Config, opts...)
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

	GeneralLogger, _, err = newGeneralLogger(cfg)
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

// BgLogger is alias of `logutil.BgLogger()`. It's initialized in tidb-server's
// main function. Don't use it in `init` or equivalent functions otherwise it
// will print to stdout.
func BgLogger() *zap.Logger {
	return log.L()
}

// LoggerWithTraceInfo attaches fields from trace info to logger
func LoggerWithTraceInfo(logger *zap.Logger, info *model.TraceInfo) *zap.Logger {
	if logger == nil {
		logger = log.L()
	}

	if fields := fieldsFromTraceInfo(info); len(fields) > 0 {
		logger = logger.With(fields...)
	}

	return logger
}

// WithConnID attaches connId to context.
func WithConnID(ctx context.Context, connID uint64) context.Context {
	return WithFields(ctx, zap.Uint64(LogFieldConn, connID))
}

// WithSessionAlias attaches session_alias to context
func WithSessionAlias(ctx context.Context, alias string) context.Context {
	return WithFields(ctx, zap.String(LogFieldSessionAlias, alias))
}

// WithCategory attaches category to context.
func WithCategory(ctx context.Context, category string) context.Context {
	return WithFields(ctx, zap.String(LogFieldCategory, category))
}

// WithTraceFields attaches trace fields to context
func WithTraceFields(ctx context.Context, info *model.TraceInfo) context.Context {
	if info == nil {
		return WithFields(ctx)
	}
	return WithFields(ctx,
		zap.Uint64(LogFieldConn, info.ConnectionID),
		zap.String(LogFieldSessionAlias, info.SessionAlias),
	)
}

func fieldsFromTraceInfo(info *model.TraceInfo) []zap.Field {
	if info == nil {
		return nil
	}

	fields := make([]zap.Field, 0, 2)
	if info.ConnectionID != 0 {
		fields = append(fields, zap.Uint64(LogFieldConn, info.ConnectionID))
	}

	if info.SessionAlias != "" {
		fields = append(fields, zap.String(LogFieldSessionAlias, info.SessionAlias))
	}

	return fields
}

// WithTraceLogger attaches trace identifier to context
func WithTraceLogger(ctx context.Context, info *model.TraceInfo) context.Context {
	var logger *zap.Logger
	if ctxLogger, ok := ctx.Value(CtxLogKey).(*zap.Logger); ok {
		logger = ctxLogger
	} else {
		logger = log.L()
	}
	return context.WithValue(ctx, CtxLogKey, wrapTraceLogger(ctx, info, logger))
}

func wrapTraceLogger(ctx context.Context, info *model.TraceInfo, logger *zap.Logger) *zap.Logger {
	return logger.WithOptions(zap.WrapCore(func(core zapcore.Core) zapcore.Core {
		tl := &traceLog{ctx: ctx}
		// cfg.Format == "", never return error
		enc, _ := log.NewTextEncoder(&log.Config{})
		traceCore := log.NewTextCore(enc, tl, tl)
		if fields := fieldsFromTraceInfo(info); len(fields) > 0 {
			traceCore = traceCore.With(fields)
		}
		return zapcore.NewTee(traceCore, core)
	}))
}

type traceLog struct {
	ctx context.Context
}

func (*traceLog) Enabled(_ zapcore.Level) bool {
	return true
}

func (t *traceLog) Write(p []byte) (n int, err error) {
	trace.Log(t.ctx, "log", string(p))
	return len(p), nil
}

func (*traceLog) Sync() error {
	return nil
}

// WithKeyValue attaches key/value to context.
func WithKeyValue(ctx context.Context, key, value string) context.Context {
	return WithFields(ctx, zap.String(key, value))
}

// WithFields attaches key/value to context.
func WithFields(ctx context.Context, fields ...zap.Field) context.Context {
	var logger *zap.Logger
	if ctxLogger, ok := ctx.Value(CtxLogKey).(*zap.Logger); ok {
		logger = ctxLogger
	} else {
		logger = log.L()
	}

	if len(fields) > 0 {
		logger = logger.With(fields...)
	}

	return context.WithValue(ctx, CtxLogKey, logger)
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
func Eventf(ctx context.Context, format string, args ...any) {
	if span := opentracing.SpanFromContext(ctx); span != nil && span.Tracer() != nil {
		span.LogFields(tlog.String(TraceEventKey, fmt.Sprintf(format, args...)))
	}
}

// SetTag sets tag kv-pair in current tracing span
func SetTag(ctx context.Context, key string, value any) {
	if span := opentracing.SpanFromContext(ctx); span != nil && span.Tracer() != nil {
		span.SetTag(key, value)
	}
}

// LogEnvVariables logs related environment variables.
func LogEnvVariables() {
	// log http proxy settings, it will be used in gRPC connection by default
	fields := proxyFields()
	if len(fields) > 0 {
		log.Info("using proxy config", fields...)
	}
}

func proxyFields() []zap.Field {
	proxyCfg := httpproxy.FromEnvironment()
	fields := make([]zap.Field, 0, 3)
	if proxyCfg.HTTPProxy != "" {
		fields = append(fields, zap.String("http_proxy", proxyCfg.HTTPProxy))
	}
	if proxyCfg.HTTPSProxy != "" {
		fields = append(fields, zap.String("https_proxy", proxyCfg.HTTPSProxy))
	}
	if proxyCfg.NoProxy != "" {
		fields = append(fields, zap.String("no_proxy", proxyCfg.NoProxy))
	}
	return fields
}
