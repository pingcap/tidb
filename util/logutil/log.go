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
// See the License for the specific language governing permissions and
// limitations under the License.

package logutil

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"time"

	"github.com/opentracing/opentracing-go"
	tlog "github.com/opentracing/opentracing-go/log"
	"github.com/pingcap/errors"
	zaplog "github.com/pingcap/log"
	log "github.com/sirupsen/logrus"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"gopkg.in/natefinch/lumberjack.v2"
)

const (
	defaultLogTimeFormat = "2006/01/02 15:04:05.000"
	// DefaultLogMaxSize is the default size of log files.
	DefaultLogMaxSize = 300 // MB
	// DefaultLogFormat is the default format of the log.
	DefaultLogFormat = "text"
	defaultLogLevel  = log.InfoLevel
	// DefaultSlowThreshold is the default slow log threshold in millisecond.
	DefaultSlowThreshold = 300
	// DefaultQueryLogMaxLen is the default max length of the query in the log.
	DefaultQueryLogMaxLen = 4096
	// DefaultRecordPlanInSlowLog is the default value for whether enable log query plan in the slow log.
	DefaultRecordPlanInSlowLog = 1
	// DefaultTiDBEnableSlowLog enables TiDB to log slow queries.
	DefaultTiDBEnableSlowLog = 1
)

// EmptyFileLogConfig is an empty FileLogConfig.
var EmptyFileLogConfig = FileLogConfig{}

// FileLogConfig serializes file log related config in toml/json.
type FileLogConfig struct {
	zaplog.FileLogConfig
}

// NewFileLogConfig creates a FileLogConfig.
func NewFileLogConfig(maxSize uint) FileLogConfig {
	return FileLogConfig{FileLogConfig: zaplog.FileLogConfig{
		MaxSize: int(maxSize),
	},
	}
}

// LogConfig serializes log related config in toml/json.
type LogConfig struct {
	zaplog.Config

	// SlowQueryFile filename, default to File log config on empty.
	SlowQueryFile string
}

// NewLogConfig creates a LogConfig.
func NewLogConfig(level, format, slowQueryFile string, fileCfg FileLogConfig, disableTimestamp bool, opts ...func(*zaplog.Config)) *LogConfig {
	c := &LogConfig{
		Config: zaplog.Config{
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

// isSKippedPackageName tests wether path name is on log library calling stack.
func isSkippedPackageName(name string) bool {
	return strings.Contains(name, "github.com/sirupsen/logrus") ||
		strings.Contains(name, "github.com/coreos/pkg/capnslog")
}

// modifyHook injects file name and line pos into log entry.
type contextHook struct{}

// Fire implements logrus.Hook interface
// https://github.com/sirupsen/logrus/issues/63
func (hook *contextHook) Fire(entry *log.Entry) error {
	pc := make([]uintptr, 4)
	cnt := runtime.Callers(6, pc)

	for i := 0; i < cnt; i++ {
		fu := runtime.FuncForPC(pc[i] - 1)
		name := fu.Name()
		if !isSkippedPackageName(name) {
			file, line := fu.FileLine(pc[i] - 1)
			entry.Data["file"] = filepath.Base(file)
			entry.Data["line"] = line
			break
		}
	}
	return nil
}

// Levels implements logrus.Hook interface.
func (hook *contextHook) Levels() []log.Level {
	return log.AllLevels
}

func stringToLogLevel(level string) log.Level {
	switch strings.ToLower(level) {
	case "fatal":
		return log.FatalLevel
	case "error":
		return log.ErrorLevel
	case "warn", "warning":
		return log.WarnLevel
	case "debug":
		return log.DebugLevel
	case "info":
		return log.InfoLevel
	}
	return defaultLogLevel
}

// textFormatter is for compatibility with ngaut/log
type textFormatter struct {
	DisableTimestamp bool
	EnableEntryOrder bool
}

// Format implements logrus.Formatter
func (f *textFormatter) Format(entry *log.Entry) ([]byte, error) {
	var b *bytes.Buffer
	if entry.Buffer != nil {
		b = entry.Buffer
	} else {
		b = &bytes.Buffer{}
	}

	if !f.DisableTimestamp {
		fmt.Fprintf(b, "%s ", entry.Time.Format(defaultLogTimeFormat))
	}
	if file, ok := entry.Data["file"]; ok {
		fmt.Fprintf(b, "%s:%v:", file, entry.Data["line"])
	}
	fmt.Fprintf(b, " [%s] %s", entry.Level.String(), entry.Message)

	if f.EnableEntryOrder {
		keys := make([]string, 0, len(entry.Data))
		for k := range entry.Data {
			if k != "file" && k != "line" {
				keys = append(keys, k)
			}
		}
		sort.Strings(keys)
		for _, k := range keys {
			fmt.Fprintf(b, " %v=%v", k, entry.Data[k])
		}
	} else {
		for k, v := range entry.Data {
			if k != "file" && k != "line" {
				fmt.Fprintf(b, " %v=%v", k, v)
			}
		}
	}

	b.WriteByte('\n')

	return b.Bytes(), nil
}

const (
	// SlowLogTimeFormat is the time format for slow log.
	SlowLogTimeFormat = time.RFC3339Nano
	// OldSlowLogTimeFormat is the first version of the the time format for slow log, This is use for compatibility.
	OldSlowLogTimeFormat = "2006-01-02-15:04:05.999999999 -0700"
)

type slowLogFormatter struct{}

func (f *slowLogFormatter) Format(entry *log.Entry) ([]byte, error) {
	var b *bytes.Buffer
	if entry.Buffer != nil {
		b = entry.Buffer
	} else {
		b = &bytes.Buffer{}
	}

	fmt.Fprintf(b, "# Time: %s\n", entry.Time.Format(SlowLogTimeFormat))
	fmt.Fprintf(b, "%s\n", entry.Message)
	return b.Bytes(), nil
}

func stringToLogFormatter(format string, disableTimestamp bool) log.Formatter {
	switch strings.ToLower(format) {
	case "text":
		return &textFormatter{
			DisableTimestamp: disableTimestamp,
		}
	default:
		return &textFormatter{}
	}
}

// initFileLog initializes file based logging options.
func initFileLog(cfg *zaplog.FileLogConfig, logger *log.Logger) error {
	if st, err := os.Stat(cfg.Filename); err == nil {
		if st.IsDir() {
			return errors.New("can't use directory as log file name")
		}
	}
	if cfg.MaxSize == 0 {
		cfg.MaxSize = DefaultLogMaxSize
	}

	// use lumberjack to logrotate
	output := &lumberjack.Logger{
		Filename:   cfg.Filename,
		MaxSize:    int(cfg.MaxSize),
		MaxBackups: int(cfg.MaxBackups),
		MaxAge:     int(cfg.MaxDays),
		LocalTime:  true,
	}

	if logger == nil {
		log.SetOutput(output)
	} else {
		logger.Out = output
	}
	return nil
}

// SlowQueryLogger is used to log slow query, InitLogger will modify it according to config file.
var SlowQueryLogger = log.StandardLogger()

// SlowQueryZapLogger is used to log slow query, InitZapLogger will modify it according to config file.
var SlowQueryZapLogger = zaplog.L()

// InitLogger initializes PD's logger.
func InitLogger(cfg *LogConfig) error {
	log.SetLevel(stringToLogLevel(cfg.Level))
	log.AddHook(&contextHook{})

	if cfg.Format == "" {
		cfg.Format = DefaultLogFormat
	}
	formatter := stringToLogFormatter(cfg.Format, cfg.DisableTimestamp)
	log.SetFormatter(formatter)

	if len(cfg.File.Filename) != 0 {
		if err := initFileLog(&cfg.File, nil); err != nil {
			return errors.Trace(err)
		}
	}

	if len(cfg.SlowQueryFile) != 0 {
		SlowQueryLogger = log.New()
		tmp := cfg.File
		tmp.Filename = cfg.SlowQueryFile
		if err := initFileLog(&tmp, SlowQueryLogger); err != nil {
			return errors.Trace(err)
		}
		SlowQueryLogger.Formatter = &slowLogFormatter{}
	}

	return nil
}

// InitZapLogger initializes a zap logger with cfg.
func InitZapLogger(cfg *LogConfig) error {
	gl, props, err := zaplog.InitLogger(&cfg.Config, zap.AddStacktrace(zapcore.FatalLevel))
	if err != nil {
		return errors.Trace(err)
	}
	zaplog.ReplaceGlobals(gl, props)

	if len(cfg.SlowQueryFile) != 0 {
		sqfCfg := zaplog.FileLogConfig{
			MaxSize:  cfg.File.MaxSize,
			Filename: cfg.SlowQueryFile,
		}
		sqCfg := &zaplog.Config{
			Level:            cfg.Level,
			Format:           cfg.Format,
			DisableTimestamp: cfg.DisableTimestamp,
			File:             sqfCfg,
		}
		sqLogger, _, err := zaplog.InitLogger(sqCfg)
		if err != nil {
			return errors.Trace(err)
		}
		SlowQueryZapLogger = sqLogger
	} else {
		SlowQueryZapLogger = gl
	}

	return nil
}

// SetLevel sets the zap logger's level.
func SetLevel(level string) error {
	l := zap.NewAtomicLevel()
	if err := l.UnmarshalText([]byte(level)); err != nil {
		return errors.Trace(err)
	}
	zaplog.SetLevel(l.Level())
	return nil
}

type ctxLogKeyType struct{}

var ctxLogKey = ctxLogKeyType{}

// Logger gets a contextual logger from current context.
// contextual logger will output common fields from context.
func Logger(ctx context.Context) *zap.Logger {
	if ctxlogger, ok := ctx.Value(ctxLogKey).(*zap.Logger); ok {
		return ctxlogger
	}
	return zaplog.L()
}

// BgLogger is alias of `logutil.BgLogger()`
func BgLogger() *zap.Logger {
	return zaplog.L()
}

// WithConnID attaches connId to context.
func WithConnID(ctx context.Context, connID uint32) context.Context {
	var logger *zap.Logger
	if ctxLogger, ok := ctx.Value(ctxLogKey).(*zap.Logger); ok {
		logger = ctxLogger
	} else {
		logger = zaplog.L()
	}
	return context.WithValue(ctx, ctxLogKey, logger.With(zap.Uint32("conn", connID)))
}

// WithKeyValue attaches key/value to context.
func WithKeyValue(ctx context.Context, key, value string) context.Context {
	var logger *zap.Logger
	if ctxLogger, ok := ctx.Value(ctxLogKey).(*zap.Logger); ok {
		logger = ctxLogger
	} else {
		logger = zaplog.L()
	}
	return context.WithValue(ctx, ctxLogKey, logger.With(zap.String(key, value)))
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
