// Copyright 2019 PingCAP, Inc.
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

package log

import (
	"context"
	"time"

	"github.com/pingcap/errors"
	pclog "github.com/pingcap/log"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	defaultLogLevel   = "info"
	defaultLogMaxDays = 7
	defaultLogMaxSize = 512 // MB
)

// Config serializes log related config in toml/json.
type Config struct {
	// Log level.
	Level string `toml:"level" json:"level"`
	// Log filename, leave empty to disable file log.
	File string `toml:"file" json:"file"`
	// Max size for a single file, in MB.
	FileMaxSize int `toml:"max-size" json:"max-size"`
	// Max log keep days, default is never deleting.
	FileMaxDays int `toml:"max-days" json:"max-days"`
	// Maximum number of old log files to retain.
	FileMaxBackups int `toml:"max-backups" json:"max-backups"`
}

func (cfg *Config) Adjust() {
	if len(cfg.Level) == 0 {
		cfg.Level = defaultLogLevel
	}
	if cfg.Level == "warning" {
		cfg.Level = "warn"
	}
	if cfg.FileMaxSize == 0 {
		cfg.FileMaxSize = defaultLogMaxSize
	}
	if cfg.FileMaxDays == 0 {
		cfg.FileMaxDays = defaultLogMaxDays
	}
}

// Logger is a simple wrapper around *zap.Logger which provides some extra
// methods to simplify Lightning's log usage.
type Logger struct {
	*zap.Logger
}

// logger for lightning, different from tidb logger.
var (
	appLogger = Logger{zap.NewNop()}
	appLevel  = zap.NewAtomicLevel()
)

// InitLogger initializes Lightning's and also the TiDB library's loggers.
func InitLogger(cfg *Config, tidbLoglevel string) error {
	tidbLogCfg := logutil.LogConfig{}
	// Disable annoying TiDB Log.
	// TODO: some error logs outputs randomly, we need to fix them in TiDB.
	tidbLogCfg.Level = "fatal"
	err := logutil.InitLogger(&tidbLogCfg)
	if err != nil {
		return errors.Trace(err)
	}

	logCfg := &pclog.Config{
		Level:         cfg.Level,
		DisableCaller: false, // FilterCore requires zap.AddCaller.
	}
	filterTiDBLog := zap.WrapCore(func(core zapcore.Core) zapcore.Core {
		// Filter logs from TiDB and PD.
		return NewFilterCore(core, "github.com/tikv/pd/")
	})
	// "-" is a special config for log to stdout.
	if len(cfg.File) > 0 && cfg.File != "-" {
		logCfg.File = pclog.FileLogConfig{
			Filename:   cfg.File,
			MaxSize:    cfg.FileMaxSize,
			MaxDays:    cfg.FileMaxDays,
			MaxBackups: cfg.FileMaxBackups,
		}
	}
	logger, props, err := pclog.InitLogger(logCfg, filterTiDBLog)
	if err != nil {
		return err
	}
	pclog.ReplaceGlobals(logger, props)

	// Do not log stack traces at all, as we'll get the stack trace from the
	// error itself.
	appLogger = Logger{logger.WithOptions(zap.AddStacktrace(zap.DPanicLevel))}
	appLevel = props.Level

	return nil
}

// SetAppLogger replaces the default logger in this package to given one
func SetAppLogger(l *zap.Logger) {
	appLogger = Logger{l.WithOptions(zap.AddStacktrace(zap.DPanicLevel))}
}

// L returns the current logger for Lightning.
func L() Logger {
	return appLogger
}

// Level returns the current global log level.
func Level() zapcore.Level {
	return appLevel.Level()
}

// SetLevel modifies the log level of the global logger. Returns the previous
// level.
func SetLevel(level zapcore.Level) zapcore.Level {
	oldLevel := appLevel.Level()
	appLevel.SetLevel(level)
	return oldLevel
}

// ShortError contructs a field which only records the error message without the
// verbose text (i.e. excludes the stack trace).
//
// In Lightning, all errors are almost always propagated back to `main()` where
// the error stack is written. Including the stack in the middle thus usually
// just repeats known information. You should almost always use `ShortError`
// instead of `zap.Error`, unless the error is no longer propagated upwards.
func ShortError(err error) zap.Field {
	if err == nil {
		return zap.Skip()
	}
	return zap.String("error", err.Error())
}

// With creates a child logger from the global logger and adds structured
// context to it.
func With(fields ...zap.Field) Logger {
	return appLogger.With(fields...)
}

// IsContextCanceledError returns whether the error is caused by context
// cancellation.
func IsContextCanceledError(err error) bool {
	err = errors.Cause(err)
	return err == context.Canceled || status.Code(err) == codes.Canceled
}

// Begin marks the beginning of a task.
func (logger Logger) Begin(level zapcore.Level, name string) *Task {
	if ce := logger.WithOptions(zap.AddCallerSkip(1)).Check(level, name+" start"); ce != nil {
		ce.Write()
	}
	return &Task{
		Logger: logger,
		level:  level,
		name:   name,
		since:  time.Now(),
	}
}

// With creates a child logger and adds structured context to it.
func (logger Logger) With(fields ...zap.Field) Logger {
	return Logger{logger.Logger.With(fields...)}
}

// Named adds a new path segment to the logger's name.
func (logger Logger) Named(name string) Logger {
	return Logger{logger.Logger.Named(name)}
}

// Task is a logger for a task spanning a period of time. This structure records
// when the task is started, so the time elapsed for the whole task can be
// logged without book-keeping.
type Task struct {
	Logger
	level zapcore.Level
	name  string
	since time.Time
}

// End marks the end of a task.
//
// The `level` is the log level if the task *failed* (i.e. `err != nil`). If the
// task *succeeded* (i.e. `err == nil`), the level from `Begin()` is used
// instead.
//
// The `extraFields` are included in the log only when the task succeeded.
func (task *Task) End(level zapcore.Level, err error, extraFields ...zap.Field) time.Duration {
	elapsed := time.Since(task.since)
	var verb string
	switch {
	case err == nil:
		level = task.level
		verb = " completed"
	case IsContextCanceledError(err):
		level = zap.DebugLevel
		verb = " canceled"
		extraFields = nil
	default:
		verb = " failed"
		extraFields = nil
	}
	if ce := task.WithOptions(zap.AddCallerSkip(1)).Check(level, task.name+verb); ce != nil {
		ce.Write(append(
			extraFields,
			zap.Duration("takeTime", elapsed),
			ShortError(err),
		)...)
	}
	return elapsed
}
