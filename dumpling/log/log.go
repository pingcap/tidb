// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package log

import (
	"github.com/pingcap/errors"
	pclog "github.com/pingcap/log"
	"go.uber.org/zap"
)

var appLogger = Logger{zap.NewNop()}

// Logger wraps the zap logger.
type Logger struct {
	*zap.Logger
}

// Zap returns the global logger.
func Zap() Logger {
	return appLogger
}

// Config serializes log related config in toml/json.
type Config struct {
	// Log level.
	// One of "debug", "info", "warn", "error", "dpanic", "panic", and "fatal".
	Level string `toml:"level" json:"level"`
	// Log filename, leave empty to disable file log.
	File string `toml:"file" json:"file"`
	// Max size for a single file, in MB.
	FileMaxSize int `toml:"max-size" json:"max-size"`
	// Max log keep days, default is never deleting.
	FileMaxDays int `toml:"max-days" json:"max-days"`
	// Maximum number of old log files to retain.
	FileMaxBackups int `toml:"max-backups" json:"max-backups"`
	// Format of the log, one of `text`, `json` or `console`.
	Format string `toml:"format" json:"format"`
}

// InitAppLogger inits the wrapped logger from config.
func InitAppLogger(cfg *Config) (Logger, *pclog.ZapProperties, error) {
	logger, props, err := pclog.InitLogger(&pclog.Config{
		Level: cfg.Level,
		File: pclog.FileLogConfig{
			Filename:   cfg.File,
			MaxSize:    cfg.FileMaxSize,
			MaxDays:    cfg.FileMaxDays,
			MaxBackups: cfg.FileMaxBackups,
		},
		Format: cfg.Format,
	})
	if err != nil {
		return appLogger, props, errors.Trace(err)
	}
	return Logger{logger.WithOptions(zap.AddStacktrace(zap.DPanicLevel))}, props, nil
}

// NewAppLogger returns the wrapped logger from config.
func NewAppLogger(logger *zap.Logger) Logger {
	return Logger{logger}
}

// ShortError contructs a field which only records the error message without the
// verbose text (i.e. excludes the stack trace).
func ShortError(err error) zap.Field {
	if err == nil {
		return zap.Skip()
	}
	return zap.String("error", err.Error())
}
