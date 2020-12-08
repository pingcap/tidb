// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package log

import (
	"github.com/pingcap/errors"
	pclog "github.com/pingcap/log"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var (
	appLogger = Logger{zap.NewNop()}
	appLevel  = zap.NewAtomicLevel()
)

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
func InitAppLogger(cfg *Config) error {
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
		return errors.Trace(err)
	}
	logger = logger.WithOptions(zap.AddCallerSkip(1))
	appLogger = Logger{logger}
	appLevel = props.Level
	return nil
}

// SetAppLogger sets the wrapped logger from config.
func SetAppLogger(logger *zap.Logger) {
	appLogger = Logger{logger}
}

// ChangeAppLogLevel changes the wrapped logger's log level.
func ChangeAppLogLevel(level zapcore.Level) {
	appLevel.SetLevel(level)
}

// Info wraps *zap.Logger's Info function.
func Info(msg string, fields ...zap.Field) {
	appLogger.Info(msg, fields...)
}

// Warn wraps *zap.Logger's Warn function.
func Warn(msg string, fields ...zap.Field) {
	appLogger.Warn(msg, fields...)
}

// Error wraps *zap.Logger's Error function.
func Error(msg string, fields ...zap.Field) {
	appLogger.Error(msg, fields...)
}

// Debug wraps *zap.Logger's Debug function.
func Debug(msg string, fields ...zap.Field) {
	appLogger.Debug(msg, fields...)
}

// Fatal wraps *zap.Logger's Fatal function.
func Fatal(msg string, fields ...zap.Field) {
	appLogger.Fatal(msg, fields...)
}

// Panic wraps *zap.Logger's Panic function.
func Panic(msg string, fields ...zap.Field) {
	appLogger.Panic(msg, fields...)
}
