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
}

func InitAppLogger(cfg *Config) error {
	logger, props, err := pclog.InitLogger(&pclog.Config{
		Level: cfg.Level,
		File: pclog.FileLogConfig{
			Filename:   cfg.File,
			MaxSize:    cfg.FileMaxSize,
			MaxDays:    cfg.FileMaxDays,
			MaxBackups: cfg.FileMaxBackups,
		},
	})
	if err != nil {
		return errors.WithStack(err)
	}
	logger = logger.WithOptions(zap.AddCallerSkip(1))
	appLogger = Logger{logger}
	appLevel = props.Level
	return nil
}

func SetAppLogger(logger *zap.Logger) {
	appLogger = Logger{logger}
}

func ChangeAppLogLevel(level zapcore.Level) {
	appLevel.SetLevel(level)
}

func Info(msg string, fields ...zap.Field) {
	appLogger.Info(msg, fields...)
}

func Warn(msg string, fields ...zap.Field) {
	appLogger.Warn(msg, fields...)
}

func Error(msg string, fields ...zap.Field) {
	appLogger.Error(msg, fields...)
}

func Debug(msg string, fields ...zap.Field) {
	appLogger.Debug(msg, fields...)
}

func Fatal(msg string, fields ...zap.Field) {
	appLogger.Fatal(msg, fields...)
}

func Panic(msg string, fields ...zap.Field) {
	appLogger.Panic(msg, fields...)
}
