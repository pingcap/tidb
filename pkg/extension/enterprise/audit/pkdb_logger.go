package audit

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
	"unicode"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"gopkg.in/natefinch/lumberjack.v2"
)

// Log formats
const (
	LogFormatText = "TEXT"
	LogFormatJSON = "JSON"
)

// Values of system variables
const (
	MaxAuditLogFileMaxSize         uint64 = 100 * 1024        // 100GB
	MaxAuditLogFileMaxLifetime     uint64 = 30 * 24 * 60 * 60 // 30 days
	MaxAuditLogFileReservedBackups uint64 = 1024
	MaxAuditLogFileReservedDays    uint64 = 1024

	DefAuditLogName                      = "tidb-audit.log"
	DefAuditLogFormat                    = LogFormatText
	DefAuditLogFileMaxSize         int64 = 100              // 100MB
	DefAuditLogFileMaxLifetime     int64 = 1 * 24 * 60 * 60 // 1 day
	DefAuditLogFileReservedBackups int   = 10
	DefAuditLogFileReservedDays    int   = 0
	DefAuditLogRedact              bool  = true
)

type zapLoggerConfig struct {
	Filepath            string
	FileMaxSize         int64
	FileMaxLifetime     int64
	FileReservedBackups int
	FileReservedDays    int
	LogFormat           string
}

func (cfg zapLoggerConfig) zapCfgChanged(cmp LoggerConfig) bool {
	return cfg != cmp.zapLoggerConfig
}

// LoggerConfig is the config for logger
type LoggerConfig struct {
	zapLoggerConfig
	Enabled    bool
	ConfigPath string
	Redact     bool
	Filter     *LogFilterRuleBundle
}

// Logger is used to log audit logs
type Logger struct {
	zapLogger *zap.Logger
	rotate    func() error
	cfg       LoggerConfig

	ctx    context.Context
	cancel context.CancelFunc
}

// LoggerWithConfig creates a new Logger, it will reuse some inner states if possible
func LoggerWithConfig(ctx context.Context, cfg LoggerConfig, old *Logger) (_ *Logger, err error) {
	if old != nil && old.cfg == cfg {
		return old, nil
	}

	var zapLogger *zap.Logger
	var rotate func() error

	if old == nil || old.cfg.zapCfgChanged(cfg) {
		if zapLogger, rotate, err = newZapLogger(cfg); err != nil {
			return nil, err
		}
	} else {
		zapLogger = old.zapLogger
		rotate = old.rotate
	}

	subCtx, cancel := context.WithCancel(ctx)
	return &Logger{
		zapLogger: zapLogger,
		rotate:    rotate,
		cfg:       cfg,
		ctx:       subCtx,
		cancel:    cancel,
	}, nil
}

// Rotate rotates the log
func (l *Logger) Rotate() error {
	if l == nil {
		return errors.New("audit logger is not initialized")
	}
	return l.rotate()
}

// Event logs the event
func (l *Logger) Event(fields []zap.Field) {
	if l == nil {
		terror.Log(errors.New("audit logger is not initialized"))
		return
	}
	l.zapLogger.Info("", fields...)
}

func (l *Logger) loopCheckLogMaxLifetime(interval time.Duration) {
	go func() {
		ticker := time.NewTicker(interval)
		for {
			select {
			case <-ticker.C:
				if err := l.Rotate(); err != nil {
					logutil.BgLogger().Error("Fail to rotate audit log by tidb_audit_log_max_lifetime", zap.Error(err))
				}
			case <-l.ctx.Done():
				ticker.Stop()
				return
			}
		}
	}()
}

func getServerEndpoint() string {
	globalConf := config.GetGlobalConfig()
	var serverEndpoint strings.Builder
	for _, c := range fmt.Sprintf("%s-%d", globalConf.AdvertiseAddress, globalConf.Port) {
		if unicode.IsSpace(c) {
			serverEndpoint.WriteRune('_')
			continue
		}

		if c == '.' {
			serverEndpoint.WriteRune('-')
			continue
		}

		serverEndpoint.WriteRune(c)
	}
	return serverEndpoint.String()
}

func getFilePath(configPath string, format string) (string, error) {
	if configPath == "" {
		configPath = DefAuditLogName
	}
	if !filepath.IsAbs(configPath) {
		baseFolder := ""
		if logFile := config.GetGlobalConfig().Log.File.Filename; logFile != "" {
			baseFolder = filepath.Dir(logFile)
		} else {
			baseFolder = filepath.Dir(configPath)
		}
		if !filepath.IsAbs(baseFolder) {
			wd, err := os.Getwd()
			if err != nil {
				return "", err
			}
			baseFolder = filepath.Join(wd, baseFolder)
		}
		configPath = filepath.Join(baseFolder, configPath)
	}

	if format == LogFormatJSON {
		configPath = configPath + ".json"
	}

	return strings.ReplaceAll(configPath, "%e", getServerEndpoint()), nil
}

func newZapLogger(cfg LoggerConfig) (*zap.Logger, func() error, error) {
	if st, err := os.Stat(cfg.Filepath); err == nil {
		if st.IsDir() {
			return nil, nil, errors.New("can't use directory as log file name")
		}
	}

	zapCfg := &log.Config{
		Level:         "info",
		DisableCaller: true,
		File: log.FileLogConfig{
			Filename:   cfg.Filepath,
			MaxSize:    int(cfg.FileMaxSize),
			MaxBackups: cfg.FileReservedBackups,
			MaxDays:    cfg.FileReservedDays,
		},
	}

	// use lumberjack to logrotate
	writer := &lumberjack.Logger{
		Filename:   zapCfg.File.Filename,
		MaxSize:    zapCfg.File.MaxSize,
		MaxBackups: zapCfg.File.MaxBackups,
		MaxAge:     zapCfg.File.MaxDays,
		LocalTime:  true,
	}

	logger, props, err := log.InitLoggerWithWriteSyncer(
		zapCfg,
		zapcore.AddSync(writer),
		zapcore.AddSync(&errLogWriter{}),
	)

	if err != nil {
		return nil, nil, err
	}

	var newEncoder zapcore.Encoder
	if cfg.LogFormat == LogFormatJSON {
		newEncoder = zapcore.NewJSONEncoder(zapcore.EncoderConfig{
			TimeKey:    LogKeyTime,
			EncodeTime: log.DefaultTimeEncoder,
		})
	}

	if newEncoder != nil {
		newCore := log.NewTextCore(newEncoder, props.Syncer, props.Level)
		logger = logger.WithOptions(zap.WrapCore(func(core zapcore.Core) zapcore.Core {
			return newCore
		}))
	}

	return logger, writer.Rotate, nil
}
