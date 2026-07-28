package audit

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
	"unicode"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/util/intest"
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
	mu        sync.RWMutex
	zapLogger *zap.Logger
	writer    LogWriter
	cfg       LoggerConfig

	stopFlag  bool
	cfgSyncCh chan struct{}
	loopWg    sync.WaitGroup
}

// NewAuditLogger creates a new Logger.
func NewAuditLogger(cfg LoggerConfig) (*Logger, error) {
	zapLogger, writer, err := newZapLogger(cfg)
	if err != nil {
		return nil, err
	}

	logger := &Logger{
		zapLogger: zapLogger,
		writer:    writer,
		cfg:       cfg,
		cfgSyncCh: make(chan struct{}, 1),
	}
	logger.loopWg.Go(logger.rotateLoop)
	return logger, nil
}

// SetConfig sets the logger's config.
func (l *Logger) SetConfig(cfg LoggerConfig) error {
	if l.GetConfig() == cfg {
		// Avoid taking the write lock on unchanged config, because logging uses read locks.
		return nil
	}

	l.mu.Lock()
	defer l.mu.Unlock()
	if l.cfg.zapCfgChanged(cfg) {
		zapLogger, writer, err := newZapLogger(cfg)
		if err != nil {
			return err
		}
		terror.Log(l.zapLogger.Sync())
		terror.Log(l.writer.Close())
		l.zapLogger = zapLogger
		l.writer = writer
	}

	l.cfg = cfg
	if !l.stopFlag {
		select {
		case l.cfgSyncCh <- struct{}{}:
		default:
		}
	}
	return nil
}

// GetConfig returns the config of the current logger.
func (l *Logger) GetConfig() LoggerConfig {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.cfg
}

// Stop stops the logger's background loops.
func (l *Logger) Stop() {
	l.mu.Lock()
	if l.stopFlag {
		l.mu.Unlock()
		l.loopWg.Wait()
		return
	}
	l.stopFlag = true
	terror.Log(l.zapLogger.Sync())
	close(l.cfgSyncCh)
	l.mu.Unlock()

	l.loopWg.Wait()
}

// Rotate rotates the log
func (l *Logger) Rotate() error {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.writer.Rotate()
}

// RotateOrNot rotates the log if the check function returns true.
func (l *Logger) RotateOrNot(check func(cfg LoggerConfig) bool) (bool, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	if check(l.cfg) {
		return true, l.writer.Rotate()
	}
	return false, nil
}

// LogEntry logs the entry.
func (l *Logger) LogEntry(entry *LogEntry, id *entryIDGenerator, force bool) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	if !l.cfg.Enabled && !force {
		return
	}
	l.doLogEntry(entry, id, !force)
}

// LogEntryFunc logs the entry produced by a function.
func (l *Logger) LogEntryFunc(fn func() *LogEntry, id *entryIDGenerator, force bool) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	if !l.cfg.Enabled && !force {
		return
	}
	l.doLogEntry(fn(), id, !force)
}

func (l *Logger) doLogEntry(entry *LogEntry, id *entryIDGenerator, filter bool) {
	if filter {
		entry = entry.Filter(l.cfg.Filter)
	}

	if fields := entry.Fields(id, l.cfg.Redact); len(fields) > 0 {
		l.zapLogger.Info("", fields...)
		failpoint.InjectCall("afterAuditLogEntryWritten")
	}
}

// rotateNotifySyncChForTest is only used for tests.
var rotateNotifySyncChForTest chan<- *maxLifeTimeRotateTicker

func notifyRotateTickerSyncFuncForTest() func(t *maxLifeTimeRotateTicker) {
	if ch := rotateNotifySyncChForTest; intest.InTest && ch != nil {
		return func(t *maxLifeTimeRotateTicker) {
			select {
			case ch <- t:
			default:
				intest.Assert(false, "should not block in test")
			}
		}
	}
	return func(*maxLifeTimeRotateTicker) {}
}

func (l *Logger) rotateLoop() {
	syncCfgTicker := time.NewTicker(10 * time.Minute)
	defer syncCfgTicker.Stop()

	rotateTicker := newMaxLifeTimeRotateTicker(l.GetConfig())
	defer rotateTicker.Stop()

	notify := notifyRotateTickerSyncFuncForTest()
	notify(rotateTicker)

	for {
		select {
		case _, ok := <-l.cfgSyncCh:
			if !ok {
				return
			}
			rotateTicker.SyncConfig(l.GetConfig())
			notify(rotateTicker)
		case <-syncCfgTicker.C:
			rotateTicker.SyncConfig(l.GetConfig())
			notify(rotateTicker)
		case <-rotateTicker.C():
			if _, err := l.RotateOrNot(rotateTicker.CheckCfg); err != nil {
				logutil.BgLogger().Error("Fail to rotate audit log by tidb_audit_log_max_lifetime", zap.Error(err))
			}
		}
	}
}

var idleTickerC = make(<-chan time.Time)

type maxLifeTimeRotateTicker struct {
	ticker            *time.Ticker
	rotateMaxLifeTime int64
	rotateFile        string
}

func newMaxLifeTimeRotateTicker(cfg LoggerConfig) *maxLifeTimeRotateTicker {
	t := &maxLifeTimeRotateTicker{}
	t.SyncConfig(cfg)
	return t
}

// C returns the current ticker channel for rotating the log file.
func (t *maxLifeTimeRotateTicker) C() <-chan time.Time {
	if t.ticker == nil {
		return idleTickerC
	}
	return t.ticker.C
}

// Stop stops the ticker.
func (t *maxLifeTimeRotateTicker) Stop() {
	if t.ticker != nil {
		t.ticker.Stop()
	}
	t.ticker = nil
	t.rotateMaxLifeTime = 0
	t.rotateFile = ""
}

// SyncConfig syncs the ticker config from the logger.
func (t *maxLifeTimeRotateTicker) SyncConfig(cfg LoggerConfig) {
	if t.CheckCfg(cfg) {
		return
	}

	t.Stop()
	if cfg.Enabled && cfg.FileMaxLifetime > 0 {
		t.rotateFile = cfg.Filepath
		t.rotateMaxLifeTime = cfg.FileMaxLifetime
		t.ticker = time.NewTicker(time.Duration(t.rotateMaxLifeTime) * time.Second)
	}
}

// CheckCfg checks if the input config matches the ticker state.
func (t *maxLifeTimeRotateTicker) CheckCfg(cfg LoggerConfig) bool {
	if !cfg.Enabled || cfg.FileMaxLifetime <= 0 {
		return t.ticker == nil
	}
	return cfg.FileMaxLifetime == t.rotateMaxLifeTime && cfg.Filepath == t.rotateFile
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

// LogWriter is the interface for audit log writer.
type LogWriter interface {
	io.Writer
	Close() error
	Rotate() error
}

func newLogWriter(zapCfg *log.Config) LogWriter {
	w := &lumberjack.Logger{
		Filename:   zapCfg.File.Filename,
		MaxSize:    zapCfg.File.MaxSize,
		MaxBackups: zapCfg.File.MaxBackups,
		MaxAge:     zapCfg.File.MaxDays,
		LocalTime:  true,
	}

	if intest.InTest && injectWriterForTest != nil {
		return injectWriterForTest(zapCfg, w)
	}
	return w
}

// injectWriterForTest is used to inject the writer for testing.
var injectWriterForTest func(zapCfg *log.Config, w *lumberjack.Logger) LogWriter

func newZapLogger(cfg LoggerConfig) (*zap.Logger, LogWriter, error) {
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

	writer := newLogWriter(zapCfg)

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

	return logger, writer, nil
}
