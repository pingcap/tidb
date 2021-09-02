package logutil

import (
	"strings"
	"sync"
	"time"

	"github.com/natefinch/lumberjack"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

const (
	generalLogBatchSize = 1024
	flushTimeout        = 1000 * time.Millisecond
)

// GeneralLogEntry represents the fields in a general query log
type GeneralLogEntry struct {
	ConnID                 uint64
	FnGetUser              func() string
	FnGetSchemaMetaVersion func() int64
	TxnStartTS             uint64
	TxnForUpdateTS         uint64
	IsReadConsistency      bool
	CurrentDB              string
	TxnMode                string
	FnGetQuery             func(*strings.Builder) string

	// following fields are for benchmark testing
	user              string
	schemaMetaVersion int64
	query             string
}

var fnGetQueryPool = sync.Pool{New: func() interface{} {
	ret := strings.Builder{}
	ret.Grow(128)
	return &ret
}}

// GeneralLogManager receives general log entry from channel, and log or drop them as needed
type GeneralLogManager struct {
	logger          *zap.Logger
	logEntryChan    chan *GeneralLogEntry
	droppedLogCount uint64
}

func newGeneralLog(logger *zap.Logger) *GeneralLogManager {
	gl := &GeneralLogManager{
		logger:          logger,
		logEntryChan:    make(chan *GeneralLogEntry, generalLogBatchSize),
		droppedLogCount: 0,
	}
	go gl.startFormatWorker()
	return gl
}

// startFormatWorker starts a log flushing worker that flushes log periodically or when batch is full
func (gl *GeneralLogManager) startFormatWorker() {
	for {
		e := <-gl.logEntryChan
		fnGetQueryBuf := fnGetQueryPool.Get().(*strings.Builder)
		gl.logger.Info("GENERAL_LOG",
			zap.Uint64("conn", e.ConnID),
			zap.String("user", e.FnGetUser()),
			zap.Int64("schemaVersion", e.FnGetSchemaMetaVersion()),
			zap.Uint64("txnStartTS", e.TxnStartTS),
			zap.Uint64("forUpdateTS", e.TxnForUpdateTS),
			zap.Bool("isReadConsistency", e.IsReadConsistency),
			zap.String("current_db", e.CurrentDB),
			zap.String("txn_mode", e.TxnMode),
			zap.String("sql", e.FnGetQuery(fnGetQueryBuf)),
		)
		fnGetQueryBuf.Reset()
		fnGetQueryPool.Put(fnGetQueryBuf)
	}
}

func newGeneralLogLogger() (*zap.Logger, error) {
	// create the general query logger
	encCfg := zap.NewProductionEncoderConfig()
	encCfg.LevelKey = zapcore.OmitKey
	encCfg.EncodeTime = zapcore.EpochNanosTimeEncoder
	jsonEncoder := zapcore.NewJSONEncoder(encCfg)
	ws := zapcore.AddSync(&lumberjack.Logger{
		Filename:   "general_log",
		MaxSize:    1000, // megabytes
		MaxBackups: 10,
	})
	wsBuffered := zapcore.BufferedWriteSyncer{
		WS:            ws,
		FlushInterval: 100 * time.Millisecond,
	}
	level := zap.NewAtomicLevelAt(zap.InfoLevel)
	ioCore := zapcore.NewCore(jsonEncoder, &wsBuffered, level)
	logger := zap.New(ioCore)

	return logger, nil
}
