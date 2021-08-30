package logutil

import (
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/natefinch/lumberjack"
	"go.uber.org/zap"
	"go.uber.org/zap/buffer"
	"go.uber.org/zap/zapcore"
)

const (
	generalLogBatchSize = 102400
	flushTimeout        = 1000 * time.Millisecond
)

type GeneralLogEntry struct {
	ConnID                 uint64
	FnGetUser              func() string
	FnGetSchemaMetaVersion func() int64
	TxnStartTS             uint64
	TxnForUpdateTS         uint64
	FnGetReadConsistency   func() bool
	CurrentDB              string
	TxnMode                string
	FnGetQuery             func(*strings.Builder) string

	buf *buffer.Buffer
	// following fields are for benchmark testing
	user              string
	schemaMetaVersion int64
	isReadConsistency bool
	query             string
}

const _hex = "0123456789abcdef"

var fnGetQueryPool = sync.Pool{New: func() interface{} {
	ret := strings.Builder{}
	ret.Grow(128)
	return &ret
}}

func (e *GeneralLogEntry) writeToBufferDirect(buf *buffer.Buffer) {
	e.buf = buf
	e.buf.AppendString("[GENERAL_LOG] [conn=")
	e.buf.AppendUint(e.ConnID)
	e.buf.AppendString("] [user=")
	e.safeAddStringWithQuote(e.user)
	e.buf.AppendString("] [schemaVersion=")
	e.buf.AppendInt(e.schemaMetaVersion)
	e.buf.AppendString("] [txnStartTS=")
	e.buf.AppendUint(e.TxnStartTS)
	e.buf.AppendString("] [forUpdateTS=")
	e.buf.AppendUint(e.TxnForUpdateTS)
	e.buf.AppendString("] [isReadConsistency=")
	e.buf.AppendBool(e.isReadConsistency)
	e.buf.AppendString("] [current_db=")
	e.buf.AppendString(e.CurrentDB)
	e.buf.AppendString("] [txn_mode=")
	e.buf.AppendString(e.TxnMode)
	e.buf.AppendString("] [sql=")
	fnGetQueryBuf := fnGetQueryPool.Get().(*strings.Builder)
	e.safeAddStringWithQuote(e.query)
	fnGetQueryBuf.Reset()
	fnGetQueryPool.Put(fnGetQueryBuf)
	e.buf.AppendString("]")
}

// adapted from pingcap/log.textEncoder
func (e *GeneralLogEntry) safeAddStringWithQuote(s string) {
	if !needDoubleQuotes(s) {
		e.safeAddString(s)
		return
	}
	e.buf.AppendByte('"')
	e.safeAddString(s)
	e.buf.AppendByte('"')
}

// adapted from pingcap/log.textEncoder
func (e *GeneralLogEntry) safeAddString(s string) {
	for i := 0; i < len(s); {
		if e.tryAddRuneSelf(s[i]) {
			i++
			continue
		}
		r, size := utf8.DecodeRuneInString(s[i:])
		if e.tryAddRuneError(r, size) {
			i++
			continue
		}
		e.buf.AppendString(s[i : i+size])
		i += size
	}
}

// adapted from pingcap/log.textEncoder
func (e *GeneralLogEntry) tryAddRuneSelf(b byte) bool {
	if b >= utf8.RuneSelf {
		return false
	}
	if 0x20 <= b && b != '\\' && b != '"' {
		e.buf.AppendByte(b)
		return true
	}
	switch b {
	case '\\', '"':
		e.buf.AppendByte('\\')
		e.buf.AppendByte(b)
	case '\n':
		e.buf.AppendByte('\\')
		e.buf.AppendByte('n')
	case '\r':
		e.buf.AppendByte('\\')
		e.buf.AppendByte('r')
	case '\t':
		e.buf.AppendByte('\\')
		e.buf.AppendByte('t')

	default:
		// Encode bytes < 0x20, except for the escape sequences above.
		e.buf.AppendString(`\u00`)
		e.buf.AppendByte(_hex[b>>4])
		e.buf.AppendByte(_hex[b&0xF])
	}
	return true
}

// adapted from pingcap/log.textEncoder
func (e *GeneralLogEntry) tryAddRuneError(r rune, size int) bool {
	if r == utf8.RuneError && size == 1 {
		e.buf.AppendString(`\ufffd`)
		return true
	}
	return false
}

// copied from pingcap/log.textEncoder
// See [log-fileds](https://github.com/tikv/rfcs/blob/master/text/0018-unified-log-format.md#log-fields-section).
func needDoubleQuotes(s string) bool {
	for i := 0; i < len(s); {
		b := s[i]
		if b <= 0x20 {
			return true
		}
		switch b {
		case '\\', '"', '[', ']', '=':
			return true
		}
		i++
	}
	return false
}

type GeneralLog struct {
	bufPool         buffer.Pool
	logger          *zap.Logger
	logEntryChan    chan *GeneralLogEntry
	droppedLogCount uint64
}

func newGeneralLog(logger *zap.Logger) *GeneralLog {
	gl := &GeneralLog{
		bufPool:         buffer.NewPool(),
		logger:          logger,
		logEntryChan:    make(chan *GeneralLogEntry, generalLogBatchSize),
		droppedLogCount: 0,
	}
	go gl.startFormatWorker()
	return gl
}

// startFormatWorker starts a log flushing worker that flushes log periodically or when batch is full
func (gl *GeneralLog) startFormatWorker() {
	for {
		e := <-gl.logEntryChan
		fnGetQueryBuf := fnGetQueryPool.Get().(*strings.Builder)
		gl.logger.Info("GENERAL_LOG",
			zap.Uint64("conn", e.ConnID),
			zap.String("user", e.FnGetUser()),
			zap.Int64("schemaVersion", e.FnGetSchemaMetaVersion()),
			zap.Uint64("txnStartTS", e.TxnStartTS),
			zap.Uint64("forUpdateTS", e.TxnForUpdateTS),
			zap.Bool("isReadConsistency", e.FnGetReadConsistency()),
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
