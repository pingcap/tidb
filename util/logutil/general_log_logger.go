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
	buf *buffer.Buffer

	ConnID                 uint64
	FnGetUser              func() string
	User                   string
	FnGetSchemaMetaVersion func() int64
	SchemaMetaVersion      int64
	TxnStartTS             uint64
	TxnForUpdateTS         uint64
	IsReadConsistency      bool
	CurrentDB              string
	TxnMode                string
	FnGetQuery             func(*strings.Builder) string
	Query                  string
}

const _hex = "0123456789abcdef"

var fnGetQueryPool = sync.Pool{New: func() interface{} {
	ret := strings.Builder{}
	ret.Grow(128)
	return &ret
}}

func (e *GeneralLogEntry) writeToBuffer(buf *buffer.Buffer) {
	e.buf = buf

	e.buf.AppendString("[GENERAL_LOG] [conn=")
	e.buf.AppendUint(e.ConnID)
	e.buf.AppendString("] [user=")
	e.safeAddStringWithQuote(e.FnGetUser())
	e.buf.AppendString("] [schemaVersion=")
	e.buf.AppendInt(e.FnGetSchemaMetaVersion())
	e.buf.AppendString("] [txnStartTS=")
	e.buf.AppendUint(e.TxnStartTS)
	e.buf.AppendString("] [forUpdateTS=")
	e.buf.AppendUint(e.TxnForUpdateTS)
	e.buf.AppendString("] [isReadConsistency=")
	e.buf.AppendBool(e.IsReadConsistency)
	e.buf.AppendString("] [current_db=")
	e.buf.AppendString(e.CurrentDB)
	e.buf.AppendString("] [txn_mode=")
	e.buf.AppendString(e.TxnMode)
	e.buf.AppendString("] [sql=")
	fnGetQueryBuf := fnGetQueryPool.Get().(*strings.Builder)
	e.safeAddStringWithQuote(e.FnGetQuery(fnGetQueryBuf))
	fnGetQueryBuf.Reset()
	fnGetQueryPool.Put(fnGetQueryBuf)
	e.buf.AppendString("]")
}

func (e *GeneralLogEntry) writeToBufferDirect(buf *buffer.Buffer) {
	e.buf = buf

	e.buf.AppendString("[GENERAL_LOG] [conn=")
	e.buf.AppendUint(e.ConnID)
	e.buf.AppendString("] [user=")
	e.safeAddStringWithQuote(e.User)
	e.buf.AppendString("] [schemaVersion=")
	e.buf.AppendInt(e.SchemaMetaVersion)
	e.buf.AppendString("] [txnStartTS=")
	e.buf.AppendUint(e.TxnStartTS)
	e.buf.AppendString("] [forUpdateTS=")
	e.buf.AppendUint(e.TxnForUpdateTS)
	e.buf.AppendString("] [isReadConsistency=")
	e.buf.AppendBool(e.IsReadConsistency)
	e.buf.AppendString("] [current_db=")
	e.buf.AppendString(e.CurrentDB)
	e.buf.AppendString("] [txn_mode=")
	e.buf.AppendString(e.TxnMode)
	e.buf.AppendString("] [sql=")
	fnGetQueryBuf := fnGetQueryPool.Get().(*strings.Builder)
	e.safeAddStringWithQuote(e.Query)
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
	bufPool      buffer.Pool
	logger       *zap.Logger
	logEntryChan chan *GeneralLogEntry
	logBufChan   chan *buffer.Buffer
}

func newGeneralLog(logger *zap.Logger) *GeneralLog {
	gl := &GeneralLog{
		bufPool:      buffer.NewPool(),
		logger:       logger,
		logEntryChan: make(chan *GeneralLogEntry, generalLogBatchSize),
		logBufChan:   make(chan *buffer.Buffer, generalLogBatchSize),
	}
	for i := 0; i < 5; i++ {
		go gl.startFormatWorker()
	}
	go gl.startLogWorker()
	return gl
}

// TODO(dragonly): try zapcore.BufferedWriteSyncer
// startFormatWorker starts a log flushing worker that flushes log periodically or when batch is full
func (gl *GeneralLog) startFormatWorker() {
	var buf *buffer.Buffer
	for {
		buf = gl.bufPool.Get()
		logEntry := <-gl.logEntryChan
		logEntry.writeToBuffer(buf)
		gl.logBufChan <- buf
	}
}

func (gl *GeneralLog) startLogWorker() {
	var buf buffer.Buffer
	logCount := 0
	timeout := time.After(flushTimeout)
	for {
		select {
		case logBuf := <-gl.logBufChan:
			if logCount > 0 {
				buf.WriteByte('\n')
			}
			buf.WriteString(logBuf.String())
			logCount += 1
			logBuf.Free()
			if logCount == generalLogBatchSize {
				gl.logger.Info(buf.String())
				buf.Reset()
				logCount = 0
			}
		case <-timeout:
			if logCount > 0 {
				gl.logger.Info(buf.String())
				buf.Reset()
				logCount = 0
			}
			timeout = time.After(flushTimeout)
		}
	}
}

func newGeneralLogLogger() (*zap.Logger, error) {
	// create the general query logger
	encCfg := zap.NewProductionEncoderConfig()
	encCfg.LevelKey = zapcore.OmitKey
	encCfg.EncodeTime = zapcore.EpochNanosTimeEncoder
	jsonEncoder := zapcore.NewJSONEncoder(encCfg)
	writeSyncer := zapcore.AddSync(&lumberjack.Logger{
		Filename:   "general_log",
		MaxSize:    1000, // megabytes
		MaxBackups: 3,
		MaxAge:     28, // days
	})
	level := zap.NewAtomicLevelAt(zap.InfoLevel)
	ioCore := zapcore.NewCore(jsonEncoder, writeSyncer, level)
	logger := zap.New(ioCore)

	return logger, nil
}
