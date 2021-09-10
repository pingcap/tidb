// Copyright 2021 PingCAP, Inc.
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

package session

import (
	"strings"
	"sync"
	"time"
	"unsafe"

	"github.com/natefinch/lumberjack"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/tidb/br/pkg/lightning/log"
	"github.com/pingcap/tidb/metrics"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var (
	// GeneralLogLogger is used to log general log
	GeneralLogLogger       = log.L()
	globalGeneralLogger    *generalLogger
	generalLogDroppedEntry = metrics.GeneralLogDroppedCount
	queryBuilderPool       = sync.Pool{New: func() interface{} {
		ret := strings.Builder{}
		ret.Grow(128)
		return &ret
	}}
	glEntryPool = sync.Pool{New: func() interface{} {
		return &generalLogEntry{}
	}}
)

// putGeneralLogOrDrop sends the general log entry to the logging goroutine
// The entry will be dropped once the channel is full, or general log has been disabled
func putGeneralLogOrDrop(entry *generalLogEntry) {
	select {
	case <-globalGeneralLogger.quit:
		generalLogDroppedEntry.Inc()
		glEntryPool.Put(entry)
	default:
	}

	select {
	case globalGeneralLogger.logEntryChan <- entry:
	// If the general logger is closed, we should not put entry to the log channel anymore.
	case <-globalGeneralLogger.quit:
		generalLogDroppedEntry.Inc()
		glEntryPool.Put(entry)
	default:
		// When logEntryChan is full, the system resource is under so much pressure that the logging system capacity
		// is reduced. We should NOT output another warning log saying that we are dropping a general log, which will
		// add more pressure on the system load.
		generalLogDroppedEntry.Inc()
		glEntryPool.Put(entry)
	}
}

// InitGeneralLog initialize general query logger, which will starts a format & logging worker goroutine
// General query logs are sent to the worker through a channel, which is asynchronously flushed to logging files.
func InitGeneralLog() {
	GeneralLogLogger := newGeneralLogLogger()
	globalGeneralLogger = newGeneralLogger(GeneralLogLogger)
}

// StopGeneralLog stops the general log worker goroutine
func StopGeneralLog() {
	globalGeneralLogger.stopLogWorker()
}

// generalLogEntry represents the fields in a general query log
type generalLogEntry struct {
	ConnID            uint64
	User              string
	SchemaMetaVersion int64
	TxnStartTS        uint64
	TxnForUpdateTS    uint64
	IsReadConsistency bool
	CurrentDB         string
	TxnMode           string
	Query             generalLogEntryQuery
}
type generalLogEntryQuery struct {
	isPrepared      bool
	originalText    string
	stmtNode        ast.StmtNode
	stmtCtx         *stmtctx.StatementContext
	preparedParams  variable.PreparedParams
	enableRedactLog bool
}

// getGeneralLogEntry get a general log entry from the object pool
func getGeneralLogEntry() *generalLogEntry {
	return glEntryPool.Get().(*generalLogEntry)
}

// generalLogger receives general log entry from channel, and log or drop them as needed
type generalLogger struct {
	logger       *zap.Logger
	logEntryChan chan *generalLogEntry
	quit         chan struct{}
	wg           sync.WaitGroup
}

func newGeneralLogger(logger *zap.Logger) *generalLogger {
	gl := &generalLogger{
		logger:       logger,
		logEntryChan: make(chan *generalLogEntry, 10000),
		quit:         make(chan struct{}),
	}
	gl.wg.Add(1)
	go gl.startLogWorker()
	return gl
}

func (gl *generalLogger) logEntry(e *generalLogEntry) {
	buf := queryBuilderPool.Get().(*strings.Builder)
	q := e.Query
	if q.isPrepared {
		buf.WriteString(q.originalText)
	} else {
		// The following logic should be kept in sync with ExecStmt.GetTextToLog
		if q.enableRedactLog {
			sql, _ := q.stmtCtx.SQLDigest()
			buf.WriteString(sql)
		} else if sensitiveStmt, ok := q.stmtNode.(ast.SensitiveStmtNode); ok {
			buf.WriteString(sensitiveStmt.SecureText())
		} else {
			buf.WriteString(q.stmtCtx.OriginalSQL)
			buf.WriteString(q.preparedParams.String())
		}
	}
	bufString := buf.String()
	queryMutable := *(*[]byte)(unsafe.Pointer(&bufString))
	for i, b := range queryMutable {
		if b == '\r' || b == '\n' || b == '\t' {
			queryMutable[i] = ' '
		}
	}
	gl.logger.Info("GENERAL_LOG",
		zap.Uint64("conn", e.ConnID),
		zap.String("user", e.User),
		zap.Int64("schemaVersion", e.SchemaMetaVersion),
		zap.Uint64("txnStartTS", e.TxnStartTS),
		zap.Uint64("forUpdateTS", e.TxnForUpdateTS),
		zap.Bool("isReadConsistency", e.IsReadConsistency),
		zap.String("current_db", e.CurrentDB),
		zap.String("txn_mode", e.TxnMode),
		zap.String("sql", buf.String()),
	)
	buf.Reset()
	queryBuilderPool.Put(buf)
	glEntryPool.Put(e)
}

// startLogWorker starts a log flushing worker that flushes log periodically or when batch is full
func (gl *generalLogger) startLogWorker() {
	for {
		select {
		case e := <-gl.logEntryChan:
			gl.logEntry(e)
		case <-gl.quit:
			// Try to flush the buffered log entries.
			// This may leave some entries which is currently in flight
			currLen := len(gl.logEntryChan)
			for i := 0; i < currLen; i++ {
				e := <-gl.logEntryChan
				gl.logEntry(e)
			}
			gl.wg.Done()
			return
		}
	}
}

func (gl *generalLogger) stopLogWorker() {
	close(gl.quit)
	gl.wg.Wait()
}

func newGeneralLogLogger() *zap.Logger {
	// create the general query logger
	encCfg := zap.NewProductionEncoderConfig()
	encCfg.LevelKey = zapcore.OmitKey
	encCfg.EncodeTime = zapcore.EpochNanosTimeEncoder
	jsonEncoder := zapcore.NewJSONEncoder(encCfg)
	ws := zapcore.AddSync(&lumberjack.Logger{
		Filename:   "general_log",
		MaxSize:    1000, // MB
		MaxBackups: 10,
	})
	wsBuffered := zapcore.BufferedWriteSyncer{
		WS:            ws,
		FlushInterval: 100 * time.Millisecond,
	}
	level := zap.NewAtomicLevelAt(zap.InfoLevel)
	ioCore := zapcore.NewCore(jsonEncoder, &wsBuffered, level)
	logger := zap.New(ioCore)

	return logger
}
