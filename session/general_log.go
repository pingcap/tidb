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

	"github.com/natefinch/lumberjack"
	"github.com/pingcap/parser/auth"
	"github.com/pingcap/tidb/br/pkg/lightning/log"
	"github.com/pingcap/tidb/metrics"
	"go.uber.org/zap"
	"go.uber.org/zap/buffer"
	"go.uber.org/zap/zapcore"
)

var (
	// GeneralLogLogger is used to log general log
	GeneralLogLogger       = log.L()
	globalGeneralLogger    *generalLogger
	generalLogDroppedEntry = metrics.GeneralLogDroppedCount
	stringBufferPool       = buffer.NewPool()
	queryBuilderPool       = sync.Pool{New: func() interface{} {
		ret := strings.Builder{}
		ret.Grow(128)
		return &ret
	}}
	glEntryPool = sync.Pool{New: func() interface{} {
		return &GeneralLogEntry{}
	}}
)

// putGeneralLogOrDrop sends the general log entry to the logging goroutine
// The entry will be dropped once the channel is full
func putGeneralLogOrDrop(entry *GeneralLogEntry) {
	select {
	case globalGeneralLogger.logEntryChan <- entry:
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

// GeneralLogEntry represents the fields in a general query log
type GeneralLogEntry struct {
	ConnID                 uint64
	User                   auth.UserIdentity
	FnGetSchemaMetaVersion func() int64
	TxnStartTS             uint64
	TxnForUpdateTS         uint64
	IsReadConsistency      bool
	CurrentDB              string
	TxnMode                string
	FnGetQuery             func(*strings.Builder) string
}

// getGeneralLogEntry get a general log entry from the object pool
func getGeneralLogEntry() *GeneralLogEntry {
	return glEntryPool.Get().(*GeneralLogEntry)
}

// generalLogger receives general log entry from channel, and log or drop them as needed
type generalLogger struct {
	logger       *zap.Logger
	logEntryChan chan *GeneralLogEntry
	quit         chan struct{}
}

func newGeneralLogger(logger *zap.Logger) *generalLogger {
	gl := &generalLogger{
		logger:       logger,
		logEntryChan: make(chan *GeneralLogEntry, 10000),
		quit:         make(chan struct{}),
	}
	go gl.startLogWorker()
	return gl
}

func userToString(user auth.UserIdentity) string {
	buf := stringBufferPool.Get()
	buf.WriteString(user.Username)
	buf.WriteByte('@')
	buf.WriteString(user.Hostname)
	ret := buf.String()
	buf.Free()
	return ret
}

func (gl *generalLogger) logEntry(e *GeneralLogEntry) {
	fnGetQueryBuf := queryBuilderPool.Get().(*strings.Builder)
	gl.logger.Info("GENERAL_LOG",
		zap.Uint64("conn", e.ConnID),
		zap.String("user", userToString(e.User)),
		zap.Int64("schemaVersion", e.FnGetSchemaMetaVersion()),
		zap.Uint64("txnStartTS", e.TxnStartTS),
		zap.Uint64("forUpdateTS", e.TxnForUpdateTS),
		zap.Bool("isReadConsistency", e.IsReadConsistency),
		zap.String("current_db", e.CurrentDB),
		zap.String("txn_mode", e.TxnMode),
		zap.String("sql", e.FnGetQuery(fnGetQueryBuf)),
	)
	fnGetQueryBuf.Reset()
	queryBuilderPool.Put(fnGetQueryBuf)
	glEntryPool.Put(e)
}

// startLogWorker starts a log flushing worker that flushes log periodically or when batch is full
func (gl *generalLogger) startLogWorker() {
	for {
		select {
		case e := <-gl.logEntryChan:
			gl.logEntry(e)
		case <-gl.quit:
			for e := range gl.logEntryChan {
				gl.logEntry(e)
			}
			return
		}
	}
}

func (gl *generalLogger) stopLogWorker() {
	close(gl.quit)
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
