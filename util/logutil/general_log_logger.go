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
}

var fnGetQueryPool = sync.Pool{New: func() interface{} {
	ret := strings.Builder{}
	ret.Grow(128)
	return &ret
}}

// GeneralLogManager receives general log entry from channel, and log or drop them as needed
type GeneralLogManager struct {
	logger       *zap.Logger
	logEntryChan chan *GeneralLogEntry
	quit         chan struct{}
}

func newGeneralLogger(logger *zap.Logger) *GeneralLogManager {
	gl := &GeneralLogManager{
		logger:       logger,
		logEntryChan: make(chan *GeneralLogEntry, generalLogBatchSize),
		quit:         make(chan struct{}),
	}
	go gl.startLogWorker()
	return gl
}

func (gl *GeneralLogManager) logEntry(e *GeneralLogEntry) {
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

// startLogWorker starts a log flushing worker that flushes log periodically or when batch is full
func (gl *GeneralLogManager) startLogWorker() {
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

func (gl *GeneralLogManager) stopLogWorker() {
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

	return logger
}
