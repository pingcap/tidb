// Copyright 2024 PingCAP, Inc.
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

package auditlog

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/pingcap/log"
	"go.uber.org/zap"
)

const (
	defaultBufferSize    = 4096
	defaultBatchSize     = 256
	defaultFlushInterval = 5 * time.Second
	maxLogFileSize       = 256 * 1024 * 1024 // 256MB
)

// WriterConfig holds configuration for the audit log writer.
type WriterConfig struct {
	LogDir        string
	BufferSize    int
	BatchSize     int
	FlushInterval time.Duration
	MaxFileSize   int64
}

// AuditWriter handles async batch writing of audit events to log files.
type AuditWriter struct {
	config    WriterConfig
	eventCh   chan *AuditEvent
	stopCh    chan struct{}
	wg        sync.WaitGroup
	mu        sync.Mutex
	batch     []*AuditEvent
	currentFn string
	fileSize  int64
	stopped   bool
}

// NewAuditWriter creates a new AuditWriter with the given configuration.
func NewAuditWriter(cfg WriterConfig) *AuditWriter {
	if cfg.BufferSize <= 0 {
		cfg.BufferSize = defaultBufferSize
	}
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = defaultBatchSize
	}
	if cfg.FlushInterval <= 0 {
		cfg.FlushInterval = defaultFlushInterval
	}
	if cfg.MaxFileSize <= 0 {
		cfg.MaxFileSize = maxLogFileSize
	}
	return &AuditWriter{
		config:  cfg,
		eventCh: make(chan *AuditEvent, cfg.BufferSize),
		stopCh:  make(chan struct{}),
		batch:   make([]*AuditEvent, 0, cfg.BatchSize),
	}
}

// Start begins the background flush goroutine.
func (w *AuditWriter) Start() error {
	if err := os.MkdirAll(w.config.LogDir, 0750); err != nil {
		return fmt.Errorf("failed to create audit log directory: %w", err)
	}
	w.currentFn = w.generateFileName()
	w.wg.Add(1)
	go w.flushLoop()
	return nil
}

// Write enqueues an audit event for async writing.
func (w *AuditWriter) Write(event *AuditEvent) {
	if w.stopped {
		return
	}
	select {
	case w.eventCh <- event:
		AuditLogBufferUsage.WithLabelValues().Set(float64(len(w.eventCh)))
	default:
		AuditLogWriteErrors.WithLabelValues("buffer_full").Inc()
		log.Warn("audit log buffer full, dropping event",
			zap.String("user", event.User),
			zap.String("query_type", string(event.QueryType)))
	}
}

// Stop gracefully stops the writer, flushing remaining events.
func (w *AuditWriter) Stop() {
	w.mu.Lock()
	if w.stopped {
		w.mu.Unlock()
		return
	}
	w.stopped = true
	w.mu.Unlock()
	close(w.stopCh)
	w.wg.Wait()
}

// flushLoop runs in a background goroutine, periodically flushing batched events.
func (w *AuditWriter) flushLoop() {
	defer w.wg.Done()
	ticker := time.NewTicker(w.config.FlushInterval)
	defer ticker.Stop()

	done := make(chan struct{})

	for {
		select {
		case event, ok := <-w.eventCh:
			if !ok {
				w.flushBatch()
				return
			}
			w.mu.Lock()
			w.batch = append(w.batch, event)
			needFlush := len(w.batch) >= w.config.BatchSize
			w.mu.Unlock()
			if needFlush {
				w.flushBatch()
			}
		case <-ticker.C:
			w.flushBatch()
		case <-done:
			w.flushBatch()
			return
		}
	}
}

// flushBatch writes the current batch of events to the log file.
func (w *AuditWriter) flushBatch() {
	w.mu.Lock()
	if len(w.batch) == 0 {
		w.mu.Unlock()
		return
	}
	events := w.batch
	w.batch = make([]*AuditEvent, 0, w.config.BatchSize)
	w.mu.Unlock()

	start := time.Now()

	for _, event := range events {
		// Open file for each event write to ensure data durability
		f, err := os.OpenFile(w.currentFn, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0640)
		if err != nil {
			AuditLogWriteErrors.WithLabelValues("file_open").Inc()
			log.Error("failed to open audit log file", zap.Error(err))
			continue
		}
		defer f.Close()

		data, err := json.Marshal(event)
		if err != nil {
			AuditLogWriteErrors.WithLabelValues("marshal").Inc()
			continue
		}
		data = append(data, '\n')

		n, err := f.Write(data)
		if err != nil {
			AuditLogWriteErrors.WithLabelValues("write").Inc()
			log.Error("failed to write audit event", zap.Error(err))
			continue
		}
		w.fileSize += int64(n)

		AuditLogEventsTotal.WithLabelValues(string(event.QueryType), "success").Inc()
	}

	// Check if file rotation is needed
	if w.fileSize >= w.config.MaxFileSize {
		w.rotateFile()
	}

	AuditLogFlushDuration.WithLabelValues().Observe(time.Since(start).Seconds())
	AuditLogBufferUsage.WithLabelValues().Set(float64(len(w.eventCh)))
}

// rotateFile creates a new log file for subsequent writes.
func (w *AuditWriter) rotateFile() {
	w.currentFn = w.generateFileName()
	w.fileSize = 0
	log.Info("audit log file rotated", zap.String("new_file", w.currentFn))
}

// generateFileName creates a timestamped log file name.
func (w *AuditWriter) generateFileName() string {
	ts := time.Now().Format("20060102-150405")
	return filepath.Join(w.config.LogDir, fmt.Sprintf("audit-%s.log", ts))
}
