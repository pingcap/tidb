// Copyright 2026 PingCAP, Inc.
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

package traceevent

import (
	"bufio"
	"context"
	"encoding/hex"
	"encoding/json"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// diagnosticSink is enabled only by TIDB_CQ_TRACE_FILE in an experiment image.
// Events retain their original monotonic timestamps; serialization and disk IO
// happen on one separate goroutine. No object/array fields or SQL text are kept.
type diagnosticSink struct {
	queue   chan Event
	stop    chan struct{}
	done    chan struct{}
	mu      sync.RWMutex
	closed  bool
	dropped atomic.Uint64
	skipped atomic.Uint64
	active  atomic.Bool
	control string
}

var diagnosticEnabled atomic.Bool
var diagnosticRecorder atomic.Pointer[diagnosticSink]

const diagnosticCategories = StmtLifecycle | TxnLifecycle | Txn2PC | KvRequest

func initDiagnosticRecorderFromEnv() {
	path := os.Getenv("TIDB_CQ_TRACE_FILE")
	if path == "" {
		return
	}
	sink, err := newDiagnosticSink(path, 65536, os.Getenv("TIDB_CQ_TRACE_CONTROL"))
	if err != nil {
		panic(err)
	}
	// Tap selected events independently of the existing sink and tracing policy.
	diagnosticRecorder.Store(sink)
	diagnosticEnabled.Store(sink.active.Load())
}

func newDiagnosticSink(path string, capacity int, control string) (*diagnosticSink, error) {
	// A fresh file prevents mixing different process lifetimes and clock origins.
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return nil, err
	}
	sink := &diagnosticSink{queue: make(chan Event, capacity), stop: make(chan struct{}), done: make(chan struct{}), control: control}
	sink.active.Store(sink.captureRequested())
	go sink.write(file, time.Now())
	return sink, nil
}

func (sink *diagnosticSink) captureRequested() bool {
	if sink.control == "" {
		return true
	}
	value, err := os.ReadFile(sink.control)
	return err == nil && strings.TrimSpace(string(value)) == "on"
}

func diagnosticEvent(name string) bool {
	switch name {
	case "stmt.start", "stmt.finish", "txn.commit.start", "txn.commit.finish",
		"kv.request.send", "kv.request.result", "prewrite.batch.start", "prewrite.batch.result",
		"commit.batch.start", "commit.batch.result":
		return true
	}
	return strings.HasPrefix(name, "batch.") || strings.HasPrefix(name, "tso.commit.") ||
		strings.HasPrefix(name, "2pc.execute.") || strings.HasPrefix(name, "sql.")
}

// Record freezes scalar metadata only. Mutable protobuf objects must never be
// dereferenced later on the writer goroutine. Closing cannot race a queue send.
func (sink *diagnosticSink) Record(_ context.Context, event Event) {
	if !sink.active.Load() || !diagnosticEvent(event.Name) {
		return
	}
	fields := make([]zap.Field, 0, len(event.Fields))
	for _, field := range event.Fields {
		if (strings.Contains(field.Key, "key") && field.Key != "keyspace_id") || field.Key == "sql" {
			sink.skipped.Add(1)
			continue
		}
		switch field.Type {
		case zapcore.BoolType, zapcore.DurationType, zapcore.Float32Type, zapcore.Float64Type,
			zapcore.Int64Type, zapcore.Int32Type, zapcore.Int16Type, zapcore.Int8Type,
			zapcore.Uint64Type, zapcore.Uint32Type, zapcore.Uint16Type, zapcore.Uint8Type,
			zapcore.StringType:
			field.Interface = nil
			fields = append(fields, field)
		default:
			sink.skipped.Add(1)
		}
	}
	event.Fields = fields
	event.TraceID = append([]byte(nil), event.TraceID...)
	sink.mu.RLock()
	defer sink.mu.RUnlock()
	if sink.closed {
		return
	}
	select {
	case sink.queue <- event:
	default:
		sink.dropped.Add(1)
	}
}

func (sink *diagnosticSink) close() {
	sink.mu.Lock()
	if !sink.closed {
		sink.closed = true
		close(sink.stop)
	}
	sink.mu.Unlock()
	<-sink.done
}

func (sink *diagnosticSink) write(file *os.File, start time.Time) {
	defer close(sink.done)
	defer file.Close()
	writer := bufio.NewWriterSize(file, 256*1024)
	jsonWriter := json.NewEncoder(writer)
	must := func(err error) {
		if err != nil {
			panic(err)
		}
	}
	must(jsonWriter.Encode(map[string]any{"event": "trace_header", "schema": 1,
		"pid": os.Getpid(), "unix_ns": start.UnixNano(), "clock": "process_monotonic_ns", "active": sink.active.Load()}))
	encoder := zapcore.NewJSONEncoder(zapcore.EncoderConfig{MessageKey: "event", EncodeDuration: zapcore.NanosDurationEncoder})
	emit := func(event Event) {
		fields := append(event.Fields, zap.String("trace_id", hex.EncodeToString(event.TraceID)),
			zap.Int64("at_ns", event.Timestamp.Sub(start).Nanoseconds()), zap.Int64("unix_ns", event.Timestamp.UnixNano()))
		buffer, err := encoder.EncodeEntry(zapcore.Entry{Message: event.Name}, fields)
		must(err)
		_, err = writer.Write(buffer.Bytes())
		buffer.Free()
		must(err)
	}
	flush := func() {
		must(jsonWriter.Encode(map[string]any{"event": "trace_stats", "dropped": sink.dropped.Load(), "skipped_fields": sink.skipped.Load(),
			"active": sink.active.Load(), "unix_ns": time.Now().UnixNano()}))
		must(writer.Flush())
	}
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	controlTicker := time.NewTicker(100 * time.Millisecond)
	defer controlTicker.Stop()
	for {
		select {
		case event := <-sink.queue:
			emit(event)
		case <-ticker.C:
			flush()
		case <-controlTicker.C:
			active := sink.captureRequested()
			if sink.active.Swap(active) != active {
				if diagnosticRecorder.Load() == sink {
					diagnosticEnabled.Store(active)
				}
				must(jsonWriter.Encode(map[string]any{"event": "trace_control", "active": active, "unix_ns": time.Now().UnixNano()}))
				flush()
			}
		case <-sink.stop:
			for {
				select {
				case event := <-sink.queue:
					emit(event)
				default:
					flush()
					return
				}
			}
		}
	}
}
