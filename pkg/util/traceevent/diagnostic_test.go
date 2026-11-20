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
	"context"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/util/tracing"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestDiagnosticModePreservesLegacyTiKVTraceLogging(t *testing.T) {
	previous := diagnosticEnabled.Swap(false)
	previousCategories := tracing.GetEnabledCategories()
	tracing.SetCategories(tracing.AllCategories)
	t.Cleanup(func() {
		diagnosticEnabled.Store(previous)
		tracing.SetCategories(previousCategories)
	})
	want := handleTraceControlExtractor(context.Background())
	require.NotZero(t, want)
	diagnosticEnabled.Store(true)
	require.Equal(t, want, handleTraceControlExtractor(context.Background()))
}

func TestDiagnosticSinkFreezesMetadataBeforeAsyncWrite(t *testing.T) {
	path := filepath.Join(t.TempDir(), "trace.jsonl")
	sink, err := newDiagnosticSink(path, 16, "")
	require.NoError(t, err)
	traceID := []byte{1, 2}
	fields := []zap.Field{zap.Uint64("stream_id", 18446744073709551615),
		zap.Any("mutable", map[string]int{"value": 1}), zap.String("sql", "private SQL")}
	sink.Record(context.Background(), Event{Name: "batch.recv", Timestamp: time.Now(), TraceID: traceID, Fields: fields})
	traceID[0] = 99
	fields[0] = zap.Uint64("stream_id", 0)
	sink.close()
	file, err := os.Open(path)
	require.NoError(t, err)
	defer file.Close()
	decoder := json.NewDecoder(file)
	decoder.UseNumber()
	var rows []map[string]any
	for {
		var row map[string]any
		err := decoder.Decode(&row)
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		rows = append(rows, row)
	}
	require.Len(t, rows, 3)
	require.Equal(t, "trace_header", rows[0]["event"])
	require.Equal(t, "0102", rows[1]["trace_id"])
	require.Equal(t, "18446744073709551615", rows[1]["stream_id"].(json.Number).String())
	require.NotContains(t, rows[1], "mutable")
	require.NotContains(t, rows[1], "sql")
	require.Equal(t, "0", rows[2]["dropped"].(json.Number).String())
	require.Equal(t, "2", rows[2]["skipped_fields"].(json.Number).String())
}

func TestDiagnosticSinkCountsFullQueue(t *testing.T) {
	sink := &diagnosticSink{queue: make(chan Event, 1)}
	sink.active.Store(true)
	event := Event{Name: "batch.recv", Timestamp: time.Now()}
	sink.Record(context.Background(), event)
	sink.Record(context.Background(), event)
	require.Equal(t, uint64(1), sink.dropped.Load())
}

func TestDiagnosticCaptureTogglesWithoutReplacingLegacySink(t *testing.T) {
	control := filepath.Join(t.TempDir(), "capture")
	path := filepath.Join(t.TempDir(), "trace.jsonl")
	require.NoError(t, os.WriteFile(control, []byte("off"), 0600))
	sink, err := newDiagnosticSink(path, 16, control)
	require.NoError(t, err)
	defer sink.close()
	previousRecorder := diagnosticRecorder.Swap(sink)
	previousEnabled := diagnosticEnabled.Swap(false)
	previousSink := CurrentSink()
	previousMode := CurrentMode()
	t.Cleanup(func() {
		diagnosticRecorder.Store(previousRecorder)
		diagnosticEnabled.Store(previousEnabled)
	})
	for _, active := range []bool{false, true, false, true} {
		value := "off"
		if active {
			value = "on"
		}
		require.NoError(t, os.WriteFile(control, []byte(value), 0600))
		require.Eventually(t, func() bool { return sink.active.Load() == active && diagnosticEnabled.Load() == active }, time.Second, 10*time.Millisecond)
		require.Equal(t, previousSink, CurrentSink())
		require.Equal(t, previousMode, CurrentMode())
		sink.Record(context.Background(), Event{Name: "batch.recv", Timestamp: time.Now()})
	}
	require.NoError(t, os.WriteFile(control, []byte("invalid"), 0600))
	require.Eventually(t, func() bool { return !sink.active.Load() && !diagnosticEnabled.Load() }, time.Second, 10*time.Millisecond)
	sink.close()
	file, err := os.Open(path)
	require.NoError(t, err)
	defer file.Close()
	decoder := json.NewDecoder(file)
	count := 0
	for {
		var row map[string]any
		err := decoder.Decode(&row)
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		if row["event"] == "batch.recv" {
			count++
		}
	}
	require.Equal(t, 2, count)
}
