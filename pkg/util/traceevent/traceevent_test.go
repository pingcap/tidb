// Copyright 2025 PingCAP, Inc.
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
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/config/kerneltype"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func installRecorderSink(t *testing.T, capacity int) *RingBufferSink {
	t.Helper()

	recorder := NewRingBufferSink(capacity)
	previous := CurrentSink()
	SetSink(recorder)
	t.Cleanup(func() {
		SetSink(previous)
	})
	return recorder
}

func TestTraceEventCategories(t *testing.T) {
	if kerneltype.IsClassic() {
		t.Skip("trace events only work for next-gen kernel")
	}
	original := GetEnabledCategories()
	t.Cleanup(func() {
		SetCategories(original)
	})

	require.True(t, IsEnabled(CoprRegionCache))

	Disable(CoprRegionCache)
	require.False(t, IsEnabled(CoprRegionCache))

	Enable(CoprRegionCache)
	require.True(t, IsEnabled(CoprRegionCache))

	SetCategories(0)
	require.False(t, IsEnabled(CoprRegionCache))
	SetCategories(AllCategories)
	require.True(t, IsEnabled(CoprRegionCache))
}

func TestTraceEventCategoryFiltering(t *testing.T) {
	if kerneltype.IsClassic() {
		t.Skip("trace events only work for next-gen kernel")
	}
	originalCategories := GetEnabledCategories()
	prevMode := CurrentMode()
	t.Cleanup(func() {
		SetCategories(originalCategories)
		_, _ = SetMode(prevMode)
		FlightRecorder().Reset()
	})

	SetCategories(0)
	_, _ = SetMode(ModeLogging)
	FlightRecorder().Reset()
	recorder := installRecorderSink(t, 8)

	ctx := context.Background()
	TraceEvent(CoprRegionCache, ctx, "should-not-record", zap.Int("value", 1))
	require.Empty(t, FlightRecorder().Snapshot())
	require.Empty(t, recorder.Snapshot())
}

func TestTraceEventModes(t *testing.T) {
	prev := CurrentMode()
	t.Cleanup(func() {
		_, _ = SetMode(prev)
	})

	mode, err := NormalizeMode(" ON ")
	require.NoError(t, err)
	require.Equal(t, ModeLogging, mode)

	mode, err = SetMode("logging")
	require.NoError(t, err)
	require.Equal(t, ModeLogging, mode)
	require.Equal(t, ModeLogging, CurrentMode())

	mode, err = SetMode("off")
	require.NoError(t, err)
	require.Equal(t, ModeOff, mode)
	require.Equal(t, ModeOff, CurrentMode())

	_, err = NormalizeMode("invalid")
	require.Error(t, err)
}

func TestTraceEventRecordsEvent(t *testing.T) {
	if kerneltype.IsClassic() {
		t.Skip("trace events only work for next-gen kernel")
	}
	originalCategories := GetEnabledCategories()
	prevMode := CurrentMode()
	prevSink := CurrentSink()
	t.Cleanup(func() {
		SetCategories(originalCategories)
		_, _ = SetMode(prevMode)
		SetSink(prevSink)
		FlightRecorder().Reset()
	})

	SetCategories(AllCategories)
	_, _ = SetMode(ModeLogging)
	FlightRecorder().Reset()
	recorder := installRecorderSink(t, 8)
	ctx := context.Background()

	TraceEvent(CoprRegionCache, ctx, "test-event",
		zap.Int("count", 42),
		zap.String("scope", "unit-test"))

	frEvents := FlightRecorder().Snapshot()
	require.Len(t, frEvents, 1)
	ev := frEvents[0]
	require.Equal(t, CoprRegionCache, ev.Category)
	require.Equal(t, "test-event", ev.Name)
	require.False(t, ev.Timestamp.IsZero())
	require.Len(t, ev.Fields, 2)

	recorded := recorder.Snapshot()
	require.Len(t, recorded, 1)
	require.Equal(t, "test-event", recorded[0].Name)
	require.Len(t, recorded[0].Fields, 2)
}

func TestTraceEventLoggingSwitch(t *testing.T) {
	if kerneltype.IsClassic() {
		t.Skip("trace events only work for next-gen kernel")
	}
	originalCategories := GetEnabledCategories()
	prevMode := CurrentMode()
	t.Cleanup(func() {
		SetCategories(originalCategories)
		_, _ = SetMode(prevMode)
		FlightRecorder().Reset()
	})

	SetCategories(AllCategories)
	_, _ = SetMode(ModeOff)
	FlightRecorder().Reset()
	recorder := installRecorderSink(t, 8)
	_, _ = SetMode(ModeOff)
	ctx := context.Background()

	flightBefore := len(FlightRecorder().Snapshot())

	require.Equal(t, ModeOff, CurrentMode())
	TraceEvent(CoprRegionCache, ctx, "disabled-log", zap.Int("value", 1))
	require.Equal(t, flightBefore+1, len(FlightRecorder().Snapshot()))
	disabledLogged := len(recorder.Snapshot())

	_, _ = SetMode(ModeLogging)
	TraceEvent(CoprRegionCache, ctx, "enabled-log", zap.Int("value", 2))
	fr := FlightRecorder().Snapshot()
	require.Len(t, fr, flightBefore+2)
	recorded := recorder.Snapshot()
	require.Len(t, recorded, disabledLogged+1)
	require.Equal(t, "enabled-log", recorded[len(recorded)-1].Name)
}

func TestRingBufferSnapshotOrder(t *testing.T) {
	recorder := NewRingBufferSink(2)

	ev1 := Event{Category: CoprRegionCache, Name: "first", Timestamp: time.Unix(0, 1)}
	ev2 := Event{Category: CoprRegionCache, Name: "second", Timestamp: time.Unix(0, 2)}
	ev3 := Event{Category: CoprRegionCache, Name: "third", Timestamp: time.Unix(0, 3)}

	recorder.Record(context.Background(), ev1)
	recorder.Record(context.Background(), ev2)
	require.Equal(t, []string{"first", "second"}, extractNames(recorder.Snapshot()))

	recorder.Record(context.Background(), ev3)
	require.Equal(t, []string{"second", "third"}, extractNames(recorder.Snapshot()))
}

func TestRingBufferFlushTo(t *testing.T) {
	recorder := NewRingBufferSink(4)
	ev := Event{
		Category:  CoprRegionCache,
		Name:      "flush",
		Timestamp: time.Unix(0, 123456000), // 123456 microseconds
		Fields: []zap.Field{
			zap.String("status", "ok"),
			zap.Int("count", 2),
		},
	}
	recorder.Record(context.Background(), ev)

	events := recorder.Snapshot()
	require.Len(t, events, 1)
	require.Equal(t, "flush", events[0].Name)
	require.Equal(t, CoprRegionCache, events[0].Category)
	require.Equal(t, ev.Timestamp, events[0].Timestamp)
	require.Len(t, events[0].Fields, 2)
}

func TestCategoryNames(t *testing.T) {
	cases := []struct {
		category TraceCategory
		name     string
	}{
		{CoprRegionCache, "copr-region-cache"},
		{TraceCategory(999), "unknown(999)"},
	}
	for _, tc := range cases {
		require.Equal(t, tc.name, getCategoryName(tc.category))
	}
}

// BenchmarkTraceEventDisabled benchmarks the overhead when tracing is disabled.
func BenchmarkTraceEventDisabled(b *testing.B) {
	ctx := context.Background()
	prevCategories := GetEnabledCategories()
	prevMode := CurrentMode()
	SetCategories(AllCategories)
	_, _ = SetMode(ModeOff)
	b.Cleanup(func() {
		SetCategories(prevCategories)
		_, _ = SetMode(prevMode)
	})

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		TraceEvent(CoprRegionCache, ctx, "benchmark-disabled",
			zap.String("key", "value"),
			zap.Int("iteration", i))
	}
}

// BenchmarkTraceEventEnabled benchmarks the overhead when tracing is enabled.
func BenchmarkTraceEventEnabled(b *testing.B) {
	ctx := context.Background()
	prevCategories := GetEnabledCategories()
	prevMode := CurrentMode()
	Enable(CoprRegionCache)
	_, _ = SetMode(ModeLogging)
	b.Cleanup(func() {
		SetCategories(prevCategories)
		_, _ = SetMode(prevMode)
	})

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		TraceEvent(CoprRegionCache, ctx, "benchmark-enabled",
			zap.String("key", "value"),
			zap.Int("iteration", i))
	}
	SetCategories(0)
}

func extractNames(events []Event) []string {
	names := make([]string, len(events))
	for i, ev := range events {
		names[i] = ev.Name
	}
	return names
}
