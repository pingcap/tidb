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
	"github.com/tikv/client-go/v2/trace"
	"go.uber.org/zap"
)

func TestSuite(t *testing.T) {
	// Those tests use global variables, make them run in sequence to avoid flaky.
	t.Run("TestTraceEventCategories", testTraceEventCategories)
	t.Run("TestTraceEventCategoryFiltering", testTraceEventCategoryFiltering)
	t.Run("TestTraceEventRecordsEvent", testTraceEventRecordsEvent)
	t.Run("TestTraceEventCarriesTraceID", testTraceEventCarriesTraceID)
	t.Run("TestTraceEventLoggingSwitch", testTraceEventLoggingSwitch)
	t.Run("TestFlightRecorderCoolingOff", testFlightRecorderCoolingOff)
}

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

func testTraceEventCategories(t *testing.T) {
	if kerneltype.IsClassic() {
		t.Skip("trace events only work for next-gen kernel")
	}

	var conf FlightRecorderConfig
	conf.Initialize()
	conf.EnabledCategories = []string{"*"}
	err := StartLogFlightRecorder(&conf)
	require.NoError(t, err)
	fr := GetFlightRecorder()
	defer fr.Close()

	require.True(t, IsEnabled(TxnLifecycle))

	fr.Disable(TxnLifecycle)
	require.False(t, IsEnabled(TxnLifecycle))

	fr.Enable(TxnLifecycle)
	require.True(t, IsEnabled(TxnLifecycle))

	fr.SetCategories(0)
	require.False(t, IsEnabled(TxnLifecycle))
	fr.SetCategories(AllCategories)
	require.True(t, IsEnabled(TxnLifecycle))
}

func testTraceEventCategoryFiltering(t *testing.T) {
	if kerneltype.IsClassic() {
		t.Skip("trace events only work for next-gen kernel")
	}

	var conf FlightRecorderConfig
	conf.Initialize()
	conf.EnabledCategories = []string{"*"}
	err := StartLogFlightRecorder(&conf)
	require.NoError(t, err)
	fr := GetFlightRecorder()
	defer fr.Close()

	fr.SetCategories(0)
	_, _ = SetMode(ModeFull)
	FlightRecorder().DiscardOrFlush()
	recorder := installRecorderSink(t, 8)

	ctx := context.Background()
	TraceEvent(ctx, TxnLifecycle, "should-not-record", zap.Int("value", 1))
	require.Empty(t, FlightRecorder().Snapshot())
	require.Empty(t, recorder.Snapshot())
}

func TestTraceEventModes(t *testing.T) {
	prev := CurrentMode()
	t.Cleanup(func() {
		_, _ = SetMode(prev)
	})

	// Test mode values
	mode, err := SetMode("base")
	require.NoError(t, err)
	require.Equal(t, ModeBase, mode)
	require.Equal(t, ModeBase, CurrentMode())

	mode, err = SetMode("full")
	require.NoError(t, err)
	require.Equal(t, ModeFull, mode)
	require.Equal(t, ModeFull, CurrentMode())

	mode, err = SetMode("off")
	require.NoError(t, err)
	require.Equal(t, ModeOff, mode)
	require.Equal(t, ModeOff, CurrentMode())

	_, err = NormalizeMode("invalid")
	require.Error(t, err)
}

func testTraceEventRecordsEvent(t *testing.T) {
	if kerneltype.IsClassic() {
		t.Skip("trace events only work for next-gen kernel")
	}

	var conf FlightRecorderConfig
	conf.Initialize()
	conf.EnabledCategories = []string{"*"}
	err := StartLogFlightRecorder(&conf)
	require.NoError(t, err)
	fr := GetFlightRecorder()
	defer fr.Close()

	fr.SetCategories(AllCategories)
	_, _ = SetMode(ModeFull)
	FlightRecorder().DiscardOrFlush()
	recorder := installRecorderSink(t, 8)
	ctx := context.Background()

	TraceEvent(ctx, TxnLifecycle, "test-event",
		zap.Int("count", 42),
		zap.String("scope", "unit-test"))

	frEvents := FlightRecorder().Snapshot()
	require.Len(t, frEvents, 1)
	ev := frEvents[0]
	require.Equal(t, TxnLifecycle, ev.Category)
	require.Equal(t, "test-event", ev.Name)
	require.False(t, ev.Timestamp.IsZero())
	require.Len(t, ev.Fields, 2)

	recorded := recorder.Snapshot()
	require.Len(t, recorded, 1)
	require.Equal(t, "test-event", recorded[0].Name)
	require.Len(t, recorded[0].Fields, 2)
}

func testTraceEventCarriesTraceID(t *testing.T) {
	if kerneltype.IsClassic() {
		t.Skip("trace events only work for next-gen kernel")
	}

	var conf FlightRecorderConfig
	conf.Initialize()
	conf.EnabledCategories = []string{"*"}
	err := StartLogFlightRecorder(&conf)
	require.NoError(t, err)
	fr := GetFlightRecorder()
	defer fr.Close()

	fr.SetCategories(AllCategories)
	_, _ = SetMode(ModeFull)
	FlightRecorder().DiscardOrFlush()

	rawTrace := []byte{0x01, 0x10, 0xFE, 0xAA}
	ctx := trace.ContextWithTraceID(context.Background(), rawTrace)
	TraceEvent(ctx, Txn2PC, "trace-id-check", zap.Int("value", 7))

	events := FlightRecorder().Snapshot()
	require.Len(t, events, 1)
	require.Equal(t, rawTrace, events[0].TraceID)
}

func testTraceEventLoggingSwitch(t *testing.T) {
	if kerneltype.IsClassic() {
		t.Skip("trace events only work for next-gen kernel")
	}

	var conf FlightRecorderConfig
	conf.Initialize()
	conf.EnabledCategories = []string{"*"}
	err := StartLogFlightRecorder(&conf)
	require.NoError(t, err)
	fr := GetFlightRecorder()
	defer fr.Close()

	fr.SetCategories(AllCategories)
	_, _ = SetMode(ModeBase)
	FlightRecorder().DiscardOrFlush()
	recorder := installRecorderSink(t, 8)
	_, _ = SetMode(ModeBase)
	ctx := context.Background()

	flightBefore := len(FlightRecorder().Snapshot())

	require.Equal(t, ModeBase, CurrentMode())
	TraceEvent(ctx, TxnLifecycle, "disabled-log", zap.Int("value", 1))
	require.Equal(t, flightBefore+1, len(FlightRecorder().Snapshot()))
	disabledLogged := len(recorder.Snapshot())

	_, _ = SetMode(ModeFull)
	TraceEvent(ctx, TxnLifecycle, "enabled-log", zap.Int("value", 2))
	fr1 := FlightRecorder().Snapshot()
	require.Len(t, fr1, flightBefore+2)
	recorded := recorder.Snapshot()
	require.Len(t, recorded, disabledLogged+1)
	require.Equal(t, "enabled-log", recorded[len(recorded)-1].Name)
}

func TestRingBufferSnapshotOrder(t *testing.T) {
	recorder := NewRingBufferSink(2)

	ev1 := Event{Category: TxnLifecycle, Name: "first", Timestamp: time.Unix(0, 1)}
	ev2 := Event{Category: TxnLifecycle, Name: "second", Timestamp: time.Unix(0, 2)}
	ev3 := Event{Category: TxnLifecycle, Name: "third", Timestamp: time.Unix(0, 3)}

	recorder.Record(context.Background(), ev1)
	recorder.Record(context.Background(), ev2)
	require.Equal(t, []string{"first", "second"}, extractNames(recorder.Snapshot()))

	recorder.Record(context.Background(), ev3)
	require.Equal(t, []string{"second", "third"}, extractNames(recorder.Snapshot()))
}

func TestRingBufferFlushTo(t *testing.T) {
	recorder := NewRingBufferSink(4)
	ev := Event{
		Category:  TxnLifecycle,
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
	require.Equal(t, TxnLifecycle, events[0].Category)
	require.Equal(t, ev.Timestamp, events[0].Timestamp)
	require.Len(t, events[0].Fields, 2)
}

func TestCategoryNames(t *testing.T) {
	cases := []struct {
		category TraceCategory
		name     string
	}{
		{TxnLifecycle, "txn_lifecycle"},
		{Txn2PC, "txn_2pc"},
		{TxnLockResolve, "txn_lock_resolve"},
		{StmtLifecycle, "stmt_lifecycle"},
		{StmtPlan, "stmt_plan"},
		{KvRequest, "kv_request"},
		{UnknownClient, "unknown_client"},
		{TraceCategory(999), "unknown(999)"},
	}
	for _, tc := range cases {
		require.Equal(t, tc.name, tc.category.String())
	}
}

// BenchmarkTraceEventDisabled benchmarks the overhead when tracing is disabled.
func BenchmarkTraceEventDisabled(b *testing.B) {
	ctx := context.Background()
	prevMode := CurrentMode()

	var conf FlightRecorderConfig
	conf.Initialize()
	conf.EnabledCategories = []string{"*"}
	err := StartLogFlightRecorder(&conf)
	require.NoError(b, err)
	fr := GetFlightRecorder()
	defer fr.Close()

	_, _ = SetMode(ModeOff)
	b.Cleanup(func() {
		_, _ = SetMode(prevMode)
	})

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		TraceEvent(ctx, TxnLifecycle, "benchmark-disabled",
			zap.String("key", "value"),
			zap.Int("iteration", i))
	}
}

// BenchmarkTraceEventEnabled benchmarks the overhead when tracing is enabled.
func BenchmarkTraceEventEnabled(b *testing.B) {
	ctx := context.Background()
	var conf FlightRecorderConfig
	conf.Initialize()
	conf.EnabledCategories = []string{"*"}
	err := StartLogFlightRecorder(&conf)
	require.NoError(b, err)
	fr := GetFlightRecorder()
	defer fr.Close()

	prevMode := CurrentMode()
	Enable(TxnLifecycle)
	_, _ = SetMode(ModeFull)
	b.Cleanup(func() {
		_, _ = SetMode(prevMode)
	})

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		TraceEvent(ctx, TxnLifecycle, "benchmark-enabled",
			zap.String("key", "value"),
			zap.Int("iteration", i))
	}
}

func extractNames(events []Event) []string {
	names := make([]string, len(events))
	for i, ev := range events {
		names[i] = ev.Name
	}
	return names
}

func testFlightRecorderCoolingOff(t *testing.T) {
	if kerneltype.IsClassic() {
		t.Skip("trace events only work for next-gen kernel")
	}

	prevMode := CurrentMode()
	t.Cleanup(func() {
		_, _ = SetMode(prevMode)
		FlightRecorder().DiscardOrFlush()
		lastDumpTime.Store(0) // Reset cooling-off state
	})

	var conf FlightRecorderConfig
	conf.Initialize()
	conf.EnabledCategories = []string{"*"}
	err := StartLogFlightRecorder(&conf)
	require.NoError(t, err)
	fr := GetFlightRecorder()
	defer fr.Close()

	fr.SetCategories(AllCategories)
	_, _ = SetMode(ModeFull)
	FlightRecorder().DiscardOrFlush()
	lastDumpTime.Store(0) // Reset cooling-off state

	ctx := context.Background()
	TraceEvent(ctx, TxnLifecycle, "cooloff-test-event", zap.Int("value", 1))

	// First dump should succeed
	DumpFlightRecorderToLogger("test-reason-1")
	firstDumpTime := lastDumpTime.Load()
	require.Greater(t, firstDumpTime, int64(0))

	// Immediate second dump should be suppressed (in cooling-off period)
	DumpFlightRecorderToLogger("test-reason-2")
	secondDumpTime := lastDumpTime.Load()
	require.Equal(t, firstDumpTime, secondDumpTime, "timestamp should not update during cooling-off")

	// Simulate passage of time by manually setting lastDumpTime to 11 seconds ago
	elevenSecondsAgo := time.Now().Unix() - 11
	lastDumpTime.Store(elevenSecondsAgo)

	// Third dump should succeed (outside cooling-off period)
	DumpFlightRecorderToLogger("test-reason-3")
	thirdDumpTime := lastDumpTime.Load()
	require.Greater(t, thirdDumpTime, elevenSecondsAgo, "timestamp should update after cooling-off period")
}
