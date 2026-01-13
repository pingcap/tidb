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

package traceevent_test

import (
	"bufio"
	"bytes"
	"context"
	"encoding/hex"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/config/kerneltype"
	"github.com/pingcap/tidb/pkg/keyspace"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/session"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/traceevent"
	"github.com/pingcap/tidb/pkg/util/tracing"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/trace"
)

func drainEvents(eventCh <-chan []traceevent.Event) {
	for {
		select {
		case <-eventCh:
		default:
			return
		}
	}
}

func TestPrevTraceIDPersistence(t *testing.T) {
	if kerneltype.IsClassic() {
		t.Skip("trace events only work for next-gen kernel")
	}

	store, dom := session.CreateStoreAndBootstrap(t)
	defer func() { require.NoError(t, store.Close()) }()
	defer dom.Close()

	se, err := session.CreateSession(store)
	require.NoError(t, err)

	// Enable trace events and install a recorder to capture events
	prevMode := traceevent.CurrentMode()
	_, err = traceevent.SetMode("full")
	require.NoError(t, err)
	defer func() {
		_, _ = traceevent.SetMode(prevMode)
	}()

	// Enable all categories for this test
	var conf traceevent.FlightRecorderConfig
	conf.Initialize()
	conf.EnabledCategories = []string{"*"}
	err = traceevent.StartLogFlightRecorder(&conf)
	require.NoError(t, err)
	fr := traceevent.GetFlightRecorder()
	defer fr.Close()

	recorder := traceevent.NewRingBufferSink(100)
	prevSink := traceevent.CurrentSink()
	traceevent.SetSink(recorder)
	defer traceevent.SetSink(prevSink)

	// Create a test table
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnOthers)
	_, err = se.ExecuteInternal(ctx, "create table test.t2 (id int primary key, value varchar(100))")
	require.NoError(t, err)

	// Clear the recorder and reset prev trace ID
	recorder.DiscardOrFlush()
	se.GetSessionVars().PrevTraceID = nil

	// Execute first statement
	stmt1, err := session.ParseWithParams4Test(ctx, se, "insert into test.t2 values (1, 'first')")
	require.NoError(t, err)
	rs1, err := se.ExecuteStmt(ctx, stmt1)
	require.NoError(t, err)
	if rs1 != nil {
		require.NoError(t, rs1.Close())
	}

	// Get the trace ID from the first statement
	firstTraceID := se.GetSessionVars().PrevTraceID
	require.NotEmpty(t, firstTraceID, "First statement should generate a trace ID")
	t.Logf("First statement trace ID: %s", hex.EncodeToString(firstTraceID))

	// Clear the recorder to capture only the second statement's events
	recorder.DiscardOrFlush()

	// Execute second statement in the same session
	stmt2, err := session.ParseWithParams4Test(ctx, se, "insert into test.t2 values (2, 'second')")
	require.NoError(t, err)
	rs2, err := se.ExecuteStmt(ctx, stmt2)
	require.NoError(t, err)
	if rs2 != nil {
		require.NoError(t, rs2.Close())
	}

	// Get the trace ID from the second statement
	secondTraceID := se.GetSessionVars().PrevTraceID
	require.NotEmpty(t, secondTraceID, "Second statement should generate a trace ID")
	t.Logf("Second statement trace ID: %s", hex.EncodeToString(secondTraceID))

	// Verify that the trace IDs are different
	require.NotEqual(t, firstTraceID, secondTraceID, "Each statement should have a unique trace ID")

	// Check recorded events for prev_trace_id field
	events := recorder.Snapshot()
	require.NotEmpty(t, events, "Should have recorded trace events")

	// Look for stmt.start events and verify prev_trace_id matches first statement
	// We need to filter for the correct statement's event to avoid flakiness from background operations
	foundPrevTraceID := false
	for _, event := range events {
		if event.Name == "stmt.start" {
			// Verify this is the stmt.start event for our second statement
			// by checking trace_id matches secondTraceID to avoid background operation events
			if !bytes.Equal(event.TraceID, secondTraceID) {
				continue // Skip events from other statements
			}

			for _, field := range event.Fields {
				if field.Key == "prev_trace_id" {
					foundPrevTraceID = true
					// The prev_trace_id should match the first statement's trace ID
					// Note: redact.Key() may uppercase the hex string, so we do case-insensitive comparison
					prevTraceIDHex := field.String
					expectedPrevTraceIDHex := hex.EncodeToString(firstTraceID)
					t.Logf("Found prev_trace_id in stmt.start event: %s (expected: %s)", prevTraceIDHex, expectedPrevTraceIDHex)
					require.Equal(t, strings.ToUpper(expectedPrevTraceIDHex), strings.ToUpper(prevTraceIDHex), "prev_trace_id should match the previous statement's trace ID")
					break
				}
			}
			if foundPrevTraceID {
				break // Found and validated the correct event
			}
		}
	}

	require.True(t, foundPrevTraceID, "Should find prev_trace_id field in stmt.start events")
}

func TestTraceControlIntegration(t *testing.T) {
	// Test that the extractor still propagates enabled categories even without a Trace sink.
	// First, enable TiKVRequest category (since defaultEnabledCategories is now 0)
	prevCategories := tracing.GetEnabledCategories()
	tracing.Enable(tracing.TiKVRequest)
	defer tracing.SetCategories(prevCategories)

	var conf traceevent.FlightRecorderConfig
	conf.Initialize()
	conf.EnabledCategories = []string{"tikv_request"}
	err := traceevent.StartLogFlightRecorder(&conf)
	require.NoError(t, err)
	fr := traceevent.GetFlightRecorder()
	defer fr.Close()

	ctx := context.Background()
	flags := trace.GetTraceControlFlags(ctx)
	require.True(t, flags.Has(trace.FlagTiKVCategoryRequest))
	require.False(t, flags.Has(trace.FlagTiKVCategoryWriteDetails))
	require.False(t, flags.Has(trace.FlagTiKVCategoryReadDetails))
	require.False(t, trace.ImmediateLoggingEnabled(ctx))

	// Test that we can set a custom extractor
	type testKey struct{}
	trace.SetTraceControlExtractor(func(ctx context.Context) trace.TraceControlFlags {
		if val, ok := ctx.Value(testKey{}).(trace.TraceControlFlags); ok {
			return val
		}
		return 0
	})
	defer func() {
		// Restore the original extractor
		traceevent.RegisterWithClientGo()
	}()

	// Without the key, should return 0
	require.Equal(t, trace.TraceControlFlags(0), trace.GetTraceControlFlags(ctx))
	require.False(t, trace.ImmediateLoggingEnabled(ctx))

	// With immediate log flag set
	ctxWithImmediate := context.WithValue(ctx, testKey{}, trace.FlagImmediateLog)
	require.True(t, trace.ImmediateLoggingEnabled(ctxWithImmediate))
	flags = trace.GetTraceControlFlags(ctxWithImmediate)
	require.True(t, flags.Has(trace.FlagImmediateLog))

	// With multiple flags set
	ctxWithMultiple := context.WithValue(ctx, testKey{},
		trace.FlagImmediateLog|trace.FlagTiKVCategoryRequest|trace.FlagTiKVCategoryWriteDetails)
	flags = trace.GetTraceControlFlags(ctxWithMultiple)
	require.True(t, flags.Has(trace.FlagImmediateLog))
	require.True(t, flags.Has(trace.FlagTiKVCategoryRequest))
	require.True(t, flags.Has(trace.FlagTiKVCategoryWriteDetails))
	require.False(t, flags.Has(trace.FlagTiKVCategoryReadDetails))
}

func TestFlightRecorder(t *testing.T) {
	eventCh := make(chan []tracing.Event, 1024)
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a varchar(10) primary key, b int, index idx(b))")

	ctx := context.Background()
	sink := traceevent.NewTraceBuf()
	ctx = tracing.WithTraceBuf(ctx, sink)

	// Basic check to see if flight recorder can dump events
	{
		var config traceevent.FlightRecorderConfig
		config.Initialize() // all events by default
		flightRecorder, err := traceevent.StartHTTPFlightRecorder(eventCh, &config)
		require.NoError(t, err)
		for _, sql := range []string{"select * from t", "select * from t where b = 5"} {
			tk.MustQueryWithContext(ctx, sql).Check(testkit.Rows())
			sink.DiscardOrFlush(ctx)
			require.GreaterOrEqual(t, len(eventCh), 1)
			drainEvents(eventCh)
		}
		flightRecorder.Close()
	}

	// Test enabled categories filter
	{
		config := traceevent.FlightRecorderConfig{
			EnabledCategories: []string{"kv_request"},
			DumpTrigger: traceevent.DumpTriggerConfig{
				Type:     "sampling",
				Sampling: 1,
			},
		}
		flightRecorder, err := traceevent.StartHTTPFlightRecorder(eventCh, &config)
		require.NoError(t, err)
		tk.MustQueryWithContext(ctx, "select * from t").Check(testkit.Rows())
		sink.DiscardOrFlush(ctx)
		require.NotEmpty(t, eventCh)
		events := <-eventCh
		for _, event := range events {
			require.Equal(t, event.Category, traceevent.KvRequest)
		}
		flightRecorder.Close()
	}

	// Test dump trigger type = sampling
	{
		config := traceevent.FlightRecorderConfig{
			EnabledCategories: []string{"*"},
			DumpTrigger: traceevent.DumpTriggerConfig{
				Type:     "sampling",
				Sampling: 5,
			},
		}
		flightRecorder, err := traceevent.StartHTTPFlightRecorder(eventCh, &config)
		require.NoError(t, err)
		for i := 0; i < 10; i++ {
			tk.MustQueryWithContext(ctx, "select * from t").Check(testkit.Rows())
			sink.DiscardOrFlush(ctx)
		}
		require.Len(t, eventCh, 2)
		flightRecorder.Close()
		drainEvents(eventCh)
	}

	// Test dump trigger type = user command
	{
		config := traceevent.FlightRecorderConfig{
			EnabledCategories: []string{"*"},
			DumpTrigger: traceevent.DumpTriggerConfig{
				Type: "user_command",
				UserCommand: &traceevent.UserCommandConfig{
					Type:      "sql_regexp",
					SQLRegexp: `select \* from t`,
				},
			},
		}
		flightRecorder, err := traceevent.StartHTTPFlightRecorder(eventCh, &config)
		require.NoError(t, err)
		tk.MustExecWithContext(ctx, "insert into t values ('aaa', 1)")
		sink.DiscardOrFlush(ctx)
		require.Empty(t, eventCh)
		tk.MustQueryWithContext(ctx, "select * from t").Check(testkit.Rows("aaa 1"))
		sink.DiscardOrFlush(ctx)
		require.Len(t, eventCh, 1)
		drainEvents(eventCh)
		flightRecorder.Close()
	}

	// Test dump trigger type = suspicious event
	{
		config := traceevent.FlightRecorderConfig{
			EnabledCategories: []string{"*"},
			DumpTrigger: traceevent.DumpTriggerConfig{
				Type: "suspicious_event",
				Event: &traceevent.SuspiciousEventConfig{
					Type: "query_fail",
				},
			},
		}
		flightRecorder, err := traceevent.StartHTTPFlightRecorder(eventCh, &config)
		require.NoError(t, err)
		_, err = tk.ExecWithContext(ctx, "insert into t values ('aaa', 2)")
		require.Error(t, err)
		sink.DiscardOrFlush(ctx)
		require.Len(t, eventCh, 1)
		drainEvents(eventCh)

		tk.MustExecWithContext(ctx, "insert into t values ('bbb', 2)")
		sink.DiscardOrFlush(ctx)
		require.Len(t, eventCh, 0)
		flightRecorder.Close()
	}
}

func TestTiDBTraceEventVariable(t *testing.T) {
	if kerneltype.IsClassic() {
		t.Skip("@@tidb_trace_event only works on nextgen tidb")
	}
	dir, err := os.MkdirTemp("", "tidb_test_trace_event")
	require.NoError(t, err)
	defer os.RemoveAll(dir) // clean up

	logFile := filepath.Join(dir, "tidb.log")
	config.UpdateGlobal(func(conf *config.Config) {
		conf.Log.File.Filename = logFile
	})
	cfg := config.GetGlobalConfig()
	// Very important, by default CreateMockStore will not initialize the logger!
	err = logutil.InitLogger(cfg.Log.ToLogConfig(), keyspace.WrapZapcoreWithKeyspace())
	require.NoError(t, err)

	ctx := context.Background()
	sink := traceevent.NewTraceBuf()
	ctx = tracing.WithTraceBuf(ctx, sink)
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExecWithContext(ctx, `set @@global.tidb_trace_event = json_object('enabled_categories', json_array('*'), 'dump_trigger', json_object('type', 'sampling', 'sampling', 1))`)
	sink.DiscardOrFlush(ctx)
	tk.MustExecWithContext(ctx, "use test")
	sink.DiscardOrFlush(ctx)
	tk.MustExecWithContext(ctx, "create table t (id int)")
	sink.DiscardOrFlush(ctx)
	tk.MustQueryWithContext(ctx, "select * from t").Check(testkit.Rows())
	sink.DiscardOrFlush(ctx)

	// Just check the log contains the trace event.
	file, err := os.Open(logFile)
	require.NoError(t, err)
	matched, err := regexp.MatchReader(`\[trace-event\]`, bufio.NewReader(file))
	require.NoError(t, err)
	require.True(t, matched)
}
