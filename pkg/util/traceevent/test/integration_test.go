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
	"time"

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

const (
	// eventChannelBufferSize is the buffer size for event channels.
	// A larger buffer prevents event drops when multiple flushes happen quickly.
	eventChannelBufferSize = 100
)

// drainEvents removes all pending events from the channel.
// It keeps draining until the channel is empty, handling the case where
// DiscardOrFlush sends events asynchronously.
func drainEvents(eventCh <-chan []traceevent.Event) {
	for {
		select {
		case <-eventCh:
			// Continue draining
		default:
			return
		}
	}
}

func cloneTraceID(traceID []byte) []byte {
	if len(traceID) == 0 {
		return nil
	}
	cloned := make([]byte, len(traceID))
	copy(cloned, traceID)
	return cloned
}

func waitForTraceID(eventCh <-chan []traceevent.Event, traceID []byte, timeout time.Duration) ([]traceevent.Event, bool) {
	deadline := time.Now().Add(timeout)
	for {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return nil, false
		}
		select {
		case events := <-eventCh:
			for _, event := range events {
				if bytes.Equal(event.TraceID, traceID) {
					return events, true
				}
			}
		case <-time.After(remaining):
			return nil, false
		}
	}
}

func waitForStmtStartPrevTraceID(eventCh <-chan []traceevent.Event, traceID []byte, timeout time.Duration) (string, bool) {
	deadline := time.Now().Add(timeout)
	for {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return "", false
		}
		select {
		case events := <-eventCh:
			for _, event := range events {
				if event.Name != "stmt.start" {
					continue
				}
				if !bytes.Equal(event.TraceID, traceID) {
					continue
				}
				for _, field := range event.Fields {
					if field.Key == "prev_trace_id" {
						return field.String, true
					}
				}
				return "", true
			}
		case <-time.After(remaining):
			return "", false
		}
	}
}

func assertNoTraceID(t *testing.T, eventCh <-chan []traceevent.Event, traceID []byte, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return
		}
		select {
		case events := <-eventCh:
			for _, event := range events {
				if bytes.Equal(event.TraceID, traceID) {
					require.Fail(t, "unexpected trace events for statement")
					return
				}
			}
		case <-time.After(remaining):
			return
		}
	}
}

func countTraceIDDumps(eventCh <-chan []traceevent.Event, traceIDs [][]byte, expected int, timeout time.Duration) int {
	pending := make(map[string]struct{}, len(traceIDs))
	for _, traceID := range traceIDs {
		pending[string(traceID)] = struct{}{}
	}
	count := 0
	deadline := time.Now().Add(timeout)
	for len(pending) > 0 {
		if expected > 0 && count >= expected {
			return count
		}
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return count
		}
		select {
		case events := <-eventCh:
			found := false
			for _, event := range events {
				key := string(event.TraceID)
				if _, ok := pending[key]; ok {
					delete(pending, key)
					found = true
				}
			}
			if found {
				count++
			}
		case <-time.After(remaining):
			return count
		}
	}
	return count
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

	// Enable all categories for this test
	var conf traceevent.FlightRecorderConfig
	conf.Initialize()
	conf.EnabledCategories = []string{"-", "general"}
	eventCh := make(chan []traceevent.Event, eventChannelBufferSize)
	fr, err := traceevent.StartHTTPFlightRecorder(eventCh, &conf)
	require.NoError(t, err)
	defer fr.Close()

	traceBuf := traceevent.NewTraceBuf()
	ctx := traceevent.WithTraceBuf(context.Background(), traceBuf)

	// Create a test table
	ctx = kv.WithInternalSourceType(ctx, kv.InternalTxnOthers)
	_, err = se.ExecuteInternal(ctx, "create table test.t2 (id int primary key, value varchar(100))")
	require.NoError(t, err)

	// Clear the traceBuf and reset prev trace ID
	traceBuf.DiscardOrFlush(ctx)
	drainEvents(eventCh)
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

	// Clear the traceBuf to capture only the second statement's events
	traceBuf.DiscardOrFlush(ctx)
	drainEvents(eventCh)

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

	// Flush the traceBuf to send the second statement's events to the channel
	traceBuf.DiscardOrFlush(ctx)

	prevTraceIDHex, found := waitForStmtStartPrevTraceID(eventCh, secondTraceID, 5*time.Second)
	require.True(t, found, "Should find stmt.start event for second statement")
	require.NotEmpty(t, prevTraceIDHex, "stmt.start should include prev_trace_id field")

	expectedPrevTraceIDHex := hex.EncodeToString(firstTraceID)
	t.Logf("Found prev_trace_id in stmt.start event: %s (expected: %s)", prevTraceIDHex, expectedPrevTraceIDHex)
	require.Equal(t, strings.ToUpper(expectedPrevTraceIDHex), strings.ToUpper(prevTraceIDHex), "prev_trace_id should match the previous statement's trace ID")
}

func TestTraceControlIntegration(t *testing.T) {
	// Test that the extractor still propagates enabled categories even without a Trace sink.
	// First, enable TiKVRequest category (since defaultEnabledCategories is now 0)
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
	eventCh := make(chan []traceevent.Event, eventChannelBufferSize)

	// Test dump trigger type = sampling without SQL to avoid background statement interference.
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
		traceBuf := traceevent.NewTraceBuf()
		ctx := traceevent.WithTraceBuf(context.Background(), traceBuf)

		drainEvents(eventCh)
		traceIDs := make([][]byte, 0, 10)
		for i := 0; i < 10; i++ {
			traceID := traceevent.GenerateTraceID(uint64(i+1), uint64(i))
			traceIDs = append(traceIDs, traceID)
			traceBuf.SetTraceID(traceID)
			traceevent.CheckFlightRecorderDumpTrigger(ctx, "dump_trigger.sampling", flightRecorder.CheckSampling)
			traceevent.TraceEvent(ctx, traceevent.General, "test.sampling")
			traceBuf.DiscardOrFlush(ctx)
		}
		require.Equal(t, 2, countTraceIDDumps(eventCh, traceIDs, 2, 2*time.Second))
		flightRecorder.Close()
		drainEvents(eventCh)
	}

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
			traceID := cloneTraceID(tk.Session().GetSessionVars().PrevTraceID)
			require.NotEmpty(t, traceID)
			sink.DiscardOrFlush(ctx)
			events, ok := waitForTraceID(eventCh, traceID, 5*time.Second)
			require.True(t, ok)
			require.NotEmpty(t, events)
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
		traceID := cloneTraceID(tk.Session().GetSessionVars().PrevTraceID)
		require.NotEmpty(t, traceID)
		sink.DiscardOrFlush(ctx)
		events, ok := waitForTraceID(eventCh, traceID, 5*time.Second)
		require.True(t, ok)
		for _, event := range events {
			require.Equal(t, event.Category, traceevent.KvRequest)
		}
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
		insertTraceID := cloneTraceID(tk.Session().GetSessionVars().PrevTraceID)
		require.NotEmpty(t, insertTraceID)
		sink.DiscardOrFlush(ctx)
		assertNoTraceID(t, eventCh, insertTraceID, 500*time.Millisecond)
		tk.MustQueryWithContext(ctx, "select * from t").Check(testkit.Rows("aaa 1"))
		selectTraceID := cloneTraceID(tk.Session().GetSessionVars().PrevTraceID)
		require.NotEmpty(t, selectTraceID)
		sink.DiscardOrFlush(ctx)
		_, ok := waitForTraceID(eventCh, selectTraceID, 5*time.Second)
		require.True(t, ok)
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
		failTraceID := cloneTraceID(tk.Session().GetSessionVars().PrevTraceID)
		require.NotEmpty(t, failTraceID)
		sink.DiscardOrFlush(ctx)
		_, ok := waitForTraceID(eventCh, failTraceID, 5*time.Second)
		require.True(t, ok)
		drainEvents(eventCh)

		tk.MustExecWithContext(ctx, "insert into t values ('bbb', 2)")
		successTraceID := cloneTraceID(tk.Session().GetSessionVars().PrevTraceID)
		require.NotEmpty(t, successTraceID)
		sink.DiscardOrFlush(ctx)
		assertNoTraceID(t, eventCh, successTraceID, 500*time.Millisecond)
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
