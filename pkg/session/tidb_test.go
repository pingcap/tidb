// Copyright 2015 PingCAP, Inc.
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
	"context"
	"encoding/hex"
	"strings"
	"testing"

	"github.com/pingcap/tidb/pkg/config/kerneltype"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/store/mockstore"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/traceevent"
	"github.com/stretchr/testify/require"
)

func TestDomapHandleNil(t *testing.T) {
	// this is required for enterprise plugins
	// ref: https://github.com/pingcap/tidb/issues/37319
	require.NotPanics(t, func() {
		_, _ = domap.Get(nil)
	})
}

func TestSysSessionPoolGoroutineLeak(t *testing.T) {
	store, dom := CreateStoreAndBootstrap(t)
	defer func() { require.NoError(t, store.Close()) }()
	defer dom.Close()

	se, err := createSession(store)
	require.NoError(t, err)

	count := 200
	stmts := make([]ast.StmtNode, count)
	for i := range count {
		stmt, err := se.ParseWithParams(context.Background(), "select * from mysql.user limit 1")
		require.NoError(t, err)
		stmts[i] = stmt
	}
	// Test an issue that sysSessionPool doesn't call session's Close, cause
	// asyncGetTSWorker goroutine leak.
	var wg util.WaitGroupWrapper
	for i := range count {
		s := stmts[i]
		wg.Run(func() {
			ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnOthers)
			_, _, err := se.ExecRestrictedStmt(ctx, s)
			require.NoError(t, err)
		})
	}
	wg.Wait()
}

func TestSchemaCacheSizeVar(t *testing.T) {
	store, err := mockstore.NewMockStore(mockstore.WithStoreType(mockstore.EmbedUnistore))
	require.NoError(t, err)

	txn, err := store.Begin()
	require.NoError(t, err)
	m := meta.NewMutator(txn)
	size, isNull, err := m.GetSchemaCacheSize()
	require.NoError(t, err)
	require.Equal(t, size, uint64(0))
	require.Equal(t, isNull, true)
	require.NoError(t, txn.Rollback())

	dom, err := BootstrapSession(store)
	require.NoError(t, err)
	defer func() { require.NoError(t, store.Close()) }()
	defer dom.Close()

	txn, err = store.Begin()
	require.NoError(t, err)
	m = meta.NewMutator(txn)
	size, isNull, err = m.GetSchemaCacheSize()
	require.NoError(t, err)
	require.Equal(t, size, uint64(vardef.DefTiDBSchemaCacheSize))
	require.Equal(t, isNull, false)
	require.NoError(t, txn.Rollback())
}

// TestPrevTraceIDPersistence verifies that prev_trace_id persists across statements
func TestPrevTraceIDPersistence(t *testing.T) {
	if kerneltype.IsClassic() {
		t.Skip("trace events only work for next-gen kernel")
	}

	store, dom := CreateStoreAndBootstrap(t)
	defer func() { require.NoError(t, store.Close()) }()
	defer dom.Close()

	se, err := createSession(store)
	require.NoError(t, err)

	// Enable trace events and install a recorder to capture events
	prevMode := traceevent.CurrentMode()
	_, err = traceevent.SetMode("full")
	require.NoError(t, err)
	defer func() {
		_, _ = traceevent.SetMode(prevMode)
	}()

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
	stmt1, err := se.ParseWithParams(ctx, "insert into test.t2 values (1, 'first')")
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
	stmt2, err := se.ParseWithParams(ctx, "insert into test.t2 values (2, 'second')")
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
	foundPrevTraceID := false
	for _, event := range events {
		if event.Name == "stmt.start" {
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
		}
	}

	require.True(t, foundPrevTraceID, "Should find prev_trace_id field in stmt.start events")
}
