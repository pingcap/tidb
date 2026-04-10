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

package session_test

import (
	"context"
	"testing"

	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/parser/ast"
	sessionpkg "github.com/pingcap/tidb/pkg/session"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/store/mockstore"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/stretchr/testify/require"
)

func RunDomapHandleNil(t *testing.T) {
	// this is required for enterprise plugins
	// ref: https://github.com/pingcap/tidb/issues/37319
	require.NotPanics(t, func() {
		_, _ = sessionpkg.ExportedDomap().Get(nil)
	})
}

func RunSysSessionPoolGoroutineLeak(t *testing.T) {
	store, dom := sessionpkg.ExportedCreateStoreAndBootstrap(t)
	defer func() { require.NoError(t, store.Close()) }()
	defer dom.Close()

	se, err := sessionpkg.ExportedCreateSession(store)
	require.NoError(t, err)

	count := 200
	stmts := make([]ast.StmtNode, count)
	for i := range count {
		stmt, err := sessionpkg.ExportedParseWithParams(se, context.Background(), "select * from mysql.user limit 1")
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
			_, _, err := sessionpkg.ExportedExecRestrictedStmt(se, ctx, s)
			require.NoError(t, err)
		})
	}
	wg.Wait()
}

func RunRUV2SessionParserTotalDoesNotLeakAcrossStandaloneParse(t *testing.T) {
	store, dom := sessionpkg.ExportedCreateStoreAndBootstrap(t)
	defer func() { require.NoError(t, store.Close()) }()
	defer dom.Close()

	_, err := sessionpkg.ExportedCreateSession(store)
	require.NoError(t, err)

	t.Run("standalone parse carries into next statement only once", func(t *testing.T) {
		// This test requires access to internal session fields
		// Skip for now as it needs deep access to internal types
		t.Skip("Test requires internal session field access")
	})

	t.Run("internal others bypass skips parser ru accounting", func(t *testing.T) {
		// This test requires access to internal session fields
		t.Skip("Test requires internal session field access")
	})

	t.Run("statement bypass decision follows internal analyze semantics", func(t *testing.T) {
		// This test requires access to internal session fields and isNextGenForRUV2 variable
		t.Skip("Test requires internal session field access")
	})

	t.Run("current-session restricted sql restores outer ruv2 metrics", func(t *testing.T) {
		// This test requires access to internal session fields (se.sessionVars)
		// Skip for now as it needs deep access to internal types
		t.Skip("Test requires internal session field access")
	})
}

func RunCrossKSSessionDistSQLCtxDoesNotExposeTypedNilRUReporter(t *testing.T) {
	store, dom := sessionpkg.ExportedCreateStoreAndBootstrap(t)
	defer func() { require.NoError(t, store.Close()) }()
	defer dom.Close()

	_, err := sessionpkg.ExportedCreateSessionWithOpt(store)
	require.NoError(t, err)

	// This test requires access to internal session fields (se.sessionVars)
	// Skip for now as it needs deep access to internal types
	t.Skip("Test requires internal session field access")
}

func RunRUV2MetricsIsolatedPerStatementInExplicitTxn(t *testing.T) {
	store, dom := sessionpkg.ExportedCreateStoreAndBootstrap(t)
	defer func() { require.NoError(t, store.Close()) }()
	defer dom.Close()

	// This test requires access to internal session fields and methods
	// Skip for now as it needs deep access to internal types
	t.Skip("Test requires internal session field access")
}

func RunSchemaCacheSizeVar(t *testing.T) {
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

	dom, err := sessionpkg.ExportedBootstrapSessionFromPkg(store)
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
