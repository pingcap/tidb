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
	"testing"

	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/util"
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
	for i := 0; i < count; i++ {
		stmt, err := se.ParseWithParams(context.Background(), "select * from mysql.user limit 1")
		require.NoError(t, err)
		stmts[i] = stmt
	}
	// Test an issue that sysSessionPool doesn't call session's Close, cause
	// asyncGetTSWorker goroutine leak.
	var wg util.WaitGroupWrapper
	for i := 0; i < count; i++ {
		s := stmts[i]
		wg.Run(func() {
			ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnOthers)
			_, _, err := se.ExecRestrictedStmt(ctx, s)
			require.NoError(t, err)
		})
	}
	wg.Wait()
}

func TestParseErrorWarn(t *testing.T) {
	ctx := core.MockContext()

	nodes, err := Parse(ctx, "select /*+ adf */ 1")
	require.NoError(t, err)
	require.Len(t, nodes, 1)
	require.Len(t, ctx.GetSessionVars().StmtCtx.GetWarnings(), 1)

	_, err = Parse(ctx, "select")
	require.Error(t, err)
}

func TestKeysNeedLock(t *testing.T) {
	rowKey := tablecodec.EncodeRowKeyWithHandle(1, kv.IntHandle(1))
	uniqueIndexKey := tablecodec.EncodeIndexSeekKey(1, 1, []byte{1})
	nonUniqueIndexKey := tablecodec.EncodeIndexSeekKey(1, 2, []byte{1})
	uniqueValue := make([]byte, 8)
	uniqueUntouched := append(uniqueValue, '1')
	nonUniqueVal := []byte{'0'}
	nonUniqueUntouched := []byte{'1'}
	var deleteVal []byte
	rowVal := []byte{'a', 'b', 'c'}
	tests := []struct {
		key  []byte
		val  []byte
		need bool
	}{
		{rowKey, rowVal, true},
		{rowKey, deleteVal, true},
		{nonUniqueIndexKey, nonUniqueVal, false},
		{nonUniqueIndexKey, nonUniqueUntouched, false},
		{uniqueIndexKey, uniqueValue, true},
		{uniqueIndexKey, uniqueUntouched, false},
		{uniqueIndexKey, deleteVal, false},
	}

	for _, test := range tests {
		need := keyNeedToLock(test.key, test.val, 0)
		require.Equal(t, test.need, need)

		flag := kv.KeyFlags(1)
		need = keyNeedToLock(test.key, test.val, flag)
		require.True(t, flag.HasPresumeKeyNotExists())
		require.True(t, need)
	}
}
