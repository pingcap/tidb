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
	"sync"
	"testing"

	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/stretchr/testify/require"
)

func TestSysSessionPoolGoroutineLeak(t *testing.T) {
	store, dom := createStoreAndBootstrap(t)
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
	var wg sync.WaitGroup
	wg.Add(count)
	for i := 0; i < count; i++ {
		go func(se *session, stmt ast.StmtNode) {
			_, _, err := se.ExecRestrictedStmt(context.Background(), stmt)
			require.NoError(t, err)
			wg.Done()
		}(se, stmts[i])
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
	indexKey := tablecodec.EncodeIndexSeekKey(1, 1, []byte{1})
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
		{indexKey, nonUniqueVal, false},
		{indexKey, nonUniqueUntouched, false},
		{indexKey, uniqueValue, true},
		{indexKey, uniqueUntouched, false},
		{indexKey, deleteVal, false},
	}

	for _, test := range tests {
		require.Equal(t, test.need, keyNeedToLock(test.key, test.val, 0))
	}

	flag := kv.KeyFlags(1)
	require.True(t, flag.HasPresumeKeyNotExists())
	require.True(t, keyNeedToLock(indexKey, deleteVal, flag))
}
