// Copyright 2023 PingCAP, Inc.
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

package test

import (
	"testing"

	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/session"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/stretchr/testify/require"
)

func TestParseErrorWarn(t *testing.T) {
	ctx := core.MockContext()
	defer func() {
		domain.GetDomain(ctx).StatsHandle().Close()
	}()
	nodes, err := session.Parse(ctx, "select /*+ adf */ 1")
	require.NoError(t, err)
	require.Len(t, nodes, 1)
	require.Len(t, ctx.GetSessionVars().StmtCtx.GetWarnings(), 1)

	_, err = session.Parse(ctx, "select")
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
		need := session.KeyNeedToLock(test.key, test.val, 0)
		require.Equal(t, test.need, need)

		flag := kv.KeyFlags(1)
		need = session.KeyNeedToLock(test.key, test.val, flag)
		require.True(t, flag.HasPresumeKeyNotExists())
		require.True(t, need)
	}
}
