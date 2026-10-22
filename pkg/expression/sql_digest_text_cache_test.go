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

package expression

import (
	"context"
	"slices"
	"testing"

	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/core/resolve"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
	"github.com/stretchr/testify/require"
)

func TestSQLDigestTextCache(t *testing.T) {
	c := newDigestTextCache(2)

	_, ok := c.get("digest1")
	require.False(t, ok)

	c.put("digest1", "text1")
	text, ok := c.get("digest1")
	require.True(t, ok)
	require.Equal(t, "text1", text)

	// Capacity-bound LRU eviction.
	c.put("digest2", "text2")
	c.get("digest1") // refresh digest1 so digest2 becomes the least recently used
	c.put("digest3", "text3")
	_, ok = c.get("digest2")
	require.False(t, ok)
	_, ok = c.get("digest3")
	require.True(t, ok)
}

// stubDigestTextExecutor mimics the statements summary tables: every query returns
// the digest-to-text pairs it asks for via args, or all pairs when no args are given.
type stubDigestTextExecutor struct {
	data    map[string]string
	queries int
}

func (s *stubDigestTextExecutor) ExecRestrictedSQL(
	_ context.Context,
	_ []sqlexec.OptionFuncAlias,
	_ string,
	args ...any,
) ([]chunk.Row, []*resolve.ResultField, error) {
	s.queries++
	wanted := make(map[string]struct{}, len(args))
	for _, arg := range args {
		wanted[arg.(string)] = struct{}{}
	}
	digests := make([]string, 0, len(s.data))
	for digest := range s.data {
		if _, ok := wanted[digest]; ok || len(wanted) == 0 {
			digests = append(digests, digest)
		}
	}
	slices.Sort(digests)
	chk := chunk.NewChunkWithCapacity(
		[]*types.FieldType{types.NewFieldType(mysql.TypeVarString), types.NewFieldType(mysql.TypeVarString)},
		len(digests),
	)
	for _, digest := range digests {
		chk.AppendString(0, digest)
		chk.AppendString(1, s.data[digest])
	}
	rows := make([]chunk.Row, chk.NumRows())
	for i := range rows {
		rows[i] = chk.GetRow(i)
	}
	return rows, nil, nil
}

func TestSQLDigestTextRetrieverCache(t *testing.T) {
	resetCache := func() {
		sqlDigestTextCache = newDigestTextCache(sqlDigestTextCacheCapacity)
	}
	resetCache()
	defer resetCache()

	exec := &stubDigestTextExecutor{data: map[string]string{"digest1": "text1"}}
	r := NewSQLDigestTextRetriever()
	r.SQLDigestsMap = map[string]string{"digest1": ""}
	require.NoError(t, r.RetrieveGlobal(context.Background(), exec))
	require.Equal(t, map[string]string{"digest1": "text1"}, r.SQLDigestsMap)
	require.Equal(t, 1, exec.queries)
	text, ok := sqlDigestTextCache.get("digest1")
	require.True(t, ok)
	require.Equal(t, "text1", text)

	// A later retrieval of a cached digest must not query the summary tables again.
	r2 := NewSQLDigestTextRetriever()
	r2.SQLDigestsMap = map[string]string{"digest1": ""}
	require.NoError(t, r2.RetrieveGlobal(context.Background(), exec))
	require.Equal(t, map[string]string{"digest1": "text1"}, r2.SQLDigestsMap)
	require.Equal(t, 1, exec.queries)

	// Mocked retrievers are test doubles and must not feed the cache.
	r3 := NewSQLDigestTextRetriever()
	r3.SQLDigestsMap = map[string]string{"digest2": ""}
	r3.mockLocalData = map[string]string{"digest2": "text2"}
	require.NoError(t, r3.RetrieveGlobal(context.Background(), nil))
	require.Equal(t, map[string]string{"digest2": "text2"}, r3.SQLDigestsMap)
	_, ok = sqlDigestTextCache.get("digest2")
	require.False(t, ok)
}
