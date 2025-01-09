// Copyright 2022 PingCAP, Inc.
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

package bindinfo

import (
	"strings"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/format"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/stretchr/testify/require"
)

func bindingNoDBDigest(t *testing.T, b *Binding) string {
	p := parser.New()
	stmt, err := p.ParseOneStmt(b.BindSQL, b.Charset, b.Collation)
	require.NoError(t, err)
	_, noDBDigest := NormalizeStmtForBinding(stmt, WithoutDB(true))
	return noDBDigest
}

func TestCrossDBBindingCache(t *testing.T) {
	fbc := newBindCache().(*bindingCache)
	b1 := &Binding{BindSQL: "SELECT * FROM db1.t1", SQLDigest: "b1"}
	fDigest1 := bindingNoDBDigest(t, b1)
	b2 := &Binding{BindSQL: "SELECT * FROM db2.t1", SQLDigest: "b2"}
	b3 := &Binding{BindSQL: "SELECT * FROM db2.t3", SQLDigest: "b3"}
	fDigest3 := bindingNoDBDigest(t, b3)

	// add 3 bindings and b1 and b2 have the same noDBDigest
	require.NoError(t, fbc.SetBinding(b1.SQLDigest, b1))
	require.NoError(t, fbc.SetBinding(b2.SQLDigest, b2))
	require.NoError(t, fbc.SetBinding(b3.SQLDigest, b3))
	require.Equal(t, len(fbc.digestBiMap.(*digestBiMapImpl).noDBDigest2SQLDigest), 2) // b1 and b2 have the same noDBDigest
	require.Equal(t, len(fbc.digestBiMap.NoDBDigest2SQLDigest(fDigest1)), 2)
	require.Equal(t, len(fbc.digestBiMap.NoDBDigest2SQLDigest(fDigest3)), 1)
	require.Equal(t, len(fbc.digestBiMap.(*digestBiMapImpl).sqlDigest2noDBDigest), 3)
	_, ok := fbc.digestBiMap.(*digestBiMapImpl).sqlDigest2noDBDigest[b1.SQLDigest]
	require.True(t, ok)
	_, ok = fbc.digestBiMap.(*digestBiMapImpl).sqlDigest2noDBDigest[b2.SQLDigest]
	require.True(t, ok)
	_, ok = fbc.digestBiMap.(*digestBiMapImpl).sqlDigest2noDBDigest[b3.SQLDigest]
	require.True(t, ok)

	// remove b2
	fbc.RemoveBinding(b2.SQLDigest)
	require.Equal(t, len(fbc.digestBiMap.(*digestBiMapImpl).noDBDigest2SQLDigest), 2)
	require.Equal(t, len(fbc.digestBiMap.(*digestBiMapImpl).noDBDigest2SQLDigest[fDigest1]), 1)
	require.Equal(t, len(fbc.digestBiMap.(*digestBiMapImpl).noDBDigest2SQLDigest[fDigest3]), 1)
	require.Equal(t, len(fbc.digestBiMap.(*digestBiMapImpl).sqlDigest2noDBDigest), 2)
	_, ok = fbc.digestBiMap.(*digestBiMapImpl).sqlDigest2noDBDigest[b1.SQLDigest]
	require.True(t, ok)
	_, ok = fbc.digestBiMap.(*digestBiMapImpl).sqlDigest2noDBDigest[b2.SQLDigest]
	require.False(t, ok) // can't find b2 now
	_, ok = fbc.digestBiMap.(*digestBiMapImpl).sqlDigest2noDBDigest[b3.SQLDigest]
	require.True(t, ok)
}

func TestBindCache(t *testing.T) {
	binding := &Binding{BindSQL: "SELECT * FROM t1"}
	kvSize := int(binding.size())
	defer func(v int64) {
		variable.MemQuotaBindingCache.Store(v)
	}(variable.MemQuotaBindingCache.Load())
	variable.MemQuotaBindingCache.Store(int64(kvSize*3) - 1)
	bindCache := newBindCache()
	defer bindCache.Close()

	err := bindCache.SetBinding("digest1", binding)
	require.Nil(t, err)
	require.NotNil(t, bindCache.GetBinding("digest1"))

	err = bindCache.SetBinding("digest2", binding)
	require.Nil(t, err)
	require.NotNil(t, bindCache.GetBinding("digest2"))

	err = bindCache.SetBinding("digest3", binding)
	require.Nil(t, err)
	require.NotNil(t, bindCache.GetBinding("digest3"))

	require.Eventually(t, func() bool {
		hit := 0
		for _, digest := range []string{"digest1", "digest2", "digest3"} {
			if bindCache.GetBinding(digest) != nil {
				hit++
			}
		}
		return hit == 2
	}, time.Second*5, time.Millisecond*100)
}

func getTableName(n []*ast.TableName) []string {
	result := make([]string, 0, len(n))
	for _, v := range n {
		var sb strings.Builder
		restoreFlags := format.RestoreKeyWordLowercase
		restoreCtx := format.NewRestoreCtx(restoreFlags, &sb)
		v.Restore(restoreCtx)
		result = append(result, sb.String())
	}
	return result
}

func TestExtractTableName(t *testing.T) {
	tc := []struct {
		sql    string
		tables []string
	}{
		{
			"select /*+ HASH_JOIN(t1, t2) */ * from t1 t1 join t1 t2 on t1.a=t2.a where t1.b is not null;",
			[]string{"t1", "t1"},
		},
		{
			"select * from t",
			[]string{"t"},
		},
		{
			"select * from t1, t2, t3;",
			[]string{"t1", "t2", "t3"},
		},
		{
			"select * from t1 where t1.a > (select max(a) from t2);",
			[]string{"t1", "t2"},
		},
		{
			"select * from t1 where t1.a > (select max(a) from t2 where t2.a > (select max(a) from t3));",
			[]string{"t1", "t2", "t3"},
		},
		{
			"select a,b,c,d,* from t1 where t1.a > (select max(a) from t2 where t2.a > (select max(a) from t3));",
			[]string{"t1", "t2", "t3"},
		},
	}
	for _, tt := range tc {
		stmt, err := parser.New().ParseOneStmt(tt.sql, "", "")
		require.NoError(t, err)
		rs := CollectTableNames(stmt)
		result := getTableName(rs)
		require.Equal(t, tt.tables, result)
	}
}
