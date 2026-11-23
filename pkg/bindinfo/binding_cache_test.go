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
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/format"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/stretchr/testify/require"
)

func bindingNoDBDigest(t *testing.T, b *Binding) string {
	p := parser.New()
	stmt, err := p.ParseOneStmt(b.BindSQL, b.Charset, b.Collation)
	require.NoError(t, err)
	_, noDBDigest := NormalizeStmtForBinding(stmt, "", true)
	return noDBDigest
}

func TestCrossDBBindingCache(t *testing.T) {
	fbc := newBindingCache(context.Background(), 1000000000).(*bindingCache)
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

func TestBindingCacheDigestSnapshot(t *testing.T) {
	b1 := &Binding{BindSQL: "SELECT * FROM db1.t1", SQLDigest: "b1"}
	b2 := &Binding{BindSQL: "SELECT * FROM db2.t1", SQLDigest: "b2"}
	noDBDigest := bindingNoDBDigest(t, b1)
	require.Equal(t, noDBDigest, bindingNoDBDigest(t, b2))

	t.Run("lookup does not allocate", func(t *testing.T) {
		digests := newDigestBiMap()
		digests.Add(noDBDigest, b1.SQLDigest)
		digests.Add(noDBDigest, b2.SQLDigest)
		var snapshot []string
		allocs := testing.AllocsPerRun(100, func() {
			snapshot = digests.NoDBDigest2SQLDigest(noDBDigest)
		})
		require.Zero(t, allocs)
		require.Equal(t, []string{b1.SQLDigest, b2.SQLDigest}, snapshot)
	})

	t.Run("insertion allocation grows linearly", func(t *testing.T) {
		const count = 10000
		sqlDigests := make([]string, count)
		for i := range sqlDigests {
			sqlDigests[i] = fmt.Sprintf("binding-%d", i)
		}
		result := testing.Benchmark(func(b *testing.B) {
			for range b.N {
				digests := newDigestBiMap()
				for _, sqlDigest := range sqlDigests {
					digests.Add(noDBDigest, sqlDigest)
				}
			}
		})
		// Allow ample room for both maps and amortized slice growth, but reject
		// copying the whole bucket on every insertion (about 800 MB here).
		t.Logf("allocated %d bytes for %d bindings", result.AllocedBytesPerOp(), count)
		require.Less(t, result.AllocedBytesPerOp(), int64(count*1024))
	})

	t.Run("snapshots survive updates", func(t *testing.T) {
		digests := newDigestBiMap().(*digestBiMapImpl)
		// Exercise append with spare capacity as well as copy-on-write deletion.
		digests.noDBDigest2SQLDigest[noDBDigest] = make([]string, 0, 4)
		digests.Add(noDBDigest, b1.SQLDigest)
		first := digests.NoDBDigest2SQLDigest(noDBDigest)
		digests.Add(noDBDigest, b2.SQLDigest)
		both := digests.NoDBDigest2SQLDigest(noDBDigest)
		digests.Add(noDBDigest, b2.SQLDigest) // duplicate insertion must not change membership
		require.Equal(t, []string{b1.SQLDigest, b2.SQLDigest}, digests.NoDBDigest2SQLDigest(noDBDigest))
		digests.Del(b1.SQLDigest)
		remaining := digests.NoDBDigest2SQLDigest(noDBDigest)
		digests.Del(b2.SQLDigest)
		require.Nil(t, digests.NoDBDigest2SQLDigest(noDBDigest))
		digests.Add(noDBDigest, b2.SQLDigest)
		require.Equal(t, []string{b2.SQLDigest}, digests.NoDBDigest2SQLDigest(noDBDigest))
		require.Equal(t, []string{b1.SQLDigest}, first)
		require.Equal(t, []string{b1.SQLDigest, b2.SQLDigest}, both)
		require.Equal(t, []string{b2.SQLDigest}, remaining)
	})

	t.Run("snapshots survive batch removal", func(t *testing.T) {
		digests := newDigestBiMap()
		for _, digest := range []string{"a", "b", "c", "d"} {
			digests.Add(noDBDigest, digest)
		}
		digests.Add("other", "e")
		digests.Add("other", "f")
		before := digests.NoDBDigest2SQLDigest(noDBDigest)
		other := digests.NoDBDigest2SQLDigest("other")
		digests.Del("b", "b", "d", "e", "missing")
		require.Equal(t, []string{"a", "c"}, digests.NoDBDigest2SQLDigest(noDBDigest))
		require.Equal(t, []string{"f"}, digests.NoDBDigest2SQLDigest("other"))
		require.ElementsMatch(t, []string{"a", "c", "f"}, digests.All())
		for _, digest := range []string{"b", "d", "e"} {
			require.Empty(t, digests.SQLDigest2NoDBDigest(digest))
		}
		remaining := digests.NoDBDigest2SQLDigest(noDBDigest)
		digests.Del("a", "c", "f")
		require.Nil(t, digests.NoDBDigest2SQLDigest(noDBDigest))
		require.Nil(t, digests.NoDBDigest2SQLDigest("other"))
		require.Empty(t, digests.All())
		require.Equal(t, []string{"a", "b", "c", "d"}, before)
		require.Equal(t, []string{"e", "f"}, other)
		require.Equal(t, []string{"a", "c"}, remaining)
	})

	t.Run("delete during iteration", func(t *testing.T) {
		cache := newBindingCache(context.Background(), 1000000000).(*bindingCache)
		defer cache.Close()
		require.NoError(t, cache.SetBinding(b1.SQLDigest, b1))
		require.NoError(t, cache.SetBinding(b2.SQLDigest, b2))

		deleteBinding := make(chan struct{})
		deleted := make(chan struct{})
		go func() {
			<-deleteBinding
			cache.RemoveBinding(b1.SQLDigest)
			close(deleted)
		}()

		// Pause a lookup after the first candidate, then delete it. Deletion must
		// not move the surviving candidate out of the lookup's snapshot.
		var bindings []*Binding
		for i, digest := range cache.digestBiMap.NoDBDigest2SQLDigest(noDBDigest) {
			if binding := cache.GetBinding(digest); binding != nil {
				bindings = append(bindings, binding)
			}
			if i == 0 {
				close(deleteBinding)
				<-deleted
			}
		}
		require.Equal(t, []*Binding{b1, b2}, bindings)
		require.Equal(t, []string{b2.SQLDigest}, cache.digestBiMap.NoDBDigest2SQLDigest(noDBDigest))
		require.Same(t, b2, cache.GetBinding(b2.SQLDigest))
	})

	t.Run("concurrent lookup and delete", func(t *testing.T) {
		digests := newDigestBiMap()
		digests.Add(noDBDigest, b1.SQLDigest)
		digests.Add(noDBDigest, b2.SQLDigest)
		start := make(chan struct{})
		result := make(chan bool)
		go func() {
			<-start
			allFound := true
			for range 1000 {
				found := false
				for _, digest := range digests.NoDBDigest2SQLDigest(noDBDigest) {
					if digest == b2.SQLDigest {
						found = true
					}
				}
				allFound = allFound && found
			}
			result <- allFound
		}()
		close(start)
		for range 1000 {
			digests.Del(b1.SQLDigest)
			digests.Add(noDBDigest, b1.SQLDigest)
		}
		require.True(t, <-result, "the surviving binding must always be present")
	})
}

func TestBindingCacheBatchDelete(t *testing.T) {
	t.Run("removes cache entries", func(t *testing.T) {
		cache := newBindingCache(context.Background(), 1000000000)
		defer cache.Close()
		for _, db := range []string{"a", "b", "c"} {
			require.NoError(t, cache.SetBinding(db, &Binding{BindSQL: "SELECT * FROM " + db + ".t", SQLDigest: db}))
		}
		cache.RemoveBinding("a", "b", "a", "missing")
		require.Nil(t, cache.GetBinding("a"))
		require.Nil(t, cache.GetBinding("b"))
		require.NotNil(t, cache.GetBinding("c"))
	})

	t.Run("concurrent lookup retains survivor", func(t *testing.T) {
		digests := newDigestBiMap()
		for _, digest := range []string{"a", "b", "survivor"} {
			digests.Add("shared", digest)
		}
		start := make(chan struct{})
		result := make(chan bool)
		go func() {
			<-start
			allFound := true
			for range 1000 {
				found := false
				for _, digest := range digests.NoDBDigest2SQLDigest("shared") {
					found = found || digest == "survivor"
				}
				allFound = allFound && found
			}
			result <- allFound
		}()
		close(start)
		for range 1000 {
			digests.Del("a", "b")
			digests.Add("shared", "a")
			digests.Add("shared", "b")
		}
		require.True(t, <-result)
	})

	t.Run("batch deletion allocation grows linearly", func(t *testing.T) {
		const count = 10000
		sqlDigests := make([]string, count)
		for i := range sqlDigests {
			sqlDigests[i] = fmt.Sprintf("binding-%d", i)
		}
		for _, deleteCount := range []int{count / 2, count} {
			result := testing.Benchmark(func(b *testing.B) {
				for range b.N {
					digests := newDigestBiMap().(*digestBiMapImpl)
					// Seed directly to keep this guard focused on deletion allocations.
					digests.noDBDigest2SQLDigest["shared"] = append([]string(nil), sqlDigests...)
					for _, digest := range sqlDigests {
						digests.sqlDigest2noDBDigest[digest] = "shared"
					}
					digests.Del(sqlDigests[:deleteCount]...)
				}
			})
			t.Logf("deleting %d of %d bindings allocated %d bytes", deleteCount, count, result.AllocedBytesPerOp())
			// Includes map setup, but excludes copying a shrinking bucket per removal.
			require.Less(t, result.AllocedBytesPerOp(), int64(count*1024))
		}
	})
}

type bindingCacheBatchLoadRecorder struct {
	BindingCache
	sets    int
	removed []string
}

func (c *bindingCacheBatchLoadRecorder) SetBinding(digest string, binding *Binding) error {
	c.sets++
	return c.BindingCache.SetBinding(digest, binding)
}

func (c *bindingCacheBatchLoadRecorder) RemoveBinding(digests ...string) {
	c.removed = append(c.removed, digests...)
	c.BindingCache.RemoveBinding(digests...)
}

func TestBindingCacheBatchLoad(t *testing.T) {
	binding := func(version int, status string) *Binding {
		return &Binding{
			BindSQL:    "SELECT * FROM db.t",
			SQLDigest:  "digest",
			Status:     status,
			UpdateTime: types.NewTime(types.FromDate(2026, 11, 17, 0, 0, version, 0), mysql.TypeTimestamp, 6),
		}
	}
	old := binding(1, StatusEnabled)
	deleted := binding(2, StatusDeleted)
	recreated := binding(3, StatusEnabled)
	equalTimeRecreated := binding(2, StatusEnabled)
	disabled := binding(3, StatusDisabled)
	for _, tc := range []struct {
		name     string
		cached   *Binding
		loaded   []*Binding
		expected *Binding
		removed  bool
	}{
		{"cold recreation", nil, []*Binding{deleted, recreated}, recreated, true},
		{"cached recreation", old, []*Binding{deleted, recreated}, recreated, true},
		{"equal time recreation", old, []*Binding{deleted, equalTimeRecreated}, equalTimeRecreated, true},
		{"cached newer than storage", recreated, []*Binding{old, deleted}, recreated, false},
		{"equal time cached wins", equalTimeRecreated, []*Binding{deleted}, equalTimeRecreated, false},
		{"disabled retained", old, []*Binding{deleted, disabled}, disabled, true},
		{"final deletion", old, []*Binding{old, deleted, deleted}, nil, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cache := newBindingCache(context.Background(), 1000000000)
			defer cache.Close()
			if tc.cached != nil {
				require.NoError(t, cache.SetBinding("digest", tc.cached))
			}
			recorder := &bindingCacheBatchLoadRecorder{BindingCache: cache}
			updater := &bindingCacheUpdater{BindingCache: recorder}
			updater.applyBindings(tc.loaded)
			if tc.removed {
				require.Equal(t, []string{"digest"}, recorder.removed)
			} else {
				require.Empty(t, recorder.removed)
			}
			if tc.expected == nil {
				require.Nil(t, cache.GetBinding("digest"))
				require.Zero(t, recorder.sets)
			} else {
				require.Same(t, tc.expected, cache.GetBinding("digest"))
				require.Equal(t, 1, recorder.sets)
			}
		})
	}
	t.Run("candidate order", func(t *testing.T) {
		for _, cached := range []bool{false, true} {
			for _, deletion := range []bool{false, true} {
				t.Run(fmt.Sprintf("cached=%t/deletion=%t", cached, deletion), func(t *testing.T) {
					cache := newBindingCache(context.Background(), 1000000000).(*bindingCache)
					defer cache.Close()
					a, b := binding(1, StatusEnabled), binding(2, StatusEnabled)
					a.SQLDigest, b.SQLDigest = "a", "b"
					b.BindSQL = "SELECT * FROM other.t"
					if cached {
						require.NoError(t, cache.SetBinding("a", a))
						require.NoError(t, cache.SetBinding("b", b))
					}
					first, latest := binding(1, StatusEnabled), binding(3, StatusEnabled)
					first.SQLDigest, latest.SQLDigest = "a", "a"
					expected := []string{"a", "b"}
					if deletion {
						first.Status = StatusDeleted
						first.UpdateTime = b.UpdateTime
						expected = []string{"b", "a"}
					}
					updater := &bindingCacheUpdater{BindingCache: cache}
					updater.applyBindings([]*Binding{first, b, latest})
					require.Equal(t, expected, cache.digestBiMap.NoDBDigest2SQLDigest(bindingNoDBDigest(t, a)))
				})
			}
		}
	})
}

func BenchmarkDigestBiMapDeleteBatch(b *testing.B) {
	for _, count := range []int{1000, 10000} {
		sqlDigests := make([]string, count)
		for i := range sqlDigests {
			sqlDigests[i] = fmt.Sprintf("%064x", i)
		}
		for _, deleteCount := range []int{count / 2, count} {
			for _, batch := range []bool{false, true} {
				b.Run(fmt.Sprintf("size=%d/delete=%d/batch=%t", count, deleteCount, batch), func(b *testing.B) {
					b.ReportAllocs()
					for range b.N {
						b.StopTimer()
						digests := newDigestBiMap().(*digestBiMapImpl)
						digests.noDBDigest2SQLDigest["shared"] = append([]string(nil), sqlDigests...)
						for _, digest := range sqlDigests {
							digests.sqlDigest2noDBDigest[digest] = "shared"
						}
						b.StartTimer()
						if batch {
							digests.Del(sqlDigests[:deleteCount]...)
						} else {
							for _, digest := range sqlDigests[:deleteCount] {
								digests.Del(digest)
							}
						}
					}
				})
			}
		}
	}
}

func TestDuplicatedBinding(t *testing.T) {
	// 3 bindings with the same noDBDigest
	bindingDB1 := &Binding{BindSQL: "SELECT * FROM db1.t1"}
	bindingDB2 := &Binding{BindSQL: "SELECT * FROM db2.t1"}
	bindingDB3 := &Binding{BindSQL: "SELECT * FROM db3.t1"}
	c := newBindingCache(context.Background(), 1000000000).(*bindingCache)
	require.Nil(t, c.SetBinding("db1", bindingDB1))
	require.Nil(t, c.SetBinding("db2", bindingDB2))
	require.Nil(t, c.SetBinding("db3", bindingDB3))

	digestMap := c.digestBiMap.(*digestBiMapImpl)
	var noDBDigest string
	for digest := range digestMap.noDBDigest2SQLDigest {
		noDBDigest = digest
	}
	require.True(t, noDBDigest != "")
	require.Equal(t, 3, len(digestMap.noDBDigest2SQLDigest[noDBDigest]))
	require.Equal(t, 3, len(digestMap.sqlDigest2noDBDigest))

	// put 3 duplicated bindings again
	require.Nil(t, c.SetBinding("db1", bindingDB1))
	require.Nil(t, c.SetBinding("db2", bindingDB2))
	require.Nil(t, c.SetBinding("db3", bindingDB3))
	require.True(t, noDBDigest != "")
	require.Equal(t, 3, len(digestMap.noDBDigest2SQLDigest[noDBDigest]))
	require.Equal(t, 3, len(digestMap.sqlDigest2noDBDigest))
}

func TestBindCache(t *testing.T) {
	binding := &Binding{BindSQL: "SELECT * FROM t1"}
	kvSize := int(binding.size())
	bindCache := newBindingCache(context.Background(), int64(kvSize*3)-1).(*bindingCache)
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

func TestBindingCacheEvictLog(t *testing.T) {
	callbackCnt := 0
	ctx := context.WithValue(context.Background(),
		bindingCacheTestKey, func(binding *Binding) { callbackCnt++ })

	largeBinding := &Binding{BindSQL: fmt.Sprintf("SELECT * FROM t1 WHERE c = '%v'", strings.Repeat("a", 200))}
	binding := &Binding{BindSQL: "SELECT * FROM t1"}
	bindingCache := newBindingCache(ctx, int64(binding.size())*3-1).(*bindingCache)
	defer bindingCache.Close()

	bindingCache.SetBinding("0", largeBinding)
	require.Equal(t, callbackCnt, 1) // large binding, reject directly
	bindingCache.SetBinding("0", largeBinding)
	require.Equal(t, callbackCnt, 2) // large binding, reject directly
	callbackCnt = 0                  // reset callback count

	bindingCache.SetBinding("1", binding) // insert the first binding four times
	bindingCache.SetBinding("1", binding)
	bindingCache.SetBinding("1", binding)
	bindingCache.SetBinding("1", binding)
	require.Equal(t, bindingCache.Size(), 1)
	require.Equal(t, bindingCache.GetMemUsage(), int64(binding.size()))
	require.Equal(t, callbackCnt, 0) // duplicated binding should not trigger eviction

	bindingCache.SetBinding("2", binding) // insert the second binding
	bindingCache.SetBinding("2", binding)
	require.Equal(t, callbackCnt, 0) // cache size is enough

	bindingCache.SetBinding("3", binding) // insert the third binding, trigger eviction
	require.Equal(t, callbackCnt, 1)

	for i := 1; i <= 10; i++ {
		bindingCache.SetBinding(fmt.Sprintf("3-%d", i), binding)
		require.Equal(t, callbackCnt, 1+i)
	}

	require.Equal(t, callbackCnt, 11)
	bindingCache.Close() // close doesn't trigger eviction log
	require.Equal(t, callbackCnt, 11)
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
		require.NoErrorf(t, err, "sql: %s", tt.sql)
		rs := CollectTableNames(stmt)
		result, err := getTableName(rs)
		require.NoErrorf(t, err, "sql: %s", tt.sql)
		require.Equalf(t, tt.tables, result, "sql: %s", tt.sql)
	}
}

func TestMayHaveSQLBinding(t *testing.T) {
	require.False(t, mayHaveSQLBinding(nil))
	require.True(t, mayHaveSQLBinding(&ast.ExplainStmt{}))

	tests := []struct {
		sql  string
		want bool
	}{
		{"insert into t values (1)", false},
		{"insert into t values (1) on duplicate key update a = values(a)", false},
		{"insert into t set a = 1", false},
		{"replace into t values (1)", false},
		{"explain insert into t values (1)", false},
		{"insert into t select * from s", true},
		{"replace into t select * from s", true},
		{"explain insert into t select * from s", true},
		{"select * from t", true},
		{"update t set a = 1", true},
		{"delete from t where a = 1", true},
	}

	p := parser.New()
	for _, tt := range tests {
		stmt, err := p.ParseOneStmt(tt.sql, "", "")
		require.NoError(t, err)
		require.Equal(t, tt.want, mayHaveSQLBinding(stmt), tt.sql)
	}
}

func TestMatchSQLBindingSkipsInsertValues(t *testing.T) {
	tests := []string{
		"insert into t values (1)",
		"insert into t values (1) on duplicate key update a = values(a)",
		"insert into t set a = 1",
		"replace into t values (1)",
		"explain insert into t values (1)",
	}

	originalGetBindingHandle := GetBindingHandle
	getBindingHandleCalls := 0
	GetBindingHandle = func(sctx sessionctx.Context) BindingHandle {
		getBindingHandleCalls++
		return nil
	}
	t.Cleanup(func() {
		GetBindingHandle = originalGetBindingHandle
	})

	sctx := mock.NewContext()
	sctx.GetSessionVars().UsePlanBaselines = true
	sctx.SetValue(SessionBindInfoKeyType, NewSessionBindingHandle())

	p := parser.New()
	for _, sql := range tests {
		stmt, err := p.ParseOneStmt(sql, "", "")
		require.NoError(t, err)

		info := &BindingMatchInfo{}
		binding, matched, scope := MatchSQLBindingWithCache(sctx, stmt, info)
		require.Nil(t, binding, sql)
		require.False(t, matched, sql)
		require.Empty(t, scope, sql)
		require.Empty(t, info.NoDBDigest, sql)
		require.Nil(t, info.TableNames, sql)
	}
	require.Zero(t, getBindingHandleCalls)
}

func getTableName(n []*ast.TableName) ([]string, error) {
	result := make([]string, 0, len(n))
	for _, v := range n {
		var sb strings.Builder
		restoreFlags := format.RestoreKeyWordLowercase
		restoreCtx := format.NewRestoreCtx(restoreFlags, &sb)
		if err := v.Restore(restoreCtx); err != nil {
			return nil, err
		}
		result = append(result, sb.String())
	}
	return result, nil
}
