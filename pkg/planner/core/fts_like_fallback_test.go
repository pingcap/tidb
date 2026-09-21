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

package core_test

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/pingcap/tidb/pkg/planner"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/util/intest"
	"github.com/stretchr/testify/require"
)

func TestFTSLikeFallbackMode(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustQuery("select @@tidb_enable_fts_like_fallback").Check(testkit.Rows("0"))
	tk.MustExec("create table fts_mode (id int primary key, a text, b text, fulltext index (a))")
	tk.MustExec("insert into fts_mode values (1, 'cat', NULL), (2, 'category', 'dog'), (3, 'dog', NULL), (4, NULL, 'cat')")
	q := "select id from fts_mode where match(a) against('cat' in boolean mode) order by id"
	require.Error(t, tk.ExecToErr(q))
	tk.MustExec("set tidb_enable_fts_like_fallback=on")
	tk.MustExec("set tidb_opt_enable_alternative_logical_plans=off")
	require.Error(t, tk.ExecToErr(q))
	for _, alt := range []string{"on"} {
		tk.MustExec("set tidb_opt_enable_alternative_logical_plans=" + alt)
		tk.MustQuery(q).Check(testkit.Rows("1", "2"))
		tk.MustQuery("select id,a from fts_mode having match(a) against('cat' in boolean mode) order by id").Check(testkit.Rows("1 cat", "2 category"))
		tk.MustQuery("select x.id from fts_mode x join fts_mode y on x.id=y.id and match(x.a) against('cat' in boolean mode) order by x.id").Check(testkit.Rows("1", "2"))
		tk.MustQuery("select id from fts_mode where match(a) against('cat' in boolean mode) and id in (select id from fts_mode where b is null) order by id").Check(testkit.Rows("1"))
		tk.MustQuery("select id from fts_mode where match(a) against('ca' in boolean mode) order by id").Check(testkit.Rows("1", "2"))
		tk.MustQuery("select id from fts_mode where match(a,b) against('+cat -dog' in boolean mode) order by id").Check(testkit.Rows("1", "4"))
		tk.MustQuery("select id from fts_mode where not(match(a) against(NULL in boolean mode))").Check(testkit.Rows())
		tk.MustQuery("select id from fts_mode where match(a) against('-dog' in boolean mode)").Check(testkit.Rows())
		tk.MustQuery("select id from fts_mode where match(a) against('cat') order by id").Check(testkit.Rows("1", "2"))
		for _, sql := range []string{
			"select match(a) against('cat' in boolean mode) from fts_mode",
			"select id from fts_mode where match(a) against('cat' in boolean mode)>0",
			"select id from fts_mode where match(a) against('cat*' in boolean mode)",
			`select id from fts_mode where match(a) against('"cat dog"' in boolean mode)`,
			"select id from fts_mode where match(a) against('cat' with query expansion)",
			"select id from fts_mode where match(id) against(NULL in boolean mode)",
			"select id from fts_mode where match(a) against(a in boolean mode)",
		} {
			require.Error(t, tk.ExecToErr(sql), sql)
		}
	}
	// With alternatives disabled, the default rewrite remains local.
	tk.MustExec("set tidb_opt_enable_alternative_logical_plans=off")
	tk.MustExec("set tidb_enable_local_match_against=on")
	tk.MustQuery(q).Check(testkit.Rows("1"))
	require.Error(t, tk.ExecToErr("select id from fts_mode where match(b) against('cat' in boolean mode)"))
	require.Error(t, tk.ExecToErr("select id from fts_mode where match(a) against('cat')"))
	tk.MustExec("set tidb_enable_fts_like_fallback=off")
	tk.MustQuery(q).Check(testkit.Rows("1"))
	// A separate session retains its own default mode.
	other := testkit.NewTestKit(t, store)
	other.MustExec("use test")
	require.Error(t, other.ExecToErr(q))
}

func TestFTSLikeFallbackPrepared(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table fts_cache (id int primary key, a text, fulltext index (a))")
	tk.MustExec("insert into fts_cache values (1,'cat'),(2,'category'),(3,'dog')")
	tk.MustExec("set tidb_enable_fts_like_fallback=on")
	tk.MustExec("set tidb_opt_enable_alternative_logical_plans=on")
	tk.MustExec("prepare ps from 'select id from fts_cache where match(a) against(? in boolean mode) order by id'")
	for _, tc := range []struct {
		value string
		rows  []string
	}{
		{"NULL", nil}, {"'cat'", []string{"1", "2"}}, {"'dog'", []string{"3"}}, {"NULL", nil},
	} {
		tk.MustExec("set @q=" + tc.value)
		tk.MustQuery("execute ps using @q").Check(testkit.Rows(tc.rows...))
	}
	// Literal searches may be cached, but switching modes must select a new plan.
	tk.MustExec("prepare lit from \"select id from fts_cache where match(a) against('cat' in boolean mode) order by id\"")
	for i := 0; i < 2; i++ {
		tk.MustQuery("execute lit").Check(testkit.Rows("1", "2"))
	}
	tk.MustExec("set tidb_enable_local_match_against=on")
	tk.MustExec("set tidb_opt_enable_alternative_logical_plans=off")
	for i := 0; i < 2; i++ {
		tk.MustQuery("execute lit").Check(testkit.Rows("1"))
	}
	tk.MustExec("set tidb_enable_local_match_against=off")
	tk.MustExec("set tidb_opt_enable_alternative_logical_plans=on")
	tk.MustQuery("execute lit").Check(testkit.Rows("1", "2"))
	tk.MustExec("set tidb_enable_fts_like_fallback=off")
	require.Error(t, tk.ExecToErr("execute lit"))
}

// TestFTSLocalWordBoundary checks STANDARD token semantics with only Local FTS.
// ILIKE fallback is incompatible here: "%cat%" also matches "category".
// Keep it disabled so this correctness test cannot select an ILIKE candidate.
func TestFTSLocalWordBoundary(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set tidb_enable_local_match_against=on, tidb_enable_fts_like_fallback=off, tidb_opt_enable_alternative_logical_plans=on")
	tk.MustExec("create table fts_local_boundary(id int primary key, a text, fulltext index(a))")
	tk.MustExec("insert into fts_local_boundary values(1,'cat'),(2,'category'),(3,'dog')")
	q := "select id from fts_local_boundary where match(a) against('cat' in boolean mode) order by id"
	tk.MustQuery(q).Check(testkit.Rows("1"))
	plan := fmt.Sprint(tk.MustQuery("explain format='brief' " + q).Rows())
	require.Contains(t, plan, "match_against")
	require.NotContains(t, plan, "ilike")
}

// TestFTSLikeAlternativeCBO checks candidate selection using data for which
// Local FTS and ILIKE return the same rows. Word-boundary correctness is tested
// separately with ILIKE disabled in TestFTSLocalWordBoundary.
func TestFTSLikeAlternativeCBO(t *testing.T) {
	if !intest.InTest {
		t.Skip("requires intest cost observer")
	}
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table fts_cbo(id int primary key, a text, fulltext index(a))")
	tk.MustExec("insert into fts_cbo values(1,'cat'),(2,'bird'),(3,'dog')")
	tk.MustExec("set tidb_enable_local_match_against=on, tidb_enable_fts_like_fallback=on, tidb_opt_enable_alternative_logical_plans=on")
	q := "select id from fts_cbo where match(a) against('cat' in boolean mode) order by id"
	for _, winner := range []string{"local", "ilike", "tie"} {
		t.Run(winner, func(t *testing.T) {
			var rounds []bool
			ctx := context.WithValue(context.Background(), planner.FTSAlternativeCostTestKey{}, func(like bool, _ float64) float64 {
				rounds = append(rounds, like)
				if winner == "tie" || (winner == "ilike") == like {
					return 0
				}
				return 1
			})
			tk.MustQueryWithContext(ctx, q).Check(testkit.Rows("1"))
			require.Equal(t, []bool{false, true}, rounds)
			plan := tk.MustQueryWithContext(ctx, "explain format='brief' "+q).Rows()
			text := strings.ToLower(strings.TrimSpace(fmt.Sprint(plan)))
			if winner == "ilike" {
				require.Contains(t, text, "ilike")
				require.NotContains(t, text, "match_against")
			} else {
				require.Contains(t, text, "match_against")
			}
			require.False(t, tk.Session().GetSessionVars().StmtCtx.InFTSLikeFallbackRound)
		})
	}
	// Unsupported ILIKE syntax drops only that candidate.
	tk.MustQuery("select id from fts_cbo where match(a) against('cat*' in boolean mode) order by id").Check(testkit.Rows("1"))
	require.False(t, tk.Session().GetSessionVars().StmtCtx.InFTSLikeFallbackRound)
	// A literal prepared plan must not bypass the alternative switch on a cache hit.
	tk.MustExec("prepare p from \"select id from fts_cbo where match(a) against('cat' in boolean mode) order by id\"")
	ctx := context.WithValue(context.Background(), planner.FTSAlternativeCostTestKey{}, func(like bool, _ float64) float64 {
		if like {
			return 0
		}
		return 1
	})
	tk.MustQueryWithContext(ctx, "execute p").Check(testkit.Rows("1"))
	tk.MustQueryWithContext(ctx, "execute p").Check(testkit.Rows("1"))
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("1"))
	tk.MustExec("set tidb_opt_enable_alternative_logical_plans=off")
	tk.MustQueryWithContext(ctx, "execute p").Check(testkit.Rows("1"))
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("0"))
	tk.MustExec("set tidb_opt_enable_alternative_logical_plans=on")
	tk.MustQueryWithContext(ctx, "execute p").Check(testkit.Rows("1"))
	// Mutable ILIKE patterns must not cache the first parameter, including NULL.
	tk.MustExec("prepare dyn from 'select id from fts_cbo where match(a) against(? in boolean mode) order by id'")
	for _, tc := range []struct {
		value string
		rows  []string
	}{{"NULL", nil}, {"'cat'", []string{"1"}}, {"'dog'", []string{"3"}}, {"NULL", nil}} {
		tk.MustExec("set @v=" + tc.value)
		tk.MustQueryWithContext(ctx, "execute dyn using @v").Check(testkit.Rows(tc.rows...))
		tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("0"))
	}
}

func TestFTSLikeAlternativeRoundBoundaries(t *testing.T) {
	if !intest.InTest {
		t.Skip("requires intest cost observer")
	}
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table fts_rounds(id int primary key,a text,fulltext index(a))")
	tk.MustExec("insert into fts_rounds values(1,'cat'),(2,'category')")
	tk.MustExec("set tidb_enable_local_match_against=on,tidb_enable_fts_like_fallback=on,tidb_opt_enable_alternative_logical_plans=on")
	var rounds []bool
	ctx := context.WithValue(context.Background(), planner.FTSAlternativeCostTestKey{}, func(like bool, cost float64) float64 { rounds = append(rounds, like); return cost })
	q := "select x.id from fts_rounds x join fts_rounds y on x.id=y.id where match(x.a) against('cat' in boolean mode) order by x.id limit 10"
	tk.MustQueryWithContext(ctx, "explain "+q)
	require.GreaterOrEqual(t, len(rounds), 3, "default, ordinary alternative and ILIKE")
	require.True(t, rounds[len(rounds)-1])
	for _, like := range rounds[:len(rounds)-1] {
		require.False(t, like)
	}
	// Disabling local MATCH does not turn the default or ordinary rounds into ILIKE.
	tk.MustExec("set tidb_enable_local_match_against=off")
	rounds = nil
	tk.MustQueryWithContext(ctx, q).Check(testkit.Rows("1", "2"))
	require.Equal(t, []bool{true}, rounds)
	// With no local candidate, malformed ILIKE syntax must surface its actual error.
	require.ErrorContains(t, tk.ExecToErr("select id from fts_rounds where match(a) against('cat*' in boolean mode)"), "LIKE fallback")
	require.False(t, tk.Session().GetSessionVars().StmtCtx.InFTSLikeFallbackRound)
	tk.MustExec("set tidb_enable_local_match_against=on")
	cancelCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cancelCtx = context.WithValue(cancelCtx, planner.FTSAlternativeCostTestKey{}, func(like bool, cost float64) float64 {
		if like {
			cancel()
		}
		return cost
	})
	rs, err := tk.ExecWithContext(cancelCtx, q)
	if rs != nil {
		require.NoError(t, rs.Close())
	}
	require.Error(t, err)
	require.False(t, tk.Session().GetSessionVars().StmtCtx.InFTSLikeFallbackRound)
}

func TestFTSLikeAlternativeQuotedWords(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table fts_quotes(id int primary key, a text, fulltext index(a))")
	tk.MustExec("insert into fts_quotes values(1,'cat'),(2,'category dog'),(3,'dog'),(4,NULL),(5,'cat dog')")
	tk.MustExec("set tidb_enable_local_match_against=off, tidb_enable_fts_like_fallback=on, tidb_opt_enable_alternative_logical_plans=on")
	for _, mode := range []string{"", " in boolean mode"} {
		for _, term := range []string{`cat`, `"cat"`} {
			sql := fmt.Sprintf("select id from fts_quotes where match(a) against('%s'%s) order by id", term, mode)
			tk.MustQuery(sql).Check(testkit.Rows("1", "2", "5"))
			plan := fmt.Sprint(tk.MustQuery("explain format='brief' " + sql).Rows())
			require.Contains(t, plan, "ilike")
			require.Contains(t, plan, "%cat%")
			require.NotContains(t, plan, "match_against")
		}
	}
	tk.MustQuery(`select id from fts_quotes where match(a) against('+"cat" -"dog"' in boolean mode) order by id`).Check(testkit.Rows("1"))
	tk.MustQuery(`select id from fts_quotes where match(a) against('-"dog"' in boolean mode) order by id`).Check(testkit.Rows())
	tk.MustQuery(`select id from fts_quotes where match(a) against('"cat" "dog"') order by id`).Check(testkit.Rows("1", "2", "3", "5"))
	for _, term := range []string{`"cat dog"`, `"cat*"`, `+"cat*"`, `"cat`, `cat"`, `""`, `+""`, `"cat"dog`, `"cat-dog"`} {
		sql := fmt.Sprintf("select id from fts_quotes where match(a) against('%s' in boolean mode)", term)
		require.ErrorContains(t, tk.ExecToErr(sql), "LIKE fallback", term)
		require.False(t, tk.Session().GetSessionVars().StmtCtx.InFTSLikeFallbackRound)
	}
	// Prepared queries must normalize each new argument, not cache old tokens.
	tk.MustExec("prepare quoted from 'select id from fts_quotes where match(a) against(? in boolean mode) order by id'")
	for _, tc := range []struct {
		term string
		rows []string
	}{
		{`"cat"`, []string{"1", "2", "5"}},
		{`+"cat" -"dog"`, []string{"1"}},
		{`"dog"`, []string{"2", "3", "5"}},
	} {
		tk.MustExec("set @term='" + tc.term + "'")
		tk.MustQuery("execute quoted using @term").Check(testkit.Rows(tc.rows...))
		tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("0"))
	}
	// Unsupported multi-word phrases must preserve a valid local candidate.
	tk.MustExec("set tidb_enable_local_match_against=on")
	tk.MustQuery(`select id from fts_quotes where match(a) against('"cat dog"' in boolean mode) order by id`).Check(testkit.Rows("5"))
	if intest.InTest {
		// A quoted single word may now participate in the ILIKE CBO round.
		ctx := context.WithValue(context.Background(), planner.FTSAlternativeCostTestKey{}, func(like bool, _ float64) float64 {
			if like {
				return 0
			}
			return 1
		})
		tk.MustQueryWithContext(ctx, `select id from fts_quotes where match(a) against('"cat"' in boolean mode) order by id`).Check(testkit.Rows("1", "2", "5"))
	}
}

func TestFTSLikeSwitchMatrix(t *testing.T) {
	if !intest.InTest {
		t.Skip("requires intest cost observer")
	}
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table delivery_matrix(id int primary key,a text,fulltext index(a))")
	tk.MustExec("insert into delivery_matrix values(1,'cat'),(2,'category'),(3,'dog')")
	q := "select id from delivery_matrix where match(a) against('cat' in boolean mode) order by id"
	for a := 0; a < 2; a++ {
		for l := 0; l < 2; l++ {
			for i := 0; i < 2; i++ {
				t.Run(fmt.Sprintf("A%dL%dI%d", a, l, i), func(t *testing.T) {
					tk.MustExec(fmt.Sprintf("set tidb_opt_enable_alternative_logical_plans=%d,tidb_enable_local_match_against=%d,tidb_enable_fts_like_fallback=%d", a, l, i))
					var rounds []bool
					ctx := context.WithValue(context.Background(), planner.FTSAlternativeCostTestKey{}, func(like bool, _ float64) float64 {
						rounds = append(rounds, like)
						if like {
							return 0
						}
						return 1
					})
					if l == 0 && (a == 0 || i == 0) {
						require.Error(t, tk.ExecToErr(q))
					} else {
						expected := testkit.Rows("1")
						if a == 1 && i == 1 {
							expected = testkit.Rows("1", "2")
						}
						tk.MustQueryWithContext(ctx, q).Check(expected)
						if a == 1 && i == 1 {
							if l == 1 {
								require.Equal(t, []bool{false, true}, rounds)
							} else {
								require.Equal(t, []bool{true}, rounds)
							}
						} else {
							for _, like := range rounds {
								require.False(t, like)
							}
						}
					}
					require.False(t, tk.Session().GetSessionVars().StmtCtx.InFTSLikeFallbackRound)
				})
			}
		}
	}
}
func TestFTSLikeConcurrentSessions(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table delivery_sessions(id int primary key,a text,fulltext index(a))")
	tk.MustExec("insert into delivery_sessions values(1,'cat'),(2,'category')")
	var wg sync.WaitGroup
	start := make(chan struct{})
	for n := 0; n < 8; n++ {
		n := n
		s := testkit.NewTestKit(t, store)
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			s.MustExec("use test")
			s.MustExec(fmt.Sprintf("set tidb_opt_enable_alternative_logical_plans=1,tidb_enable_local_match_against=%d,tidb_enable_fts_like_fallback=%d", n%2, 1-n%2))
			rows := testkit.Rows("1")
			if n%2 == 0 {
				rows = testkit.Rows("1", "2")
			}
			for j := 0; j < 10; j++ {
				s.MustQuery("select id from delivery_sessions where match(a) against('cat' in boolean mode) order by id").Check(rows)
				require.False(t, s.Session().GetSessionVars().StmtCtx.InFTSLikeFallbackRound)
			}
		}()
	}
	close(start)
	wg.Wait()
}

func TestFTSLikeErrorBoundaries(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table delivery_edges(id int,a text,b text,fulltext index(a,b))")
	tk.MustExec("insert into delivery_edges values(1,'cat','dog'),(2,'category',NULL)")
	tk.MustExec("set tidb_opt_enable_alternative_logical_plans=1,tidb_enable_local_match_against=1,tidb_enable_fts_like_fallback=1")
	for _, q := range []string{
		"select id from delivery_edges where match(a) against('cat' in boolean mode)",
		"select id from delivery_edges where match(b,a) against('cat' in boolean mode)",
		"select id from delivery_edges where match(a,b) against('cat')",
		"select match(a,b) against('cat' in boolean mode) from delivery_edges",
		"select id from delivery_edges where match(a,b) against('cat' in boolean mode)>0",
		"select id from delivery_edges order by match(a,b) against('cat' in boolean mode)",
	} {
		require.Error(t, tk.ExecToErr(q), q)
		require.False(t, tk.Session().GetSessionVars().StmtCtx.InFTSLikeFallbackRound)
	}
	tk.MustExec("set tidb_enable_local_match_against=0")
	tk.MustQuery("select id from delivery_edges where match(a) against('cat' in boolean mode) order by id").Check(testkit.Rows("1", "2"))
	tk.MustQuery("select id from delivery_edges where match(b,a) against('cat' in boolean mode) order by id").Check(testkit.Rows("1", "2"))
	tk.MustExec("set tidb_enable_local_match_against=1,tidb_enable_fts_like_fallback=0")
	tk.MustQuery("select id from delivery_edges where match(a,b) against('cat' in boolean mode) order by id").Check(testkit.Rows("1"))
}
