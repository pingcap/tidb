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

package tici

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	ingesttestutil "github.com/pingcap/tidb/pkg/ddl/ingest/testutil"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/domain/infosync"
	"github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/store/mockstore"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/external"
	"github.com/stretchr/testify/require"
)

func TestLocalMatchIndexConfig(t *testing.T) {
	for _, name := range []string{"MockCreateTiCIIndexSuccess", "MockFinishIndexUpload", "MockCheckAddIndexProgress"} {
		path := "github.com/pingcap/tidb/pkg/tici/" + name
		require.NoError(t, failpoint.Enable(path, `return(true)`))
		t.Cleanup(func() { require.NoError(t, failpoint.Disable(path)) })
	}
	store := testkit.CreateMockStoreWithSchemaLease(t, time.Second, mockstore.WithMockTiFlash(2))
	defer ingesttestutil.InjectMockBackendCtx(t, store)()
	tk := testkit.NewTestKit(t, store)
	tiflash := infosync.NewMockTiFlash()
	infosync.SetMockTiFlash(tiflash)
	defer func() {
		tiflash.Lock()
		tiflash.StatusServer.Close()
		tiflash.Unlock()
	}()
	tk.MustExec("use test")
	oldSizes := tk.MustQuery("select @@global.innodb_ft_min_token_size, @@global.innodb_ft_max_token_size, @@global.ngram_token_size").Rows()[0]
	defer func() {
		tk.MustExec(fmt.Sprintf("set global innodb_ft_min_token_size = %v", oldSizes[0]))
		tk.MustExec(fmt.Sprintf("set global innodb_ft_max_token_size = %v", oldSizes[1]))
		tk.MustExec(fmt.Sprintf("set global ngram_token_size = %v", oldSizes[2]))
	}()
	tk.MustExec("set global innodb_ft_min_token_size = 2")
	tk.MustExec("set global innodb_ft_max_token_size = 8")
	tk.MustExec("set global ngram_token_size = 2")
	tk.MustExec("set innodb_ft_enable_stopword = off")
	tk.MustExec("create table local_fts(id int primary key, body text, fulltext index ft(body))")
	tk.MustExec("insert into local_fts values (1, 'an cat'), (2, 'elephant'), (3, 'encyclopedia'), (4, null)")
	tk.MustExec("create table local_ngram(id int primary key, body text, fulltext index ft(body) with parser ngram)")
	tk.MustExec("insert into local_ngram values (1, '数据库'), (2, '数据'), (3, '库')")
	// Query-time variables deliberately disagree with both index snapshots.
	tk.MustExec("set global innodb_ft_min_token_size = 4")
	tk.MustExec("set global innodb_ft_max_token_size = 5")
	tk.MustExec("set global ngram_token_size = 3")
	tk.MustExec("set innodb_ft_enable_stopword = on")
	tk.MustExec("set tidb_enable_local_match_against = on")
	tk.MustQuery("select id from local_fts where match(body) against('+an' in boolean mode)").Check(testkit.Rows("1"))
	tk.MustQuery("select id from local_fts where match(body) against('+elephant' in boolean mode)").Check(testkit.Rows("2"))
	tk.MustQuery("select id from local_fts where match(body) against('+encyclopedia' in boolean mode)").Check(testkit.Rows())
	tk.MustQuery("select id from local_ngram where match(body) against('+数据' in boolean mode) order by id").Check(testkit.Rows("1", "2"))
	tk.MustQuery("select id from local_fts where not match(body) against('+cat' in boolean mode) order by id").Check(testkit.Rows("2", "3", "4"))
	tk.MustQuery("select id from local_fts where id=3 or match(body) against('+cat' in boolean mode) order by id").Check(testkit.Rows("1", "3"))
	tk.MustQuery("select id, body from local_fts having match(body) against('+cat' in boolean mode)").Check(testkit.Rows("1 an cat"))
	tk.MustQuery("select a.id from local_fts a join local_fts b on a.id=b.id and match(a.body) against('+cat' in boolean mode)").Check(testkit.Rows("1"))
	require.Error(t, tk.ExecToErr("select match(body) against('+cat' in boolean mode) from local_fts"))
	require.Error(t, tk.ExecToErr("select id from local_fts where match(body) against('+cat' in boolean mode) > 0.5"))
	// Search parameters must not retain the previous execution's compiled query.
	tk.MustExec("prepare local_stmt from 'select id from local_fts where match(body) against(? in boolean mode)'")
	for _, value := range []string{"+an", "+elephant", "+encyclopedia", "+cat"} {
		tk.MustExec("set @search = '" + value + "'")
		expected := map[string][]string{"+an": {"1"}, "+elephant": {"2"}, "+encyclopedia": {}, "+cat": {"1"}}
		tk.MustQuery("execute local_stmt using @search").Check(testkit.Rows(expected[value]...))
	}
	prepared, err := tk.Session().GetSessionVars().GetPreparedStmtByName("local_stmt")
	require.NoError(t, err)
	localKey, _, cacheable, _, err := core.NewPlanCacheKey(tk.Session(), prepared.(*core.PlanCacheStmt))
	require.NoError(t, err)
	require.True(t, cacheable)
	tk.Session().GetSessionVars().EnableLocalMatchAgainst = false
	nativeKey, _, cacheable, _, err := core.NewPlanCacheKey(tk.Session(), prepared.(*core.PlanCacheStmt))
	tk.Session().GetSessionVars().EnableLocalMatchAgainst = true
	require.NoError(t, err)
	require.True(t, cacheable)
	require.NotEqual(t, localKey, nativeKey)
	tk.MustExec("begin")
	tk.MustExec("insert into local_fts values (5, 'cat')")
	tk.MustQuery("select id from local_fts where match(body) against('+cat' in boolean mode) order by id").Check(testkit.Rows("1", "5"))
	tk.MustExec("rollback")
	localPlan := fmt.Sprint(tk.MustQuery("explain select id from local_fts where match(body) against('+cat' in boolean mode)").Rows())
	require.Contains(t, localPlan, "match_against(")
	require.NotContains(t, strings.ToLower(localPlan), "search func:")
	require.Contains(t, localPlan, "cop[tikv]")
	// Restricting reads to TiKV makes the native TiCI round unavailable.
	// Local evaluation still uses tokenization from the index snapshot.
	tk.MustExec("set tidb_opt_enable_alternative_logical_plans = on")
	tk.MustExec("set tidb_isolation_read_engines = 'tikv'")
	tk.MustQuery("select id from local_fts where match(body) against('+an' in boolean mode)").Check(testkit.Rows("1"))
	tk.MustQuery("select id from local_fts where id=3 or match(body) against('+cat' in boolean mode) order by id").Check(testkit.Rows("1", "3"))
	tk.MustQuery("select a.id from local_fts a join local_fts b on a.id=b.id and match(a.body) against('+cat' in boolean mode)").Check(testkit.Rows("1"))
	for _, value := range []string{"+an", "+elephant", "+an"} {
		tk.MustExec("set @search = '" + value + "'")
		want := "1"
		if value == "+elephant" {
			want = "2"
		}
		tk.MustQuery("execute local_stmt using @search").Check(testkit.Rows(want))
	}
	tk.MustExec("begin")
	tk.MustExec("insert into local_fts values (5, 'cat')")
	tk.MustQuery("select id from local_fts where match(body) against('+cat' in boolean mode) order by id").Check(testkit.Rows("1", "5"))
	tk.MustExec("rollback")
	// If both rounds reject a mixed score/predicate query, preserve the native
	// error instead of reporting a generic no-plan error.
	require.ErrorContains(t, tk.ExecToErr("select match(body) against('+cat' in boolean mode) from local_fts where match(body) against('+cat' in boolean mode)"), "SELECT")
	tk.MustExec("create table no_local_index(body text)")
	require.Error(t, tk.ExecToErr("select * from no_local_index where match(body) against('+cat' in boolean mode)"))
	require.Error(t, tk.ExecToErr("select match(body) against('+cat' in boolean mode) from local_fts"))
	require.False(t, tk.Session().GetSessionVars().StmtCtx.AlternativeLogicalPlanLocalFTS)
	tk.MustExec("set tidb_isolation_read_engines = 'tikv,tiflash'")
	testkit.SetTiFlashReplica(t, domain.GetDomain(tk.Session()), "test", "local_fts")
	// Both viable plans must compete, rather than selecting a fixed engine.
	costPoint := "github.com/pingcap/tidb/pkg/planner/forceLocalFTSAlternativeCost"
	t.Cleanup(func() { require.NoError(t, failpoint.Disable(costPoint)) })
	for _, winner := range []string{"native", "local", "tie"} {
		require.NoError(t, failpoint.Enable(costPoint, fmt.Sprintf("return(%q)", winner)))
		plan := fmt.Sprint(tk.MustQuery("explain select id from local_fts where match(body) against('+cat' in boolean mode)").Rows())
		if winner == "local" {
			require.Contains(t, plan, "match_against(")
			require.NotContains(t, plan, "search func:")
		} else {
			require.Contains(t, plan, "search func:fts_match_word")
		}
		require.False(t, tk.Session().GetSessionVars().StmtCtx.AlternativeLogicalPlanLocalFTS)
	}
	require.NoError(t, failpoint.Disable(costPoint))
	// Switching alternative planning must not reuse a direct-local cache entry.
	altKey, _, cacheable, _, err := core.NewPlanCacheKey(tk.Session(), prepared.(*core.PlanCacheStmt))
	require.NoError(t, err)
	require.True(t, cacheable)
	tk.Session().GetSessionVars().EnableAlternativeLogicalPlans = false
	directKey, _, cacheable, _, err := core.NewPlanCacheKey(tk.Session(), prepared.(*core.PlanCacheStmt))
	require.NoError(t, err)
	require.True(t, cacheable)
	require.NotEqual(t, altKey, directKey)
	tk.MustExec("set tidb_opt_enable_alternative_logical_plans = on")

	indexInfo := external.GetTableByName(t, tk, "test", "local_fts").Meta().FindIndexByName("ft").FullTextInfo
	config := indexInfo.ParserConfig
	indexInfo.ParserConfig = nil
	legacyPlan := fmt.Sprint(tk.MustQuery("explain select id from local_fts where match(body) against('+cat' in boolean mode)").Rows())
	indexInfo.ParserConfig = config
	require.Contains(t, legacyPlan, "index:ft(body)")
	require.Contains(t, legacyPlan, "search func:fts_match_word")
	tk.MustExec("set tidb_enable_local_match_against = off")
	nativePlan := fmt.Sprint(tk.MustQuery("explain select id from local_fts where match(body) against('+cat' in boolean mode)").Rows())
	require.Contains(t, nativePlan, "index:ft(body)")
	require.Contains(t, nativePlan, "search func:fts_match_word")
}

// TestLocalMatchSemantics ports the local MATCH regression cases from
// 282e2d3698, using TiCI FULLTEXT DDL and the index-backed local execution path.
func TestLocalMatchSemantics(t *testing.T) {
	for _, name := range []string{"MockCreateTiCIIndexSuccess", "MockFinishIndexUpload", "MockCheckAddIndexProgress"} {
		path := "github.com/pingcap/tidb/pkg/tici/" + name
		require.NoError(t, failpoint.Enable(path, `return(true)`))
		t.Cleanup(func() { require.NoError(t, failpoint.Disable(path)) })
	}
	store := testkit.CreateMockStoreWithSchemaLease(t, time.Second, mockstore.WithMockTiFlash(2))
	defer ingesttestutil.InjectMockBackendCtx(t, store)()
	tk := testkit.NewTestKit(t, store)
	tiflash := infosync.NewMockTiFlash()
	infosync.SetMockTiFlash(tiflash)
	defer func() {
		tiflash.Lock()
		tiflash.StatusServer.Close()
		tiflash.Unlock()
	}()
	tk.MustExec("use test")
	tk.MustQuery("select @@global.innodb_ft_min_token_size, @@global.innodb_ft_max_token_size").Check(testkit.Rows("3 84"))
	tk.MustExec("set innodb_ft_enable_stopword = on")
	tk.MustExec(`create table articles (
		id int primary key, title varchar(200), body text,
		fulltext index ft_title(title), fulltext index ft_title_body(title, body))`)
	tk.MustExec(`insert into articles values
		(1, 'MySQL Tutorial', 'This tutorial provides a basic MySQL tutorial'),
		(2, 'How To Use MySQL Well', 'After you went through a MySQL tutorial'),
		(3, 'Optimizing MySQL', 'In this tutorial we will show how to optimize MySQL'),
		(4, 'MySQL vs. PostgreSQL', 'This article compares MySQL and PostgreSQL'),
		(5, 'MySQL Security', 'How to secure your MySQL database')`)
	tk.MustExec("set tidb_enable_local_match_against = on")
	tk.MustExec("set tidb_opt_enable_alternative_logical_plans = off")
	for _, tt := range []struct {
		name      string
		predicate string
		rows      []string
	}{
		{"required_prohibited", `match(title) against('+MySQL -tutorial' in boolean mode)`, []string{"2 How To Use MySQL Well", "3 Optimizing MySQL", "4 MySQL vs. PostgreSQL", "5 MySQL Security"}},
		{"word_boundary", `match(title) against('+Optimiz' in boolean mode)`, nil},
		{"whole_word", `match(title) against('+Optimizing' in boolean mode)`, []string{"3 Optimizing MySQL"}},
		{"prefix", `match(title) against('Optim*' in boolean mode)`, []string{"3 Optimizing MySQL"}},
		{"multi_column_phrase", `match(title, body) against('"MySQL tutorial"' in boolean mode)`, []string{"1 MySQL Tutorial", "2 How To Use MySQL Well"}},
		{"phrase_order", `match(title, body) against('"tutorial MySQL"' in boolean mode)`, nil},
		{"short_token", `match(title) against('+vs' in boolean mode)`, nil},
		{"null_search", `match(title) against(NULL in boolean mode)`, nil},
		{"negated_null_search", `not match(title) against(NULL in boolean mode)`, nil},
	} {
		t.Run(tt.name, func(t *testing.T) {
			tk.MustQuery("select id, title from articles where " + tt.predicate + " order by id").Check(testkit.Rows(tt.rows...))
		})
	}
	tk.MustExec("insert into articles values (6, 'Indexing Basics', NULL), (7, 'MySQL x tutorial', NULL), (8, 'MySQL', 'tutorial')")
	tk.MustQuery(`select id, title from articles where match(title, body) against('+Indexing -PostgreSQL' in boolean mode)`).Check(testkit.Rows("6 Indexing Basics"))
	// Filtering a short token must not close phrase gaps, and phrases cannot
	// bridge the boundary between two matched columns.
	tk.MustQuery(`select id from articles where id in (7, 8) and match(title, body) against('"MySQL tutorial"' in boolean mode)`).Check(testkit.Rows())
	tk.MustExec("delete from articles where id >= 6")
	require.ErrorContains(t, tk.ExecToErr(`select id, match(title) against('+MySQL' in boolean mode) as score from articles`), "cannot be used in SELECT")
	require.ErrorContains(t, tk.ExecToErr(`select id, title from articles order by match(title) against('+MySQL' in boolean mode) desc`), "ORDER BY")
	tk.MustExec("set tidb_opt_enable_alternative_logical_plans = on")
	tk.MustExec("set tidb_isolation_read_engines = 'tikv'")
	tk.MustQuery(`select id from articles where match(title) against('+MySQL -tutorial' in boolean mode) order by id`).Check(testkit.Rows("2", "3", "4", "5"))
	// Regression from 81a1fe7fb9 / TestIssue70706: aggregate-column
	// substitution must preserve the local MATCH signature and analyzer state.
	for _, alternative := range []string{"off", "on"} {
		t.Run("group_by_having_"+alternative, func(t *testing.T) {
			tk.MustExec("set tidb_opt_enable_alternative_logical_plans = " + alternative)
			for _, clause := range []string{"where", "having", "group by id, title having"} {
				sql := "select id, title from articles " + clause + " match(title) against('+PostgreSQL' in boolean mode)"
				tk.MustQuery(sql).Check(testkit.Rows("4 MySQL vs. PostgreSQL"))
			}
			plan := fmt.Sprint(tk.MustQuery("explain format='brief' select id, title from articles group by id, title having match(title) against('+PostgreSQL' in boolean mode)").Rows())
			require.Contains(t, plan, "match_against(")
			require.NotContains(t, plan, "search func:")
		})
	}
	// On this branch, disabling local evaluation restores native TiCI routing.
	tk.MustExec("set tidb_isolation_read_engines = 'tikv,tiflash'")
	testkit.SetTiFlashReplica(t, domain.GetDomain(tk.Session()), "test", "articles")
	tk.MustExec("set tidb_enable_local_match_against = off")
	nativePlan := fmt.Sprint(tk.MustQuery(`explain select id from articles where match(title) against('+MySQL -tutorial' in boolean mode)`).Rows())
	require.Contains(t, nativePlan, "search func:")
	require.Contains(t, nativePlan, "fts_match_word")
	require.NotContains(t, nativePlan, "match_against(")
}
