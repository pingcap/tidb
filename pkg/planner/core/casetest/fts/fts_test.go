// Copyright 2024 PingCAP, Inc.
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

package fts

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/config/deploymode"
	"github.com/pingcap/tidb/pkg/config/kerneltype"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/domain/infosync"
	"github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	"github.com/pingcap/tidb/pkg/planner/core/resolve"
	"github.com/pingcap/tidb/pkg/planner/core/rule"
	"github.com/pingcap/tidb/pkg/store/mockstore"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testdata"
	"github.com/stretchr/testify/require"
)

func runFTSExplainCases(t *testing.T, tk *testkit.TestKit, caseName string) {
	var input []string
	var output []struct {
		SQL  string
		Plan []string
		Warn []string
	}
	integrationSuiteData := GetFTSIndexSuiteData()
	integrationSuiteData.LoadTestCasesByName(caseName, t, &input, &output)
	for i, sql := range input {
		testdata.OnRecord(func() {
			output[i].SQL = sql
			output[i].Plan = testdata.ConvertRowsToStrings(tk.MustQuery(sql).Rows())
			output[i].Warn = testdata.ConvertSQLWarnToStrings(tk.Session().GetSessionVars().StmtCtx.GetWarnings())
		})
		tk.MustQuery(sql).Check(testkit.Rows(output[i].Plan...))
		require.Equal(t, output[i].Warn, testdata.ConvertSQLWarnToStrings(tk.Session().GetSessionVars().StmtCtx.GetWarnings()))
	}
}

func TestTiFlashFTSIndexWhere(t *testing.T) {
	setupStarterFTSTest(t)
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/ddl/MockCheckColumnarIndexProcess", `return(1)`))
	defer func() {
		err := failpoint.Disable("github.com/pingcap/tidb/pkg/ddl/MockCheckColumnarIndexProcess")
		require.NoError(t, err)
	}()

	store := testkit.CreateMockStoreWithSchemaLease(t, 1*time.Second, mockstore.WithMockTiFlash(2))

	tk := testkit.NewTestKit(t, store)

	tiflash := infosync.NewMockTiFlash()
	infosync.SetMockTiFlash(tiflash)
	defer func() {
		tiflash.Lock()
		tiflash.StatusServer.Close()
		tiflash.Unlock()
	}()

	tk.MustExec("use test")
	tk.MustExec(`create table t1(
		id INT PRIMARY KEY, title TEXT, body TEXT,
		FULLTEXT KEY (title),
		FULLTEXT KEY (body)
	)`)
	tk.MustExec("insert into t1 values (1, 'this is title', 'this is body')")
	dom := domain.GetDomain(tk.Session())
	testkit.SetTiFlashReplica(t, dom, "test", "t1")
	tk.MustExec(`create table t_multi(
		id int primary key, a text, b text collate utf8mb4_bin, c text, d text,
		fulltext key ft_multi(
			a with (standard),
			b with (ngram(min_gram=3)),
			c with (path_hierarchy(delimiter='/')),
			d with (exact)
		)
	)`)
	testkit.SetTiFlashReplica(t, dom, "test", "t_multi")
	tk.MustExec("create table t2(id int primary key, title text, fulltext key (title))")
	testkit.SetTiFlashReplica(t, dom, "test", "t2")
	tk.MustExec("create table t_common(a int, b int, title text, primary key(a,b) clustered, fulltext key (title))")
	tk.MustExec("create table t_rowid(title text, fulltext key (title))")
	testkit.SetTiFlashReplica(t, dom, "test", "t_common")
	testkit.SetTiFlashReplica(t, dom, "test", "t_rowid")

	plan := strings.Join(testdata.ConvertRowsToStrings(tk.MustQuery(
		"explain format='brief' select id from t_multi where fts_match_word('foo',a) and b like 'abcd%wxyz' order by fts_match_word('foo',a) desc limit 10",
	).Rows()), "\n")
	require.Contains(t, plan, "Selection")
	require.Contains(t, plan, "abcd")
	require.NotContains(t, plan, "ftsIndex:top10", "lossy inverted access needs a table recheck")
	runFTSExplainCases(t, tk, "TestTiFlashFTSIndexWhere")

	tk.MustExec(`prepare fts_exact_parameter from "explain format='brief'
		select * from t_multi where fts_match_word('foo', a) and d = ?"`)
	tk.MustExec("set @fts_exact_value = 'first'")
	preparedPlan := strings.Join(testdata.ConvertRowsToStrings(
		tk.MustQuery("execute fts_exact_parameter using @fts_exact_value").Rows(),
	), "\n")
	require.Contains(t, preparedPlan, "first IN test.t_multi.d")
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("0"))
	tk.MustExec("set @fts_exact_value = 'second'")
	preparedPlan = strings.Join(testdata.ConvertRowsToStrings(
		tk.MustQuery("execute fts_exact_parameter using @fts_exact_value").Rows(),
	), "\n")
	require.Contains(t, preparedPlan, "second IN test.t_multi.d")
	require.NotContains(t, preparedPlan, "first IN test.t_multi.d")
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("0"))
	tk.MustExec("deallocate prepare fts_exact_parameter")

	tk.MustExec(`prepare fts_score_explain_parameter from "explain format='brief'
		select id, d, fts_match_word('foo', a) as score from t_multi
		where fts_match_word('foo', a) and d = ?
		order by score desc limit 10"`)
	for _, value := range []string{"first", "second"} {
		tk.MustExec("set @fts_score_value = '" + value + "'")
		preparedPlan = strings.Join(testdata.ConvertRowsToStrings(
			tk.MustQuery("execute fts_score_explain_parameter using @fts_score_value").Rows(),
		), "\n")
		require.Contains(t, preparedPlan, "ftsIndex:top10 ")
		require.Contains(t, preparedPlan, value+" IN test.t_multi.d")
		tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("0"))
	}
	tk.MustExec("deallocate prepare fts_score_explain_parameter")

	tk.MustExec(`prepare fts_path_parameter from "explain format='brief'
		select * from t_multi where fts_match_word('foo', a) and c like ?"`)
	tk.MustExec("set @fts_path_value = '/first/path/%'")
	preparedPlan = strings.Join(testdata.ConvertRowsToStrings(
		tk.MustQuery("execute fts_path_parameter using @fts_path_value").Rows(),
	), "\n")
	require.Contains(t, preparedPlan, "/first/path/ IN test.t_multi.c")
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("0"))
	tk.MustExec("set @fts_path_value = '/second/path/%'")
	preparedPlan = strings.Join(testdata.ConvertRowsToStrings(
		tk.MustQuery("execute fts_path_parameter using @fts_path_value").Rows(),
	), "\n")
	require.Contains(t, preparedPlan, "/second/path/ IN test.t_multi.c")
	require.NotContains(t, preparedPlan, "/first/path/ IN test.t_multi.c")
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("0"))
	tk.MustExec("deallocate prepare fts_path_parameter")

	for _, testCase := range []struct {
		sql         string
		errContains string
	}{
		{"select * from t1 where fts_match_word('hello', title) or id=1", "does not cover column"},
		{"select * from t1 where not fts_match_word('hello', title)", "scored pure-negative FTS match expression is not supported"},
		{"select * from t1 where fts_match_word('hello', title)=1", "matching current TiFlash full-text index path"},
		{"select * from t1 where fts_match_word('hello', title) and fts_match_word('world', title)", "must be used alone"},
		{"select count(*) from t1 group by fts_match_word('hello', title)", "GROUP BY"},
		{"select sum(fts_match_word('hello', title)) from t1", "aggregate function arguments"},
		{"select sum(fts_match_word('hello', title)) over () from t1", "requires a matching 'FTS_MATCH_WORD()' in WHERE"},
	} {
		tk.MustContainErrMsg("explain "+testCase.sql, testCase.errContains)
	}

	tk.MustContainErrMsg("explain select * from t1 force index(primary) where fts_match_word('hello', title)", "matching current TiFlash full-text index path")
	tk.MustContainErrMsg("explain select * from t1 ignore index(title) where fts_match_word('hello', title)", "matching current TiFlash full-text index path")
	tk.MustContainErrMsg("explain select /*+ read_from_storage(tikv[t1]) */ * from t1 where fts_match_word('hello', title)", "TiKV-only storage preference")

	tk.MustExec("set @@tidb_isolation_read_engines='tikv'")
	tk.MustContainErrMsg("explain select * from t1 where fts_match_word('hello', title)", "requires TiFlash in tidb_isolation_read_engines")
	tk.MustExec("set @@tidb_isolation_read_engines='tikv,tiflash'")
	tk.MustExec("create table t_no_replica(id int primary key, title text, fulltext key (title))")
	tk.MustContainErrMsg("explain select * from t_no_replica where fts_match_word('hello', title)", "requires an available TiFlash replica")

	tk.MustExec("set @@tidb_enable_prepared_plan_cache=1")
	tk.MustExec("prepare fts_stmt from \"select * from t2 where fts_match_word('hello', title)\"")
	tk.MustQuery("execute fts_stmt")
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("0"))
	tk.MustQuery("execute fts_stmt")
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("0"))
	tk.MustExec("deallocate prepare fts_stmt")

	tk.MustExec("begin")
	tk.MustExec("insert into t1 values (2, 'dirty title', 'dirty body')")
	tk.MustContainErrMsg("explain select * from t1 where fts_match_word('hello', title)", "uncommitted changes")
	tk.MustExec("rollback")
}

func TestTiFlashFTSPathUsesPrimaryIndexForCommonHandleRange(t *testing.T) {
	setupStarterFTSTest(t)
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/ddl/MockCheckColumnarIndexProcess", `return(1)`))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/ddl/MockCheckColumnarIndexProcess"))
	}()

	store := testkit.CreateMockStoreWithSchemaLease(t, 1*time.Second, mockstore.WithMockTiFlash(2))
	tk := testkit.NewTestKit(t, store)
	tiflash := infosync.NewMockTiFlash()
	infosync.SetMockTiFlash(tiflash)
	defer func() {
		tiflash.Lock()
		tiflash.StatusServer.Close()
		tiflash.Unlock()
	}()

	tk.MustExec("use test")
	tk.MustExec(`create table file_nodes(
		fs_id bigint not null,
		node_id bigint not null,
		path text not null,
		primary key(fs_id, node_id) clustered,
		fulltext key idx_path_fts(
			fs_id with (exact),
			path with (ngram(min_gram=2, max_gram=4, granularity='char'), exact)
		)
	) default charset=utf8mb4 collate=utf8mb4_bin`)
	testkit.SetTiFlashReplica(t, domain.GetDomain(tk.Session()), "test", "file_nodes")

	runFTSExplainCases(t, tk, "TestTiFlashFTSPathUsesPrimaryIndexForCommonHandleRange")
}

func TestTiFlashFTSLikePrefilterExplain(t *testing.T) {
	setupStarterFTSTest(t)
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/ddl/MockCheckColumnarIndexProcess", `return(1)`))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/ddl/MockCheckColumnarIndexProcess"))
	}()

	store := testkit.CreateMockStoreWithSchemaLease(t, 1*time.Second, mockstore.WithMockTiFlash(2))
	tk := testkit.NewTestKit(t, store)
	tiflash := infosync.NewMockTiFlash()
	infosync.SetMockTiFlash(tiflash)
	defer func() {
		tiflash.Lock()
		tiflash.StatusServer.Close()
		tiflash.Unlock()
	}()

	tk.MustExec("use test")
	tk.MustExec(`create table t(
		id int primary key,
		anchor text,
		char_ngram text collate utf8mb4_bin,
		word_ci text,
		path text,
		fulltext key ft(
			anchor with (standard),
			char_ngram with (ngram(min_gram=3, granularity='char')),
			word_ci with (ngram(min_gram=3, granularity='word', lower_case=true)),
			path with (path_hierarchy(delimiter='/'))
		)
	)`)
	testkit.SetTiFlashReplica(t, domain.GetDomain(tk.Session()), "test", "t")

	runFTSExplainCases(t, tk, "TestTiFlashFTSLikePrefilterExplain")
}

func TestTiFlashFTSOrderByWithMultipleCoveredConditions(t *testing.T) {
	setupStarterFTSTest(t)
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/ddl/MockCheckColumnarIndexProcess", `return(1)`))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/ddl/MockCheckColumnarIndexProcess"))
	}()

	store := testkit.CreateMockStoreWithSchemaLease(t, 1*time.Second, mockstore.WithMockTiFlash(2))
	tk := testkit.NewTestKit(t, store)
	tiflash := infosync.NewMockTiFlash()
	infosync.SetMockTiFlash(tiflash)
	defer func() {
		tiflash.Lock()
		tiflash.StatusServer.Close()
		tiflash.Unlock()
	}()

	tk.MustExec("use test")
	tk.MustExec(`create table t(
		id int primary key,
		anchor text,
		filter_a text,
		filter_b text,
		filter_c text,
		fulltext key ft(
			anchor with (standard),
			filter_a with (exact),
			filter_b with (exact),
			filter_c with (exact)
		)
	)`)
	testkit.SetTiFlashReplica(t, domain.GetDomain(tk.Session()), "test", "t")

	runFTSExplainCases(t, tk, "TestTiFlashFTSOrderByWithMultipleCoveredConditions")
}

func TestTiFlashFTSIndexTopN(t *testing.T) {
	setupStarterFTSTest(t)
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/ddl/MockCheckColumnarIndexProcess", `return(1)`))
	defer func() {
		err := failpoint.Disable("github.com/pingcap/tidb/pkg/ddl/MockCheckColumnarIndexProcess")
		require.NoError(t, err)
	}()

	store := testkit.CreateMockStoreWithSchemaLease(t, 1*time.Second, mockstore.WithMockTiFlash(2))

	tk := testkit.NewTestKit(t, store)

	tiflash := infosync.NewMockTiFlash()
	infosync.SetMockTiFlash(tiflash)
	defer func() {
		tiflash.Lock()
		tiflash.StatusServer.Close()
		tiflash.Unlock()
	}()

	tk.MustExec("use test")
	tk.MustExec("create table t1(id INT, title TEXT, body TEXT, FULLTEXT KEY (title))")
	tk.MustExec("insert into t1 values (1, 'this is title', 'this is body')")
	dom := domain.GetDomain(tk.Session())
	testkit.SetTiFlashReplica(t, dom, "test", "t1")

	runFTSExplainCases(t, tk, "TestTiFlashFTSIndexTopN")
}

func TestTiFlashFTSIndexJoin(t *testing.T) {
	setupStarterFTSTest(t)
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/ddl/MockCheckColumnarIndexProcess", `return(1)`))
	defer func() {
		err := failpoint.Disable("github.com/pingcap/tidb/pkg/ddl/MockCheckColumnarIndexProcess")
		require.NoError(t, err)
	}()

	store := testkit.CreateMockStoreWithSchemaLease(t, 1*time.Second, mockstore.WithMockTiFlash(2))

	tk := testkit.NewTestKit(t, store)

	tiflash := infosync.NewMockTiFlash()
	infosync.SetMockTiFlash(tiflash)
	defer func() {
		tiflash.Lock()
		tiflash.StatusServer.Close()
		tiflash.Unlock()
	}()

	tk.MustExec("use test")
	tk.MustExec("create table file_chunks(chunk_id INT, file_id INT, body TEXT, FULLTEXT KEY (body))")
	tk.MustExec("create table files(file_id INT, path TEXT)")
	dom := domain.GetDomain(tk.Session())
	testkit.SetTiFlashReplica(t, dom, "test", "file_chunks")

	runFTSExplainCases(t, tk, "TestTiFlashFTSIndexJoin")
}

// TestTiFlashFTSPredicateContracts checks that index coverage changes execution,
// without hiding logical predicates from range, statistics, or FD reasoning.
func TestTiFlashFTSPredicateContracts(t *testing.T) {
	setupStarterFTSTest(t)
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/ddl/MockCheckColumnarIndexProcess", `return(1)`))
	t.Cleanup(func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/ddl/MockCheckColumnarIndexProcess"))
	})
	store := testkit.CreateMockStoreWithSchemaLease(t, time.Second, mockstore.WithMockTiFlash(2))
	tk := testkit.NewTestKit(t, store)
	tiflash := infosync.NewMockTiFlash()
	infosync.SetMockTiFlash(tiflash)
	t.Cleanup(func() {
		tiflash.Lock()
		tiflash.StatusServer.Close()
		tiflash.Unlock()
	})
	tk.MustExec("use test")
	tk.MustExec(`create table t_int(id bigint primary key clustered, body text, tag bigint,
		fulltext key ft_text(body with (standard)),
		fulltext key ft_cover(body with (standard), id with (exact), tag with (exact)))`)
	tk.MustExec(`create table t_common(k1 bigint not null, k2 bigint not null, body text, tag bigint,
		primary key(k1,k2) clustered,
		fulltext key ft_text(body with (standard)),
		fulltext key ft_cover(body with (standard), k1 with (exact), k2 with (exact), tag with (exact)))`)
	dom := domain.GetDomain(tk.Session())
	testkit.SetTiFlashReplica(t, dom, "test", "t_int")
	testkit.SetTiFlashReplica(t, dom, "test", "t_common")
	for _, tc := range []struct{ table, predicate, expectedRange string }{
		{"t_int", "id=42", "range:[42,42]"},
		{"t_common", "k1=42 and k2=7", "range:[42 7,42 7]"},
	} {
		t.Run(tc.table+" range", func(t *testing.T) {
			for _, index := range []string{"ft_text", "ft_cover"} {
				plan := strings.Join(testdata.ConvertRowsToStrings(tk.MustQuery(
					"explain format='brief' select body from "+tc.table+" force index("+index+") where fts_match_word('foo',body) and "+tc.predicate,
				).Rows()), "\n")
				require.Contains(t, plan, "TableRangeScan", index)
				require.Contains(t, plan, tc.expectedRange, index)
			}
		})
		t.Run(tc.table+" estimates", func(t *testing.T) {
			var expected any
			for _, index := range []string{"ft_text", "ft_cover"} {
				rows := tk.MustQuery("explain format='brief' select body from " + tc.table + " force index(" + index + ") where fts_match_word('foo',body) and tag=7").Rows()
				if expected == nil {
					expected = rows[0][1]
				} else {
					require.Equal(t, expected, rows[0][1])
					require.NotContains(t, strings.Join(testdata.ConvertRowsToStrings(rows), "\n"), "Selection")
				}
			}
		})
	}

	t.Run("covered predicates still request column statistics", func(t *testing.T) {
		ctx := context.Background()
		stmts, err := tk.Session().Parse(ctx, "select body from t_int force index(ft_cover) where fts_match_word('foo',body) and tag=7")
		require.NoError(t, err)
		node := resolve.NewNodeW(stmts[0])
		require.NoError(t, core.Preprocess(ctx, tk.Session(), node,
			core.WithPreprocessorReturn(&core.PreprocessorReturn{InfoSchema: dom.InfoSchema()})))
		plan, err := core.BuildLogicalPlanForTest(ctx, tk.Session(), node, dom.InfoSchema())
		require.NoError(t, err)
		logical, _, err := (&core.PPDSolver{}).Optimize(ctx, plan.(base.LogicalPlan))
		require.NoError(t, err)
		columns, _, _, _ := rule.CollectColumnStatsUsage(logical)
		// Both body and tag remain predicate columns even though FTS evaluates them.
		require.Len(t, columns, 2)
		for column, fullLoad := range columns {
			require.Contains(t, []int64{2, 3}, column.ID)
			require.True(t, fullLoad)
		}
		_, _, err = core.RecursiveDeriveStats4Test(logical)
		require.NoError(t, err)
		var checkAccessCounts func(base.LogicalPlan)
		checked := false
		checkAccessCounts = func(plan base.LogicalPlan) {
			if ds, ok := plan.(*logicalop.DataSource); ok {
				require.Len(t, ds.PossibleAccessPaths, 1)
				path := ds.PossibleAccessPaths[0]
				require.Empty(t, path.IndexFilters)
				require.Equal(t, path.CountAfterAccess, path.CountAfterIndex, "a fused inverted query is one access stage")
				require.Equal(t, ds.StatsInfo().RowCount, path.CountAfterAccess, "all predicates are covered by access")
				checked = true
			}
			for _, child := range plan.Children() {
				checkAccessCounts(child)
			}
		}
		checkAccessCounts(logical)
		require.True(t, checked)

	})
	tk.MustExec("create table t_outer(id bigint primary key)")
	t.Run("non-null FD permits anti semi join", func(t *testing.T) {
		for _, index := range []string{"ft_text", "ft_cover"} {
			plan := strings.Join(testdata.ConvertRowsToStrings(tk.MustQuery(
				"explain format='brief' select l.id from t_outer l left join (select id,tag from t_int force index("+index+") where fts_match_word('foo',body) and tag=7) r on l.id=r.id where r.tag is null",
			).Rows()), "\n")
			require.Contains(t, plan, "anti semi join", index)
			require.NotContains(t, plan, "left outer join", index)
		}
	})
	t.Run("correlated alternatives retain inverted access", func(t *testing.T) {
		for _, mode := range []string{"OFF", "ON"} {
			tk.MustExec("set tidb_opt_enable_alternative_logical_plans=" + mode)
			plan := strings.Join(testdata.ConvertRowsToStrings(tk.MustQuery(
				"explain format='brief' select id from t_outer where id<10 and id in (select tag from t_int force index(ft_cover) where fts_match_word('foo',body) and id=42)",
			).Rows()), "\n")
			require.Contains(t, plan, "ftsIndex:")
			require.Contains(t, plan, "42 IN test.t_int.id")
		}
		tk.MustExec("set tidb_opt_enable_alternative_logical_plans=OFF")
	})
	t.Run("TopK waits for handle range classification", func(t *testing.T) {
		for _, tc := range []struct {
			table, index, predicate string
			bounded                 bool
		}{
			{"t_int", "ft_text", "id=42", false},
			{"t_int", "ft_cover", "id=42", true},
			{"t_int", "ft_cover", "id>42", false},
			{"t_common", "ft_text", "k1=42 and k2=7", false},
			{"t_common", "ft_cover", "k1=42 and k2=7", true},
		} {
			plan := strings.Join(testdata.ConvertRowsToStrings(tk.MustQuery(
				"explain format='brief' select body from "+tc.table+" force index("+tc.index+") where fts_match_word('foo',body) and "+tc.predicate+" order by fts_match_word('foo',body) desc limit 10",
			).Rows()), "\n")
			require.Contains(t, plan, "TableRangeScan")
			require.NotContains(t, plan, "Selection")
			require.Equal(t, tc.bounded, strings.Contains(plan, "ftsIndex:top10"), plan)
		}
	})
	t.Run("TopK depends on residual execution", func(t *testing.T) {
		for _, predicate := range []string{"tag=7", "tag=7 and tag>1", "tag>7"} {
			plan := strings.Join(testdata.ConvertRowsToStrings(tk.MustQuery(
				"explain format='brief' select id,fts_match_word('foo',body) score from t_int force index(ft_cover) where fts_match_word('foo',body) and "+predicate+" order by score desc limit 10",
			).Rows()), "\n")
			if strings.HasPrefix(predicate, "tag=7") {
				require.Contains(t, plan, "ftsIndex:top10")
				require.NotContains(t, plan, "Selection")
			} else {
				require.NotContains(t, plan, "ftsIndex:top10")
				require.Contains(t, plan, "Selection")
			}
		}
	})
}

func setupStarterFTSTest(t *testing.T) {
	t.Helper()
	if !kerneltype.IsNextGen() {
		t.Skip("starter deploy mode is nextgen-only")
	}
	previous := deploymode.Get()
	require.NoError(t, deploymode.Set(deploymode.Starter))
	t.Cleanup(func() { require.NoError(t, deploymode.Set(previous)) })
}
