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
	"strings"
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/config/deploymode"
	"github.com/pingcap/tidb/pkg/config/kerneltype"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/domain/infosync"
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

func setupStarterFTSTest(t *testing.T) {
	t.Helper()
	if !kerneltype.IsNextGen() {
		t.Skip("starter deploy mode is nextgen-only")
	}
	previous := deploymode.Get()
	require.NoError(t, deploymode.Set(deploymode.Starter))
	t.Cleanup(func() { require.NoError(t, deploymode.Set(previous)) })
}
