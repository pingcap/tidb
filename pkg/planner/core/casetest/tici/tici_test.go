// Copyright 2025 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package tici

import (
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	ingesttestutil "github.com/pingcap/tidb/pkg/ddl/ingest/testutil"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/domain/infosync"
	"github.com/pingcap/tidb/pkg/store/mockstore"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testdata"
	"github.com/stretchr/testify/require"
)

func TestTiCISearchExplain(t *testing.T) {
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/tici/MockCreateTiCIIndexSuccess", `return(true)`))
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/tici/MockFinishIndexUpload", `return(true)`))
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/tici/MockCheckAddIndexProgress", `return(true)`))
	defer func() {
		err := failpoint.Disable("github.com/pingcap/tidb/pkg/tici/MockCreateTiCIIndexSuccess")
		require.NoError(t, err)
		err = failpoint.Disable("github.com/pingcap/tidb/pkg/tici/MockFinishIndexUpload")
		require.NoError(t, err)
		err = failpoint.Disable("github.com/pingcap/tidb/pkg/tici/MockCheckAddIndexProgress")
		require.NoError(t, err)
	}()

	store := testkit.CreateMockStoreWithSchemaLease(t, 1*time.Second, mockstore.WithMockTiFlash(2))
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
	tk.MustExec(`create table t1(
		id INT PRIMARY KEY, title TEXT, body TEXT, field1 int,
		FULLTEXT INDEX idx_title (title),
		index idx_field1 (field1)
	)`)
	tk.MustExec(`create table t6(
		id INT UNSIGNED PRIMARY KEY, title TEXT,
		FULLTEXT INDEX idx_title (title) WITH PARSER ngram
	)`)
	tk.MustExec(`create table t7(
		id INT PRIMARY KEY, title TEXT, body TEXT,
		FULLTEXT INDEX idx_title_body (title, body)
	)`)
	dom := domain.GetDomain(tk.Session())
	testkit.SetTiFlashReplica(t, dom, "test", "t1")
	testkit.SetTiFlashReplica(t, dom, "test", "t6")
	testkit.SetTiFlashReplica(t, dom, "test", "t7")
	tk.MustExec("create table t2(a int, col text)")

	tk.MustExec(`create table t3(
		a int,
		b int,
		title text,
		primary key(a, b),
		fulltext key(title)
	)`)

	tk.MustExec("create table t4(i bigint, ts timestamp(5), d datetime(3), t text, primary key(i))")
	tk.MustExec(`create hybrid index idx1 on t4(i, ts, d, t) parameter '{
		"inverted": {
			"columns": ["i", "ts", "d", "t"]
		},
		"sort": {
			"columns": ["i", "ts", "d"],
			"order": ["asc", "desc", "asc"]
		},
		"sharding_key": {
			"columns": ["i", "ts"]
		}
	}'`)
	tk.MustExec("create table t5(i bigint, ts timestamp(5), d datetime(3), t text, primary key(i))")
	tk.MustExec(`create hybrid index idx1 on t5(ts, d, t) parameter '{
		"inverted": {
			"columns": ["ts", "d", "t"]
		},
		"sort": {
			"columns": ["ts", "d"],
			"order": ["asc", "desc"]
		},
		"sharding_key": {
			"columns": ["ts"]
		}
	}'`)

	var input []string
	var output []struct {
		SQL  string
		Plan []string
		Warn []string
	}
	integrationSuiteData := GetFTSIndexSuiteData()
	integrationSuiteData.LoadTestCases(t, &input, &output)
	for i, tt := range input {
		testdata.OnRecord(func() {
			output[i].SQL = tt
			output[i].Plan = testdata.ConvertRowsToStrings(tk.MustQuery(tt).Rows())
			output[i].Warn = testdata.ConvertSQLWarnToStrings(tk.Session().GetSessionVars().StmtCtx.GetWarnings())
		})
		tk.MustQuery(tt).Check(testkit.Rows(output[i].Plan...))
		require.Equal(t, output[i].Warn, testdata.ConvertSQLWarnToStrings(tk.Session().GetSessionVars().StmtCtx.GetWarnings()))
	}
}

func TestTiCIWithIndexHintCases(t *testing.T) {
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/tici/MockCreateTiCIIndexSuccess", `return(true)`))
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/tici/MockFinishIndexUpload", `return(true)`))
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/tici/MockCheckAddIndexProgress", `return(true)`))
	defer func() {
		err := failpoint.Disable("github.com/pingcap/tidb/pkg/tici/MockCreateTiCIIndexSuccess")
		require.NoError(t, err)
		err = failpoint.Disable("github.com/pingcap/tidb/pkg/tici/MockFinishIndexUpload")
		require.NoError(t, err)
		err = failpoint.Disable("github.com/pingcap/tidb/pkg/tici/MockCheckAddIndexProgress")
		require.NoError(t, err)
	}()

	store := testkit.CreateMockStoreWithSchemaLease(t, 1*time.Second, mockstore.WithMockTiFlash(2))
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
	tk.MustExec(`create table t1(
		id INT PRIMARY KEY, title TEXT, body TEXT, field1 int,
		FULLTEXT INDEX idx_title (title),
		index idx_field1 (field1)
	)`)
	tk.MustExec("create table t2(i bigint, ts timestamp(5), d datetime(3), t text, primary key(i))")
	tk.MustExec(`create hybrid index idx1 on t2(i, ts, d, t) parameter '{
		"inverted": {
			"columns": ["i", "ts", "d", "t"]
		},
		"sort": {
			"columns": ["i", "ts", "d"],
			"order": ["asc", "desc", "asc"]
		},
		"sharding_key": {
			"columns": ["i", "ts"]
		}
	}'`)
	dom := domain.GetDomain(tk.Session())
	testkit.SetTiFlashReplica(t, dom, "test", "t1")

	var input []string
	var output []struct {
		SQL  string
		Plan []string
		Warn []string
	}
	integrationSuiteData := GetFTSIndexSuiteData()
	integrationSuiteData.LoadTestCases(t, &input, &output)
	for i, tt := range input {
		testdata.OnRecord(func() {
			output[i].SQL = tt
			output[i].Plan = testdata.ConvertRowsToStrings(tk.MustQuery(tt).Rows())
			output[i].Warn = testdata.ConvertSQLWarnToStrings(tk.Session().GetSessionVars().StmtCtx.GetWarnings())
		})
		tk.MustQuery(tt).Check(testkit.Rows(output[i].Plan...))
		require.Equal(t, output[i].Warn, testdata.ConvertSQLWarnToStrings(tk.Session().GetSessionVars().StmtCtx.GetWarnings()))
	}
}

func TestTiCIMatchAgainstValidation(t *testing.T) {
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/tici/MockCreateTiCIIndexSuccess", `return(true)`))
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/tici/MockFinishIndexUpload", `return(true)`))
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/tici/MockCheckAddIndexProgress", `return(true)`))
	defer func() {
		err := failpoint.Disable("github.com/pingcap/tidb/pkg/tici/MockCreateTiCIIndexSuccess")
		require.NoError(t, err)
		err = failpoint.Disable("github.com/pingcap/tidb/pkg/tici/MockFinishIndexUpload")
		require.NoError(t, err)
		err = failpoint.Disable("github.com/pingcap/tidb/pkg/tici/MockCheckAddIndexProgress")
		require.NoError(t, err)
	}()

	store := testkit.CreateMockStoreWithSchemaLease(t, 1*time.Second, mockstore.WithMockTiFlash(2))
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
	tk.MustExec(`create table t1(
		id INT PRIMARY KEY, title TEXT, body TEXT, field1 int,
		FULLTEXT INDEX idx_title (title),
		index idx_field1 (field1)
	)`)
	tk.MustExec(`create table t6(
		id INT PRIMARY KEY, title TEXT,
		FULLTEXT INDEX idx_title (title) WITH PARSER ngram
	)`)
	tk.MustExec(`create table t7(
		id INT PRIMARY KEY, title TEXT, body TEXT
	)`)
	tk.MustExec(`create hybrid index idx_title_body on t7(id, title, body) parameter '{
		"fulltext": [{"columns": ["title", "body"]}],
		"sharding_key": {"columns": ["id"]}
	}'`)
	dom := domain.GetDomain(tk.Session())
	testkit.SetTiFlashReplica(t, dom, "test", "t1")
	testkit.SetTiFlashReplica(t, dom, "test", "t6")
	testkit.SetTiFlashReplica(t, dom, "test", "t7")

	// Non-BOOLEAN MODE is rejected.
	tk.MustContainErrMsg(
		"explain format='brief' select * from t1 where match(title) against ('hello')",
		"Currently TiDB only supports BOOLEAN MODE in MATCH AGAINST",
	)
	tk.MustContainErrMsg(
		"explain format='brief' select * from t1 where match(title) against ('hello' IN NATURAL LANGUAGE MODE)",
		"Currently TiDB only supports BOOLEAN MODE in MATCH AGAINST",
	)
	tk.MustContainErrMsg(
		"explain format='brief' select * from t1 where match(title) against ('hello' WITH QUERY EXPANSION)",
		"Currently TiDB only supports BOOLEAN MODE in MATCH AGAINST",
	)

	// BOOLEAN MODE should allow multiple SHOULD terms. With a no-score TiCI query,
	// SHOULD terms are only kept as filters when there is no MUST term.
	tk.MustQuery(
		"explain format='brief' select * from t1 where match(title) against ('hello world' IN BOOLEAN MODE)",
	).CheckContain(`search func:or(istrue_with_null(fts_match_word("hello", test.t1.title)), istrue_with_null(fts_match_word("world", test.t1.title)))`)
	tk.MustQuery(
		"explain format='brief' select * from t1 where match(title) against ('+hello world' IN BOOLEAN MODE)",
	).CheckContain(`search func:fts_match_word("hello", test.t1.title)`)
	tk.MustQuery(
		"explain format='brief' select * from t1 where match(title) against ('+hello world' IN BOOLEAN MODE)",
	).CheckNotContain(`world`)

	// Parser should follow the fulltext index parser type.
	tk.MustContainErrMsg(
		"explain format='brief' select * from t6 where match(title) against ('>hello' IN BOOLEAN MODE)",
		"unsupported operator '>' in BOOLEAN MODE query",
	)
	tk.MustQuery(
		"explain format='brief' select * from t6 where match(title) against ('hello' IN BOOLEAN MODE)",
	).CheckContain(`search func:fts_match_phrase("hello", test.t6.title)`)

	// Hybrid fulltext components keep the original helper-function behavior:
	// multi-column helper functions and MATCH ... AGAINST are both rejected, and
	// multi-column search should use multiple single-column fts_match_xxx calls.
	tk.MustContainErrMsg(
		"explain format='brief' select * from t7 where match(title, body) against ('hello' IN BOOLEAN MODE)",
		"Multi-column MATCH AGAINST is not supported on hybrid fulltext indexes",
	)
	tk.MustContainErrMsg(
		"explain format='brief' select * from t7 where fts_match_word('hello', title, body)",
		"Multi-column fts_match_xxx is not supported on hybrid fulltext indexes",
	)
	tk.MustQuery(
		"explain format='brief' select * from t7 where fts_match_word('hello', title)",
	).CheckContain("idx_title_body")
	tk.MustQuery(
		"explain format='brief' select * from t7 where fts_match_word('hello', title) and fts_match_word('hello', body)",
	).CheckContain("idx_title_body")
}

func TestTiCISearchWithPrepare(t *testing.T) {
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/tici/MockCreateTiCIIndexSuccess", `return(true)`))
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/tici/MockFinishIndexUpload", `return(true)`))
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/tici/MockCheckAddIndexProgress", `return(true)`))
	defer func() {
		err := failpoint.Disable("github.com/pingcap/tidb/pkg/tici/MockCreateTiCIIndexSuccess")
		require.NoError(t, err)
		err = failpoint.Disable("github.com/pingcap/tidb/pkg/tici/MockFinishIndexUpload")
		require.NoError(t, err)
		err = failpoint.Disable("github.com/pingcap/tidb/pkg/tici/MockCheckAddIndexProgress")
		require.NoError(t, err)
	}()

	store := testkit.CreateMockStoreWithSchemaLease(t, 1*time.Second, mockstore.WithMockTiFlash(2))
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
	tk.MustExec(`create table t1(
		id INT PRIMARY KEY, title TEXT, body TEXT, field1 int,
		FULLTEXT INDEX idx_title (title),
		index idx_field1 (field1)
	)`)
	dom := domain.GetDomain(tk.Session())
	testkit.SetTiFlashReplica(t, dom, "test", "t1")

	tk.MustExec(`set tidb_enable_prepared_plan_cache=1`)
	tk.MustExec(`prepare stmt from 'explain select * from t1 where fts_match_word(?, title)'`)

	tk.MustExec(`set @a = 'hello'`)
	tk.MustQuery(`execute stmt using @a`).CheckContain(`fts_match_word("hello"`)

	tk.MustExec(`set @a = 'world'`)
	tk.MustQuery(`execute stmt using @a`).CheckContain(`fts_match_word("world"`)
}

func TestTiCIWithDirtyWrites(t *testing.T) {
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/tici/MockCreateTiCIIndexSuccess", `return(true)`))
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/tici/MockFinishIndexUpload", `return(true)`))
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/tici/MockCheckAddIndexProgress", `return(true)`))
	defer func() {
		err := failpoint.Disable("github.com/pingcap/tidb/pkg/tici/MockCreateTiCIIndexSuccess")
		require.NoError(t, err)
		err = failpoint.Disable("github.com/pingcap/tidb/pkg/tici/MockFinishIndexUpload")
		require.NoError(t, err)
		err = failpoint.Disable("github.com/pingcap/tidb/pkg/tici/MockCheckAddIndexProgress")
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
	tk.MustExec(`create table t1(i bigint, ts timestamp(5), d datetime(3), t text, primary key(i))`)
	tk.MustExec(`create hybrid index idx1 on t1(i, ts, d, t) parameter '{
		"inverted": {
			"columns": ["i", "ts", "d"]
		},
		"sort": {
			"columns": ["i", "ts", "d"],
			"order": ["asc", "desc", "asc"]
		},
		"sharding_key": {
			"columns": ["i", "ts"]
		}
	}'`)
	dom := domain.GetDomain(tk.Session())
	testkit.SetTiFlashReplica(t, dom, "test", "t1")

	// Without dirty write, TiCI index can be used.
	tk.MustQuery("explain format='brief' select * from t1 use index(idx1) where d between '2026-01-01' and '2026-01-03'").CheckContain("idx1")

	// With dirty write, TiCI index cannot be used.
	tk.MustExec("begin")
	tk.MustExec("insert into t1 values(1, '2026-01-01 10:10:10', '2026-01-01 10:10:10', 'text1')")
	tk.MustQuery("explain format='brief' select * from t1 use index(idx1) where d between '2026-01-01' and '2026-01-03'").CheckNotContain("idx1")
	tk.MustExec("rollback")

	tk.MustExec("create table t2(a int primary key, b int, c longtext)")
	tk.MustExec(`create fulltext index idx_c on t2(c)`)
	testkit.SetTiFlashReplica(t, dom, "test", "t2")

	// FTS predicates on another table should not block the local non-FTS TiCI path.
	tk.MustQuery("explain format='brief' select /*+ use_index(t1, idx1) */ * from t1, t2 where d between '2026-01-01' and '2026-01-03' and fts_match_word('apple', c)").CheckContain("idx1")
	tk.MustQuery("explain format='brief' select /*+ use_index(t1, idx1) */ * from t1, t2 where d between '2026-01-01' and '2026-01-03' and fts_match_word('apple', c)").CheckContain("idx_c")

	// Dirty writes on a table without local FTS predicates should not make the whole query fail.
	tk.MustExec("begin")
	tk.MustExec("insert into t1 values(1, '2026-01-01 10:10:10', '2026-01-01 10:10:10', 'text1')")
	tk.MustQuery("explain format='brief' select * from t1 join t2 on t1.i = t2.a where d between '2026-01-01' and '2026-01-03' and fts_match_word('apple', c)").CheckContain("idx_c")
	tk.MustQuery("explain format='brief' select * from t1 join t2 on t1.i = t2.a where d between '2026-01-01' and '2026-01-03' and fts_match_word('apple', c)").CheckNotContain("idx1")
	tk.MustExec("rollback")

	// With dirty write, TiCI index cannot be used.
	tk.MustExec("begin")
	tk.MustExec("insert into t2 values(1, 1, 'text1')")
	tk.MustExecToErr("explain select * from t2 where fts_match_word('apple', c)")
	tk.MustExec("rollback")
}

func TestTiCIWithWrongColumn(t *testing.T) {
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/tici/MockCreateTiCIIndexSuccess", `return(true)`))
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/tici/MockFinishIndexUpload", `return(true)`))
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/tici/MockCheckAddIndexProgress", `return(true)`))
	defer func() {
		err := failpoint.Disable("github.com/pingcap/tidb/pkg/tici/MockCreateTiCIIndexSuccess")
		require.NoError(t, err)
		err = failpoint.Disable("github.com/pingcap/tidb/pkg/tici/MockFinishIndexUpload")
		require.NoError(t, err)
		err = failpoint.Disable("github.com/pingcap/tidb/pkg/tici/MockCheckAddIndexProgress")
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
	tk.MustExec(`create table t(a int primary key, b text,fulltext index(b))`)
	dom := domain.GetDomain(tk.Session())
	testkit.SetTiFlashReplica(t, dom, "test", "t")

	tk.MustContainErrMsg("explain select * from t where match(a) against('text1' IN BOOLEAN MODE)", "Doesn't support match search on a non-string column without fulltext index")
	tk.MustContainErrMsg("explain select * from t where fts_match_word('text1', a)", "Doesn't support match search on a non-string column without fulltext index")
	tk.MustContainErrMsg("explain select * from t where fts_match_phrase('text1', a)", "Doesn't support match search on a non-string column without fulltext index")
}

func runTiCITest(t *testing.T, fn func(*testkit.TestKit)) {
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/tici/MockCreateTiCIIndexSuccess", `return(true)`))
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/tici/MockFinishIndexUpload", `return(true)`))
	defer func() {
		err := failpoint.Disable("github.com/pingcap/tidb/pkg/tici/MockCreateTiCIIndexSuccess")
		require.NoError(t, err)
		err = failpoint.Disable("github.com/pingcap/tidb/pkg/tici/MockFinishIndexUpload")
		require.NoError(t, err)
	}()

	store := testkit.CreateMockStoreWithSchemaLease(t, 1*time.Second, mockstore.WithMockTiFlash(2))
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
	fn(tk)
}

func TestTiCINonCoveringUsesDoubleRead(t *testing.T) {
	runTiCITest(t, func(tk *testkit.TestKit) {
		tk.MustExec(`create table fts1(
			id bigint primary key,
			page_num bigint,
			content text,
			fulltext index ft_content(content)
		)`)
		dom := domain.GetDomain(tk.Session())
		testkit.SetTiFlashReplica(t, dom, "test", "fts1")

		sql := `explain format='brief' select * from fts1 where match(content) against('"red"' in boolean mode)`
		tk.MustQuery(sql).CheckContain("IndexLookUp")
		tk.MustQuery(sql).CheckContain("IndexRangeScan")
		tk.MustQuery(sql).CheckContain("TableRowIDScan")
		tk.MustQuery(sql).CheckContain("cop[tici]")
		tk.MustQuery(sql).CheckContain("cop[tikv]")
		tk.MustQuery(sql).CheckNotContain("mpp[tiflash]")
	})
}

func TestTiCICoveringWithoutTiFlashReplicaNoMPP(t *testing.T) {
	runTiCITest(t, func(tk *testkit.TestKit) {
		tk.MustExec(`create table fts_no_replica(
			id bigint primary key,
			content text,
			fulltext index ft_content(content)
		)`)
		tk.MustExec("set tidb_allow_mpp=on")
		tk.MustExec("set tidb_enforce_mpp=off")

		sql := `explain format='brief' select id from fts_no_replica where match(content) against('"red"' in boolean mode)`
		tk.MustQuery(sql).CheckContain("mpp[tiflash]")
	})
}

func TestTiCIJoinNoMPPPlan(t *testing.T) {
	runTiCITest(t, func(tk *testkit.TestKit) {
		tk.MustExec(`create table fts1(
			id bigint primary key,
			page_num bigint,
			content text,
			fulltext index ft_content(content)
		)`)
		tk.MustExec(`create table fts2(
			id bigint primary key,
			page_num bigint,
			content text,
			fulltext index ft_content(content)
		)`)
		dom := domain.GetDomain(tk.Session())
		testkit.SetTiFlashReplica(t, dom, "test", "fts1")
		testkit.SetTiFlashReplica(t, dom, "test", "fts2")

		tk.MustExec("set tidb_allow_mpp=off")
		tk.MustExec("set tidb_enforce_mpp=off")

		sql := `explain format='brief' select f.id, t.id from fts1 f join fts2 t on f.page_num=t.page_num where match(f.content) against('"red"' in boolean mode)`
		tk.MustQuery(sql).CheckContain("HashJoin")
		tk.MustQuery(sql).CheckContain("IndexRangeScan")
		tk.MustQuery(sql).CheckContain("cop[tici]")
		tk.MustQuery(sql).CheckContain("cop[tikv]")
		tk.MustQuery(sql).CheckNotContain("mpp[tiflash]")
	})
}

func TestTiCIWithTiFlashReplicaNoMPPNoTiFlashCop(t *testing.T) {
	runTiCITest(t, func(tk *testkit.TestKit) {
		tk.MustExec(`create table docs_ng(
			id bigint primary key,
			content text,
			fulltext index ft(content)
		)`)
		dom := domain.GetDomain(tk.Session())
		testkit.SetTiFlashReplica(t, dom, "test", "docs_ng")

		tk.MustExec("set tidb_allow_mpp=off")
		tk.MustExec("set tidb_allow_tiflash_cop=off")
		tk.MustExec("set tidb_enforce_mpp=off")

		sql := `explain format='brief' select max(id) from docs_ng where match(content) against('"red"' in boolean mode)`
		tk.MustQuery(sql).CheckContain("mpp[tiflash]")
	})
}

func TestTiCIJoinWithNonTiCITable(t *testing.T) {
	runTiCITest(t, func(tk *testkit.TestKit) {
		tk.MustExec(`create table fts1(
			id bigint primary key,
			page_num bigint,
			content text,
			fulltext index ft_content(content)
		)`)
		tk.MustExec(`create table t2(
			id bigint primary key,
			page_num bigint
		)`)
		dom := domain.GetDomain(tk.Session())
		testkit.SetTiFlashReplica(t, dom, "test", "fts1")

		tk.MustExec("set tidb_allow_mpp=off")
		tk.MustExec("set tidb_enforce_mpp=off")

		sql := `explain format='brief' select f.id, t.id from fts1 f join t2 t on f.page_num=t.page_num where match(f.content) against('"red"' in boolean mode)`
		tk.MustQuery(sql).CheckContain("HashJoin")
		tk.MustQuery(sql).CheckContain("IndexRangeScan")
		tk.MustQuery(sql).CheckContain("cop[tici]")
		tk.MustQuery(sql).CheckContain("cop[tikv]")
		tk.MustQuery(sql).CheckNotContain("mpp[tiflash]")
	})
}
