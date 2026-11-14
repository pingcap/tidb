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
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/config/deploymode"
	"github.com/pingcap/tidb/pkg/config/kerneltype"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/domain/infosync"
	"github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/store/mockstore"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testdata"
	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
	"github.com/stretchr/testify/require"
)

func TestFTSDeploymentModes(t *testing.T) {
	if !kerneltype.IsNextGen() {
		t.Skip("deployment modes are nextgen-only")
	}

	for _, mode := range deploymode.ModeList() {
		t.Run(mode.String(), func(t *testing.T) {
			originDeployMode := deploymode.Get()
			require.NoError(t, deploymode.Set(mode))
			t.Cleanup(func() { require.NoError(t, deploymode.Set(originDeployMode)) })
			testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/ddl/MockCheckColumnarIndexProcess", "return(1)")
			store, dom := testkit.CreateMockStoreAndDomain(t, mockstore.WithMockTiFlash(2))
			tk := testkit.NewTestKit(t, store)
			setupTiFlashFTSMatchWordTable(t, tk, dom)

			tk.MustExec("alter table fts_t add fulltext index ft_body(body)")
			tk.MustExec("create fulltext index ft_title_body on fts_t(title, body)")
			testkit.SetTiFlashReplica(t, dom, "test", "fts_t")
			plan := testdata.ConvertRowsToStrings(tk.MustQuery("explain format = 'plan_tree' select * from fts_t where fts_match_word('hello', title)").Rows())
			require.Contains(t, plan[len(plan)-1], "ftsIndex:hello IN test.fts_t.title")
			plan = testdata.ConvertRowsToStrings(tk.MustQuery("explain format = 'plan_tree' select id, fts_match_word('hello', title) as score from fts_t where fts_match_word('hello', title) order by score desc limit 10").Rows())
			require.Contains(t, plan[len(plan)-1], "ftsIndex:top10 hello IN test.fts_t.title->_FTS_SCORE")
			tk.MustContainErrMsg("explain select fts_match_word('hello', title) from fts_t", "'FTS_MATCH_WORD()' in SELECT requires a matching 'FTS_MATCH_WORD()' in WHERE")
			tk.MustContainErrMsg("explain select * from fts_t where fts_match_word(title, body)", "match against a non-constant string")
			tk.MustExec("create table no_replica(title text)")
			tk.MustContainErrMsg("alter table no_replica add fulltext index ft_title(title)", "columnar replica must exist")
		})
	}
}

func TestTiFlashFTSMatchWordPushDown(t *testing.T) {
	setStarterDeployModeForFTSTest(t)

	testkit.RunTestUnderCascadesAndDomainWithSchemaLease(t, 600*time.Millisecond, []mockstore.MockTiKVStoreOption{mockstore.WithMockTiFlash(2)}, func(t *testing.T, tk *testkit.TestKit, dom *domain.Domain, cascades, caller string) {
		var input []struct {
			SQL       string
			RedactLog bool
		}
		var output []struct {
			SQL       string
			RedactLog bool
			Plan      []string
		}
		ftsResolveIndexSuiteData := core.GetFTSResolveIndexSuiteData()
		ftsResolveIndexSuiteData.LoadTestCases(t, &input, &output, cascades, caller)
		require.Equal(t, len(input), len(output))

		setupTiFlashFTSMatchWordTable(t, tk, dom)

		for i, tt := range input {
			redactLog := "OFF"
			if tt.RedactLog {
				redactLog = "ON"
			}
			tk.MustExec("set global tidb_redact_log=" + redactLog)
			testdata.OnRecord(func() {
				output[i].SQL = tt.SQL
				output[i].RedactLog = tt.RedactLog
				output[i].Plan = testdata.ConvertRowsToStrings(tk.MustQuery("explain format = 'plan_tree' " + tt.SQL).Rows())
			})
			require.Equal(t, tt.SQL, output[i].SQL)
			require.Equal(t, tt.RedactLog, output[i].RedactLog)
			tk.MustQuery("explain format = 'plan_tree' " + tt.SQL).Check(testkit.Rows(output[i].Plan...))
		}
		tk.MustExec("set global tidb_redact_log=OFF")

		tk.MustContainErrMsg("explain select fts_match_word('hello', title) from fts_t", "'FTS_MATCH_WORD()' in SELECT requires a matching 'FTS_MATCH_WORD()' in WHERE")
		tk.MustContainErrMsg("explain select fts_match_word('hello', title) * 2 from fts_t where fts_match_word('hello', title)", "'FTS_MATCH_WORD()' in SELECT must not be wrapped in expressions")
		tk.MustContainErrMsg("explain select fts_match_word('world', title) from fts_t where fts_match_word('hello', title)", "'FTS_MATCH_WORD()' in SELECT must match the one in WHERE")
		tk.MustContainErrMsg("explain select * from fts_t where fts_match_word('hello', title) and fts_match_word('world', title)", "Currently 'FTS_MATCH_WORD()' must be used alone")
		tk.MustContainErrMsg("explain select * from fts_t where fts_match_word('hello', body)", "full-text search index ft_title does not cover column test.fts_t.body")
		tk.MustContainErrMsg("explain select * from fts_t order by fts_match_word('hello', title)", "Currently 'FTS_MATCH_WORD()' in ORDER BY without a LIMIT clause is not supported")
	})
}

func TestTiFlashFTSMatchWordPreparedPlanCache(t *testing.T) {
	setStarterDeployModeForFTSTest(t)

	testkit.RunTestUnderCascadesAndDomainWithSchemaLease(t, 600*time.Millisecond, []mockstore.MockTiKVStoreOption{mockstore.WithMockTiFlash(2)}, func(t *testing.T, tk *testkit.TestKit, dom *domain.Domain, cascades, caller string) {
		setupTiFlashFTSMatchWordTable(t, tk, dom)
		tk.MustExec("set @@tidb_enable_prepared_plan_cache=1")
		tk.MustExec("set @@tidb_enable_non_prepared_plan_cache=0")

		tk.MustExec("prepare stmt from \"select * from fts_t where fts_match_word('hello', title)\"")
		tk.MustExec("execute stmt")
		tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("0"))

		tk.MustExec("execute stmt")
		tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("0"))
		tk.MustExec("deallocate prepare stmt")

		tk.MustContainErrMsg("prepare stmt_param from 'select * from fts_t where fts_match_word(?, title)'", "match against a non-constant string")
	})
}

func TestTiFlashFTSMatchWordDirtyTxn(t *testing.T) {
	setStarterDeployModeForFTSTest(t)

	testkit.RunTestUnderCascadesAndDomainWithSchemaLease(t, 600*time.Millisecond, []mockstore.MockTiKVStoreOption{mockstore.WithMockTiFlash(2)}, func(t *testing.T, tk *testkit.TestKit, dom *domain.Domain, cascades, caller string) {
		setupTiFlashFTSMatchWordTable(t, tk, dom)

		tk.MustQuery("select * from fts_t where fts_match_word('hello', title)").Check(testkit.Rows())

		tk.MustExec("begin")
		tk.MustExec("insert into fts_t values (1, 'hello', 'dirty')")
		tk.MustContainErrMsg("select * from fts_t where fts_match_word('hello', title)", "FTS_MATCH_WORD() cannot be used in a transaction with uncommitted changes")
		tk.MustExec("rollback")
	})
}

func setStarterDeployModeForFTSTest(t *testing.T) {
	t.Helper()
	if !kerneltype.IsNextGen() {
		t.Skip("starter deploy mode is nextgen-only")
	}
	originDeployMode := deploymode.Get()
	require.NoError(t, deploymode.Set(deploymode.Starter))
	t.Cleanup(func() {
		require.NoError(t, deploymode.Set(originDeployMode))
	})
}

func setupTiFlashFTSMatchWordTable(t *testing.T, tk *testkit.TestKit, dom *domain.Domain) {
	t.Helper()
	tiflash := infosync.NewMockTiFlash()
	infosync.SetMockTiFlash(tiflash)
	t.Cleanup(func() {
		tiflash.Lock()
		tiflash.StatusServer.Close()
		tiflash.Unlock()
	})
	t.Cleanup(func() {
		tk.MustExec("set global tidb_redact_log=OFF")
	})

	tk.MustExec("use test")
	tk.MustExec("set global tidb_redact_log=OFF")
	tk.MustExec("create table fts_t(id int primary key, title text, body text, fulltext key ft_title(title))")
	tk.MustExec("alter table fts_t set tiflash replica 1")
	testkit.SetTiFlashReplica(t, dom, "test", "fts_t")
}
