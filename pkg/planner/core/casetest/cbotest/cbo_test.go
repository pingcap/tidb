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

package cbotest

import (
	"context"
	"fmt"
	"testing"

	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/executor"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/planner"
	"github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/planner/core/resolve"
	"github.com/pingcap/tidb/pkg/session"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/statistics"
	statstestutil "github.com/pingcap/tidb/pkg/statistics/handle/ddl/testutil"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testdata"
	"github.com/stretchr/testify/require"
)

// TestCBOWithoutAnalyze tests the plan with stats that only have count info.
func TestCBOWithoutAnalyze(t *testing.T) {
	testkit.RunTestUnderCascadesWithDomain(t, func(t *testing.T, tk *testkit.TestKit, dom *domain.Domain, cascades, caller string) {
		tk.MustExec("use test")
		tk.MustExec("create table t1 (a int)")
		tk.MustExec("create table t2 (a int)")
		h := dom.StatsHandle()
		err := statstestutil.HandleNextDDLEventWithTxn(h)
		require.NoError(t, err)

		err = statstestutil.HandleNextDDLEventWithTxn(h)
		require.NoError(t, err)
		tk.MustExec("insert into t1 values (1), (2), (3), (4), (5), (6)")
		tk.MustExec("insert into t2 values (1), (2), (3), (4), (5), (6)")
		require.NoError(t, h.DumpStatsDeltaToKV(true))
		require.NoError(t, h.Update(context.Background(), dom.InfoSchema()))
		var input []string
		var output []struct {
			SQL  string
			Plan []string
		}
		analyzeSuiteData := GetAnalyzeSuiteData()
		analyzeSuiteData.LoadTestCases(t, &input, &output, cascades, caller)
		for i, sql := range input {
			plan := tk.MustQuery(sql)
			testdata.OnRecord(func() {
				output[i].SQL = sql
				output[i].Plan = testdata.ConvertRowsToStrings(plan.Rows())
			})
			plan.Check(testkit.Rows(output[i].Plan...))
		}
	})
}

func TestAnalyzeSuiteRegression(t *testing.T) {
	testkit.RunTestUnderCascadesWithDomain(t, func(t *testing.T, tk *testkit.TestKit, dom *domain.Domain, cascades, caller string) {
		tk.MustExec("use test")

		// issue:62438
		tk.MustExec("CREATE TABLE `objects` (\n  `id` bigint NOT NULL AUTO_INCREMENT,\n  `path` varchar(1024) NOT NULL,\n  `updated_ms` bigint DEFAULT NULL,\n  `size` bigint DEFAULT NULL,\n  `etag` varchar(128) DEFAULT NULL,\n  `seq` bigint DEFAULT NULL,\n  `last_seen_ms` bigint DEFAULT NULL,\n  `metastore_uuid` binary(16) NOT NULL,\n  `securable_id` bigint NOT NULL,\n  PRIMARY KEY (`id`) /*T![clustered_index] CLUSTERED */,\n  KEY `idx_metastore_securable_seq` (`metastore_uuid`,`securable_id`,`seq`)\n)")
		require.NoError(t, testkit.LoadTableStats("issue62438.json", dom))
		var input62438 []string
		var output62438 []struct {
			SQL  string
			Plan []string
		}
		analyzeSuiteData := GetAnalyzeSuiteData()
		analyzeSuiteData.LoadTestCasesByName("TestIssue62438", t, &input62438, &output62438, cascades, caller)
		for i, sql := range input62438 {
			plan := tk.MustQuery(sql)
			testdata.OnRecord(func() {
				output62438[i].SQL = sql
				output62438[i].Plan = testdata.ConvertRowsToStrings(plan.Rows())
			})
			plan.Check(testkit.Rows(output62438[i].Plan...))
		}

		// issue:61389
		tk.MustExec("use test;")
		tk.MustExec("CREATE TABLE `t19f3e4f1` (\n  `colc864` enum('d9','dt5w4','wsg','i','3','5ur3','s0m4','mmhw6','rh','ge9d','nm') DEFAULT 'dt5w4',\n  `colaadb` smallint DEFAULT '7697',\n  UNIQUE KEY `ee56e6aa` (`colc864`)\n);")
		tk.MustExec("CREATE TABLE `t0da79f8d` (\n  `colf2af` enum('xrsg','go9yf','mj4','u1l','8c','at','o','e9','bh','r','yah') DEFAULT 'r'\n);")
		require.NoError(t, testkit.LoadTableStats("test.t0da79f8d.json", dom))
		require.NoError(t, testkit.LoadTableStats("test.t19f3e4f1.json", dom))
		var input61389 []string
		var output61389 []struct {
			SQL  string
			Plan []string
			Warn []string
		}
		analyzeSuiteData.LoadTestCasesByName("TestIssue61389", t, &input61389, &output61389, cascades, caller)
		for i, tt := range input61389 {
			testdata.OnRecord(func() {
				output61389[i].SQL = tt
				output61389[i].Plan = testdata.ConvertRowsToStrings(tk.MustQuery(tt).Rows())
				output61389[i].Warn = testdata.ConvertRowsToStrings(tk.MustQuery("show warnings").Rows())
			})
			tk.MustQuery(tt).Check(testkit.Rows(output61389[i].Plan...))
			tk.MustQuery("show warnings").Check(testkit.Rows(output61389[i].Warn...))
		}

		// issue:61792
		tk.MustExec("set @@session.tidb_executor_concurrency = 4;")
		tk.MustExec("set @@session.tidb_hash_join_concurrency = 5;")
		tk.MustExec("set @@session.tidb_distsql_scan_concurrency = 15;")

		tk.MustExec("use test;")
		tk.MustExec("CREATE TABLE `tbl_cardcore_statement` (" +
			"  `ID` varchar(30) NOT NULL," +
			"  `latest_stmt_print_date` date DEFAULT NULL COMMENT 'KUSTMD'," +
			"  `created_domain` varchar(10) DEFAULT NULL," +
			"  PRIMARY KEY (`ID`)," +
			"  KEY `tbl_cardcore_statement_ix7` (`latest_stmt_print_date`)" +
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin COMMENT='CCDSTMT';")
		require.NoError(t, testkit.LoadTableStats("issue61792.json", dom))

		var input61792 []string
		var output61792 []struct {
			SQL  string
			Plan []string
			Warn []string
		}
		analyzeSuiteData.LoadTestCasesByName("TestIssue61792", t, &input61792, &output61792, cascades, caller)
		for i, tt := range input61792 {
			testdata.OnRecord(func() {
				output61792[i].SQL = tt
				output61792[i].Plan = testdata.ConvertRowsToStrings(tk.MustQuery(tt).Rows())
				output61792[i].Warn = testdata.ConvertRowsToStrings(tk.MustQuery("show warnings").Rows())
			})
			tk.MustQuery(tt).Check(testkit.Rows(output61792[i].Plan...))
			tk.MustQuery("show warnings").Check(testkit.Rows(output61792[i].Warn...))
		}

		// issue:59563
		tk.MustExec("set @@session.tidb_executor_concurrency = 4;")
		tk.MustExec("set @@session.tidb_hash_join_concurrency = 5;")
		tk.MustExec("set @@session.tidb_distsql_scan_concurrency = 15;")

		tk.MustExec("create database if not exists cardcore_issuing;")
		tk.MustExec("use cardcore_issuing;")
		tk.MustExec("CREATE TABLE `tbl_cardcore_transaction` (" +
			" `ID` varchar(30) NOT NULL," +
			" `period` varchar(6) DEFAULT NULL," +
			" `account_number` varchar(19) DEFAULT NULL," +
			" `transaction_status` varchar(3) DEFAULT NULL," +
			" `entry_date` date DEFAULT NULL," +
			" `value_date` date DEFAULT NULL," +
			" `group_acount_number` varchar(19) DEFAULT NULL," +
			" `payment_date` timestamp NULL DEFAULT NULL," +
			" PRIMARY KEY (`ID`)," +
			" KEY `tbl_cardcore_transaction_ix10` (`account_number`,`entry_date`,`value_date`)," +
			" KEY `tbl_cardcore_transaction_ix17` (`period`,`group_acount_number`,`transaction_status`));")
		require.NoError(t, testkit.LoadTableStats("issue59563.json", dom))

		var input59563 []string
		var output59563 []struct {
			SQL  string
			Plan []string
			Warn []string
		}
		analyzeSuiteData.LoadTestCasesByName("TestIssue59563", t, &input59563, &output59563, cascades, caller)
		for i, tt := range input59563 {
			testdata.OnRecord(func() {
				output59563[i].SQL = tt
				output59563[i].Plan = testdata.ConvertRowsToStrings(tk.MustQuery(tt).Rows())
				output59563[i].Warn = testdata.ConvertRowsToStrings(tk.MustQuery("show warnings").Rows())
			})
			tk.MustQuery(tt).Check(testkit.Rows(output59563[i].Plan...))
			tk.MustQuery("show warnings").Check(testkit.Rows(output59563[i].Warn...))
		}

		// issue:9562
		tk.MustExec("use test")
		var input9562 [][]string
		var output9562 []struct {
			SQL  []string
			Plan []string
		}
		analyzeSuiteData.LoadTestCasesByName("TestIssue9562", t, &input9562, &output9562, cascades, caller)
		for i, ts := range input9562 {
			for j, tt := range ts {
				if j != len(ts)-1 {
					tk.MustExec(tt)
				}
				testdata.OnRecord(func() {
					output9562[i].SQL = ts
					if j == len(ts)-1 {
						output9562[i].Plan = testdata.ConvertRowsToStrings(tk.MustQuery(tt).Rows())
					}
				})
				if j == len(ts)-1 {
					tk.MustQuery(tt).Check(testkit.Rows(output9562[i].Plan...))
				}
			}
		}
	})
}

func TestStraightJoin(t *testing.T) {
	testkit.RunTestUnderCascadesWithDomain(t, func(t *testing.T, tk *testkit.TestKit, dom *domain.Domain, cascades, caller string) {
		tk.MustExec("use test")
		h := dom.StatsHandle()
		for _, tblName := range []string{"t1", "t2", "t3", "t4"} {
			tk.MustExec(fmt.Sprintf("create table %s (a int)", tblName))
			err := statstestutil.HandleNextDDLEventWithTxn(h)
			require.NoError(t, err)
		}
		var input []string
		var output [][]string
		analyzeSuiteData := GetAnalyzeSuiteData()
		analyzeSuiteData.LoadTestCases(t, &input, &output, cascades, caller)
		for i, tt := range input {
			testdata.OnRecord(func() {
				output[i] = testdata.ConvertRowsToStrings(tk.MustQuery(tt).Rows())
			})
			tk.MustQuery(tt).Check(testkit.Rows(output[i]...))
		}
	})
}

func TestTableDual(t *testing.T) {
	testkit.RunTestUnderCascadesWithDomain(t, func(t *testing.T, tk *testkit.TestKit, dom *domain.Domain, cascades, caller string) {
		tk.MustExec(`use test`)
		h := dom.StatsHandle()
		tk.MustExec(`create table t(a int)`)
		tk.MustExec("insert into t values (1), (2), (3), (4), (5), (6), (7), (8), (9), (10)")
		err := statstestutil.HandleNextDDLEventWithTxn(h)
		require.NoError(t, err)
		require.NoError(t, h.DumpStatsDeltaToKV(true))
		require.NoError(t, h.Update(context.Background(), dom.InfoSchema()))
		var input []string
		var output []struct {
			SQL  string
			Plan []string
		}
		analyzeSuiteData := GetAnalyzeSuiteData()
		analyzeSuiteData.LoadTestCases(t, &input, &output, cascades, caller)
		for i, sql := range input {
			plan := tk.MustQuery(sql)
			testdata.OnRecord(func() {
				output[i].SQL = sql
				output[i].Plan = testdata.ConvertRowsToStrings(plan.Rows())
			})
			plan.Check(testkit.Rows(output[i].Plan...))
		}
	})
}

func TestEstimation(t *testing.T) {
	testkit.RunTestUnderCascadesWithDomain(t, func(t *testing.T, tk *testkit.TestKit, dom *domain.Domain, cascades, caller string) {
		statistics.RatioOfPseudoEstimate.Store(10.0)
		defer statistics.RatioOfPseudoEstimate.Store(0.7)
		tk.MustExec("use test")
		tk.MustExec("create table t (a int)")
		tk.MustExec("insert into t values (1), (2), (3), (4), (5), (6), (7), (8), (9), (10)")
		tk.MustExec("insert into t select * from t")
		tk.MustExec("insert into t select * from t")
		h := dom.StatsHandle()
		err := statstestutil.HandleNextDDLEventWithTxn(h)
		require.NoError(t, err)
		require.NoError(t, h.DumpStatsDeltaToKV(true))
		tk.MustExec("analyze table t all columns")
		for i := 1; i <= 8; i++ {
			tk.MustExec("delete from t where a = ?", i)
		}
		require.NoError(t, h.DumpStatsDeltaToKV(true))
		require.NoError(t, h.Update(context.Background(), dom.InfoSchema()))
		var input []string
		var output []struct {
			SQL  string
			Plan []string
		}
		analyzeSuiteData := GetAnalyzeSuiteData()
		analyzeSuiteData.LoadTestCases(t, &input, &output, cascades, caller)
		for i, sql := range input {
			plan := tk.MustQuery(sql)
			testdata.OnRecord(func() {
				output[i].SQL = sql
				output[i].Plan = testdata.ConvertRowsToStrings(plan.Rows())
			})
			plan.Check(testkit.Rows(output[i].Plan...))
		}
	})
}

func TestIndexRead(t *testing.T) {
	testkit.RunTestUnderCascadesWithDomain(t, func(t *testing.T, tk *testkit.TestKit, dom *domain.Domain, cascades, caller string) {
		tk.MustExec("set @@session.tidb_executor_concurrency = 4;")
		tk.MustExec("set @@session.tidb_hash_join_concurrency = 5;")
		tk.MustExec("set @@session.tidb_distsql_scan_concurrency = 15;")

		tk.MustExec("use test")
		tk.MustExec("drop table if exists t, t1")
		tk.MustExec("create table t (a int primary key, b int, c varchar(200), d datetime DEFAULT CURRENT_TIMESTAMP, e int, ts timestamp DEFAULT CURRENT_TIMESTAMP)")
		tk.MustExec("create index b on t (b)")
		tk.MustExec("create index d on t (d)")
		tk.MustExec("create index e on t (e)")
		tk.MustExec("create index b_c on t (b,c)")
		tk.MustExec("create index ts on t (ts)")
		tk.MustExec("create table t1 (a int, b int, index idx(a), index idxx(b))")

		// Default RPC encoding may cause statistics explain result differ and then the test unstable.
		tk.MustExec("set @@tidb_enable_chunk_rpc = on")

		// This stats is generated by following format:
		// fill (a, b, c, e) as (i*100+j, i, i+j, i*100+j), i and j is dependent and range of this two are [0, 99].
		require.NoError(t, testkit.LoadTableStats("analyzesSuiteTestIndexReadT.json", dom))
		for i := 1; i < 16; i++ {
			tk.MustExec(fmt.Sprintf("insert into t1 values(%v, %v)", i, i))
		}
		tk.MustExec("analyze table t1")
		ctx := tk.Session()
		var input, output []string
		analyzeSuiteData := GetAnalyzeSuiteData()
		analyzeSuiteData.LoadTestCases(t, &input, &output, cascades, caller)

		for i, tt := range input {
			stmts, err := session.Parse(ctx, tt)
			require.NoError(t, err)
			require.Len(t, stmts, 1)
			stmt := stmts[0]
			ret := &core.PreprocessorReturn{}
			nodeW := resolve.NewNodeW(stmt)
			err = core.Preprocess(context.Background(), ctx, nodeW, core.WithPreprocessorReturn(ret))
			require.NoError(t, err)
			p, _, err := planner.Optimize(context.TODO(), ctx, nodeW, ret.InfoSchema)
			require.NoError(t, err)
			planString := core.ToString(p)
			testdata.OnRecord(func() {
				output[i] = planString
			})
			require.Equalf(t, output[i], planString, "case: %v", tt)
		}
	})
}

func TestEmptyTable(t *testing.T) {
	testkit.RunTestUnderCascades(t, func(t *testing.T, tk *testkit.TestKit, cascades, caller string) {
		tk.MustExec("use test")
		tk.MustExec("drop table if exists t, t1")
		tk.MustExec("create table t (c1 int)")
		tk.MustExec("create table t1 (c1 int)")
		tk.MustExec("analyze table t, t1")
		var input, output []string
		analyzeSuiteData := GetAnalyzeSuiteData()
		analyzeSuiteData.LoadTestCases(t, &input, &output, cascades, caller)
		for i, tt := range input {
			ctx := tk.Session()
			stmts, err := session.Parse(ctx, tt)
			require.NoError(t, err)
			require.Len(t, stmts, 1)
			stmt := stmts[0]
			ret := &core.PreprocessorReturn{}
			nodeW := resolve.NewNodeW(stmt)
			err = core.Preprocess(context.Background(), ctx, nodeW, core.WithPreprocessorReturn(ret))
			require.NoError(t, err)
			p, _, err := planner.Optimize(context.TODO(), ctx, nodeW, ret.InfoSchema)
			require.NoError(t, err)
			planString := core.ToString(p)
			testdata.OnRecord(func() {
				output[i] = planString
			})
			require.Equalf(t, output[i], planString, "case: %v", tt)
		}
	})
}

func TestAnalyze(t *testing.T) {
	testkit.RunTestUnderCascades(t, func(t *testing.T, tk *testkit.TestKit, cascades, caller string) {
		tk.MustExec("use test")
		tk.MustExec("drop table if exists t, t1, t2, t3")
		tk.MustExec("create table t (a int, b int)")
		tk.MustExec("create index a on t (a)")
		tk.MustExec("create index b on t (b)")
		tk.MustExec("insert into t (a,b) values (1,1),(1,2),(1,3),(1,4),(2,5),(2,6),(2,7),(2,8)")
		tk.MustExec("analyze table t")

		tk.MustExec("create table t1 (a int, b int)")
		tk.MustExec("create index a on t1 (a)")
		tk.MustExec("create index b on t1 (b)")
		tk.MustExec("insert into t1 (a,b) values (1,1),(1,2),(1,3),(1,4),(2,5),(2,6),(2,7),(2,8)")

		tk.MustExec("create table t2 (a int, b int)")
		tk.MustExec("create index a on t2 (a)")
		tk.MustExec("create index b on t2 (b)")
		tk.MustExec("insert into t2 (a,b) values (1,1),(1,2),(1,3),(1,4),(2,5),(2,6),(2,7),(2,8)")
		tk.MustExec("analyze table t2 index a")

		tk.MustExec("create table t3 (a int, b int)")
		tk.MustExec("create index a on t3 (a)")

		tk.MustExec("set @@tidb_partition_prune_mode = 'static';")
		tk.MustExec("create table t4 (a int, b int) partition by range (a) (partition p1 values less than (2), partition p2 values less than (3))")
		tk.MustExec("create index a on t4 (a)")
		tk.MustExec("create index b on t4 (b)")
		tk.MustExec("insert into t4 (a,b) values (1,1),(1,2),(1,3),(1,4),(2,5),(2,6),(2,7),(2,8)")
		tk.MustExec("analyze table t4")

		tk.MustExec("create view v as select * from t")
		_, err := tk.Exec("analyze table v")
		require.EqualError(t, err, "analyze view v is not supported now")
		tk.MustExec("drop view v")

		tk.MustExec("create sequence seq")
		_, err = tk.Exec("analyze table seq")
		require.EqualError(t, err, "analyze sequence seq is not supported now")
		tk.MustExec("drop sequence seq")

		var input, output []string
		analyzeSuiteData := GetAnalyzeSuiteData()
		analyzeSuiteData.LoadTestCases(t, &input, &output, cascades, caller)

		for i, tt := range input {
			ctx := tk.Session()
			stmts, err := session.Parse(ctx, tt)
			require.NoError(t, err)
			require.Len(t, stmts, 1)
			stmt := stmts[0]
			err = executor.ResetContextOfStmt(ctx, stmt)
			require.NoError(t, err)
			ret := &core.PreprocessorReturn{}
			nodeW := resolve.NewNodeW(stmt)
			err = core.Preprocess(context.Background(), ctx, nodeW, core.WithPreprocessorReturn(ret))
			require.NoError(t, err)
			p, _, err := planner.Optimize(context.TODO(), ctx, nodeW, ret.InfoSchema)
			require.NoError(t, err)
			planString := core.ToString(p)
			testdata.OnRecord(func() {
				output[i] = planString
			})
			require.Equalf(t, output[i], planString, "case: %v", tt)
		}
	})
}

func TestOutdatedAnalyze(t *testing.T) {
	testkit.RunTestUnderCascadesWithDomain(t, func(t *testing.T, tk *testkit.TestKit, dom *domain.Domain, cascades, caller string) {
		tk.MustExec("use test")
		tk.MustExec("create table t (a int, b int, index idx(a))")
		for i := range 10 {
			tk.MustExec(fmt.Sprintf("insert into t values (%d,%d)", i, i))
		}
		h := dom.StatsHandle()
		err := statstestutil.HandleNextDDLEventWithTxn(h)
		require.NoError(t, err)
		require.NoError(t, h.DumpStatsDeltaToKV(true))
		tk.MustExec("analyze table t all columns")
		tk.MustExec("insert into t select * from t")
		tk.MustExec("insert into t select * from t")
		tk.MustExec("insert into t select * from t")
		require.NoError(t, h.DumpStatsDeltaToKV(true))
		require.NoError(t, h.Update(context.Background(), dom.InfoSchema()))
		var input []struct {
			SQL                          string
			EnablePseudoForOutdatedStats bool
			RatioOfPseudoEstimate        float64
		}
		var output []struct {
			SQL                          string
			EnablePseudoForOutdatedStats bool
			RatioOfPseudoEstimate        float64
			Plan                         []string
		}
		analyzeSuiteData := GetAnalyzeSuiteData()
		analyzeSuiteData.LoadTestCases(t, &input, &output, cascades, caller)
		for i, tt := range input {
			tk.Session().GetSessionVars().SetEnablePseudoForOutdatedStats(tt.EnablePseudoForOutdatedStats)
			statistics.RatioOfPseudoEstimate.Store(tt.RatioOfPseudoEstimate)
			plan := tk.MustQuery(tt.SQL)
			testdata.OnRecord(func() {
				output[i].SQL = tt.SQL
				output[i].EnablePseudoForOutdatedStats = tt.EnablePseudoForOutdatedStats
				output[i].RatioOfPseudoEstimate = tt.RatioOfPseudoEstimate
				output[i].Plan = testdata.ConvertRowsToStrings(plan.Rows())
			})
			plan.Check(testkit.Rows(output[i].Plan...))
		}
	})
}

func TestNullCount(t *testing.T) {
	testkit.RunTestUnderCascadesWithDomain(t, func(t *testing.T, tk *testkit.TestKit, dom *domain.Domain, cascades, caller string) {
		tk.MustExec("use test")
		tk.MustExec("drop table if exists t")
		tk.MustExec("create table t (a int, b int, index idx(a))")
		tk.MustExec("insert into t values (null, null), (null, null)")
		tk.MustExec("analyze table t all columns")
		var input []string
		var output [][]string
		analyzeSuiteData := GetAnalyzeSuiteData()
		analyzeSuiteData.LoadTestCases(t, &input, &output, cascades, caller)
		for i := range 2 {
			testdata.OnRecord(func() {
				output[i] = testdata.ConvertRowsToStrings(tk.MustQuery(input[i]).Rows())
			})
			tk.MustQuery(input[i]).Check(testkit.Rows(output[i]...))
		}
		h := dom.StatsHandle()
		h.Clear()
		require.NoError(t, h.Update(context.Background(), dom.InfoSchema()))
		for i := 2; i < 4; i++ {
			testdata.OnRecord(func() {
				output[i] = testdata.ConvertRowsToStrings(tk.MustQuery(input[i]).Rows())
			})
			tk.MustQuery(input[i]).Check(testkit.Rows(output[i]...))
		}
	})
}

func TestCorrelatedEstimation(t *testing.T) {
	testkit.RunTestUnderCascades(t, func(t *testing.T, tk *testkit.TestKit, cascades, caller string) {
		tk.MustExec("use test")
		tk.MustExec("set sql_mode='STRICT_TRANS_TABLES'") // disable only full group by
		tk.MustExec("create table t(a int, b int, c int, index idx(c,b,a))")
		tk.MustExec("insert into t values(1,1,1), (2,2,2), (3,3,3), (4,4,4), (5,5,5), (6,6,6), (7,7,7), (8,8,8), (9,9,9),(10,10,10)")
		tk.MustExec("analyze table t")
		var (
			input  []string
			output [][]string
		)
		analyzeSuiteData := GetAnalyzeSuiteData()
		analyzeSuiteData.LoadTestCases(t, &input, &output, cascades, caller)
		for i, tt := range input {
			rs := tk.MustQuery(tt)
			testdata.OnRecord(func() {
				output[i] = testdata.ConvertRowsToStrings(rs.Rows())
			})
			rs.Check(testkit.Rows(output[i]...))
		}
	})
}

func testInconsistentEstimation(t *testing.T, tk *testkit.TestKit, dom *domain.Domain, cascades, caller string) {
	tk.MustExec("use test")
	tk.MustExec("create table t(a int, b int, c int, index ab(a,b), index ac(a,c))")
	tk.MustExec("insert into t values (1,1,1), (1000,1000,1000)")
	for range 10 {
		tk.MustExec("insert into t values (5,5,5), (10,10,10)")
	}
	tk.MustExec("set @@tidb_analyze_version=1")
	tk.MustExec("analyze table t all columns with 2 buckets ")
	// Force using the histogram to estimate.
	tk.MustExec("update mysql.stats_histograms set stats_ver = 0")
	dom.StatsHandle().Clear()
	require.NoError(t, dom.StatsHandle().Update(context.Background(), dom.InfoSchema()))
	var input []string
	var output []struct {
		SQL  string
		Plan []string
	}
	analyzeSuiteData := GetAnalyzeSuiteData()
	analyzeSuiteData.LoadTestCases(t, &input, &output, cascades, caller)
	for i, sql := range input {
		plan := tk.MustQuery(sql)
		testdata.OnRecord(func() {
			output[i].SQL = sql
			output[i].Plan = testdata.ConvertRowsToStrings(plan.Rows())
		})
		plan.Check(testkit.Rows(output[i].Plan...))
	}
}

func TestInconsistentEstimation(t *testing.T) {
	testkit.RunTestUnderCascadesWithDomain(t, testInconsistentEstimation)
}

func TestLimitCrossEstimation(t *testing.T) {
	testkit.RunTestUnderCascades(t, func(t *testing.T, tk *testkit.TestKit, cascades, caller string) {
		tk.MustExec("set @@session.tidb_executor_concurrency = 4;")
		tk.MustExec("set @@session.tidb_hash_join_concurrency = 5;")
		tk.MustExec("set @@session.tidb_distsql_scan_concurrency = 15;")
		tk.MustExec("use test")
		tk.MustExec("drop table if exists t")
		tk.MustExec("create table t(a int primary key, b int not null, c int not null default 0, index idx_bc(b, c))")
		var input [][]string
		var output []struct {
			SQL  []string
			Plan []string
		}
		analyzeSuiteData := GetAnalyzeSuiteData()
		analyzeSuiteData.LoadTestCases(t, &input, &output, cascades, caller)
		for i, ts := range input {
			for j, tt := range ts {
				if j != len(ts)-1 {
					tk.MustExec(tt)
				}
				testdata.OnRecord(func() {
					output[i].SQL = ts
					if j == len(ts)-1 {
						output[i].Plan = testdata.ConvertRowsToStrings(tk.MustQuery(tt).Rows())
					}
				})
				if j == len(ts)-1 {
					tk.MustQuery(tt).Check(testkit.Rows(output[i].Plan...))
				}
			}
		}
	})
}

func TestLowSelIndexGreedySearch(t *testing.T) {
	testkit.RunTestUnderCascadesWithDomain(t, func(t *testing.T, tk *testkit.TestKit, dom *domain.Domain, cascades, caller string) {
		tk.MustExec("use test")
		tk.MustExec(`set tidb_opt_limit_push_down_threshold=0`)
		tk.MustExec("drop table if exists t")
		tk.MustExec("create table t (a varchar(32) default null, b varchar(10) default null, c varchar(12) default null, d varchar(32) default null, e bigint(10) default null, key idx1 (d,a), key idx2 (a,c), key idx3 (c,b), key idx4 (e))")
		require.NoError(t, testkit.LoadTableStats("analyzeSuiteTestLowSelIndexGreedySearchT.json", dom))
		var input []string
		var output []struct {
			SQL  string
			Plan []string
		}
		// The test purposes are:
		// - index `idx2` runs much faster than `idx4` experimentally;
		// - estimated row count of IndexLookUp should be 0;
		analyzeSuiteData := GetAnalyzeSuiteData()
		analyzeSuiteData.LoadTestCases(t, &input, &output, cascades, caller)
		for i, tt := range input {
			testdata.OnRecord(func() {
				output[i].SQL = tt
				output[i].Plan = testdata.ConvertRowsToStrings(tk.MustQuery(tt).Rows())
			})
			tk.MustQuery(tt).Check(testkit.Rows(output[i].Plan...))
		}
	})
}

func TestIndexChoiceByNDV(t *testing.T) {
	testkit.RunTestUnderCascades(t, func(t *testing.T, tk *testkit.TestKit, cascades, caller string) {
		tk.MustExec(`use test`)
		tk.MustExec(`create table ts (idx int, code int, a int, key k(idx, code))`)
		tk.MustExec(`insert into ts select * from (
		  with recursive tt as (
			select 0 as idx, 0 as code, 0 as a
			union all
			select mod(a, 100) as idx, 0 as code, a+1 as a from tt where a<200
		  ) select * from tt) tt`)
		tk.MustExec(`create table h (idx int, code int, typ1 int, typ2 int, update_time int, key k1(idx, typ1, typ2), key k2(idx, update_time))`)
		tk.MustExec(`insert into h select * from (
		  with recursive tt as (
			select 0 idx, 0 as code, 0 as typ1, 0 as typ2, 0 as update_time
			union all
			select mod(update_time, 5) as idx, 0 as code, 0 as typ1, 0 as typ2, update_time+1 as update_time from tt where update_time<200
		  ) select * from tt) tt`)
		tk.MustExec(`analyze table ts, h`)
		// use the index k2(idx, update_time) since update_time has a higher NDV than typ1 and typ2
		tk.MustUseIndex(`select /* issue:63869 */ /*+ tidb_inlj(h) */ 1 from ts inner join h on ts.idx=h.idx and ts.code=h.code
				where h.typ1=0 and h.typ2=0 and h.update_time>0 and h.update_time<2 and h.code=0`, "k2")
	})
}

func TestTiFlashCostModel(t *testing.T) {
	testkit.RunTestUnderCascadesWithDomain(t, func(t *testing.T, tk *testkit.TestKit, dom *domain.Domain, cascades, caller string) {
		tk.MustExec("use test")
		tk.MustExec("create table t (a int, b int, c int, primary key(a))")
		tk.MustExec("insert into t values(1,1,1), (2,2,2), (3,3,3)")

		tbl, err := dom.InfoSchema().TableByName(context.Background(), ast.CIStr{O: "test", L: "test"}, ast.CIStr{O: "t", L: "t"})
		require.NoError(t, err)
		// Set the hacked TiFlash replica for explain tests.
		tbl.Meta().TiFlashReplica = &model.TiFlashReplicaInfo{Count: 1, Available: true}

		var input, output [][]string
		analyzeSuiteData := GetAnalyzeSuiteData()
		analyzeSuiteData.LoadTestCases(t, &input, &output, cascades, caller)
		for i, ts := range input {
			for j, tt := range ts {
				if j != len(ts)-1 {
					tk.MustExec(tt)
				}
				testdata.OnRecord(func() {
					if j == len(ts)-1 {
						output[i] = testdata.ConvertRowsToStrings(tk.MustQuery(tt).Rows())
					}
				})
				if j == len(ts)-1 {
					tk.MustQuery(tt).Check(testkit.Rows(output[i]...))
				}
			}
		}
	})
}

func TestIndexEqualUnknown(t *testing.T) {
	testkit.RunTestUnderCascadesWithDomain(t, func(t *testing.T, tk *testkit.TestKit, dom *domain.Domain, cascades, caller string) {
		tk.MustExec("use test")
		tk.MustExec("drop table if exists t, t1")
		tk.Session().GetSessionVars().EnableClusteredIndex = vardef.ClusteredIndexDefModeIntOnly
		tk.MustExec("CREATE TABLE t(a bigint(20) NOT NULL, b bigint(20) NOT NULL, c bigint(20) NOT NULL, PRIMARY KEY (a,c,b), KEY (b))")
		require.NoError(t, testkit.LoadTableStats("analyzeSuiteTestIndexEqualUnknownT.json", dom))
		var input []string
		var output []struct {
			SQL  string
			Plan []string
		}
		analyzeSuiteData := GetAnalyzeSuiteData()
		analyzeSuiteData.LoadTestCases(t, &input, &output, cascades, caller)
		for i, tt := range input {
			testdata.OnRecord(func() {
				output[i].SQL = tt
				output[i].Plan = testdata.ConvertRowsToStrings(tk.MustQuery(tt).Rows())
			})
			tk.MustQuery(tt).Check(testkit.Rows(output[i].Plan...))
		}
	})
}

func TestLimitIndexEstimation(t *testing.T) {
	testkit.RunTestUnderCascadesWithDomain(t, func(t *testing.T, tk *testkit.TestKit, dom *domain.Domain, cascades, caller string) {
		tk.MustExec("use test")
		tk.MustExec("drop table if exists t")
		tk.MustExec("create table t(a int, b int, key idx_a(a), key idx_b(b))")
		tk.MustExec("set session tidb_enable_extended_stats = on")
		// Values in column a are from 1 to 1000000, values in column b are from 1000000 to 1,
		// these 2 columns are strictly correlated in reverse order.
		require.NoError(t, testkit.LoadTableStats("analyzeSuiteTestLimitIndexEstimationT.json", dom))
		var input []string
		var output []struct {
			SQL  string
			Plan []string
		}

		analyzeSuiteData := GetAnalyzeSuiteData()
		analyzeSuiteData.LoadTestCases(t, &input, &output, cascades, caller)
		for i, tt := range input {
			testdata.OnRecord(func() {
				output[i].SQL = tt
				output[i].Plan = testdata.ConvertRowsToStrings(tk.MustQuery(tt).Rows())
			})
			tk.MustQuery(tt).Check(testkit.Rows(output[i].Plan...))
		}
	})
}
