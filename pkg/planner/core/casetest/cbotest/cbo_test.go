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

func TestIssue62438(t *testing.T) {
	testkit.RunTestUnderCascadesWithDomain(t, func(t *testing.T, testKit *testkit.TestKit, dom *domain.Domain, cascades, caller string) {
		testKit.MustExec("use test")
		testKit.MustExec("CREATE TABLE `objects` (\n  `id` bigint NOT NULL AUTO_INCREMENT,\n  `path` varchar(1024) NOT NULL,\n  `updated_ms` bigint DEFAULT NULL,\n  `size` bigint DEFAULT NULL,\n  `etag` varchar(128) DEFAULT NULL,\n  `seq` bigint DEFAULT NULL,\n  `last_seen_ms` bigint DEFAULT NULL,\n  `metastore_uuid` binary(16) NOT NULL,\n  `securable_id` bigint NOT NULL,\n  PRIMARY KEY (`id`) /*T![clustered_index] CLUSTERED */,\n  KEY `idx_metastore_securable_seq` (`metastore_uuid`,`securable_id`,`seq`)\n)")
		require.NoError(t, testkit.LoadTableStats("issue62438.json", dom))
		var input []string
		var output []struct {
			SQL  string
			Plan []string
		}
		analyzeSuiteData := GetAnalyzeSuiteData()
		analyzeSuiteData.LoadTestCases(t, &input, &output, cascades, caller)
		for i, sql := range input {
			plan := testKit.MustQuery(sql)
			testdata.OnRecord(func() {
				output[i].SQL = sql
				output[i].Plan = testdata.ConvertRowsToStrings(plan.Rows())
			})
			plan.Check(testkit.Rows(output[i].Plan...))
		}
	})
}

// TestCBOWithoutAnalyze tests the plan with stats that only have count info.
func TestCBOWithoutAnalyze(t *testing.T) {
	testkit.RunTestUnderCascadesWithDomain(t, func(t *testing.T, testKit *testkit.TestKit, dom *domain.Domain, cascades, caller string) {
		testKit.MustExec("use test")
		testKit.MustExec("create table t1 (a int)")
		testKit.MustExec("create table t2 (a int)")
		h := dom.StatsHandle()
		err := statstestutil.HandleNextDDLEventWithTxn(h)
		require.NoError(t, err)

		err = statstestutil.HandleNextDDLEventWithTxn(h)
		require.NoError(t, err)
		testKit.MustExec("insert into t1 values (1), (2), (3), (4), (5), (6)")
		testKit.MustExec("insert into t2 values (1), (2), (3), (4), (5), (6)")
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
			plan := testKit.MustQuery(sql)
			testdata.OnRecord(func() {
				output[i].SQL = sql
				output[i].Plan = testdata.ConvertRowsToStrings(plan.Rows())
			})
			plan.Check(testkit.Rows(output[i].Plan...))
		}
	})
}

func TestStraightJoin(t *testing.T) {
	testkit.RunTestUnderCascadesWithDomain(t, func(t *testing.T, testKit *testkit.TestKit, dom *domain.Domain, cascades, caller string) {
		testKit.MustExec("use test")
		h := dom.StatsHandle()
		for _, tblName := range []string{"t1", "t2", "t3", "t4"} {
			testKit.MustExec(fmt.Sprintf("create table %s (a int)", tblName))
			err := statstestutil.HandleNextDDLEventWithTxn(h)
			require.NoError(t, err)
		}
		var input []string
		var output [][]string
		analyzeSuiteData := GetAnalyzeSuiteData()
		analyzeSuiteData.LoadTestCases(t, &input, &output, cascades, caller)
		for i, tt := range input {
			testdata.OnRecord(func() {
				output[i] = testdata.ConvertRowsToStrings(testKit.MustQuery(tt).Rows())
			})
			testKit.MustQuery(tt).Check(testkit.Rows(output[i]...))
		}
	})
}

func TestTableDual(t *testing.T) {
	testkit.RunTestUnderCascadesWithDomain(t, func(t *testing.T, testKit *testkit.TestKit, dom *domain.Domain, cascades, caller string) {
		testKit.MustExec(`use test`)
		h := dom.StatsHandle()
		testKit.MustExec(`create table t(a int)`)
		testKit.MustExec("insert into t values (1), (2), (3), (4), (5), (6), (7), (8), (9), (10)")
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
			plan := testKit.MustQuery(sql)
			testdata.OnRecord(func() {
				output[i].SQL = sql
				output[i].Plan = testdata.ConvertRowsToStrings(plan.Rows())
			})
			plan.Check(testkit.Rows(output[i].Plan...))
		}
	})
}

func TestEstimation(t *testing.T) {
	testkit.RunTestUnderCascadesWithDomain(t, func(t *testing.T, testKit *testkit.TestKit, dom *domain.Domain, cascades, caller string) {
		statistics.RatioOfPseudoEstimate.Store(10.0)
		defer statistics.RatioOfPseudoEstimate.Store(0.7)
		testKit.MustExec("use test")
		testKit.MustExec("create table t (a int)")
		testKit.MustExec("insert into t values (1), (2), (3), (4), (5), (6), (7), (8), (9), (10)")
		testKit.MustExec("insert into t select * from t")
		testKit.MustExec("insert into t select * from t")
		h := dom.StatsHandle()
		err := statstestutil.HandleNextDDLEventWithTxn(h)
		require.NoError(t, err)
		require.NoError(t, h.DumpStatsDeltaToKV(true))
		testKit.MustExec("analyze table t all columns")
		for i := 1; i <= 8; i++ {
			testKit.MustExec("delete from t where a = ?", i)
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
			plan := testKit.MustQuery(sql)
			testdata.OnRecord(func() {
				output[i].SQL = sql
				output[i].Plan = testdata.ConvertRowsToStrings(plan.Rows())
			})
			plan.Check(testkit.Rows(output[i].Plan...))
		}
	})
}

func TestIssue61389(t *testing.T) {
	testkit.RunTestUnderCascadesWithDomain(t, func(t *testing.T, testKit *testkit.TestKit, dom *domain.Domain, cascades, caller string) {
		testKit.MustExec("use test;")
		testKit.MustExec("CREATE TABLE `t19f3e4f1` (\n  `colc864` enum('d9','dt5w4','wsg','i','3','5ur3','s0m4','mmhw6','rh','ge9d','nm') DEFAULT 'dt5w4',\n  `colaadb` smallint DEFAULT '7697',\n  UNIQUE KEY `ee56e6aa` (`colc864`)\n);")
		testKit.MustExec("CREATE TABLE `t0da79f8d` (\n  `colf2af` enum('xrsg','go9yf','mj4','u1l','8c','at','o','e9','bh','r','yah') DEFAULT 'r'\n);")
		require.NoError(t, testkit.LoadTableStats("test.t0da79f8d.json", dom))
		require.NoError(t, testkit.LoadTableStats("test.t19f3e4f1.json", dom))

		var input []string
		var output []struct {
			SQL  string
			Plan []string
			Warn []string
		}
		analyzeSuiteData := GetAnalyzeSuiteData()
		analyzeSuiteData.LoadTestCases(t, &input, &output, cascades, caller)
		for i, tt := range input {
			testdata.OnRecord(func() {
				output[i].SQL = tt
				output[i].Plan = testdata.ConvertRowsToStrings(testKit.MustQuery(tt).Rows())
				output[i].Warn = testdata.ConvertRowsToStrings(testKit.MustQuery("show warnings").Rows())
			})
			testKit.MustQuery(tt).Check(testkit.Rows(output[i].Plan...))
			testKit.MustQuery("show warnings").Check(testkit.Rows(output[i].Warn...))
		}
	})
}

func TestIssue61792(t *testing.T) {
	testkit.RunTestUnderCascadesWithDomain(t, func(t *testing.T, testKit *testkit.TestKit, dom *domain.Domain, cascades, caller string) {
		testKit.MustExec("set @@session.tidb_executor_concurrency = 4;")
		testKit.MustExec("set @@session.tidb_hash_join_concurrency = 5;")
		testKit.MustExec("set @@session.tidb_distsql_scan_concurrency = 15;")

		testKit.MustExec("use test;")
		testKit.MustExec("CREATE TABLE `tbl_cardcore_statement` (" +
			"  `ID` varchar(30) NOT NULL," +
			"  `latest_stmt_print_date` date DEFAULT NULL COMMENT 'KUSTMD'," +
			"  `created_domain` varchar(10) DEFAULT NULL," +
			"  PRIMARY KEY (`ID`)," +
			"  KEY `tbl_cardcore_statement_ix7` (`latest_stmt_print_date`)" +
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin COMMENT='CCDSTMT';")
		require.NoError(t, testkit.LoadTableStats("issue61792.json", dom))

		var input []string
		var output []struct {
			SQL  string
			Plan []string
			Warn []string
		}
		analyzeSuiteData := GetAnalyzeSuiteData()
		analyzeSuiteData.LoadTestCases(t, &input, &output, cascades, caller)
		for i, tt := range input {
			testdata.OnRecord(func() {
				output[i].SQL = tt
				output[i].Plan = testdata.ConvertRowsToStrings(testKit.MustQuery(tt).Rows())
				output[i].Warn = testdata.ConvertRowsToStrings(testKit.MustQuery("show warnings").Rows())
			})
			testKit.MustQuery(tt).Check(testkit.Rows(output[i].Plan...))
			testKit.MustQuery("show warnings").Check(testkit.Rows(output[i].Warn...))
		}
	})
}

func TestIssue59563(t *testing.T) {
	testkit.RunTestUnderCascadesWithDomain(t, func(t *testing.T, testKit *testkit.TestKit, dom *domain.Domain, cascades, caller string) {
		testKit.MustExec("set @@session.tidb_executor_concurrency = 4;")
		testKit.MustExec("set @@session.tidb_hash_join_concurrency = 5;")
		testKit.MustExec("set @@session.tidb_distsql_scan_concurrency = 15;")

		testKit.MustExec("create database cardcore_issuing;")
		testKit.MustExec("use cardcore_issuing;")
		testKit.MustExec("CREATE TABLE `tbl_cardcore_transaction` (" +
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

		var input []string
		var output []struct {
			SQL  string
			Plan []string
			Warn []string
		}
		analyzeSuiteData := GetAnalyzeSuiteData()
		analyzeSuiteData.LoadTestCases(t, &input, &output, cascades, caller)
		for i, tt := range input {
			testdata.OnRecord(func() {
				output[i].SQL = tt
				output[i].Plan = testdata.ConvertRowsToStrings(testKit.MustQuery(tt).Rows())
				output[i].Warn = testdata.ConvertRowsToStrings(testKit.MustQuery("show warnings").Rows())
			})
			testKit.MustQuery(tt).Check(testkit.Rows(output[i].Plan...))
			testKit.MustQuery("show warnings").Check(testkit.Rows(output[i].Warn...))
		}
	})
}

func TestIndexRead(t *testing.T) {
	testkit.RunTestUnderCascadesWithDomain(t, func(t *testing.T, testKit *testkit.TestKit, dom *domain.Domain, cascades, caller string) {
		testKit.MustExec("set @@session.tidb_executor_concurrency = 4;")
		testKit.MustExec("set @@session.tidb_hash_join_concurrency = 5;")
		testKit.MustExec("set @@session.tidb_distsql_scan_concurrency = 15;")

		testKit.MustExec("use test")
		testKit.MustExec("drop table if exists t, t1")
		testKit.MustExec("create table t (a int primary key, b int, c varchar(200), d datetime DEFAULT CURRENT_TIMESTAMP, e int, ts timestamp DEFAULT CURRENT_TIMESTAMP)")
		testKit.MustExec("create index b on t (b)")
		testKit.MustExec("create index d on t (d)")
		testKit.MustExec("create index e on t (e)")
		testKit.MustExec("create index b_c on t (b,c)")
		testKit.MustExec("create index ts on t (ts)")
		testKit.MustExec("create table t1 (a int, b int, index idx(a), index idxx(b))")

		// Default RPC encoding may cause statistics explain result differ and then the test unstable.
		testKit.MustExec("set @@tidb_enable_chunk_rpc = on")

		// This stats is generated by following format:
		// fill (a, b, c, e) as (i*100+j, i, i+j, i*100+j), i and j is dependent and range of this two are [0, 99].
		require.NoError(t, testkit.LoadTableStats("analyzesSuiteTestIndexReadT.json", dom))
		for i := 1; i < 16; i++ {
			testKit.MustExec(fmt.Sprintf("insert into t1 values(%v, %v)", i, i))
		}
		testKit.MustExec("analyze table t1")
		ctx := testKit.Session()
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
	testkit.RunTestUnderCascades(t, func(t *testing.T, testKit *testkit.TestKit, cascades, caller string) {
		testKit.MustExec("use test")
		testKit.MustExec("drop table if exists t, t1")
		testKit.MustExec("create table t (c1 int)")
		testKit.MustExec("create table t1 (c1 int)")
		testKit.MustExec("analyze table t, t1")
		var input, output []string
		analyzeSuiteData := GetAnalyzeSuiteData()
		analyzeSuiteData.LoadTestCases(t, &input, &output, cascades, caller)
		for i, tt := range input {
			ctx := testKit.Session()
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
	testkit.RunTestUnderCascades(t, func(t *testing.T, testKit *testkit.TestKit, cascades, caller string) {
		testKit.MustExec("use test")
		testKit.MustExec("drop table if exists t, t1, t2, t3")
		testKit.MustExec("create table t (a int, b int)")
		testKit.MustExec("create index a on t (a)")
		testKit.MustExec("create index b on t (b)")
		testKit.MustExec("insert into t (a,b) values (1,1),(1,2),(1,3),(1,4),(2,5),(2,6),(2,7),(2,8)")
		testKit.MustExec("analyze table t")

		testKit.MustExec("create table t1 (a int, b int)")
		testKit.MustExec("create index a on t1 (a)")
		testKit.MustExec("create index b on t1 (b)")
		testKit.MustExec("insert into t1 (a,b) values (1,1),(1,2),(1,3),(1,4),(2,5),(2,6),(2,7),(2,8)")

		testKit.MustExec("create table t2 (a int, b int)")
		testKit.MustExec("create index a on t2 (a)")
		testKit.MustExec("create index b on t2 (b)")
		testKit.MustExec("insert into t2 (a,b) values (1,1),(1,2),(1,3),(1,4),(2,5),(2,6),(2,7),(2,8)")
		testKit.MustExec("analyze table t2 index a")

		testKit.MustExec("create table t3 (a int, b int)")
		testKit.MustExec("create index a on t3 (a)")

		testKit.MustExec("set @@tidb_partition_prune_mode = 'static';")
		testKit.MustExec("create table t4 (a int, b int) partition by range (a) (partition p1 values less than (2), partition p2 values less than (3))")
		testKit.MustExec("create index a on t4 (a)")
		testKit.MustExec("create index b on t4 (b)")
		testKit.MustExec("insert into t4 (a,b) values (1,1),(1,2),(1,3),(1,4),(2,5),(2,6),(2,7),(2,8)")
		testKit.MustExec("analyze table t4")

		testKit.MustExec("create view v as select * from t")
		_, err := testKit.Exec("analyze table v")
		require.EqualError(t, err, "analyze view v is not supported now")
		testKit.MustExec("drop view v")

		testKit.MustExec("create sequence seq")
		_, err = testKit.Exec("analyze table seq")
		require.EqualError(t, err, "analyze sequence seq is not supported now")
		testKit.MustExec("drop sequence seq")

		var input, output []string
		analyzeSuiteData := GetAnalyzeSuiteData()
		analyzeSuiteData.LoadTestCases(t, &input, &output, cascades, caller)

		for i, tt := range input {
			ctx := testKit.Session()
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
	testkit.RunTestUnderCascadesWithDomain(t, func(t *testing.T, testKit *testkit.TestKit, dom *domain.Domain, cascades, caller string) {
		testKit.MustExec("use test")
		testKit.MustExec("create table t (a int, b int, index idx(a))")
		for i := range 10 {
			testKit.MustExec(fmt.Sprintf("insert into t values (%d,%d)", i, i))
		}
		h := dom.StatsHandle()
		err := statstestutil.HandleNextDDLEventWithTxn(h)
		require.NoError(t, err)
		require.NoError(t, h.DumpStatsDeltaToKV(true))
		testKit.MustExec("analyze table t all columns")
		testKit.MustExec("insert into t select * from t")
		testKit.MustExec("insert into t select * from t")
		testKit.MustExec("insert into t select * from t")
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
			testKit.Session().GetSessionVars().SetEnablePseudoForOutdatedStats(tt.EnablePseudoForOutdatedStats)
			statistics.RatioOfPseudoEstimate.Store(tt.RatioOfPseudoEstimate)
			plan := testKit.MustQuery(tt.SQL)
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
	testkit.RunTestUnderCascadesWithDomain(t, func(t *testing.T, testKit *testkit.TestKit, dom *domain.Domain, cascades, caller string) {
		testKit.MustExec("use test")
		testKit.MustExec("drop table if exists t")
		testKit.MustExec("create table t (a int, b int, index idx(a))")
		testKit.MustExec("insert into t values (null, null), (null, null)")
		testKit.MustExec("analyze table t all columns")
		var input []string
		var output [][]string
		analyzeSuiteData := GetAnalyzeSuiteData()
		analyzeSuiteData.LoadTestCases(t, &input, &output, cascades, caller)
		for i := range 2 {
			testdata.OnRecord(func() {
				output[i] = testdata.ConvertRowsToStrings(testKit.MustQuery(input[i]).Rows())
			})
			testKit.MustQuery(input[i]).Check(testkit.Rows(output[i]...))
		}
		h := dom.StatsHandle()
		h.Clear()
		require.NoError(t, h.Update(context.Background(), dom.InfoSchema()))
		for i := 2; i < 4; i++ {
			testdata.OnRecord(func() {
				output[i] = testdata.ConvertRowsToStrings(testKit.MustQuery(input[i]).Rows())
			})
			testKit.MustQuery(input[i]).Check(testkit.Rows(output[i]...))
		}
	})
}

func TestCorrelatedEstimation(t *testing.T) {
	testkit.RunTestUnderCascades(t, func(t *testing.T, testKit *testkit.TestKit, cascades, caller string) {
		testKit.MustExec("use test")
		testKit.MustExec("set sql_mode='STRICT_TRANS_TABLES'") // disable only full group by
		testKit.MustExec("create table t(a int, b int, c int, index idx(c,b,a))")
		testKit.MustExec("insert into t values(1,1,1), (2,2,2), (3,3,3), (4,4,4), (5,5,5), (6,6,6), (7,7,7), (8,8,8), (9,9,9),(10,10,10)")
		testKit.MustExec("analyze table t")
		var (
			input  []string
			output [][]string
		)
		analyzeSuiteData := GetAnalyzeSuiteData()
		analyzeSuiteData.LoadTestCases(t, &input, &output, cascades, caller)
		for i, tt := range input {
			rs := testKit.MustQuery(tt)
			testdata.OnRecord(func() {
				output[i] = testdata.ConvertRowsToStrings(rs.Rows())
			})
			rs.Check(testkit.Rows(output[i]...))
		}
	})
}

func testInconsistentEstimation(t *testing.T, testKit *testkit.TestKit, dom *domain.Domain, cascades, caller string) {
	testKit.MustExec("use test")
	testKit.MustExec("create table t(a int, b int, c int, index ab(a,b), index ac(a,c))")
	testKit.MustExec("insert into t values (1,1,1), (1000,1000,1000)")
	for range 10 {
		testKit.MustExec("insert into t values (5,5,5), (10,10,10)")
	}
	testKit.MustExec("set @@tidb_analyze_version=1")
	testKit.MustExec("analyze table t all columns with 2 buckets ")
	// Force using the histogram to estimate.
	testKit.MustExec("update mysql.stats_histograms set stats_ver = 0")
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
		plan := testKit.MustQuery(sql)
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

func TestIssue9562(t *testing.T) {
	testkit.RunTestUnderCascades(t, func(t *testing.T, testKit *testkit.TestKit, cascades, caller string) {
		testKit.MustExec("use test")
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
					testKit.MustExec(tt)
				}
				testdata.OnRecord(func() {
					output[i].SQL = ts
					if j == len(ts)-1 {
						output[i].Plan = testdata.ConvertRowsToStrings(testKit.MustQuery(tt).Rows())
					}
				})
				if j == len(ts)-1 {
					testKit.MustQuery(tt).Check(testkit.Rows(output[i].Plan...))
				}
			}
		}
	})
}

func TestLimitCrossEstimation(t *testing.T) {
	testkit.RunTestUnderCascades(t, func(t *testing.T, testKit *testkit.TestKit, cascades, caller string) {
		testKit.MustExec("set @@session.tidb_executor_concurrency = 4;")
		testKit.MustExec("set @@session.tidb_hash_join_concurrency = 5;")
		testKit.MustExec("set @@session.tidb_distsql_scan_concurrency = 15;")
		testKit.MustExec("use test")
		testKit.MustExec("drop table if exists t")
		testKit.MustExec("create table t(a int primary key, b int not null, c int not null default 0, index idx_bc(b, c))")
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
					testKit.MustExec(tt)
				}
				testdata.OnRecord(func() {
					output[i].SQL = ts
					if j == len(ts)-1 {
						output[i].Plan = testdata.ConvertRowsToStrings(testKit.MustQuery(tt).Rows())
					}
				})
				if j == len(ts)-1 {
					testKit.MustQuery(tt).Check(testkit.Rows(output[i].Plan...))
				}
			}
		}
	})
}

func TestLowSelIndexGreedySearch(t *testing.T) {
	testkit.RunTestUnderCascadesWithDomain(t, func(t *testing.T, testKit *testkit.TestKit, dom *domain.Domain, cascades, caller string) {
		testKit.MustExec("use test")
		testKit.MustExec(`set tidb_opt_limit_push_down_threshold=0`)
		testKit.MustExec("drop table if exists t")
		testKit.MustExec("create table t (a varchar(32) default null, b varchar(10) default null, c varchar(12) default null, d varchar(32) default null, e bigint(10) default null, key idx1 (d,a), key idx2 (a,c), key idx3 (c,b), key idx4 (e))")
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
				output[i].Plan = testdata.ConvertRowsToStrings(testKit.MustQuery(tt).Rows())
			})
			testKit.MustQuery(tt).Check(testkit.Rows(output[i].Plan...))
		}
	})
}

func TestTiFlashCostModel(t *testing.T) {
	testkit.RunTestUnderCascadesWithDomain(t, func(t *testing.T, testKit *testkit.TestKit, dom *domain.Domain, cascades, caller string) {
		testKit.MustExec("use test")
		testKit.MustExec("create table t (a int, b int, c int, primary key(a))")
		testKit.MustExec("insert into t values(1,1,1), (2,2,2), (3,3,3)")

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
					testKit.MustExec(tt)
				}
				testdata.OnRecord(func() {
					if j == len(ts)-1 {
						output[i] = testdata.ConvertRowsToStrings(testKit.MustQuery(tt).Rows())
					}
				})
				if j == len(ts)-1 {
					testKit.MustQuery(tt).Check(testkit.Rows(output[i]...))
				}
			}
		}
	})
}

func TestIndexEqualUnknown(t *testing.T) {
	testkit.RunTestUnderCascadesWithDomain(t, func(t *testing.T, testKit *testkit.TestKit, dom *domain.Domain, cascades, caller string) {
		testKit.MustExec("use test")
		testKit.MustExec("drop table if exists t, t1")
		testKit.Session().GetSessionVars().EnableClusteredIndex = vardef.ClusteredIndexDefModeIntOnly
		testKit.MustExec("CREATE TABLE t(a bigint(20) NOT NULL, b bigint(20) NOT NULL, c bigint(20) NOT NULL, PRIMARY KEY (a,c,b), KEY (b))")
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
				output[i].Plan = testdata.ConvertRowsToStrings(testKit.MustQuery(tt).Rows())
			})
			testKit.MustQuery(tt).Check(testkit.Rows(output[i].Plan...))
		}
	})
}

func TestLimitIndexEstimation(t *testing.T) {
	testkit.RunTestUnderCascadesWithDomain(t, func(t *testing.T, testKit *testkit.TestKit, dom *domain.Domain, cascades, caller string) {
		testKit.MustExec("use test")
		testKit.MustExec("drop table if exists t")
		testKit.MustExec("create table t(a int, b int, key idx_a(a), key idx_b(b))")
		testKit.MustExec("set session tidb_enable_extended_stats = on")
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
				output[i].Plan = testdata.ConvertRowsToStrings(testKit.MustQuery(tt).Rows())
			})
			testKit.MustQuery(tt).Check(testkit.Rows(output[i].Plan...))
		}
	})
}

func TestIndexJoinPreferIndexCoversMoreJoinKeyCols(t *testing.T) {
	testkit.RunTestUnderCascadesWithDomain(t, func(t *testing.T, testKit *testkit.TestKit, dom *domain.Domain, cascades, caller string) {
		testKit.MustExec("use test")
		testKit.MustExec("drop table if exists mp, ab")
		testKit.MustExec("CREATE TABLE mp (\n  col1 BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,\n  col2 BIGINT NOT NULL DEFAULT '0',\n  col3 INT UNSIGNED NOT NULL DEFAULT '0',\n  col4 INT UNSIGNED NOT NULL DEFAULT '0',\n  col5 VARCHAR(30) NOT NULL DEFAULT '',\n  col6 VARCHAR(64) NOT NULL DEFAULT '',\n  col7 int unsigned NOT NULL DEFAULT '0',\n  PRIMARY KEY (col1),\n  KEY `idx_1` (`col2`,`col6`,`col7`),\n  KEY `idx_2`(`col3`, `col5`, `col6`, `col4`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;")
		testKit.MustExec("CREATE TABLE ab (\n  col1 BIGINT NOT NULL,\n  col2 VARCHAR(64) NOT NULL,\n  col3 VARCHAR(60) NOT NULL,\n  PRIMARY KEY (col1),\n  UNIQUE KEY idx_1 (col3, col2)\n)ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;")
		// Values in column a are from 1 to 1000000, values in column b are from 1000000 to 1,
		// these 2 columns are strictly correlated in reverse order.
		require.NoError(t, testkit.LoadTableStats("ab.simplified.json", dom))
		require.NoError(t, testkit.LoadTableStats("mp.simplified.json", dom))
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
				output[i].Plan = testdata.ConvertRowsToStrings(testKit.MustQuery(tt).Rows())
			})
			testKit.MustQuery(tt).Check(testkit.Rows(output[i].Plan...))
		}
	})
}
