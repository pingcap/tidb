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

package hint

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/domain/infosync"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/store/mockstore"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testdata"
	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
	"github.com/stretchr/testify/require"
)

func TestReadFromStorageHint(t *testing.T) {
	testkit.RunTestUnderCascadesWithDomain(t, func(t *testing.T, testKit *testkit.TestKit, dom *domain.Domain, cascades, caller string) {
		testKit.MustExec("use test")
		testKit.MustExec("drop table if exists t, tt, ttt")
		testKit.MustExec("set session tidb_allow_mpp=OFF")
		// since allow-mpp is adjusted to false, there will be no physical plan if TiFlash cop is banned.
		testKit.MustExec("set @@session.tidb_allow_tiflash_cop=ON")
		testKit.MustExec("create table t(a int, b int, index ia(a))")
		testKit.MustExec("create table tt(a int, b int, primary key(a))")
		testKit.MustExec("create table ttt(a int, primary key (a desc))")

		// Create virtual tiflash replica info.
		testkit.SetTiFlashReplica(t, dom, "test", "t")
		testkit.SetTiFlashReplica(t, dom, "test", "tt")
		testkit.SetTiFlashReplica(t, dom, "test", "ttt")

		var input []string
		var output []struct {
			SQL  string
			Plan []string
			Warn []string
		}
		integrationSuiteData := GetIntegrationSuiteData()
		integrationSuiteData.LoadTestCases(t, &input, &output, cascades, caller)
		for i, tt := range input {
			testdata.OnRecord(func() {
				output[i].SQL = tt
				output[i].Plan = testdata.ConvertRowsToStrings(testKit.MustQuery(tt).Rows())
				output[i].Warn = testdata.ConvertSQLWarnToStrings(testKit.Session().GetSessionVars().StmtCtx.GetWarnings())
			})
			res := testKit.MustQuery(tt)
			res.Check(testkit.Rows(output[i].Plan...))
			require.Equal(t, output[i].Warn, testdata.ConvertSQLWarnToStrings(testKit.Session().GetSessionVars().StmtCtx.GetWarnings()))
		}
	})
}

func TestAllViewHintType(t *testing.T) {
	testkit.RunTestUnderCascades(t, func(t *testing.T, testKit *testkit.TestKit, cascades, caller string) {
		testKit.MustExec("use test")
		testKit.MustExec("set @@session.tidb_allow_mpp=ON")
		testKit.MustExec("set @@session.tidb_isolation_read_engines='tiflash, tikv'")
		testKit.MustExec("drop view if exists v, v1, v2, v3, v4, v5, v6, v7, v8, v9, v10, v11, v12")
		testKit.MustExec("drop table if exists t, t1, t2, t4, t3, t5")
		testKit.MustExec("create table t(a int not null, b int, index idx_a(a));")
		testKit.MustExec("create table t1(a int not null, b int, index idx_a(a));")
		testKit.MustExec("create table t2(a int, b int, index idx_a(a));")
		testKit.MustExec("create table t3(a int, b int, index idx_a(a));")
		testKit.MustExec("create table t4(a int, b int, index idx_a(a));")
		testKit.MustExec("create table t5(a int, b int, index idx_a(a), index idx_b(b));")

		// Create virtual tiflash replica info.
		dom := domain.GetDomain(testKit.Session())
		is := dom.InfoSchema()
		tbl, err := is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("t"))
		require.NoError(t, err)
		tbl.Meta().TiFlashReplica = &model.TiFlashReplicaInfo{
			Count:     1,
			Available: true,
		}

		testKit.MustExec("create definer='root'@'localhost' view v as select t.a, t.b from t join t1 on t.a = t1.a;")
		testKit.MustExec("create definer='root'@'localhost' view v1 as select t2.a, t2.b from t2 join t3 join v where t2.b = t3.b and t3.a = v.a;")
		testKit.MustExec("create definer='root'@'localhost' view v2 as select t.a, t.b from t join (select count(*) as a from t1 join v1 on t1.b=v1.b group by v1.a) tt on t.a = tt.a;")
		testKit.MustExec("create definer='root'@'localhost' view v3 as select * from t5 where a > 1 and b < 2;")
		testKit.MustExec("create definer='root'@'localhost' view v4 as select * from t5 where a > 1 or b < 2;")
		testKit.MustExec("create definer='root'@'localhost' view v5 as SELECT * FROM t WHERE EXISTS (SELECT 1 FROM t1 WHERE t1.b = t.b);")
		testKit.MustExec("create definer='root'@'localhost' view v6 as select * from t1 where t1.a < (select sum(t2.a) from t2 where t2.b = t1.b);")
		testKit.MustExec("create definer='root'@'localhost' view v7 as WITH CTE AS (SELECT * FROM t WHERE t.a < 60) SELECT * FROM CTE WHERE CTE.a <18 union select * from cte where cte.b > 1;")
		testKit.MustExec("create definer='root'@'localhost' view v8 as WITH CTE1 AS (SELECT b FROM t1), CTE2 AS (WITH CTE3 AS (SELECT a FROM t2), CTE4 AS (SELECT a FROM t3) SELECT CTE3.a FROM CTE3, CTE4) SELECT b FROM CTE1, CTE2 union select * from CTE1;")
		testKit.MustExec("create definer='root'@'localhost' view v9 as select sum(a) from t;")
		testKit.MustExec("create definer='root'@'localhost' view v10 as SELECT * FROM t WHERE a > 10 ORDER BY b LIMIT 1;")
		testKit.MustExec("create definer='root'@'localhost' view v11 as select a, sum(b) from t group by a")
		testKit.MustExec("create definer='root'@'localhost' view v12 as select t.a, t.b from t join t t1 on t.a = t1.a;")

		var input []string
		var output []struct {
			SQL  string
			Plan []string
			Warn []string
		}
		integrationSuiteData := GetIntegrationSuiteData()
		integrationSuiteData.LoadTestCases(t, &input, &output, cascades, caller)
		for i, tt := range input {
			testdata.OnRecord(func() {
				output[i].SQL = tt
				output[i].Plan = testdata.ConvertRowsToStrings(testKit.MustQuery(tt).Rows())
				output[i].Warn = testdata.ConvertSQLWarnToStrings(testKit.Session().GetSessionVars().StmtCtx.GetWarnings())
			})
			res := testKit.MustQuery(tt)
			res.Check(testkit.Rows(output[i].Plan...))
			require.Equal(t, output[i].Warn, testdata.ConvertSQLWarnToStrings(testKit.Session().GetSessionVars().StmtCtx.GetWarnings()))
		}
	})
}

func TestJoinHintCompatibility(t *testing.T) {
	testkit.RunTestUnderCascadesWithDomain(t, func(t *testing.T, testKit *testkit.TestKit, dom *domain.Domain, cascades, caller string) {
		testKit.MustExec("use test")
		testKit.MustExec("set @@session.tidb_allow_mpp=ON")
		testKit.MustExec("set @@session.tidb_isolation_read_engines='tiflash, tikv'")
		testKit.MustExec("drop view if exists v, v1, v2")
		testKit.MustExec("drop table if exists t, t1, t2, t3, t4, t5, t6, t7, t8, t9;")
		testKit.MustExec("create table t(a int not null, b int, index idx_a(a), index idx_b(b));")
		testKit.MustExec("create table t1(a int not null, b int, index idx_a(a), index idx_b(b));")
		testKit.MustExec("create table t2(a int, b int, index idx_a(a), index idx_b(b));")
		testKit.MustExec("create table t3(a int, b int, index idx_a(a), index idx_b(b));")
		testKit.MustExec("create table t4(a int, b int, index idx_a(a), index idx_b(b));")
		testKit.MustExec("create table t5(a int, b int, index idx_a(a), index idx_b(b));")
		testKit.MustExec("create table t6(a int, b int, index idx_a(a), index idx_b(b));")
		testKit.MustExec("create table t7(a int, b int, index idx_a(a), index idx_b(b)) partition by hash(a) partitions 4;")
		testKit.MustExec("create table t8(a int, b int, index idx_a(a), index idx_b(b)) partition by hash(a) partitions 4;")
		testKit.MustExec("create table t9(a int, b int, index idx_a(a), index idx_b(b)) partition by hash(a) partitions 4;")
		testKit.MustExec("analyze table t7, t8, t9")

		// Create virtual tiflash replica info.
		testkit.SetTiFlashReplica(t, dom, "test", "t4")
		testkit.SetTiFlashReplica(t, dom, "test", "t5")
		testkit.SetTiFlashReplica(t, dom, "test", "t6")

		testKit.MustExec("create definer='root'@'localhost' view v as select /*+ leading(t1), inl_join(t1) */ t.a from t join t1 join t2 where t.a = t1.a and t1.b = t2.b;")
		testKit.MustExec("create definer='root'@'localhost' view v1 as select /*+ leading(t2), merge_join(t) */ t.a from t join t1 join t2 where t.a = t1.a and t1.b = t2.b;")
		testKit.MustExec("create definer='root'@'localhost' view v2 as select t.a from t join t1 join t2 where t.a = t1.a and t1.b = t2.b;")

		var input []string
		var output []struct {
			SQL  string
			Plan []string
			Warn []string
		}
		integrationSuiteData := GetIntegrationSuiteData()
		integrationSuiteData.LoadTestCases(t, &input, &output, cascades, caller)
		for i, tt := range input {
			testdata.OnRecord(func() {
				output[i].SQL = tt
				output[i].Plan = testdata.ConvertRowsToStrings(testKit.MustQuery(tt).Rows())
				output[i].Warn = testdata.ConvertSQLWarnToStrings(testKit.Session().GetSessionVars().StmtCtx.GetWarnings())
			})
			res := testKit.MustQuery(tt)
			res.Check(testkit.Rows(output[i].Plan...))
			require.Equal(t, output[i].Warn, testdata.ConvertSQLWarnToStrings(testKit.Session().GetSessionVars().StmtCtx.GetWarnings()))
		}
	})
}

func TestReadFromStorageHintAndIsolationRead(t *testing.T) {
	testkit.RunTestUnderCascadesWithDomain(t, func(t *testing.T, testKit *testkit.TestKit, dom *domain.Domain, cascades, caller string) {
		testKit.MustExec("use test")
		testKit.MustExec("drop table if exists t, tt, ttt")
		testKit.MustExec("create table t(a int, b int, index ia(a))")
		testKit.MustExec("set @@session.tidb_isolation_read_engines=\"tikv\"")

		// Create virtual tiflash replica info.
		testkit.SetTiFlashReplica(t, dom, "test", "t")

		var input []string
		var output []struct {
			SQL  string
			Plan []string
			Warn []string
		}
		integrationSuiteData := GetIntegrationSuiteData()
		integrationSuiteData.LoadTestCases(t, &input, &output, cascades, caller)
		for i, tt := range input {
			testdata.OnRecord(func() {
				output[i].SQL = tt
				output[i].Plan = testdata.ConvertRowsToStrings(testKit.MustQuery(tt).Rows())
				output[i].Warn = testdata.ConvertSQLWarnToStrings(testKit.Session().GetSessionVars().StmtCtx.GetWarnings())
			})
			res := testKit.MustQuery(tt)
			res.Check(testkit.Rows(output[i].Plan...))
			require.Equal(t, output[i].Warn, testdata.ConvertSQLWarnToStrings(testKit.Session().GetSessionVars().StmtCtx.GetWarnings()))
		}
	})
}

func TestIsolationReadTiFlashUseIndexHint(t *testing.T) {
	testkit.RunTestUnderCascadesAndDomainWithSchemaLease(t, 200*time.Millisecond, []mockstore.MockTiKVStoreOption{mockstore.WithMockTiFlash(2)}, func(t *testing.T, tk *testkit.TestKit, dom *domain.Domain, cascades, caller string) {
		tiflash := infosync.NewMockTiFlash()
		infosync.SetMockTiFlash(tiflash)
		defer func() {
			tiflash.Lock()
			tiflash.StatusServer.Close()
			tiflash.Unlock()
		}()

		testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/ddl/MockCheckColumnarIndexProcess", `return(1)`)

		tk.MustExec("use test")
		tk.MustExec("drop table if exists t")
		tk.MustExec("create table t(a int, vec vector(3), PRIMARY KEY (`a`) /*T![clustered_index] CLUSTERED */, index idx(a), vector index idx_vec ((VEC_COSINE_DISTANCE(`vec`))) USING HNSW);")
		tblInfo, err := dom.InfoSchema().TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("t"))
		require.NoError(t, err)
		err = domain.GetDomain(tk.Session()).DDLExecutor().UpdateTableReplicaInfo(tk.Session(), tblInfo.Meta().ID, true)
		require.NoError(t, err)

		tk.MustExec("set @@session.tidb_isolation_read_engines=\"tiflash\"")
		var input []string
		var output []struct {
			SQL  string
			Plan []string
			Warn []string
		}
		integrationSuiteData := GetIntegrationSuiteData()
		integrationSuiteData.LoadTestCases(t, &input, &output, cascades, caller)
		for i, tt := range input {
			testdata.OnRecord(func() {
				output[i].SQL = tt
				output[i].Plan = testdata.ConvertRowsToStrings(tk.MustQuery(tt).Rows())
				output[i].Warn = testdata.ConvertSQLWarnToStrings(tk.Session().GetSessionVars().StmtCtx.GetWarnings())
			})
			res := tk.MustQuery(tt)
			res.Check(testkit.Rows(output[i].Plan...))
			require.Equal(t, output[i].Warn, testdata.ConvertSQLWarnToStrings(tk.Session().GetSessionVars().StmtCtx.GetWarnings()))
		}
	})
}

func TestOptimizeHintOnPartitionTable(t *testing.T) {
	testkit.RunTestUnderCascades(t, func(t *testing.T, testKit *testkit.TestKit, cascades, caller string) {
		testKit.MustExec("use test")
		testKit.MustExec("drop table if exists t")
		testKit.MustExec(`create table t (
					a int, b int, c varchar(20),
					primary key(a), key(b), key(c)
				) partition by range columns(a) (
					partition p0 values less than(6),
					partition p1 values less than(11),
					partition p2 values less than(16));`)
		testKit.MustExec(`insert into t values (1,1,"1"), (2,2,"2"), (8,8,"8"), (11,11,"11"), (15,15,"15")`)
		testKit.MustExec("set @@tidb_enable_index_merge = off")
		defer func() {
			testKit.MustExec("set @@tidb_enable_index_merge = on")
		}()

		// Create virtual tiflash replica info.
		dom := domain.GetDomain(testKit.Session())
		is := dom.InfoSchema()
		tbl, err := is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("t"))
		require.NoError(t, err)
		tbl.Meta().TiFlashReplica = &model.TiFlashReplicaInfo{
			Count:     1,
			Available: true,
		}

		testKit.MustExec(`set @@tidb_partition_prune_mode='` + string(variable.Static) + `'`)

		var input []string
		var output []struct {
			SQL  string
			Plan []string
			Warn []string
		}
		integrationSuiteData := GetIntegrationSuiteData()
		integrationSuiteData.LoadTestCases(t, &input, &output, cascades, caller)
		for i, tt := range input {
			testdata.OnRecord(func() {
				output[i].SQL = tt
				output[i].Plan = testdata.ConvertRowsToStrings(testKit.MustQuery("explain format = 'plan_tree' " + tt).Rows())
				output[i].Warn = testdata.ConvertRowsToStrings(testKit.MustQuery("show warnings").Rows())
			})
			testKit.MustQuery("explain format = 'plan_tree' " + tt).Check(testkit.Rows(output[i].Plan...))
			testKit.MustQuery("show warnings").Check(testkit.Rows(output[i].Warn...))
		}
		// Verify hint parsing with an explicitly disabled deadline, even when statement
		// preparation takes longer than the old 10 ms timeout.
		testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/session/mockStmtSlow", "return(20)")
		for warnCount, sql := range []string{
			"SELECT /*+ MAX_EXECUTION_TIME(0) */ /* sleep */ SLEEP(0.001)",
			"SELECT /*+ MAX_EXECUTION_TIME(0), dtc(name=tt) */ /* sleep */ SLEEP(0.001)",
			"SELECT /*+ MAX_EXECUTION_TIME(0), dtc(name=tt) unknow(t1,t2) */ /* sleep */ SLEEP(0.001)",
		} {
			testKit.MustQuery(sql).Check(testkit.Rows("0"))
			stmtCtx := testKit.Session().GetSessionVars().StmtCtx
			require.True(t, stmtCtx.HasMaxExecutionTime, "sql: %s", sql)
			require.Zero(t, stmtCtx.MaxExecutionTime, "sql: %s", sql)
			require.Len(t, stmtCtx.GetWarnings(), warnCount, "sql: %s", sql)
		}
	})
}

func TestHints(t *testing.T) {
	testkit.RunTestUnderCascades(t, func(t *testing.T, testKit *testkit.TestKit, cascades, caller string) {
		testKit.MustExec("use test")
		testKit.MustExec("create table t1 (a int);")
		testKit.MustExec("create table t2 (a int);")
		testKit.MustExec("create table t3 (a int);")
		var input []string
		var output []struct {
			SQL  string
			Plan []string
			Warn []string
		}
		integrationSuiteData := GetIntegrationSuiteData()
		integrationSuiteData.LoadTestCases(t, &input, &output, cascades, caller)
		for i, tt := range input {
			testdata.OnRecord(func() {
				output[i].SQL = tt
				output[i].Plan = testdata.ConvertRowsToStrings(testKit.MustQuery("explain format = 'plan_tree' " + tt).Rows())
				output[i].Warn = testdata.ConvertRowsToStrings(testKit.MustQuery("show warnings").Rows())
			})
			testKit.MustQuery("explain format = 'plan_tree' " + tt).Check(testkit.Rows(output[i].Plan...))
			testKit.MustQuery("show warnings").Check(testkit.Rows(output[i].Warn...))
		}
	})
}

func TestQBHintHandlerDuplicateObjects(t *testing.T) {
	testkit.RunTestUnderCascades(t, func(t *testing.T, testKit *testkit.TestKit, cascades, caller string) {
		testKit.MustExec("use test")
		testKit.MustExec("CREATE TABLE t_employees  (id INT NOT NULL AUTO_INCREMENT PRIMARY KEY, fname VARCHAR(25) NOT NULL, lname VARCHAR(25) NOT NULL, store_id INT NOT NULL, department_id INT NOT NULL);")
		testKit.MustExec("ALTER TABLE t_employees ADD INDEX idx(department_id);")

		var input []string
		var output []struct {
			SQL  string
			Plan []string
			Warn []string
		}
		integrationSuiteData := GetIntegrationSuiteData()
		integrationSuiteData.LoadTestCases(t, &input, &output, cascades, caller)
		for i, tt := range input {
			testdata.OnRecord(func() {
				output[i].SQL = tt
				output[i].Plan = testdata.ConvertRowsToStrings(testKit.MustQuery(tt).Rows())
				output[i].Warn = testdata.ConvertSQLWarnToStrings(testKit.Session().GetSessionVars().StmtCtx.GetWarnings())
			})
			res := testKit.MustQuery(tt)
			res.Check(testkit.Rows(output[i].Plan...))
			require.Equal(t, output[i].Warn, testdata.ConvertSQLWarnToStrings(testKit.Session().GetSessionVars().StmtCtx.GetWarnings()))
		}
	})
}

func TestOptimizerCostFactorHints(t *testing.T) {
	testkit.RunTestUnderCascades(t, func(t *testing.T, testKit *testkit.TestKit, cascades, caller string) {
		testKit.MustExec("use test")
		testKit.MustExec("drop table if exists t")
		testKit.MustExec("create table t(a int, b int, c int, primary key (a), key(b))")
		// Insert some data
		testKit.MustExec("insert into t values (1,1,1),(2,2,2),(3,3,3),(4,4,4),(5,5,5)")
		// Analyze table to update statistics
		testKit.MustExec("analyze table t")

		// Test tableFullScan cost factor increase via hint
		// Set index scan cost factor variable to isolate testing to TableFullScan
		testKit.MustExec("set @@session.tidb_opt_index_scan_cost_factor=100")
		rs := testKit.MustQuery("explain format=verbose select * from t").Rows()
		planCost1, err1 := strconv.ParseFloat(rs[0][2].(string), 64)
		require.Nil(t, err1)
		rs = testKit.MustQuery("explain format=verbose select /*+ SET_VAR(tidb_opt_table_full_scan_cost_factor=2) */ * from t").Rows()
		planCost2, err2 := strconv.ParseFloat(rs[0][2].(string), 64)
		require.Nil(t, err2)
		// 1st query should be cheaper than 2nd query
		require.Less(t, planCost1, planCost2)
		// Reset to index scan to default
		testKit.MustExec("set @@session.tidb_opt_index_scan_cost_factor=1")

		// Test tableReader cost factor increase via hint
		// Set index scan cost factor variable to isolate testing to TableReader
		testKit.MustExec("set @@session.tidb_opt_index_scan_cost_factor=100")
		rs = testKit.MustQuery("explain format=verbose select * from t").Rows()
		planCost1, err1 = strconv.ParseFloat(rs[0][2].(string), 64)
		require.Nil(t, err1)
		rs = testKit.MustQuery("explain format=verbose select /*+ SET_VAR(tidb_opt_table_reader_cost_factor=2) */ * from t").Rows()
		planCost2, err2 = strconv.ParseFloat(rs[0][2].(string), 64)
		require.Nil(t, err2)
		// 1st query should be cheaper than 2nd query
		require.Less(t, planCost1, planCost2)
		// Reset to index scan variable to default
		testKit.MustExec("set @@session.tidb_opt_index_scan_cost_factor=1")

		// Test tableRangeScan cost factor increase
		// Set index scan and table full scan cost factor variables to isolate testing to TableRangeScan
		testKit.MustExec("set @@session.tidb_opt_index_scan_cost_factor=100")
		testKit.MustExec("set @@session.tidb_opt_table_full_scan_cost_factor=100")
		rs = testKit.MustQuery("explain format=verbose select * from t where a > 3").Rows()
		planCost1, err1 = strconv.ParseFloat(rs[0][2].(string), 64)
		require.Nil(t, err1)
		rs = testKit.MustQuery("explain format=verbose select /*+ SET_VAR(tidb_opt_table_range_scan_cost_factor=2) */ * from t where a > 3").Rows()
		planCost2, err2 = strconv.ParseFloat(rs[0][2].(string), 64)
		require.Nil(t, err2)
		// 1st query should be cheaper than 2nd query
		require.Less(t, planCost1, planCost2)
		// Reset to index and table full scan variables to default
		testKit.MustExec("set @@session.tidb_opt_index_scan_cost_factor=1")
		testKit.MustExec("set @@session.tidb_opt_table_full_scan_cost_factor=1")

		// Test IndexScan cost factor increase
		// Increase table scan cost factor to isolate testing to IndexScan
		testKit.MustExec("set @@session.tidb_opt_table_full_scan_cost_factor=100")
		rs = testKit.MustQuery("explain format=verbose select b from t where b > 3").Rows()
		planCost1, err1 = strconv.ParseFloat(rs[0][2].(string), 64)
		require.Nil(t, err1)
		rs = testKit.MustQuery("explain format=verbose select /*+ SET_VAR(tidb_opt_index_scan_cost_factor=2) */ b from t where b > 3").Rows()
		planCost2, err2 = strconv.ParseFloat(rs[0][2].(string), 64)
		require.Nil(t, err2)
		// 1st query should be cheaper than 2nd query
		require.Less(t, planCost1, planCost2)
		// Reset table full scan variable to default
		testKit.MustExec("set @@session.tidb_opt_table_full_scan_cost_factor=1")

		// Test IndexReadercost factor increase
		// Increase table scan cost factor variable to isolate testing to IndexReader
		testKit.MustExec("set @@session.tidb_opt_table_full_scan_cost_factor=100")
		rs = testKit.MustQuery("explain format=verbose select b from t where b > 3").Rows()
		planCost1, err1 = strconv.ParseFloat(rs[0][2].(string), 64)
		require.Nil(t, err1)
		rs = testKit.MustQuery("explain format=verbose select  /*+ SET_VAR(tidb_opt_index_reader_cost_factor=2) */ b from t where b > 3").Rows()
		planCost2, err2 = strconv.ParseFloat(rs[0][2].(string), 64)
		require.Nil(t, err2)
		// 1st query should be cheaper than 2nd query
		require.Less(t, planCost1, planCost2)
		// Reset table scan cost factor variable to default
		testKit.MustExec("set @@session.tidb_opt_table_full_scan_cost_factor=1")
	})
}

func TestHintTableNameWithoutDBName(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("create database hintdb1")
	tk.MustExec("create table hintdb1.j (ns bigint, id bigint, primary key (ns, id) clustered)")
	tk.MustExec("create table hintdb1.c (oid bigint, hlc bigint, ns bigint, sjid bigint, primary key (oid, hlc) clustered, key ns_sjid (ns, sjid))")
	tk.MustExec("create table hintdb1.u (a int, b int, unique key ub (b))")

	planHas := func(sql, op string) bool {
		for _, row := range tk.MustQuery("explain format = 'brief' " + sql).Rows() {
			if strings.Contains(fmt.Sprint(row...), op) {
				return true
			}
		}
		return false
	}
	join := " j.id, %s.oid from hintdb1.j left join hintdb1.c %s on %s.ns = j.ns and %s.sjid = j.id where j.ns = 1 and j.id in (1, 2)"
	joinSQL := func(hint, alias string) string {
		name := "c"
		if alias != "" {
			name = alias
		}
		return "select /*+ " + hint + " */" + fmt.Sprintf(join, name, alias, name, name)
	}

	// The tables are in hintdb1, which is not the current database (first "test", then none).
	for _, useDB := range []bool{true, false} {
		if useDB {
			tk.MustExec("use test")
		} else {
			tk.RefreshSession()
			require.Equal(t, "", tk.Session().GetSessionVars().CurrentDB)
		}
		// A hint table name without a database name matches the table or alias in any database.
		for _, tc := range []struct{ hint, alias string }{
			{"hash_join(c)", ""},
			{"hash_join(x)", "x"},
			{"hash_join_build(c)", ""},
		} {
			sql := joinSQL(tc.hint, tc.alias)
			require.True(t, planHas(sql, "HashJoin"), sql)
			tk.MustQuery("show warnings").Check(testkit.Rows())
		}
		require.True(t, planHas(joinSQL("inl_join(c)", ""), "IndexJoin"))
		tk.MustQuery("show warnings").Check(testkit.Rows())
		require.True(t, planHas(joinSQL("merge_join(c)", ""), "MergeJoin"))
		tk.MustQuery("show warnings").Check(testkit.Rows())

		// Index hints, including the point-get fast path.
		require.True(t, planHas("select /*+ use_index(c, ns_sjid) */ oid from hintdb1.c where ns = 1", "ns_sjid"))
		tk.MustQuery("show warnings").Check(testkit.Rows())
		require.True(t, planHas("select /*+ use_index(x, primary) */ oid from hintdb1.c x where ns = 1", "TableFullScan"))
		tk.MustQuery("show warnings").Check(testkit.Rows())
		require.True(t, planHas("select * from hintdb1.u where b = 1", "Point_Get"))
		require.False(t, planHas("select /*+ ignore_index(u, ub) */ * from hintdb1.u where b = 1", "Point_Get"))
		tk.MustQuery("show warnings").Check(testkit.Rows())

		// An explicit database name must still match exactly.
		require.False(t, planHas(joinSQL("hash_join(test.c)", ""), "HashJoin"))
		tk.MustQuery("show warnings").CheckContain("There are no matching table names for (c)")
		require.True(t, planHas(joinSQL("hash_join(hintdb1.c)", ""), "HashJoin"))
		tk.MustQuery("show warnings").Check(testkit.Rows())
	}

	// The same table name in two databases: the table in the current database wins, as before,
	// and with neither in the current database the name is ambiguous and the hint does not match.
	tk.MustExec("create database hintdb2")
	tk.MustExec("create table hintdb1.t (a int, key ia1 (a))")
	tk.MustExec("create table hintdb2.t (a int, key ia2 (a))")
	twoSchemas := "select /*+ %s */ * from hintdb1.t join hintdb2.t on hintdb1.t.a = hintdb2.t.a"
	buildSideIndex := func(hint string) string {
		rows := tk.MustQuery("explain format = 'brief' " + fmt.Sprintf(twoSchemas, hint)).Rows()
		require.Contains(t, fmt.Sprint(rows[0]), "HashJoin")
		require.Contains(t, fmt.Sprint(rows[1]), "(Build)")
		return fmt.Sprint(rows[2])
	}
	// `t` must match only hintdb1.t (index ia1). If it also matched hintdb2.t, both join sides would be
	// hinted, the hints would conflict and a warning would be raised.
	tk.MustExec("use hintdb1")
	require.Contains(t, buildSideIndex("hash_join_build(t)"), "index:ia1(a)")
	tk.MustQuery("show warnings").Check(testkit.Rows())
	require.Contains(t, buildSideIndex("hash_join_probe(t)"), "index:ia2(a)")
	tk.MustQuery("show warnings").Check(testkit.Rows())
	tk.MustExec("use test")
	tk.MustQuery("explain format = 'brief' " + fmt.Sprintf(twoSchemas, "hash_join_build(t)"))
	tk.MustQuery("show warnings").CheckContain("There are no matching table names for (t)")
	require.Contains(t, buildSideIndex("hash_join_build(hintdb2.t)"), "index:ia2(a)")
	tk.MustQuery("show warnings").Check(testkit.Rows())

	// Unlike MySQL's alias-only hint matching, TiDB keeps the current-database
	// preference for duplicate table names. Index hints must resolve identically
	// to join hints, without leaking an index name to the other database's table.
	for _, currentDB := range []string{"hintdb1", "hintdb2"} {
		tk.MustExec("use " + currentDB)
		indexName := "ia1"
		if currentDB == "hintdb2" {
			indexName = "ia2"
		}
		sql := fmt.Sprintf(twoSchemas, "use_index(t, "+indexName+")")
		require.True(t, planHas(sql, "index:ia1(a)"), sql)
		require.True(t, planHas(sql, "index:ia2(a)"), sql)
		tk.MustQuery("show warnings").Check(testkit.Rows())
	}
	for _, currentDB := range []string{"test", ""} {
		if currentDB == "" {
			tk.RefreshSession()
		} else {
			tk.MustExec("use " + currentDB)
		}
		// With no current-database match, an unqualified duplicate name stays
		// inapplicable. Distinct aliases unambiguously select each table instead.
		tk.MustQuery("explain format = 'brief' " + fmt.Sprintf(twoSchemas, "use_index(t, ia1)"))
		tk.MustQuery("show warnings").CheckContain("is inapplicable")
		aliased := "select /*+ use_index(x, ia1) use_index(y, ia2) */ * from hintdb1.t x join hintdb2.t y on x.a = y.a"
		require.True(t, planHas(aliased, "index:ia1(a)"))
		require.True(t, planHas(aliased, "index:ia2(a)"))
		tk.MustQuery("show warnings").Check(testkit.Rows())
	}
	tk.MustExec("use test")

	// A binding resolves the unqualified hint name the same way when it is created.
	tk.MustExec("create table test.j (ns bigint, id bigint, primary key (ns, id) clustered)")
	bindingQuery := "select j.id, c.oid from j left join hintdb1.c on c.ns = j.ns and c.sjid = j.id where j.ns = 1 and j.id in (1, 2)"
	tk.MustExec("create global binding for " + bindingQuery + " using " +
		strings.Replace(bindingQuery, "select ", "select /*+ hash_join(c) */ ", 1))
	require.True(t, planHas(bindingQuery, "HashJoin"))
	tk.MustQuery("explain format = 'brief' " + bindingQuery)
	tk.MustQuery("select @@last_plan_from_binding").Check(testkit.Rows("1"))
	tk.MustQuery("show warnings").Check(testkit.Rows())
	tk.MustExec("drop global binding for " + bindingQuery)
}
