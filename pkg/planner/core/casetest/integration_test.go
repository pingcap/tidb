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

package casetest

import (
	"context"
	"strings"
	"testing"

	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	statstestutil "github.com/pingcap/tidb/pkg/statistics/handle/ddl/testutil"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testdata"
	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
	"github.com/stretchr/testify/require"
)

func TestVerboseExplain(t *testing.T) {
	testkit.RunTestUnderCascadesWithDomain(t, func(t *testing.T, tk *testkit.TestKit, dom *domain.Domain, cascades, caller string) {
		tk.MustExec("use test")
		tk.MustExec(`set tidb_opt_limit_push_down_threshold=0`)
		tk.MustExec("drop table if exists t1, t2, t3, t31240, partsupp, supplier, first_range")
		tk.MustExec("create table t1(a int, b int)")
		tk.MustExec("create table t2(a int, b int)")
		tk.MustExec("create table t3(a int, b int, index c(b))")
		tk.MustExec("create table t31240(a int, b int)")
		tk.MustExec("create table first_range(a int)")
		tk.MustExec("insert into t1 values(1,2)")
		tk.MustExec("insert into t1 values(3,4)")
		tk.MustExec("insert into t1 values(5,6)")
		tk.MustExec("insert into t2 values(1,2)")
		tk.MustExec("insert into t2 values(3,4)")
		tk.MustExec("insert into t2 values(5,6)")
		tk.MustExec("insert into t3 values(1,2)")
		tk.MustExec("insert into t3 values(3,4)")
		tk.MustExec("insert into t3 values(5,6)")
		tk.MustExec("analyze table t1 all columns")
		tk.MustExec("analyze table t2 all columns")
		tk.MustExec("analyze table t3 all columns")

		// Default RPC encoding may cause statistics explain result differ and then the test unstable.
		tk.MustExec("set @@tidb_enable_chunk_rpc = on")

		// Create virtual tiflash replica info.
		testkit.SetTiFlashReplica(t, dom, "test", "t1")
		testkit.SetTiFlashReplica(t, dom, "test", "t2")
		testkit.SetTiFlashReplica(t, dom, "test", "t31240")
		testkit.SetTiFlashReplica(t, dom, "test", "first_range")

		var input []string
		var output []struct {
			SQL  string
			Plan []string
		}
		integrationSuiteData := GetIntegrationSuiteData()
		tk.MustExec("CREATE TABLE `partsupp` (" +
			" `PS_PARTKEY` bigint(20) NOT NULL," +
			"`PS_SUPPKEY` bigint(20) NOT NULL," +
			"`PS_AVAILQTY` bigint(20) NOT NULL," +
			"`PS_SUPPLYCOST` decimal(15,2) NOT NULL," +
			"`PS_COMMENT` varchar(199) NOT NULL," +
			"PRIMARY KEY (`PS_PARTKEY`,`PS_SUPPKEY`) /*T![clustered_index] NONCLUSTERED */)")
		tk.MustExec("CREATE TABLE `supplier` (" +
			"`S_SUPPKEY` bigint(20) NOT NULL," +
			"`S_NAME` char(25) NOT NULL," +
			"`S_ADDRESS` varchar(40) NOT NULL," +
			"`S_NATIONKEY` bigint(20) NOT NULL," +
			"`S_PHONE` char(15) NOT NULL," +
			"`S_ACCTBAL` decimal(15,2) NOT NULL," +
			"`S_COMMENT` varchar(101) NOT NULL," +
			"PRIMARY KEY (`S_SUPPKEY`) /*T![clustered_index] CLUSTERED */)")
		h := dom.StatsHandle()
		err := statstestutil.HandleNextDDLEventWithTxn(h)
		require.NoError(t, err)

		testkit.SetTiFlashReplica(t, dom, "test", "partsupp")
		testkit.SetTiFlashReplica(t, dom, "test", "supplier")

		tbl1, err := dom.InfoSchema().TableByName(context.Background(), ast.CIStr{O: "test", L: "test"}, ast.CIStr{O: "partsupp", L: "partsupp"})
		require.NoError(t, err)
		tbl2, err := dom.InfoSchema().TableByName(context.Background(), ast.CIStr{O: "test", L: "test"}, ast.CIStr{O: "supplier", L: "supplier"})
		require.NoError(t, err)
		statsTbl1 := h.GetPhysicalTableStats(tbl1.Meta().ID, tbl1.Meta())
		statsTbl1.RealtimeCount = 800000
		statsTbl2 := h.GetPhysicalTableStats(tbl2.Meta().ID, tbl2.Meta())
		statsTbl2.RealtimeCount = 10000

		integrationSuiteData.LoadTestCases(t, &input, &output, cascades, caller)
		for i, tt := range input {
			setStmt := strings.HasPrefix(tt, "set")
			testdata.OnRecord(func() {
				output[i].SQL = tt
				if !setStmt {
					output[i].Plan = testdata.ConvertRowsToStrings(tk.MustQuery(tt).Rows())
				}
			})
			if setStmt {
				tk.MustExec(tt)
				continue
			}
			tk.MustQuery(tt).Check(testkit.Rows(output[i].Plan...))
		}

		tk.MustExec("drop table if exists t1, t2, t3, t31240, partsupp, supplier, first_range")
	})
}

func TestIsolationReadTiFlashNotChoosePointGet(t *testing.T) {
	testkit.RunTestUnderCascadesWithDomain(t, func(t *testing.T, tk *testkit.TestKit, dom *domain.Domain, cascades, caller string) {
		tk.MustExec("use test")
		tk.MustExec("drop table if exists t")
		tk.MustExec("create table t(a int, b int, primary key (a))")

		// Create virtual tiflash replica info.
		tblInfo, err := dom.InfoSchema().TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("t"))
		require.NoError(t, err)
		tblInfo.Meta().TiFlashReplica = &model.TiFlashReplicaInfo{
			Count:     1,
			Available: true,
		}

		tk.MustExec("set @@session.tidb_isolation_read_engines=\"tiflash\"")
		var input []string
		var output []struct {
			SQL    string
			Result []string
		}
		integrationSuiteData := GetIntegrationSuiteData()
		integrationSuiteData.LoadTestCases(t, &input, &output, cascades, caller)
		for i, tt := range input {
			testdata.OnRecord(func() {
				output[i].SQL = tt
				output[i].Result = testdata.ConvertRowsToStrings(tk.MustQuery(tt).Rows())
			})
			tk.MustQuery(tt).Check(testkit.Rows(output[i].Result...))
		}
	})
}

func TestIsolationReadDoNotFilterSystemDB(t *testing.T) {
	testkit.RunTestUnderCascades(t, func(t *testing.T, tk *testkit.TestKit, cascades, caller string) {
		tk.MustExec("use test")
		tk.MustExec("set @@tidb_isolation_read_engines = \"tiflash\"")
		var input []string
		var output []struct {
			SQL  string
			Plan []string
		}
		integrationSuiteData := GetIntegrationSuiteData()
		integrationSuiteData.LoadTestCases(t, &input, &output, cascades, caller)
		for i, tt := range input {
			testdata.OnRecord(func() {
				output[i].SQL = tt
				output[i].Plan = testdata.ConvertRowsToStrings(tk.MustQuery(tt).Rows())
			})
			res := tk.MustQuery(tt)
			res.Check(testkit.Rows(output[i].Plan...))
		}
	})
}

func TestMergeContinuousSelections(t *testing.T) {
	testkit.RunTestUnderCascadesWithDomain(t, func(t *testing.T, tk *testkit.TestKit, dom *domain.Domain, cascades, caller string) {
		tk.MustExec("use test")
		tk.MustExec("drop table if exists ts")
		tk.MustExec("create table ts (col_char_64 char(64), col_varchar_64_not_null varchar(64) not null, col_varchar_key varchar(1), id int primary key, col_varchar_64 varchar(64),col_char_64_not_null char(64) not null);")

		// Create virtual tiflash replica info.
		testkit.SetTiFlashReplica(t, dom, "test", "ts")

		tk.MustExec(" set @@tidb_allow_mpp=1;")

		var input []string
		var output []struct {
			SQL  string
			Plan []string
		}
		integrationSuiteData := GetIntegrationSuiteData()
		integrationSuiteData.LoadTestCases(t, &input, &output, cascades, caller)
		for i, tt := range input {
			testdata.OnRecord(func() {
				output[i].SQL = tt
				output[i].Plan = testdata.ConvertRowsToStrings(tk.MustQuery(tt).Rows())
			})
			res := tk.MustQuery(tt)
			res.Check(testkit.Rows(output[i].Plan...))
		}
	})
}

func TestTiFlashPartitionTableScan(t *testing.T) {
	testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/planner/core/forceDynamicPrune", `return(true)`)

	testkit.RunTestUnderCascadesWithDomain(t, func(t *testing.T, tk *testkit.TestKit, dom *domain.Domain, cascades, caller string) {
		tk.MustExec("use test")
		tk.MustExec("set tidb_cost_model_version=1")
		tk.MustExec("set @@tidb_partition_prune_mode = 'dynamic'")
		tk.MustExec("set @@tidb_isolation_read_engines = 'tiflash'")
		tk.MustExec("set @@tidb_enforce_mpp = on")
		tk.MustExec("set @@tidb_allow_batch_cop = 2")
		tk.MustExec("drop table if exists rp_t;")
		tk.MustExec("drop table if exists hp_t;")
		tk.MustExec("create table rp_t(a int) partition by RANGE (a) (PARTITION p0 VALUES LESS THAN (6),PARTITION p1 VALUES LESS THAN (11), PARTITION p2 VALUES LESS THAN (16), PARTITION p3 VALUES LESS THAN (21));")
		tk.MustExec("create table hp_t(a int) partition by hash(a) partitions 4;")
		tbl1, err := dom.InfoSchema().TableByName(context.Background(), ast.CIStr{O: "test", L: "test"}, ast.CIStr{O: "rp_t", L: "rp_t"})
		require.NoError(t, err)
		tbl2, err := dom.InfoSchema().TableByName(context.Background(), ast.CIStr{O: "test", L: "test"}, ast.CIStr{O: "hp_t", L: "hp_t"})
		require.NoError(t, err)
		// Set the hacked TiFlash replica for explain tests.
		tbl1.Meta().TiFlashReplica = &model.TiFlashReplicaInfo{Count: 1, Available: true}
		tbl2.Meta().TiFlashReplica = &model.TiFlashReplicaInfo{Count: 1, Available: true}
		var input []string
		var output []struct {
			SQL  string
			Plan []string
		}
		integrationSuiteData := GetIntegrationSuiteData()
		integrationSuiteData.LoadTestCases(t, &input, &output, cascades, caller)
		for i, tt := range input {
			testdata.OnRecord(func() {
				output[i].SQL = tt
				output[i].Plan = testdata.ConvertRowsToStrings(tk.MustQuery(tt).Rows())
			})
			tk.MustQuery(tt).Check(testkit.Rows(output[i].Plan...))
		}
		tk.MustExec("drop table rp_t;")
		tk.MustExec("drop table hp_t;")
	})
}

func TestTiFlashFineGrainedShuffle(t *testing.T) {
	testkit.RunTestUnderCascadesWithDomain(t, func(t *testing.T, tk *testkit.TestKit, dom *domain.Domain, cascades, caller string) {
		tk.MustExec("use test")
		tk.MustExec("set @@tidb_isolation_read_engines = 'tiflash'")
		tk.MustExec("set @@tidb_enforce_mpp = on")
		tk.MustExec("drop table if exists t1;")
		tk.MustExec("create table t1(c1 int, c2 int)")

		tbl1, err := dom.InfoSchema().TableByName(context.Background(), ast.CIStr{O: "test", L: "test"}, ast.CIStr{O: "t1", L: "t1"})
		require.NoError(t, err)
		// Set the hacked TiFlash replica for explain tests.
		tbl1.Meta().TiFlashReplica = &model.TiFlashReplicaInfo{Count: 1, Available: true}
		var input []string
		var output []struct {
			SQL    string
			Plan   []string
			Redact []string
		}
		integrationSuiteData := GetIntegrationSuiteData()
		integrationSuiteData.LoadTestCases(t, &input, &output, cascades, caller)
		for i, tt := range input {
			testdata.OnRecord(func() {
				output[i].SQL = tt
				tk.MustExec("set global tidb_redact_log=off")
				output[i].Plan = testdata.ConvertRowsToStrings(tk.MustQuery(tt).Rows())
				tk.MustExec("set global tidb_redact_log=on")
				output[i].Redact = testdata.ConvertRowsToStrings(tk.MustQuery(tt).Rows())
			})
			tk.MustExec("set global tidb_redact_log=off")
			tk.MustQuery(tt).Check(testkit.Rows(output[i].Plan...))
			tk.MustExec("set global tidb_redact_log=on")
			tk.MustQuery(tt).Check(testkit.Rows(output[i].Redact...))
		}
	})
}

func TestFixControl43817(t *testing.T) {
	testkit.RunTestUnderCascades(t, func(t *testing.T, tk *testkit.TestKit, cascades, caller string) {
		tk.MustExec(`use test`)
		tk.MustExec(`create table t1 (a int)`)
		tk.MustExec(`create table t2 (a int)`)
		tk.MustQuery(`select * from t1 where t1.a > (select max(a) from t2)`).Check(testkit.Rows()) // no error
		tk.MustExec(`set tidb_opt_fix_control="43817:on"`)
		tk.MustContainErrMsg(`select * from t1 where t1.a > (select max(a) from t2)`, "evaluate non-correlated sub-queries during optimization phase is not allowed by fix-control 43817")
		tk.MustExec(`set tidb_opt_fix_control="43817:off"`)
		tk.MustQuery(`select * from t1 where t1.a > (select max(a) from t2)`).Check(testkit.Rows()) // no error
	})
}

func TestFixControl45132(t *testing.T) {
	testkit.RunTestUnderCascades(t, func(t *testing.T, tk *testkit.TestKit, cascades, caller string) {
		tk.MustExec(`use test`)
		tk.MustExec(`create table t (a int, b int, key(a))`)
		values := make([]string, 0, 101)
		for range 100 {
			values = append(values, "(1, 1)")
		}
		values = append(values, "(2, 2)") // count(1) : count(2) == 100 : 1
		tk.MustExec(`insert into t values ` + strings.Join(values, ","))
		for range 7 {
			tk.MustExec(`insert into t select * from t`)
		}
		tk.MustExec(`analyze table t`)
		// the cost model prefers to use TableScan instead of IndexLookup to avoid double requests.
		tk.MustHavePlan(`select * from t where a=2`, `TableFullScan`)

		tk.MustExec(`set @@tidb_opt_fix_control = "45132:99"`)
		tk.MustExec(`analyze table t`)
		tk.EventuallyMustIndexLookup(`select * from t where a=2`) // index lookup

		tk.MustExec(`set @@tidb_opt_fix_control = "45132:500"`)
		tk.MustHavePlan(`select * from t where a=2`, `TableFullScan`)

		tk.MustExec(`set @@tidb_opt_fix_control = "45132:0"`)
		tk.MustHavePlan(`select * from t where a=2`, `TableFullScan`)
	})
}

func TestTiFlashExtraColumnPrune(t *testing.T) {
	testkit.RunTestUnderCascadesWithDomain(t, func(t *testing.T, tk *testkit.TestKit, dom *domain.Domain, cascades, caller string) {
		tk.MustExec("use test")
		tk.MustExec("set @@tidb_isolation_read_engines = 'tiflash'")
		tk.MustExec("set @@tidb_enforce_mpp = on")
		tk.MustExec("drop table if exists t1;")
		tk.MustExec("create table t1(c1 int, c2 int)")

		tbl1, err := dom.InfoSchema().TableByName(context.Background(), ast.CIStr{O: "test", L: "test"}, ast.CIStr{O: "t1", L: "t1"})
		require.NoError(t, err)
		// Set the hacked TiFlash replica for explain tests.
		tbl1.Meta().TiFlashReplica = &model.TiFlashReplicaInfo{Count: 1, Available: true}
		var input []string
		var output []struct {
			SQL  string
			Plan []string
		}
		integrationSuiteData := GetIntegrationSuiteData()
		integrationSuiteData.LoadTestCases(t, &input, &output, cascades, caller)
		for i, tt := range input {
			testdata.OnRecord(func() {
				output[i].SQL = tt
				output[i].Plan = testdata.ConvertRowsToStrings(tk.MustQuery(tt).Rows())
			})
			tk.MustQuery(tt).Check(testkit.Rows(output[i].Plan...))
		}
	})
}

func TestIndexMergeJSONMemberOf2FlakyPart(t *testing.T) {
	testkit.RunTestUnderCascades(t, func(t *testing.T, tk *testkit.TestKit, cascades, caller string) {
		// The following tests are flaky, we add it in unit test to have more chance to debug the issue.
		tk.MustExec(`use test`)
		tk.MustExec(`drop table if exists t`)
		tk.MustExec(`create table t(a int, b int, c int, d json, index iad(a, (cast(d->'$.b' as signed array))));`)
		tk.MustExec(`insert into t value(1,1,1, '{"b":[1,2,3,4]}');`)
		tk.MustExec(`insert into t value(2,2,2, '{"b":[3,4,5,6]}');`)
		tk.MustExec(`set tidb_analyze_version=2;`)
		tk.MustExec(`analyze table t all columns;`)
		tk.MustQuery("explain format = 'brief' select * from t use index (iad) where a = 1;").Check(testkit.Rows(
			"TableReader 1.00 root  data:Selection",
			"└─Selection 1.00 cop[tikv]  eq(test.t.a, 1)",
			"  └─TableFullScan 2.00 cop[tikv] table:t keep order:false",
		))
		tk.MustQuery("explain format = 'brief' select * from t use index (iad) where a = 1 and (2 member of (d->'$.b'));").Check(testkit.Rows(
			"IndexMerge 1.00 root  type: union",
			"├─IndexRangeScan(Build) 1.00 cop[tikv] table:t, index:iad(a, cast(json_extract(`d`, _utf8mb4'$.b') as signed array)) range:[1 2,1 2], keep order:false, stats:partial[d:unInitialized]",
			"└─TableRowIDScan(Probe) 1.00 cop[tikv] table:t keep order:false, stats:partial[d:unInitialized]",
		))
	})
}

func TestIntegrationRegression(t *testing.T) {
	testkit.RunTestUnderCascades(t, func(t *testing.T, tk *testkit.TestKit, cascades, caller string) {
		defer config.RestoreFunc()()
		config.UpdateGlobal(func(conf *config.Config) {
			conf.Status.RecordQPSbyDB = true
		})

		tk.MustExec("use test")
		// issue:29503
		tk.MustExec("drop table if exists t;")
		tk.MustExec("create table t(a int);")
		require.NoError(t, tk.ExecToErr("create binding for select 1 using select 1;"))
		require.NoError(t, tk.ExecToErr("create binding for select a from t using select a from t;"))
		res := tk.MustQuery("show session bindings;")
		require.Len(t, res.Rows(), 2)

		// issue:33175
		tk.MustExec("drop table if exists t;")
		tk.MustExec("create table t (id bigint(45) unsigned not null, c varchar(20), primary key(id));")
		tk.MustExec("insert into t values (9734095886065816707, 'a'), (10353107668348738101, 'b'), (0, 'c');")
		tk.MustExec("begin")
		tk.MustExec("insert into t values (33, 'd');")
		tk.MustQuery("select /* issue:33175 */ max(id) from t;").Check(testkit.Rows("10353107668348738101"))
		tk.MustExec("rollback")

		tk.MustExec("alter table t cache")
		for {
			tk.MustQuery("select /* issue:33175 */ max(id) from t;").Check(testkit.Rows("10353107668348738101"))
			if tk.Session().GetSessionVars().StmtCtx.ReadFromTableCache {
				break
			}
		}

		// With subquery, like the original issue case.
		for {
			tk.MustQuery("select /* issue:33175 */ * from t where id > (select  max(id) from t where t.id > 0);").Check(testkit.Rows())
			if tk.Session().GetSessionVars().StmtCtx.ReadFromTableCache {
				break
			}
		}

		// Test order by desc / asc.
		tk.MustQuery("select /* issue:33175 */ id from t order by id desc;").Check(testkit.Rows(
			"10353107668348738101",
			"9734095886065816707",
			"0"))

		tk.MustQuery("select /* issue:33175 */ id from t order by id asc;").Check(testkit.Rows(
			"0",
			"9734095886065816707",
			"10353107668348738101"))

		tk.MustExec("alter table t nocache")
		tk.MustExec("drop table t")

		// Cover more code that use union scan
		// TableReader/IndexReader/IndexLookup
		for idx, q := range []string{
			"create temporary table t (id bigint unsigned, c int default null, index(id))",
			"create temporary table t (id bigint unsigned primary key)",
		} {
			tk.MustExec(q)
			tk.MustExec("insert into t(id) values (1), (3), (9734095886065816707), (9734095886065816708)")
			tk.MustQuery("select /* issue:33175 */ min(id) from t").Check(testkit.Rows("1"))
			tk.MustQuery("select /* issue:33175 */ max(id) from t").Check(testkit.Rows("9734095886065816708"))
			tk.MustQuery("select /* issue:33175 */ id from t order by id asc").Check(testkit.Rows(
				"1", "3", "9734095886065816707", "9734095886065816708"))
			tk.MustQuery("select /* issue:33175 */ id from t order by id desc").Check(testkit.Rows(
				"9734095886065816708", "9734095886065816707", "3", "1"))
			if idx == 0 {
				tk.MustQuery("select /* issue:33175 */ * from t order by id asc").Check(testkit.Rows(
					"1 <nil>",
					"3 <nil>",
					"9734095886065816707 <nil>",
					"9734095886065816708 <nil>"))
				tk.MustQuery("select /* issue:33175 */ * from t order by id desc").Check(testkit.Rows(
					"9734095886065816708 <nil>",
					"9734095886065816707 <nil>",
					"3 <nil>",
					"1 <nil>"))
			}
			tk.MustExec("drop table t")
		}

		// More and more test
		tk.MustExec("create global temporary table `tmp1` (id bigint unsigned primary key) on commit delete rows;")
		tk.MustExec("begin")
		tk.MustExec("insert into tmp1 values (0),(1),(2),(65536),(9734095886065816707),(9734095886065816708);")
		tk.MustQuery("select /* issue:33175 */ * from tmp1 where id <= 65534 or (id > 65535 and id < 9734095886065816700) or id >= 9734095886065816707 order by id desc;").Check(testkit.Rows(
			"9734095886065816708", "9734095886065816707", "65536", "2", "1", "0"))

		tk.MustQuery("select /* issue:33175 */ * from tmp1 where id <= 65534 or (id > 65535 and id < 9734095886065816700) or id >= 9734095886065816707 order by id asc;").Check(testkit.Rows(
			"0", "1", "2", "65536", "9734095886065816707", "9734095886065816708"))

		tk.MustExec("create global temporary table `tmp2` (id bigint primary key) on commit delete rows;")
		tk.MustExec("begin")
		tk.MustExec("insert into tmp2 values(-2),(-1),(0),(1),(2);")
		tk.MustQuery("select /* issue:33175 */ * from tmp2 where id <= -1 or id > 0 order by id desc;").Check(testkit.Rows("2", "1", "-1", "-2"))
		tk.MustQuery("select /* issue:33175 */ * from tmp2 where id <= -1 or id > 0 order by id asc;").Check(testkit.Rows("-2", "-1", "1", "2"))

		// issue:51873
		tk.MustExec(`CREATE TABLE h1 (
  id bigint(20) NOT NULL AUTO_INCREMENT,
  position_date date NOT NULL,
  asset_id varchar(32) DEFAULT NULL,
  portfolio_code varchar(50) DEFAULT NULL,
  PRIMARY KEY (id,position_date) /*T![clustered_index] NONCLUSTERED */,
  UNIQUE KEY uidx_posi_asset_balance_key (position_date,portfolio_code,asset_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin AUTO_INCREMENT=30002
PARTITION BY RANGE COLUMNS(position_date)
(PARTITION p202401 VALUES LESS THAN ('2024-02-01'))`)
		tk.MustExec(`create table h2 like h1`)
		tk.MustExec(`insert into h1 values(1,'2024-01-01',1,1)`)
		tk.MustExec(`insert into h2 values(1,'2024-01-01',1,1)`)
		tk.MustExec(`analyze table h1`)
		tk.MustExec(`set @@tidb_skip_missing_partition_stats=0`)
		tk.MustQuery(`with assetBalance AS
    (SELECT asset_id, portfolio_code FROM h1 pab WHERE pab.position_date = '2024-01-01' ),
cashBalance AS (SELECT portfolio_code, asset_id
    FROM h2 pcb WHERE pcb.position_date = '2024-01-01' ),
assetIdList AS (SELECT DISTINCT asset_id AS assetId
    FROM assetBalance )
SELECT main.portfolioCode
FROM (SELECT DISTINCT balance.portfolio_code AS portfolioCode
    FROM assetBalance balance
    LEFT JOIN assetIdList
        ON balance.asset_id = assetIdList.assetId ) main`).Check(testkit.Rows("1"))

		// issue:50926
		tk.MustExec("create table t_issue50926 (a int)")
		tk.MustExec("create or replace definer='root'@'localhost' view v_issue50926 (a,b) AS select 1 as a, json_object('k', '0') as b from t_issue50926")
		tk.MustQuery("select sum(json_extract(b, '$.path')) from v_issue50926 group by a").Check(testkit.Rows())

		// issue:49438
		tk.MustExec(`drop table if exists tx`)
		tk.MustExec(`create table tx (a int, b json, key k(a, (cast(b as date array))))`)
		tk.MustQuery(`select /* issue:49438 */ 1 from tx where a in (1)`).Check(testkit.Rows())

		// issue:52023
		tk.MustExec(`CREATE TABLE t_issue52023 (
			a binary(1) NOT NULL,
			PRIMARY KEY (a)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin
		PARTITION BY RANGE COLUMNS(a)
		(PARTITION P0 VALUES LESS THAN (_binary 0x03),
		PARTITION P4 VALUES LESS THAN (_binary 0xc0),
		PARTITION PMX VALUES LESS THAN (MAXVALUE))`)
		tk.MustExec(`insert into t_issue52023 values (0x5)`)
		tk.MustExec(`analyze table t_issue52023`)
		tk.MustQuery(`select * from t_issue52023`).Check(testkit.Rows("\u0005"))
		tk.MustQuery(`select * from t_issue52023 where a = 0x5`).Check(testkit.Rows("\u0005"))
		tk.MustQuery(`select * from t_issue52023 where a = 5`).Check(testkit.Rows())
		tk.MustQuery(`select * from t_issue52023 where a IN (5,55)`).Check(testkit.Rows())
		tk.MustQuery(`select * from t_issue52023 where a IN (0x5,55)`).Check(testkit.Rows("\u0005"))
		tk.MustQuery(`explain format='brief' select * from t_issue52023 where a = 0x5`).Check(testkit.Rows("Point_Get 1.00 root table:t_issue52023, partition:P4, clustered index:PRIMARY(a) "))
		tk.MustQuery(`explain format='brief' select * from t_issue52023 where a = 5`).Check(testkit.Rows(""+
			"TableReader 1.00 root partition:all data:Selection",
			"└─Selection 1.00 cop[tikv]  eq(cast(test.t_issue52023.a, double BINARY), 5)",
			"  └─TableFullScan 1.00 cop[tikv] table:t_issue52023 keep order:false"))
		tk.MustQuery(`explain format='brief' select * from t_issue52023 where a IN (5,55)`).Check(testkit.Rows(""+
			"TableReader 1.00 root partition:all data:Selection",
			"└─Selection 1.00 cop[tikv]  or(eq(cast(test.t_issue52023.a, double BINARY), 5), eq(cast(test.t_issue52023.a, double BINARY), 55))",
			"  └─TableFullScan 1.00 cop[tikv] table:t_issue52023 keep order:false"))
		tk.MustQuery(`explain format='brief' select * from t_issue52023 where a IN (0x5,55)`).Check(testkit.Rows(""+
			"TableReader 1.00 root partition:all data:Selection",
			"└─Selection 1.00 cop[tikv]  or(eq(test.t_issue52023.a, \"0x05\"), eq(cast(test.t_issue52023.a, double BINARY), 55))",
			"  └─TableFullScan 1.00 cop[tikv] table:t_issue52023 keep order:false"))

		// issue:56915
		tk.MustExec(`drop table if exists t_issue56915`)
		tk.MustExec(`create table t_issue56915(a int, b int, j json, index ia(a), index mvi( (cast(j as signed array)), a, b) );`)
		tk.MustExec(`insert into t_issue56915 value(1,1,'[1,2,3,4,5]');`)
		tk.MustExec(`insert into t_issue56915 value(1,1,'[1,2,3,4,5]');`)
		tk.MustExec(`insert into t_issue56915 value(1,1,'[1,2,3,4,5]');`)
		tk.MustExec(`insert into t_issue56915 value(1,1,'[1,2,3,4,5]');`)
		tk.MustExec(`insert into t_issue56915 value(1,1,'[6]');`)
		tk.MustExec(`analyze table t_issue56915 all columns;`)
		tk.MustQuery("explain format = brief select /* issue:56915 */ * from t_issue56915 where a = 1 and 6 member of (j);").Check(testkit.Rows(
			"IndexMerge 1.00 root  type: union",
			"├─IndexRangeScan(Build) 1.00 cop[tikv] table:t_issue56915, index:mvi(cast(`j` as signed array), a, b) range:[6 1,6 1], keep order:false, stats:partial[j:unInitialized]",
			"└─TableRowIDScan(Probe) 1.00 cop[tikv] table:t_issue56915 keep order:false, stats:partial[j:unInitialized]",
		))

		// issue:63290
		tk.MustExec("drop table if exists t1, t2, t3")
		tk.MustExec("create table t1 (a int, b int, c int, key(a), key(b))")
		tk.MustExec("create table t2 (a int, b int, c int, key(a), key(b))")
		tk.MustExec("create table t3 (a int, b int, c int, key(a), key(b))")
		tk.MustExec(`insert into t1 values (1, 1, 1)`)
		tk.MustExec(`insert into t3 values (1, 1, 1)`)
		tk.MustExec(`analyze table t1, t2, t3`)

		// Cartesian Join t1 and t3 first, then join t2.
		tk.MustQuery(`explain format='plan_tree' select /* issue:63290 */ /*+ set_var(tidb_opt_cartesian_join_order_threshold=100) */ 1 from t1, t2, t3 where t1.a = t2.a and t2.b = t3.b`).Check(testkit.Rows(
			`Projection root  1->Column`,
			`└─IndexHashJoin root  inner join, inner:IndexLookUp, outer key:test.t1.a, inner key:test.t2.a, equal cond:eq(test.t1.a, test.t2.a), eq(test.t3.b, test.t2.b)`,
			`  ├─HashJoin(Build) root  CARTESIAN inner join`,
			`  │ ├─IndexReader(Build) root  index:IndexFullScan`,
			`  │ │ └─IndexFullScan cop[tikv] table:t3, index:b(b) keep order:false`,
			`  │ └─IndexReader(Probe) root  index:IndexFullScan`,
			`  │   └─IndexFullScan cop[tikv] table:t1, index:a(a) keep order:false`,
			`  └─IndexLookUp(Probe) root  `,
			`    ├─Selection(Build) cop[tikv]  not(isnull(test.t2.a))`,
			`    │ └─IndexRangeScan cop[tikv] table:t2, index:a(a) range: decided by [eq(test.t2.a, test.t1.a)], keep order:false, stats:pseudo`,
			`    └─Selection(Probe) cop[tikv]  not(isnull(test.t2.b))`,
			`      └─TableRowIDScan cop[tikv] table:t2 keep order:false, stats:pseudo`))
	})
}
