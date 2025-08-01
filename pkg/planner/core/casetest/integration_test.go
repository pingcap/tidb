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

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	statstestutil "github.com/pingcap/tidb/pkg/statistics/handle/ddl/testutil"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testdata"
	"github.com/stretchr/testify/require"
)

func TestVerboseExplain(t *testing.T) {
	testkit.RunTestUnderCascadesWithDomain(t, func(t *testing.T, testKit *testkit.TestKit, dom *domain.Domain, cascades, caller string) {
		testKit.MustExec("use test")
		testKit.MustExec(`set tidb_opt_limit_push_down_threshold=0`)
		testKit.MustExec("drop table if exists t1, t2, t3")
		testKit.MustExec("create table t1(a int, b int)")
		testKit.MustExec("create table t2(a int, b int)")
		testKit.MustExec("create table t3(a int, b int, index c(b))")
		testKit.MustExec("insert into t1 values(1,2)")
		testKit.MustExec("insert into t1 values(3,4)")
		testKit.MustExec("insert into t1 values(5,6)")
		testKit.MustExec("insert into t2 values(1,2)")
		testKit.MustExec("insert into t2 values(3,4)")
		testKit.MustExec("insert into t2 values(5,6)")
		testKit.MustExec("insert into t3 values(1,2)")
		testKit.MustExec("insert into t3 values(3,4)")
		testKit.MustExec("insert into t3 values(5,6)")
		testKit.MustExec("analyze table t1 all columns")
		testKit.MustExec("analyze table t2 all columns")
		testKit.MustExec("analyze table t3 all columns")

		// Default RPC encoding may cause statistics explain result differ and then the test unstable.
		testKit.MustExec("set @@tidb_enable_chunk_rpc = on")

		// Create virtual tiflash replica info.
		testkit.SetTiFlashReplica(t, dom, "test", "t1")
		testkit.SetTiFlashReplica(t, dom, "test", "t2")

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
				output[i].Plan = testdata.ConvertRowsToStrings(testKit.MustQuery(tt).Rows())
			})
			res := testKit.MustQuery(tt)
			res.Check(testkit.Rows(output[i].Plan...))
		}
	})
}

func TestIsolationReadTiFlashNotChoosePointGet(t *testing.T) {
	testkit.RunTestUnderCascadesWithDomain(t, func(t *testing.T, testKit *testkit.TestKit, dom *domain.Domain, cascades, caller string) {
		testKit.MustExec("use test")
		testKit.MustExec("drop table if exists t")
		testKit.MustExec("create table t(a int, b int, primary key (a))")

		// Create virtual tiflash replica info.
		tblInfo, err := dom.InfoSchema().TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("t"))
		require.NoError(t, err)
		tblInfo.Meta().TiFlashReplica = &model.TiFlashReplicaInfo{
			Count:     1,
			Available: true,
		}

		testKit.MustExec("set @@session.tidb_isolation_read_engines=\"tiflash\"")
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
				output[i].Result = testdata.ConvertRowsToStrings(testKit.MustQuery(tt).Rows())
			})
			testKit.MustQuery(tt).Check(testkit.Rows(output[i].Result...))
		}
	})
}

func TestIsolationReadDoNotFilterSystemDB(t *testing.T) {
	testkit.RunTestUnderCascades(t, func(t *testing.T, testKit *testkit.TestKit, cascades, caller string) {
		testKit.MustExec("use test")
		testKit.MustExec("set @@tidb_isolation_read_engines = \"tiflash\"")
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
				output[i].Plan = testdata.ConvertRowsToStrings(testKit.MustQuery(tt).Rows())
			})
			res := testKit.MustQuery(tt)
			res.Check(testkit.Rows(output[i].Plan...))
		}
	})
}

func TestMergeContinuousSelections(t *testing.T) {
	testkit.RunTestUnderCascadesWithDomain(t, func(t *testing.T, testKit *testkit.TestKit, dom *domain.Domain, cascades, caller string) {
		testKit.MustExec("use test")
		testKit.MustExec("drop table if exists ts")
		testKit.MustExec("create table ts (col_char_64 char(64), col_varchar_64_not_null varchar(64) not null, col_varchar_key varchar(1), id int primary key, col_varchar_64 varchar(64),col_char_64_not_null char(64) not null);")

		// Create virtual tiflash replica info.
		testkit.SetTiFlashReplica(t, dom, "test", "ts")

		testKit.MustExec(" set @@tidb_allow_mpp=1;")

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
				output[i].Plan = testdata.ConvertRowsToStrings(testKit.MustQuery(tt).Rows())
			})
			res := testKit.MustQuery(tt)
			res.Check(testkit.Rows(output[i].Plan...))
		}
	})
}

func TestIssue31240(t *testing.T) {
	testkit.RunTestUnderCascadesWithDomain(t, func(t *testing.T, testKit *testkit.TestKit, dom *domain.Domain, cascades, caller string) {
		testKit.MustExec("use test")
		testKit.MustExec("create table t31240(a int, b int);")
		testKit.MustExec("set @@tidb_allow_mpp = 0")

		// since allow-mpp is adjusted to false, there will be no physical plan if TiFlash cop is banned.
		testKit.MustExec("set @@session.tidb_allow_tiflash_cop=ON")

		tbl, err := dom.InfoSchema().TableByName(context.Background(), ast.CIStr{O: "test", L: "test"}, ast.CIStr{O: "t31240", L: "t31240"})
		require.NoError(t, err)
		// Set the hacked TiFlash replica for explain tests.
		tbl.Meta().TiFlashReplica = &model.TiFlashReplicaInfo{Count: 1, Available: true}

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
			})
			if strings.HasPrefix(tt, "set") {
				testKit.MustExec(tt)
				continue
			}
			testdata.OnRecord(func() {
				output[i].Plan = testdata.ConvertRowsToStrings(testKit.MustQuery(tt).Rows())
			})
			testKit.MustQuery(tt).Check(testkit.Rows(output[i].Plan...))
		}
		testKit.MustExec("drop table if exists t31240")
	})
}
func TestIssue32632(t *testing.T) {
	testkit.RunTestUnderCascadesWithDomain(t, func(t *testing.T, testKit *testkit.TestKit, dom *domain.Domain, cascades, caller string) {
		testKit.MustExec("use test")
		testKit.MustExec("CREATE TABLE `partsupp` (" +
			" `PS_PARTKEY` bigint(20) NOT NULL," +
			"`PS_SUPPKEY` bigint(20) NOT NULL," +
			"`PS_AVAILQTY` bigint(20) NOT NULL," +
			"`PS_SUPPLYCOST` decimal(15,2) NOT NULL," +
			"`PS_COMMENT` varchar(199) NOT NULL," +
			"PRIMARY KEY (`PS_PARTKEY`,`PS_SUPPKEY`) /*T![clustered_index] NONCLUSTERED */)")
		testKit.MustExec("CREATE TABLE `supplier` (" +
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
		testKit.MustExec("set @@tidb_enforce_mpp = 1")

		tbl1, err := dom.InfoSchema().TableByName(context.Background(), ast.CIStr{O: "test", L: "test"}, ast.CIStr{O: "partsupp", L: "partsupp"})
		require.NoError(t, err)
		tbl2, err := dom.InfoSchema().TableByName(context.Background(), ast.CIStr{O: "test", L: "test"}, ast.CIStr{O: "supplier", L: "supplier"})
		require.NoError(t, err)
		// Set the hacked TiFlash replica for explain tests.
		tbl1.Meta().TiFlashReplica = &model.TiFlashReplicaInfo{Count: 1, Available: true}
		tbl2.Meta().TiFlashReplica = &model.TiFlashReplicaInfo{Count: 1, Available: true}

		statsTbl1 := h.GetTableStats(tbl1.Meta())
		statsTbl1.RealtimeCount = 800000
		statsTbl2 := h.GetTableStats(tbl2.Meta())
		statsTbl2.RealtimeCount = 10000
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
				output[i].Plan = testdata.ConvertRowsToStrings(testKit.MustQuery(tt).Rows())
			})
			testKit.MustQuery(tt).Check(testkit.Rows(output[i].Plan...))
		}
		testKit.MustExec("drop table if exists partsupp")
		testKit.MustExec("drop table if exists supplier")
	})
}

func TestTiFlashPartitionTableScan(t *testing.T) {
	failpoint.Enable("github.com/pingcap/tidb/pkg/planner/core/forceDynamicPrune", `return(true)`)
	defer failpoint.Disable("github.com/pingcap/tidb/pkg/planner/core/forceDynamicPrune")

	testkit.RunTestUnderCascadesWithDomain(t, func(t *testing.T, testKit *testkit.TestKit, dom *domain.Domain, cascades, caller string) {
		testKit.MustExec("use test")
		testKit.MustExec("set tidb_cost_model_version=1")
		testKit.MustExec("set @@tidb_partition_prune_mode = 'dynamic'")
		testKit.MustExec("set @@tidb_isolation_read_engines = 'tiflash'")
		testKit.MustExec("set @@tidb_enforce_mpp = on")
		testKit.MustExec("set @@tidb_allow_batch_cop = 2")
		testKit.MustExec("drop table if exists rp_t;")
		testKit.MustExec("drop table if exists hp_t;")
		testKit.MustExec("create table rp_t(a int) partition by RANGE (a) (PARTITION p0 VALUES LESS THAN (6),PARTITION p1 VALUES LESS THAN (11), PARTITION p2 VALUES LESS THAN (16), PARTITION p3 VALUES LESS THAN (21));")
		testKit.MustExec("create table hp_t(a int) partition by hash(a) partitions 4;")
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
				output[i].Plan = testdata.ConvertRowsToStrings(testKit.MustQuery(tt).Rows())
			})
			testKit.MustQuery(tt).Check(testkit.Rows(output[i].Plan...))
		}
		testKit.MustExec("drop table rp_t;")
		testKit.MustExec("drop table hp_t;")
	})
}

func TestTiFlashFineGrainedShuffle(t *testing.T) {
	testkit.RunTestUnderCascadesWithDomain(t, func(t *testing.T, testKit *testkit.TestKit, dom *domain.Domain, cascades, caller string) {
		testKit.MustExec("use test")
		testKit.MustExec("set @@tidb_isolation_read_engines = 'tiflash'")
		testKit.MustExec("set @@tidb_enforce_mpp = on")
		testKit.MustExec("drop table if exists t1;")
		testKit.MustExec("create table t1(c1 int, c2 int)")

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
				testKit.MustExec("set session tidb_redact_log=off")
				output[i].Plan = testdata.ConvertRowsToStrings(testKit.MustQuery(tt).Rows())
				testKit.MustExec("set session tidb_redact_log=on")
				output[i].Redact = testdata.ConvertRowsToStrings(testKit.MustQuery(tt).Rows())
			})
			testKit.MustExec("set session tidb_redact_log=off")
			testKit.MustQuery(tt).Check(testkit.Rows(output[i].Plan...))
			testKit.MustExec("set session tidb_redact_log=on")
			testKit.MustQuery(tt).Check(testkit.Rows(output[i].Redact...))
		}
	})
}

func TestIssue51873(t *testing.T) {
	testkit.RunTestUnderCascades(t, func(t *testing.T, testKit *testkit.TestKit, cascades, caller string) {
		testKit.MustExec(`use test`)
		testKit.MustExec(`CREATE TABLE h1 (
  id bigint(20) NOT NULL AUTO_INCREMENT,
  position_date date NOT NULL,
  asset_id varchar(32) DEFAULT NULL,
  portfolio_code varchar(50) DEFAULT NULL,
  PRIMARY KEY (id,position_date) /*T![clustered_index] NONCLUSTERED */,
  UNIQUE KEY uidx_posi_asset_balance_key (position_date,portfolio_code,asset_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin AUTO_INCREMENT=30002
PARTITION BY RANGE COLUMNS(position_date)
(PARTITION p202401 VALUES LESS THAN ('2024-02-01'))`)
		testKit.MustExec(`create table h2 like h1`)
		testKit.MustExec(`insert into h1 values(1,'2024-01-01',1,1)`)
		testKit.MustExec(`insert into h2 values(1,'2024-01-01',1,1)`)
		testKit.MustExec(`analyze table h1`)
		testKit.MustExec(`set @@tidb_skip_missing_partition_stats=0`)
		testKit.MustQuery(`with assetBalance AS
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
	})
}

func TestIssue50926(t *testing.T) {
	testkit.RunTestUnderCascades(t, func(t *testing.T, testKit *testkit.TestKit, cascades, caller string) {
		testKit.MustExec("use test")
		testKit.MustExec("create table t (a int)")
		testKit.MustExec("create or replace definer='root'@'localhost' view v (a,b) AS select 1 as a, json_object('k', '0') as b from t")
		testKit.MustQuery("select sum(json_extract(b, '$.path')) from v group by a").Check(testkit.Rows()) // no error
	})
}

func TestFixControl43817(t *testing.T) {
	testkit.RunTestUnderCascades(t, func(t *testing.T, testKit *testkit.TestKit, cascades, caller string) {
		testKit.MustExec(`use test`)
		testKit.MustExec(`create table t1 (a int)`)
		testKit.MustExec(`create table t2 (a int)`)
		testKit.MustQuery(`select * from t1 where t1.a > (select max(a) from t2)`).Check(testkit.Rows()) // no error
		testKit.MustExec(`set tidb_opt_fix_control="43817:on"`)
		testKit.MustContainErrMsg(`select * from t1 where t1.a > (select max(a) from t2)`, "evaluate non-correlated sub-queries during optimization phase is not allowed by fix-control 43817")
		testKit.MustExec(`set tidb_opt_fix_control="43817:off"`)
		testKit.MustQuery(`select * from t1 where t1.a > (select max(a) from t2)`).Check(testkit.Rows()) // no error
	})
}

func TestFixControl45132(t *testing.T) {
	testkit.RunTestUnderCascades(t, func(t *testing.T, testKit *testkit.TestKit, cascades, caller string) {
		testKit.MustExec(`use test`)
		testKit.MustExec(`create table t (a int, b int, key(a))`)
		values := make([]string, 0, 101)
		for range 100 {
			values = append(values, "(1, 1)")
		}
		values = append(values, "(2, 2)") // count(1) : count(2) == 100 : 1
		testKit.MustExec(`insert into t values ` + strings.Join(values, ","))
		for range 7 {
			testKit.MustExec(`insert into t select * from t`)
		}
		testKit.MustExec(`analyze table t`)
		// the cost model prefers to use TableScan instead of IndexLookup to avoid double requests.
		testKit.MustHavePlan(`select * from t where a=2`, `TableFullScan`)

		testKit.MustExec(`set @@tidb_opt_fix_control = "45132:99"`)
		testKit.MustExec(`analyze table t`)
		testKit.EventuallyMustIndexLookup(`select * from t where a=2`) // index lookup

		testKit.MustExec(`set @@tidb_opt_fix_control = "45132:500"`)
		testKit.MustHavePlan(`select * from t where a=2`, `TableFullScan`)

		testKit.MustExec(`set @@tidb_opt_fix_control = "45132:0"`)
		testKit.MustHavePlan(`select * from t where a=2`, `TableFullScan`)
	})
}

func TestIssue49438(t *testing.T) {
	testkit.RunTestUnderCascades(t, func(t *testing.T, testKit *testkit.TestKit, cascades, caller string) {
		testKit.MustExec(`use test`)
		testKit.MustExec(`drop table if exists tx`)
		testKit.MustExec(`create table tx (a int, b json, key k(a, (cast(b as date array))))`)
		testKit.MustQuery(`select 1 from tx where a in (1)`).Check(testkit.Rows()) // no error
	})
}

func TestIssue52023(t *testing.T) {
	testkit.RunTestUnderCascades(t, func(t *testing.T, testKit *testkit.TestKit, cascades, caller string) {
		testKit.MustExec(`use test`)
		testKit.MustExec(`CREATE TABLE t (
			a binary(1) NOT NULL,
			PRIMARY KEY (a)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin
		PARTITION BY RANGE COLUMNS(a)
		(PARTITION P0 VALUES LESS THAN (_binary 0x03),
		PARTITION P4 VALUES LESS THAN (_binary 0xc0),
		PARTITION PMX VALUES LESS THAN (MAXVALUE))`)
		testKit.MustExec(`insert into t values (0x5)`)
		testKit.MustExec(`analyze table t`)
		testKit.MustQuery(`select * from t`).Check(testkit.Rows("\u0005"))
		testKit.MustQuery(`select * from t where a = 0x5`).Check(testkit.Rows("\u0005"))
		testKit.MustQuery(`select * from t where a = 5`).Check(testkit.Rows())
		testKit.MustQuery(`select * from t where a IN (5,55)`).Check(testkit.Rows())
		testKit.MustQuery(`select * from t where a IN (0x5,55)`).Check(testkit.Rows("\u0005"))
		testKit.MustQuery(`explain select * from t where a = 0x5`).Check(testkit.Rows("Point_Get_1 1.00 root table:t, partition:P4, clustered index:PRIMARY(a) "))
		testKit.MustQuery(`explain format='brief' select * from t where a = 5`).Check(testkit.Rows(""+
			"TableReader 1.00 root partition:all data:Selection",
			"└─Selection 1.00 cop[tikv]  eq(cast(test.t.a, double BINARY), 5)",
			"  └─TableFullScan 1.00 cop[tikv] table:t keep order:false"))
		testKit.MustQuery(`explain format='brief' select * from t where a IN (5,55)`).Check(testkit.Rows(""+
			"TableReader 1.00 root partition:all data:Selection",
			"└─Selection 1.00 cop[tikv]  or(eq(cast(test.t.a, double BINARY), 5), eq(cast(test.t.a, double BINARY), 55))",
			"  └─TableFullScan 1.00 cop[tikv] table:t keep order:false"))
		testKit.MustQuery(`explain format='brief' select * from t where a IN (0x5,55)`).Check(testkit.Rows(""+
			"TableReader 1.00 root partition:all data:Selection",
			"└─Selection 1.00 cop[tikv]  or(eq(test.t.a, \"0x05\"), eq(cast(test.t.a, double BINARY), 55))",
			"  └─TableFullScan 1.00 cop[tikv] table:t keep order:false"))
	})
}

func TestTiFlashExtraColumnPrune(t *testing.T) {
	testkit.RunTestUnderCascadesWithDomain(t, func(t *testing.T, testKit *testkit.TestKit, dom *domain.Domain, cascades, caller string) {
		testKit.MustExec("use test")
		testKit.MustExec("set @@tidb_isolation_read_engines = 'tiflash'")
		testKit.MustExec("set @@tidb_enforce_mpp = on")
		testKit.MustExec("drop table if exists t1;")
		testKit.MustExec("create table t1(c1 int, c2 int)")

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
				output[i].Plan = testdata.ConvertRowsToStrings(testKit.MustQuery(tt).Rows())
			})
			testKit.MustQuery(tt).Check(testkit.Rows(output[i].Plan...))
		}
	})
}

func TestIndexMergeJSONMemberOf2FlakyPart(t *testing.T) {
	testkit.RunTestUnderCascades(t, func(t *testing.T, testKit *testkit.TestKit, cascades, caller string) {
		// The following tests are flaky, we add it in unit test to have more chance to debug the issue.
		testKit.MustExec(`use test`)
		testKit.MustExec(`drop table if exists t`)
		testKit.MustExec(`create table t(a int, b int, c int, d json, index iad(a, (cast(d->'$.b' as signed array))));`)
		testKit.MustExec(`insert into t value(1,1,1, '{"b":[1,2,3,4]}');`)
		testKit.MustExec(`insert into t value(2,2,2, '{"b":[3,4,5,6]}');`)
		testKit.MustExec(`set tidb_analyze_version=2;`)
		testKit.MustExec(`analyze table t all columns;`)
		testKit.MustQuery("explain select * from t use index (iad) where a = 1;").Check(testkit.Rows(
			"TableReader_8 1.00 root  data:Selection_7",
			"└─Selection_7 1.00 cop[tikv]  eq(test.t.a, 1)",
			"  └─TableFullScan_6 2.00 cop[tikv] table:t keep order:false",
		))
		testKit.MustQuery("explain select * from t use index (iad) where a = 1 and (2 member of (d->'$.b'));").Check(testkit.Rows(
			"IndexMerge_8 1.00 root  type: union",
			"├─IndexRangeScan_6(Build) 1.00 cop[tikv] table:t, index:iad(a, cast(json_extract(`d`, _utf8mb4'$.b') as signed array)) range:[1 2,1 2], keep order:false, stats:partial[d:unInitialized]",
			"└─TableRowIDScan_7(Probe) 1.00 cop[tikv] table:t keep order:false, stats:partial[d:unInitialized]",
		))
	})
}

func TestIssue56915(t *testing.T) {
	testkit.RunTestUnderCascades(t, func(t *testing.T, testKit *testkit.TestKit, cascades, caller string) {
		testKit.MustExec(`use test`)
		testKit.MustExec(`drop table if exists t`)
		testKit.MustExec(`create table t(a int, b int, j json, index ia(a), index mvi( (cast(j as signed array)), a, b) );`)
		testKit.MustExec(`insert into t value(1,1,'[1,2,3,4,5]');`)
		testKit.MustExec(`insert into t value(1,1,'[1,2,3,4,5]');`)
		testKit.MustExec(`insert into t value(1,1,'[1,2,3,4,5]');`)
		testKit.MustExec(`insert into t value(1,1,'[1,2,3,4,5]');`)
		testKit.MustExec(`insert into t value(1,1,'[6]');`)
		testKit.MustExec(`analyze table t all columns;`)
		testKit.MustQuery("explain format = brief select * from t where a = 1 and 6 member of (j);").Check(testkit.Rows(
			"IndexMerge 1.00 root  type: union",
			"├─IndexRangeScan(Build) 1.00 cop[tikv] table:t, index:mvi(cast(`j` as signed array), a, b) range:[6 1,6 1], keep order:false, stats:partial[j:unInitialized]",
			"└─TableRowIDScan(Probe) 1.00 cop[tikv] table:t keep order:false, stats:partial[j:unInitialized]",
		))
	})
}
