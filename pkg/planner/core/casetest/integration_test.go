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
	"strings"
	"testing"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testdata"
	"github.com/stretchr/testify/require"
)

func TestVerboseExplain(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set tidb_cost_model_version=2")
	tk.MustExec(`set tidb_opt_limit_push_down_threshold=0`)
	tk.MustExec("drop table if exists t1, t2, t3")
	tk.MustExec("create table t1(a int, b int)")
	tk.MustExec("create table t2(a int, b int)")
	tk.MustExec("create table t3(a int, b int, index c(b))")
	tk.MustExec("insert into t1 values(1,2)")
	tk.MustExec("insert into t1 values(3,4)")
	tk.MustExec("insert into t1 values(5,6)")
	tk.MustExec("insert into t2 values(1,2)")
	tk.MustExec("insert into t2 values(3,4)")
	tk.MustExec("insert into t2 values(5,6)")
	tk.MustExec("insert into t3 values(1,2)")
	tk.MustExec("insert into t3 values(3,4)")
	tk.MustExec("insert into t3 values(5,6)")
	tk.MustExec("analyze table t1")
	tk.MustExec("analyze table t2")
	tk.MustExec("analyze table t3")

	// Default RPC encoding may cause statistics explain result differ and then the test unstable.
	tk.MustExec("set @@tidb_enable_chunk_rpc = on")

	// Create virtual tiflash replica info.
	dom := domain.GetDomain(tk.Session())
	is := dom.InfoSchema()
	db, exists := is.SchemaByName(model.NewCIStr("test"))
	require.True(t, exists)
	for _, tblInfo := range db.Tables {
		if tblInfo.Name.L == "t1" || tblInfo.Name.L == "t2" {
			tblInfo.TiFlashReplica = &model.TiFlashReplicaInfo{
				Count:     1,
				Available: true,
			}
		}
	}

	var input []string
	var output []struct {
		SQL  string
		Plan []string
	}
	integrationSuiteData := GetIntegrationSuiteData()
	integrationSuiteData.LoadTestCases(t, &input, &output)
	for i, tt := range input {
		testdata.OnRecord(func() {
			output[i].SQL = tt
			output[i].Plan = testdata.ConvertRowsToStrings(tk.MustQuery(tt).Rows())
		})
		res := tk.MustQuery(tt)
		res.Check(testkit.Rows(output[i].Plan...))
	}
}

func TestIsolationReadTiFlashNotChoosePointGet(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b int, primary key (a))")

	// Create virtual tiflash replica info.
	dom := domain.GetDomain(tk.Session())
	is := dom.InfoSchema()
	db, exists := is.SchemaByName(model.NewCIStr("test"))
	require.True(t, exists)
	for _, tblInfo := range db.Tables {
		tblInfo.TiFlashReplica = &model.TiFlashReplicaInfo{
			Count:     1,
			Available: true,
		}
	}

	tk.MustExec("set @@session.tidb_isolation_read_engines=\"tiflash\"")
	var input []string
	var output []struct {
		SQL    string
		Result []string
	}
	integrationSuiteData := GetIntegrationSuiteData()
	integrationSuiteData.LoadTestCases(t, &input, &output)
	for i, tt := range input {
		testdata.OnRecord(func() {
			output[i].SQL = tt
			output[i].Result = testdata.ConvertRowsToStrings(tk.MustQuery(tt).Rows())
		})
		tk.MustQuery(tt).Check(testkit.Rows(output[i].Result...))
	}
}

func TestIsolationReadDoNotFilterSystemDB(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec("set @@tidb_isolation_read_engines = \"tiflash\"")
	var input []string
	var output []struct {
		SQL  string
		Plan []string
	}
	integrationSuiteData := GetIntegrationSuiteData()
	integrationSuiteData.LoadTestCases(t, &input, &output)
	for i, tt := range input {
		testdata.OnRecord(func() {
			output[i].SQL = tt
			output[i].Plan = testdata.ConvertRowsToStrings(tk.MustQuery(tt).Rows())
		})
		res := tk.MustQuery(tt)
		res.Check(testkit.Rows(output[i].Plan...))
	}
}

func TestMergeContinuousSelections(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists ts")
	tk.MustExec("create table ts (col_char_64 char(64), col_varchar_64_not_null varchar(64) not null, col_varchar_key varchar(1), id int primary key, col_varchar_64 varchar(64),col_char_64_not_null char(64) not null);")

	// Create virtual tiflash replica info.
	dom := domain.GetDomain(tk.Session())
	is := dom.InfoSchema()
	db, exists := is.SchemaByName(model.NewCIStr("test"))
	require.True(t, exists)
	for _, tblInfo := range db.Tables {
		if tblInfo.Name.L == "ts" {
			tblInfo.TiFlashReplica = &model.TiFlashReplicaInfo{
				Count:     1,
				Available: true,
			}
		}
	}

	tk.MustExec(" set @@tidb_allow_mpp=1;")

	var input []string
	var output []struct {
		SQL  string
		Plan []string
	}
	integrationSuiteData := GetIntegrationSuiteData()
	integrationSuiteData.LoadTestCases(t, &input, &output)
	for i, tt := range input {
		testdata.OnRecord(func() {
			output[i].SQL = tt
			output[i].Plan = testdata.ConvertRowsToStrings(tk.MustQuery(tt).Rows())
		})
		res := tk.MustQuery(tt)
		res.Check(testkit.Rows(output[i].Plan...))
	}
}

func TestIssue31240(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec("create table t31240(a int, b int);")
	tk.MustExec("set @@tidb_allow_mpp = 0")
	tk.MustExec("set tidb_cost_model_version=2")
	// since allow-mpp is adjusted to false, there will be no physical plan if TiFlash cop is banned.
	tk.MustExec("set @@session.tidb_allow_tiflash_cop=ON")

	tbl, err := dom.InfoSchema().TableByName(model.CIStr{O: "test", L: "test"}, model.CIStr{O: "t31240", L: "t31240"})
	require.NoError(t, err)
	// Set the hacked TiFlash replica for explain tests.
	tbl.Meta().TiFlashReplica = &model.TiFlashReplicaInfo{Count: 1, Available: true}

	var input []string
	var output []struct {
		SQL  string
		Plan []string
	}
	integrationSuiteData := GetIntegrationSuiteData()
	integrationSuiteData.LoadTestCases(t, &input, &output)
	for i, tt := range input {
		testdata.OnRecord(func() {
			output[i].SQL = tt
		})
		if strings.HasPrefix(tt, "set") {
			tk.MustExec(tt)
			continue
		}
		testdata.OnRecord(func() {
			output[i].Plan = testdata.ConvertRowsToStrings(tk.MustQuery(tt).Rows())
		})
		tk.MustQuery(tt).Check(testkit.Rows(output[i].Plan...))
	}
	tk.MustExec("drop table if exists t31240")
}

func TestIssue32632(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
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
	require.NoError(t, h.HandleDDLEvent(<-h.DDLEventCh()))
	tk.MustExec("set @@tidb_enforce_mpp = 1")

	tbl1, err := dom.InfoSchema().TableByName(model.CIStr{O: "test", L: "test"}, model.CIStr{O: "partsupp", L: "partsupp"})
	require.NoError(t, err)
	tbl2, err := dom.InfoSchema().TableByName(model.CIStr{O: "test", L: "test"}, model.CIStr{O: "supplier", L: "supplier"})
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
	integrationSuiteData.LoadTestCases(t, &input, &output)
	for i, tt := range input {
		testdata.OnRecord(func() {
			output[i].SQL = tt
			output[i].Plan = testdata.ConvertRowsToStrings(tk.MustQuery(tt).Rows())
		})
		tk.MustQuery(tt).Check(testkit.Rows(output[i].Plan...))
	}
	tk.MustExec("drop table if exists partsupp")
	tk.MustExec("drop table if exists supplier")
}

func TestTiFlashPartitionTableScan(t *testing.T) {
	failpoint.Enable("github.com/pingcap/tidb/pkg/planner/core/forceDynamicPrune", `return(true)`)
	defer failpoint.Disable("github.com/pingcap/tidb/pkg/planner/core/forceDynamicPrune")

	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
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
	tbl1, err := dom.InfoSchema().TableByName(model.CIStr{O: "test", L: "test"}, model.CIStr{O: "rp_t", L: "rp_t"})
	require.NoError(t, err)
	tbl2, err := dom.InfoSchema().TableByName(model.CIStr{O: "test", L: "test"}, model.CIStr{O: "hp_t", L: "hp_t"})
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
	integrationSuiteData.LoadTestCases(t, &input, &output)
	for i, tt := range input {
		testdata.OnRecord(func() {
			output[i].SQL = tt
			output[i].Plan = testdata.ConvertRowsToStrings(tk.MustQuery(tt).Rows())
		})
		tk.MustQuery(tt).Check(testkit.Rows(output[i].Plan...))
	}
	tk.MustExec("drop table rp_t;")
	tk.MustExec("drop table hp_t;")
}

func TestTiFlashFineGrainedShuffle(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set tidb_cost_model_version=2")
	tk.MustExec("set @@tidb_isolation_read_engines = 'tiflash'")
	tk.MustExec("set @@tidb_enforce_mpp = on")
	tk.MustExec("drop table if exists t1;")
	tk.MustExec("create table t1(c1 int, c2 int)")

	tbl1, err := dom.InfoSchema().TableByName(model.CIStr{O: "test", L: "test"}, model.CIStr{O: "t1", L: "t1"})
	require.NoError(t, err)
	// Set the hacked TiFlash replica for explain tests.
	tbl1.Meta().TiFlashReplica = &model.TiFlashReplicaInfo{Count: 1, Available: true}
	var input []string
	var output []struct {
		SQL  string
		Plan []string
	}
	integrationSuiteData := GetIntegrationSuiteData()
	integrationSuiteData.LoadTestCases(t, &input, &output)
	for i, tt := range input {
		testdata.OnRecord(func() {
			output[i].SQL = tt
			output[i].Plan = testdata.ConvertRowsToStrings(tk.MustQuery(tt).Rows())
		})
		tk.MustQuery(tt).Check(testkit.Rows(output[i].Plan...))
	}
}

func TestFixControl45132(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`use test`)
	tk.MustExec(`create table t (a int, b int, key(a))`)
	values := make([]string, 0, 101)
	for i := 0; i < 100; i++ {
		values = append(values, "(1, 1)")
	}
	values = append(values, "(2, 2)") // count(1) : count(2) == 100 : 1
	tk.MustExec(`insert into t values ` + strings.Join(values, ","))
	for i := 0; i < 7; i++ {
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
}
