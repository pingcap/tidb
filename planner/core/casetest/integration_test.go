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
	"fmt"
	"strings"
	"testing"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/statistics/handle"
	"github.com/pingcap/tidb/testkit"
	"github.com/pingcap/tidb/testkit/testdata"
	"github.com/stretchr/testify/require"
)

func TestAggColumnPrune(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec("set tidb_cost_model_version=2")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int)")
	tk.MustExec("insert into t values(1),(2)")

	var input []string
	var output []struct {
		SQL string
		Res []string
	}
	integrationSuiteData := GetIntegrationSuiteData()
	integrationSuiteData.LoadTestCases(t, &input, &output)
	for i, tt := range input {
		testdata.OnRecord(func() {
			output[i].SQL = tt
			output[i].Res = testdata.ConvertRowsToStrings(tk.MustQuery(tt).Rows())
		})
		tk.MustQuery(tt).Check(testkit.Rows(output[i].Res...))
	}
}

func TestIsFromUnixtimeNullRejective(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set tidb_cost_model_version=2")
	tk.MustExec(`drop table if exists t;`)
	tk.MustExec(`create table t(a bigint, b bigint);`)
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

func TestSimplifyOuterJoinWithCast(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("set tidb_cost_model_version=2")
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int not null, b datetime default null)")

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

func TestPartitionTableStats(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	{
		tk.MustExec(`set @@tidb_partition_prune_mode='` + string(variable.Static) + `'`)
		tk.MustExec("use test")
		tk.MustExec(`set tidb_opt_limit_push_down_threshold=0`)
		tk.MustExec("drop table if exists t")
		tk.MustExec("create table t(a int, b int)partition by range columns(a)(partition p0 values less than (10), partition p1 values less than(20), partition p2 values less than(30));")
		tk.MustExec("insert into t values(21, 1), (22, 2), (23, 3), (24, 4), (15, 5)")
		tk.MustExec("analyze table t")

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
}

func TestPartitionPruningForInExpr(t *testing.T) {
	failpoint.Enable("github.com/pingcap/tidb/planner/core/forceDynamicPrune", `return(true)`)
	defer failpoint.Disable("github.com/pingcap/tidb/planner/core/forceDynamicPrune")
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int(11) not null, b int) partition by range (a) (partition p0 values less than (4), partition p1 values less than(10), partition p2 values less than maxvalue);")
	tk.MustExec("insert into t values (1, 1),(10, 10),(11, 11)")

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

func TestMaxMinEliminate(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("set tidb_cost_model_version=2")
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int primary key)")
	tk.Session().GetSessionVars().EnableClusteredIndex = variable.ClusteredIndexDefModeOn
	tk.MustExec("create table cluster_index_t(a int, b int, c int, primary key (a, b));")

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

func TestSubqueryWithTopN(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("set tidb_cost_model_version=2")
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b int)")

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

func TestApproxPercentile(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b int)")
	tk.MustExec("insert into t values(1, 1), (2, 1), (3, 2), (4, 2), (5, 2)")

	var input []string
	var output []struct {
		SQL  string
		Plan []string
		Res  []string
	}
	integrationSuiteData := GetIntegrationSuiteData()
	integrationSuiteData.LoadTestCases(t, &input, &output)
	for i, tt := range input {
		testdata.OnRecord(func() {
			output[i].SQL = tt
			output[i].Plan = testdata.ConvertRowsToStrings(tk.MustQuery("explain " + tt).Rows())
			output[i].Res = testdata.ConvertRowsToStrings(tk.MustQuery(tt).Rows())
		})
		tk.MustQuery("explain " + tt).Check(testkit.Rows(output[i].Plan...))
		tk.MustQuery(tt).Check(testkit.Rows(output[i].Res...))
	}
}

func TestStreamAggProp(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int)")
	tk.MustExec("insert into t values(1),(1),(2)")

	var input []string
	var output []struct {
		SQL  string
		Plan []string
		Res  []string
	}
	integrationSuiteData := GetIntegrationSuiteData()
	integrationSuiteData.LoadTestCases(t, &input, &output)
	for i, tt := range input {
		testdata.OnRecord(func() {
			output[i].SQL = tt
			output[i].Plan = testdata.ConvertRowsToStrings(tk.MustQuery("explain format = 'brief' " + tt).Rows())
			output[i].Res = testdata.ConvertRowsToStrings(tk.MustQuery(tt).Rows())
		})
		tk.MustQuery("explain format = 'brief' " + tt).Check(testkit.Rows(output[i].Plan...))
		tk.MustQuery(tt).Check(testkit.Rows(output[i].Res...))
	}
}

func TestPartitionExplain(t *testing.T) {
	failpoint.Enable("github.com/pingcap/tidb/planner/core/forceDynamicPrune", `return(true)`)
	defer failpoint.Disable("github.com/pingcap/tidb/planner/core/forceDynamicPrune")
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec(`create table pt (id int, c int, key i_id(id), key i_c(c)) partition by range (c) (
partition p0 values less than (4),
partition p1 values less than (7),
partition p2 values less than (10))`)

	tk.MustExec("set @@tidb_enable_index_merge = 1;")

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
			output[i].Plan = testdata.ConvertRowsToStrings(tk.MustQuery("explain " + tt).Rows())
		})
		tk.MustQuery("explain " + tt).Check(testkit.Rows(output[i].Plan...))
	}
}

func TestIssue20710(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t;")
	tk.MustExec("drop table if exists s;")
	tk.MustExec("create table t(a int, b int)")
	tk.MustExec("create table s(a int, b int, index(a))")
	tk.MustExec("insert into t values(1,1),(1,2),(2,2)")
	tk.MustExec("insert into s values(1,1),(2,2),(2,1)")

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

// Apply operator may got panic because empty Projection is eliminated.
func TestIssue23887(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t(a int, b int);")
	tk.MustExec("insert into t values(1, 2), (3, 4);")
	var input []string
	var output []struct {
		SQL  string
		Plan []string
		Res  []string
	}
	integrationSuiteData := GetIntegrationSuiteData()
	integrationSuiteData.LoadTestCases(t, &input, &output)
	for i, tt := range input {
		testdata.OnRecord(func() {
			output[i].SQL = tt
			output[i].Plan = testdata.ConvertRowsToStrings(tk.MustQuery("explain format = 'brief' " + tt).Rows())
			output[i].Res = testdata.ConvertRowsToStrings(tk.MustQuery(tt).Sort().Rows())
		})
		tk.MustQuery("explain format = 'brief' " + tt).Check(testkit.Rows(output[i].Plan...))
		tk.MustQuery(tt).Sort().Check(testkit.Rows(output[i].Res...))
	}

	tk.MustExec("drop table if exists t1;")
	tk.MustExec("create table t1 (c1 int primary key, c2 int, c3 int, index c2 (c2));")
	tk.MustQuery("select count(1) from (select count(1) from (select * from t1 where c3 = 100) k) k2;").Check(testkit.Rows("1"))
}

func TestReorderSimplifiedOuterJoins(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec("set tidb_cost_model_version=2")
	tk.MustExec("drop table if exists t1,t2,t3")
	tk.MustExec("create table t1 (pk char(32) primary key nonclustered, col1 char(32), col2 varchar(40), col3 char(32), key (col1), key (col3), key (col2,col3), key (col1,col3))")
	tk.MustExec("create table t2 (pk char(32) primary key nonclustered, col1 varchar(100))")
	tk.MustExec("create table t3 (pk char(32) primary key nonclustered, keycol varchar(100), pad1 tinyint(1) default null, pad2 varchar(40), key (keycol,pad1,pad2))")

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

// TestIsMatchProp is used to test https://github.com/pingcap/tidb/issues/26017.
func TestIsMatchProp(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1, t2")
	tk.MustExec("create table t1(a int, b int, c int, d int, index idx_a_b_c(a, b, c))")
	tk.MustExec("create table t2(a int, b int, c int, d int, index idx_a_b_c_d(a, b, c, d))")

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
			output[i].Plan = testdata.ConvertRowsToStrings(tk.MustQuery("explain format = 'brief' " + tt).Rows())
		})
		tk.MustQuery("explain format = 'brief' " + tt).Check(testkit.Rows(output[i].Plan...))
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

func TestDecorrelateInnerJoinInSubquery(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int not null, b int not null)")

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

func TestDecorrelateLimitInSubquery(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists test")
	tk.MustExec("create table test(id int, value int)")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(c int)")
	tk.MustExec("insert t values(10), (8), (7), (9), (11)")

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

func TestConvertRangeToPoint(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t0")
	tk.MustExec("create table t0 (a int, b int, index(a, b))")
	tk.MustExec("insert into t0 values (1, 1)")
	tk.MustExec("insert into t0 values (2, 2)")
	tk.MustExec("insert into t0 values (2, 2)")
	tk.MustExec("insert into t0 values (2, 2)")
	tk.MustExec("insert into t0 values (2, 2)")
	tk.MustExec("insert into t0 values (2, 2)")
	tk.MustExec("insert into t0 values (3, 3)")

	tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t1 (a int, b int, c int, index(a, b, c))")

	tk.MustExec("drop table if exists t2")
	tk.MustExec("create table t2 (a float, b float, index(a, b))")

	tk.MustExec("drop table if exists t3")
	tk.MustExec("create table t3 (a char(10), b char(10), c char(10), index(a, b, c))")

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

func TestIssue22105(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec(`CREATE TABLE t1 (
  key1 int(11) NOT NULL,
  key2 int(11) NOT NULL,
  key3 int(11) NOT NULL,
  key4 int(11) NOT NULL,
  key5 int(11) DEFAULT NULL,
  key6 int(11) DEFAULT NULL,
  key7 int(11) NOT NULL,
  key8 int(11) NOT NULL,
  KEY i1 (key1),
  KEY i2 (key2),
  KEY i3 (key3),
  KEY i4 (key4),
  KEY i5 (key5),
  KEY i6 (key6)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin`)

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

func TestRegardNULLAsPoint(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustExec("drop table if exists tpk")
	tk.MustExec(`create table tuk (a int, b int, c int, unique key (a, b, c))`)
	tk.MustExec(`create table tik (a int, b int, c int, key (a, b, c))`)
	for _, va := range []string{"NULL", "1"} {
		for _, vb := range []string{"NULL", "1"} {
			for _, vc := range []string{"NULL", "1"} {
				tk.MustExec(fmt.Sprintf(`insert into tuk values (%v, %v, %v)`, va, vb, vc))
				tk.MustExec(fmt.Sprintf(`insert into tik values (%v, %v, %v)`, va, vb, vc))
				if va == "1" && vb == "1" && vc == "1" {
					continue
				}
				// duplicated NULL rows
				tk.MustExec(fmt.Sprintf(`insert into tuk values (%v, %v, %v)`, va, vb, vc))
				tk.MustExec(fmt.Sprintf(`insert into tik values (%v, %v, %v)`, va, vb, vc))
			}
		}
	}

	var input []string
	var output []struct {
		SQL          string
		PlanEnabled  []string
		PlanDisabled []string
		Result       []string
	}
	integrationSuiteData := GetIntegrationSuiteData()
	integrationSuiteData.LoadTestCases(t, &input, &output)
	for i, tt := range input {
		testdata.OnRecord(func() {
			output[i].SQL = tt
			tk.MustExec(`set @@session.tidb_regard_null_as_point=true`)
			output[i].PlanEnabled = testdata.ConvertRowsToStrings(tk.MustQuery("explain " + tt).Rows())
			output[i].Result = testdata.ConvertRowsToStrings(tk.MustQuery(tt).Rows())

			tk.MustExec(`set @@session.tidb_regard_null_as_point=false`)
			output[i].PlanDisabled = testdata.ConvertRowsToStrings(tk.MustQuery("explain " + tt).Rows())
		})
		tk.MustExec(`set @@session.tidb_regard_null_as_point=true`)
		tk.MustQuery("explain " + tt).Check(testkit.Rows(output[i].PlanEnabled...))
		tk.MustQuery(tt).Check(testkit.Rows(output[i].Result...))

		tk.MustExec(`set @@session.tidb_regard_null_as_point=false`)
		tk.MustQuery("explain " + tt).Check(testkit.Rows(output[i].PlanDisabled...))
		tk.MustQuery(tt).Check(testkit.Rows(output[i].Result...))
	}
}

func TestIssue30200(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1;")
	tk.MustExec("create table t1(c1 varchar(100), c2 varchar(100), key(c1), key(c2), c3 varchar(100));")
	tk.MustExec("insert into t1 values('ab', '10', '10');")

	tk.MustExec("drop table if exists tt1;")
	tk.MustExec("create table tt1(c1 varchar(100), c2 varchar(100), c3 varchar(100), c4 varchar(100), key idx_0(c1), key idx_1(c2, c3));")
	tk.MustExec("insert into tt1 values('ab', '10', '10', '10');")

	tk.MustExec("drop table if exists tt2;")
	tk.MustExec("create table tt2 (c1 int , pk int, primary key( pk ) , unique key( c1));")
	tk.MustExec("insert into tt2 values(-3896405, -1), (-2, 1), (-1, -2);")

	tk.MustExec("drop table if exists tt3;")
	tk.MustExec("create table tt3(c1 int, c2 int, c3 int as (c1 + c2), key(c1), key(c2), key(c3));")
	tk.MustExec("insert into tt3(c1, c2) values(1, 1);")

	oriIndexMergeSwitcher := tk.MustQuery("select @@tidb_enable_index_merge;").Rows()[0][0].(string)
	tk.MustExec("set tidb_enable_index_merge = on;")
	defer func() {
		tk.MustExec(fmt.Sprintf("set tidb_enable_index_merge = %s;", oriIndexMergeSwitcher))
	}()

	var input []string
	var output []struct {
		SQL  string
		Plan []string
		Res  []string
	}
	integrationSuiteData := GetIntegrationSuiteData()
	integrationSuiteData.LoadTestCases(t, &input, &output)
	for i, tt := range input {
		testdata.OnRecord(func() {
			output[i].SQL = tt
			output[i].Plan = testdata.ConvertRowsToStrings(tk.MustQuery("explain format=brief " + tt).Rows())
			output[i].Res = testdata.ConvertRowsToStrings(tk.MustQuery(tt).Rows())
		})
		tk.MustQuery("explain format=brief " + tt).Check(testkit.Rows(output[i].Plan...))
		tk.MustQuery(tt).Check(testkit.Rows(output[i].Res...))
	}
}

func TestMultiColMaxOneRow(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1,t2")
	tk.MustExec("create table t1(a int)")
	tk.MustExec("create table t2(a int, b int, c int, primary key(a,b) nonclustered)")

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
			output[i].Plan = testdata.ConvertRowsToStrings(tk.MustQuery("explain format = 'brief' " + tt).Rows())
		})
		tk.MustQuery("explain format = 'brief' " + tt).Check(testkit.Rows(output[i].Plan...))
	}
}

// TestSequenceAsDataSource is used to test https://github.com/pingcap/tidb/issues/24383.
func TestSequenceAsDataSource(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec("set tidb_cost_model_version=2")
	tk.MustExec("drop sequence if exists s1, s2")
	tk.MustExec("create sequence s1")
	tk.MustExec("create sequence s2")

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
			output[i].Plan = testdata.ConvertRowsToStrings(tk.MustQuery("explain format = 'brief' " + tt).Rows())
		})
		tk.MustQuery("explain format = 'brief' " + tt).Check(testkit.Rows(output[i].Plan...))
	}
}

func TestOutputSkylinePruningInfo(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set tidb_cost_model_version=2")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b int, c int, d int, e int, f int, g int, primary key (a), unique key c_d_e (c, d, e), unique key f (f), unique key f_g (f, g), key g (g))")

	// Default RPC encoding may cause statistics explain result differ and then the test unstable.
	tk.MustExec("set @@tidb_enable_chunk_rpc = on")

	var input []string
	var output []struct {
		SQL      string
		Plan     []string
		Warnings []string
	}
	integrationSuiteData := GetIntegrationSuiteData()
	integrationSuiteData.LoadTestCases(t, &input, &output)
	for i, tt := range input {
		testdata.OnRecord(func() {
			output[i].SQL = tt
			output[i].Plan = testdata.ConvertRowsToStrings(tk.MustQuery("explain format = 'verbose' " + tt).Rows())
			output[i].Warnings = testdata.ConvertRowsToStrings(tk.MustQuery("show warnings").Rows())
		})
		tk.MustQuery("explain format = 'verbose' " + tt).Check(testkit.Rows(output[i].Plan...))
		tk.MustQuery("show warnings").Check(testkit.Rows(output[i].Warnings...))
	}
}

func TestPreferRangeScanForUnsignedIntHandle(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set tidb_cost_model_version=2")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int unsigned primary key, b int, c int, index idx_b(b))")
	tk.MustExec("insert into t values (1,2,3), (4,5,6), (7,8,9), (10,11,12), (13,14,15)")
	do, _ := session.GetDomain(store)
	require.Nil(t, do.StatsHandle().DumpStatsDeltaToKV(handle.DumpAll))
	tk.MustExec("analyze table t")

	// Default RPC encoding may cause statistics explain result differ and then the test unstable.
	tk.MustExec("set @@tidb_enable_chunk_rpc = on")

	var input []string
	var output []struct {
		SQL      string
		Plan     []string
		Warnings []string
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
			output[i].SQL = tt
			output[i].Plan = testdata.ConvertRowsToStrings(tk.MustQuery(tt).Rows())
			output[i].Warnings = testdata.ConvertRowsToStrings(tk.MustQuery("show warnings").Rows())
		})
		tk.MustQuery(tt).Check(testkit.Rows(output[i].Plan...))
		tk.MustQuery("show warnings").Check(testkit.Rows(output[i].Warnings...))
	}
}

func TestIssue27083(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int primary key, b int, c int, index idx_b(b))")
	tk.MustExec("insert into t values (1,2,3), (4,5,6), (7,8,9), (10, 11, 12), (13,14,15), (16, 17, 18)")
	do, _ := session.GetDomain(store)
	require.Nil(t, do.StatsHandle().DumpStatsDeltaToKV(handle.DumpAll))
	tk.MustExec("analyze table t")

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
			output[i].Plan = testdata.ConvertRowsToStrings(tk.MustQuery("explain format = 'brief' " + tt).Rows())
		})
		tk.MustQuery("explain format = 'brief' " + tt).Check(testkit.Rows(output[i].Plan...))
	}
}

func TestGroupBySetVar(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t1(c1 int);")
	tk.MustExec("insert into t1 values(1), (2), (3), (4), (5), (6);")
	rows := tk.MustQuery("select floor(dt.rn/2) rownum, count(c1) from (select @rownum := @rownum + 1 rn, c1 from (select @rownum := -1) drn, t1) dt group by floor(dt.rn/2) order by rownum;")
	rows.Check(testkit.Rows("0 2", "1 2", "2 2"))

	tk.MustExec("create table ta(a int, b int);")
	tk.MustExec("set sql_mode='';")

	var input []string
	var output []struct {
		SQL  string
		Plan []string
	}
	integrationSuiteData := GetIntegrationSuiteData()
	integrationSuiteData.LoadTestCases(t, &input, &output)
	for i, tt := range input {
		res := tk.MustQuery("explain format = 'brief' " + tt)
		testdata.OnRecord(func() {
			output[i].SQL = tt
			output[i].Plan = testdata.ConvertRowsToStrings(res.Rows())
		})
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
	failpoint.Enable("github.com/pingcap/tidb/planner/core/forceDynamicPrune", `return(true)`)
	defer failpoint.Disable("github.com/pingcap/tidb/planner/core/forceDynamicPrune")

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

func TestDowncastPointGetOrRangeScan(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t1 (a bigint key)")
	tk.MustExec("create table t2 (a int key)")
	tk.MustExec("create definer=`root`@`127.0.0.1` view v1 as (select a from t1) union (select a from t2)")
	// select * from v where a = 1 will lead a condition: EQ(cast(t2.a as bigint), 1),
	// we should downcast it, utilizing t2.a =1 to walking through the pk point-get. Because cast doesn't contain any precision loss.

	tk.MustExec("create table t3 (a varchar(100) key)")
	tk.MustExec("create table t4 (a varchar(10) key)")
	tk.MustExec("create definer=`root`@`127.0.0.1` view v2 as (select a from t3) union (select a from t4)")
	// select * from v2 where a = 'test' will lead a condition: EQ(cast(t2.a as varchar(100) same collation), 1),
	// we should downcast it, utilizing t2.a = 'test' to walking through the pk point-get. Because cast doesn't contain any precision loss.

	tk.MustExec("create table t5 (a char(100) key)")
	tk.MustExec("create table t6 (a char(10) key)")
	tk.MustExec("create definer=`root`@`127.0.0.1` view v3 as (select a from t5) union (select a from t6)")
	// select * from v3 where a = 'test' will lead a condition: EQ(cast(t2.a as char(100) same collation), 1),
	// for char type, it depends, with binary collate, the appended '0' after cast column a from char(10) to char(100) will make some difference
	// on comparison on where a = 'test' before and after the UNION operator; so we didn't allow this kind of type downcast currently (precision diff).

	tk.MustExec("create table t7 (a varchar(100) key)")
	tk.MustExec("create table t8 (a int key)")
	tk.MustExec("create definer=`root`@`127.0.0.1` view v4 as (select a from t7) union (select a from t8)")
	// since UNION OP will unify the a(int) and a(varchar100) as varchar(100)
	// select * from v4 where a = "test" will lead a condition: EQ(cast(t2.a as varchar(100)), "test"), and since
	// cast int to varchar(100) may have some precision loss, we couldn't utilize a="test" to get the range directly.

	var input []string
	var output []struct {
		SQL    string
		Plan   []string
		Result []string
	}
	integrationSuiteData := GetIntegrationSuiteData()
	integrationSuiteData.LoadTestCases(t, &input, &output)
	for i, tt := range input {
		testdata.OnRecord(func() {
			output[i].SQL = tt
			output[i].Plan = testdata.ConvertRowsToStrings(tk.MustQuery("explain format='brief' " + tt).Rows())
			output[i].Result = testdata.ConvertRowsToStrings(tk.MustQuery(tt).Sort().Rows())
		})
		tk.MustQuery("explain format='brief' " + tt).Check(testkit.Rows(output[i].Plan...))
		tk.MustQuery(tt).Sort().Check(testkit.Rows(output[i].Result...))
	}
}

// https://github.com/pingcap/tidb/issues/24095
func TestIssue24095(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test;")
	tk.MustExec("set tidb_cost_model_version=2")
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t (id int, value decimal(10,5));")
	tk.MustExec("desc format = 'brief' select count(*) from t join (select t.id, t.value v1 from t join t t1 on t.id = t1.id order by t.value limit 1) v on v.id = t.id and v.v1 = t.value;")

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
			output[i].Plan = testdata.ConvertRowsToStrings(tk.MustQuery("explain format = 'brief' " + tt).Rows())
		})
		tk.MustQuery("explain format = 'brief' " + tt).Check(testkit.Rows(output[i].Plan...))
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
	tk.MustQuery(`explain select * from t where a=2`).Check(testkit.Rows(
		`TableReader_7 128.00 root  data:Selection_6`,
		`└─Selection_6 128.00 cop[tikv]  eq(test.t.a, 2)`,
		`  └─TableFullScan_5 12928.00 cop[tikv] table:t keep order:false`))

	tk.MustExec(`set @@tidb_opt_fix_control = "45132:99"`)
	tk.MustQuery(`explain select * from t where a=2`).Check(testkit.Rows(
		`IndexLookUp_7 128.00 root  `,
		`├─IndexRangeScan_5(Build) 128.00 cop[tikv] table:t, index:a(a) range:[2,2], keep order:false`,
		`└─TableRowIDScan_6(Probe) 128.00 cop[tikv] table:t keep order:false`))

	tk.MustExec(`set @@tidb_opt_fix_control = "45132:500"`)
	tk.MustQuery(`explain select * from t where a=2`).Check(testkit.Rows(
		`TableReader_7 128.00 root  data:Selection_6`,
		`└─Selection_6 128.00 cop[tikv]  eq(test.t.a, 2)`,
		`  └─TableFullScan_5 12928.00 cop[tikv] table:t keep order:false`))

	tk.MustExec(`set @@tidb_opt_fix_control = "45132:0"`)
	tk.MustQuery(`explain select * from t where a=2`).Check(testkit.Rows(
		`TableReader_7 128.00 root  data:Selection_6`,
		`└─Selection_6 128.00 cop[tikv]  eq(test.t.a, 2)`,
		`  └─TableFullScan_5 12928.00 cop[tikv] table:t keep order:false`))
}

func TestFixControl44262(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`use test`)
	tk.MustExec(`set tidb_partition_prune_mode='dynamic'`)
	tk.MustExec(`create table t1 (a int, b int)`)
	tk.MustExec(`create table t2_part (a int, b int, key(a)) partition by hash(a) partitions 4`)

	testJoin := func(q, join string) {
		found := false
		for _, x := range tk.MustQuery(`explain ` + q).Rows() {
			if strings.Contains(x[0].(string), join) {
				found = true
			}
		}
		if !found {
			t.Fatal(q, join)
		}
	}

	testJoin(`select /*+ TIDB_INLJ(t2_part@sel_2) */ * from t1 where t1.b<10 and not exists (select 1 from t2_part where t1.a=t2_part.a and t2_part.b<20)`, "HashJoin")
	tk.MustQuery(`show warnings`).Sort().Check(testkit.Rows(
		`Warning 1105 disable dynamic pruning due to t2_part has no global stats`,
		`Warning 1815 Optimizer Hint /*+ INL_JOIN(t2_part) */ or /*+ TIDB_INLJ(t2_part) */ is inapplicable`))
	tk.MustExec(`set @@tidb_opt_fix_control = "44262:ON"`)
	testJoin(`select /*+ TIDB_INLJ(t2_part@sel_2) */ * from t1 where t1.b<10 and not exists (select 1 from t2_part where t1.a=t2_part.a and t2_part.b<20)`, "IndexJoin")
	tk.MustQuery(`show warnings`).Sort().Check(testkit.Rows()) // no warning
}
