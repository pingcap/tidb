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

package rule

import (
	"testing"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testdata"
	"github.com/stretchr/testify/require"
)

func runJoinReorderTestData(t *testing.T, tk *testkit.TestKit, name string) {
	var input []string
	var output []struct {
		SQL     string
		Plan    []string
		Warning []string
	}
	joinReorderSuiteData := GetJoinReorderSuiteData()
	joinReorderSuiteData.LoadTestCasesByName(name, t, &input, &output)
	require.Equal(t, len(input), len(output))
	for i := range input {
		testdata.OnRecord(func() {
			output[i].SQL = input[i]
			output[i].Plan = testdata.ConvertRowsToStrings(tk.MustQuery("explain format = 'brief' " + input[i]).Rows())
			output[i].Warning = testdata.ConvertRowsToStrings(tk.MustQuery("show warnings").Rows())
		})
		tk.MustQuery("explain format = 'brief' " + input[i]).Check(testkit.Rows(output[i].Plan...))
		tk.MustQuery("show warnings").Check(testkit.Rows(output[i].Warning...))
	}
}

// test the global/session variable tidb_opt_enable_hash_join being set to no
func TestOptEnableHashJoin(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set tidb_opt_enable_hash_join=off")
	tk.MustExec("create table t1(a int, b int, key(a));")
	tk.MustExec("create table t2(a int, b int, key(a));")
	tk.MustExec("create table t3(a int, b int, key(a));")
	tk.MustExec("create table t4(a int, b int, key(a));")
	runJoinReorderTestData(t, tk, "TestOptEnableHashJoin")
}

func TestJoinOrderHint4TiFlash(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t, t1, t2, t3;")
	tk.MustExec("create table t(a int, b int, key(a));")
	tk.MustExec("create table t1(a int, b int, key(a));")
	tk.MustExec("create table t2(a int, b int, key(a));")
	tk.MustExec("create table t3(a int, b int, key(a));")
	tk.MustExec("create table t4(a int, b int, key(a));")
	tk.MustExec("create table t5(a int, b int, key(a));")
	tk.MustExec("create table t6(a int, b int, key(a));")
	tk.MustExec("set @@tidb_enable_outer_join_reorder=true")

	// Create virtual tiflash replica info.
	dom := domain.GetDomain(tk.Session())
	is := dom.InfoSchema()
	db, exists := is.SchemaByName(model.NewCIStr("test"))
	require.True(t, exists)
	for _, tbl := range is.SchemaTables(db.Name) {
		tblInfo := tbl.Meta()
		tableName := tblInfo.Name.L
		if tableName == "t" || tableName == "t1" || tableName == "t2" || tableName == "t3" || tableName == "t4" || tableName == "t5" || tableName == "t6" {
			tblInfo.TiFlashReplica = &model.TiFlashReplicaInfo{
				Count:     1,
				Available: true,
			}
		}
	}

	tk.MustExec("set @@tidb_allow_mpp=1; set @@tidb_enforce_mpp=1;")
	runJoinReorderTestData(t, tk, "TestJoinOrderHint4TiFlash")
}

func TestJoinOrderHint4DynamicPartitionTable(t *testing.T) {
	failpoint.Enable("github.com/pingcap/tidb/pkg/planner/core/forceDynamicPrune", `return(true)`)
	defer failpoint.Disable("github.com/pingcap/tidb/pkg/planner/core/forceDynamicPrune")
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t, t1, t2, t3;")
	tk.MustExec(`create table t(a int, b int) partition by hash(a) partitions 3`)
	tk.MustExec(`create table t1(a int, b int) partition by hash(a) partitions 4`)
	tk.MustExec(`create table t2(a int, b int) partition by hash(a) partitions 5`)
	tk.MustExec(`create table t3(a int, b int) partition by hash(b) partitions 3`)
	tk.MustExec(`create table t4(a int, b int) partition by hash(a) partitions 4`)
	tk.MustExec(`create table t5(a int, b int) partition by hash(a) partitions 5`)
	tk.MustExec(`create table t6(a int, b int) partition by hash(b) partitions 3`)

	tk.MustExec(`set @@tidb_partition_prune_mode="dynamic"`)
	tk.MustExec("set @@tidb_enable_outer_join_reorder=true")
	runJoinReorderTestData(t, tk, "TestJoinOrderHint4DynamicPartitionTable")
}
