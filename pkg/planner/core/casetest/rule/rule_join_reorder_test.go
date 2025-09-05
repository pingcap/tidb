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
	"strconv"
	"testing"

	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testdata"
	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
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

func TestCartesianJoinOrder(t *testing.T) {
	testkit.RunTestUnderCascades(t, func(t *testing.T, tk *testkit.TestKit, cascades, caller string) {
		tk.MustExec("use test")
		tk.MustExec("create table t1 (a int, b int, c int, key(a), key(b))")
		tk.MustExec("create table t2 (a int, b int, c int, key(a), key(b))")
		tk.MustExec("create table t3 (a int, b int, c int, key(a), key(b))")
		tk.MustExec(`insert into t1 select * from (
    with recursive x as (
        select 1 as a, 1 as b, 1 as c, 1 as n
        union all
        select a+1 as a, b+1 as b, c+1 as c, n+1 from x where n < 10
    )
    select a, b, c from x
) tt`)
		tk.MustExec(`set @@cte_max_recursion_depth=10000`)
		tk.MustExec(`insert into t2 select * from (
    with recursive x as (
        select 2 as a, 2 as b, 2 as c, 1 as n
        union all
        select a, b, c, n+1 from x where n < 10000
    )
    select a, b, c from x
) tt`)
		tk.MustExec(`insert into t3 select * from (
    with recursive x as (
        select 2 as a, 2 as b, 2 as c, 1 as n
        union all
        select a, b, c, n+1 from x where n < 5
    )
    select a, b, c from x
) tt`)
		tk.MustExec(`analyze table t1, t2, t3 all columns`)
		runJoinReorderTestData(t, tk, caller)

		// after setting tidb_opt_cartesian_join_order_threshold, we could get a plan with cartesian join and lower cost.
		costWithoutCartesian, err := strconv.ParseFloat(
			tk.MustQuery(`explain format='verbose' select /*+ set_var(tidb_opt_cartesian_join_order_threshold=0) */ 1
								from t1, t2, t3 where t1.a = t2.a and t2.b = t3.b`).Rows()[0][2].(string), 64)
		require.NoError(t, err)
		costWithCartesian, err := strconv.ParseFloat(
			tk.MustQuery(`explain format='verbose' select /*+ set_var(tidb_opt_cartesian_join_order_threshold=100) */ 1
								from t1, t2, t3 where t1.a = t2.a and t2.b = t3.b`).Rows()[0][2].(string), 64)
		require.NoError(t, err)
		require.Less(t, costWithCartesian, costWithoutCartesian)
	})
}

// test the global/session variable tidb_opt_enable_hash_join being set to no
func TestOptEnableHashJoin(t *testing.T) {
	testkit.RunTestUnderCascades(t, func(t *testing.T, testKit *testkit.TestKit, cascades, caller string) {
		testKit.MustExec("use test")
		testKit.MustExec("set tidb_opt_enable_hash_join=off")
		testKit.MustExec("create table t1(a int, b int, key(a));")
		testKit.MustExec("create table t2(a int, b int, key(a));")
		testKit.MustExec("create table t3(a int, b int, key(a));")
		testKit.MustExec("create table t4(a int, b int, key(a));")
		runJoinReorderTestData(t, testKit, "TestOptEnableHashJoin")
	})
}

func TestJoinOrderHint4TiFlash(t *testing.T) {
	testkit.RunTestUnderCascadesWithDomain(t, func(t *testing.T, testKit *testkit.TestKit, dom *domain.Domain, cascades, caller string) {
		testKit.MustExec("use test")
		testKit.MustExec("drop table if exists t, t1, t2, t3;")
		testKit.MustExec("create table t(a int, b int, key(a));")
		testKit.MustExec("create table t1(a int, b int, key(a));")
		testKit.MustExec("create table t2(a int, b int, key(a));")
		testKit.MustExec("create table t3(a int, b int, key(a));")
		testKit.MustExec("create table t4(a int, b int, key(a));")
		testKit.MustExec("create table t5(a int, b int, key(a));")
		testKit.MustExec("create table t6(a int, b int, key(a));")
		testKit.MustExec("set @@tidb_enable_outer_join_reorder=true")

		// Create virtual tiflash replica info.
		testkit.SetTiFlashReplica(t, dom, "test", "t")
		testkit.SetTiFlashReplica(t, dom, "test", "t1")
		testkit.SetTiFlashReplica(t, dom, "test", "t2")
		testkit.SetTiFlashReplica(t, dom, "test", "t3")
		testkit.SetTiFlashReplica(t, dom, "test", "t4")
		testkit.SetTiFlashReplica(t, dom, "test", "t5")
		testkit.SetTiFlashReplica(t, dom, "test", "t6")

		testKit.MustExec("set @@tidb_allow_mpp=1; set @@tidb_enforce_mpp=1;")
		runJoinReorderTestData(t, testKit, "TestJoinOrderHint4TiFlash")
	})
}

func TestJoinOrderHint4DynamicPartitionTable(t *testing.T) {
	testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/planner/core/forceDynamicPrune", `return(true)`)
	testkit.RunTestUnderCascades(t, func(t *testing.T, testKit *testkit.TestKit, cascades, caller string) {
		testKit.MustExec("use test")
		testKit.MustExec("drop table if exists t, t1, t2, t3;")
		testKit.MustExec(`create table t(a int, b int) partition by hash(a) partitions 3`)
		testKit.MustExec(`create table t1(a int, b int) partition by hash(a) partitions 4`)
		testKit.MustExec(`create table t2(a int, b int) partition by hash(a) partitions 5`)
		testKit.MustExec(`create table t3(a int, b int) partition by hash(b) partitions 3`)
		testKit.MustExec(`create table t4(a int, b int) partition by hash(a) partitions 4`)
		testKit.MustExec(`create table t5(a int, b int) partition by hash(a) partitions 5`)
		testKit.MustExec(`create table t6(a int, b int) partition by hash(b) partitions 3`)

		testKit.MustExec(`set @@tidb_partition_prune_mode="dynamic"`)
		testKit.MustExec("set @@tidb_enable_outer_join_reorder=true")
		runJoinReorderTestData(t, testKit, "TestJoinOrderHint4DynamicPartitionTable")
	})
}
