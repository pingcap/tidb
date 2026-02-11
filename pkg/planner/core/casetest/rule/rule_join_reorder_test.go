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

func TestJoinOrderHint4NestedLeading(t *testing.T) {
	testkit.RunTestUnderCascadesWithDomain(t, func(t *testing.T, testKit *testkit.TestKit, dom *domain.Domain, cascades, caller string) {
		testKit.MustExec("use test")
		testKit.MustExec("drop table if exists t, t1, t2, t3, t4, t5, t6;")
		testKit.MustExec("create table t(a int, b int, key(a));")
		testKit.MustExec("create table t1(a int, b int, key(a));")
		testKit.MustExec("create table t2(a int, b int, key(a));")
		testKit.MustExec("create table t3(a int, b int, key(a));")
		testKit.MustExec("create table t4(a int, b int, key(a));")
		testKit.MustExec("create table t5(a int, b int, key(a));")
		testKit.MustExec("create table t6(a int, b int, key(a));")
		runJoinReorderTestData(t, testKit, "TestJoinOrderHint4NestedLeading")
	})
}

func TestLeadingHintInapplicableKeepsOtherConds(t *testing.T) {
	testkit.RunTestUnderCascades(t, func(t *testing.T, testKit *testkit.TestKit, cascades, caller string) {
		testKit.MustExec("use test")
		testKit.MustExec("set @@tidb_enable_outer_join_reorder=true")
		testKit.MustExec("drop table if exists t0_lh, t1_lh, t2_lh, t3_lh;")
		testKit.MustExec("create table t0_lh(k0 int, k1 int, k2 int);")
		testKit.MustExec("create table t1_lh(k0 int, k1 int, k2 int);")
		testKit.MustExec("create table t2_lh(k0 int, k1 int, k2 int);")
		testKit.MustExec("create table t3_lh(k0 int, k1 int, k2 int);")

		testKit.MustQuery("explain format = 'brief' " +
			"select /*+ leading(t0_lh, t2_lh, t3_lh, t1_lh) */ t1_lh.k0 " +
			"from t0_lh right join t2_lh on (t0_lh.k1 = t2_lh.k1) " +
			"join t3_lh on (t0_lh.k2 = t3_lh.k2 and t2_lh.k1 < t3_lh.k2) " +
			"left join t1_lh on (t0_lh.k0 <=> t1_lh.k0);").
			CheckContain("lt(test.t2_lh.k1, test.t3_lh.k2)")
		testKit.MustQuery("show warnings").CheckContain("leading hint is inapplicable")
	})
}
