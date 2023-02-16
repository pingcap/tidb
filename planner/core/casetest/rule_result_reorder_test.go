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
	"testing"

	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/testkit"
	"github.com/pingcap/tidb/testkit/testdata"
	"github.com/stretchr/testify/require"
)

func runTestData(t *testing.T, tk *testkit.TestKit, name string) {
	var input []string
	var output []struct {
		Plan []string
	}
	statsSuiteData := GetOrderedResultModeSuiteData()
	statsSuiteData.LoadTestCasesByName(name, t, &input, &output)
	require.Equal(t, len(input), len(output))
	for i := range input {
		testdata.OnRecord(func() {
			output[i].Plan = testdata.ConvertRowsToStrings(tk.MustQuery("explain " + input[i]).Rows())
		})
		tk.MustQuery("explain " + input[i]).Check(testkit.Rows(output[i].Plan...))
	}
}

func TestOrderedResultMode(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set tidb_cost_model_version=2")
	tk.MustExec(`set tidb_opt_limit_push_down_threshold=0`)
	tk.MustExec("set tidb_enable_ordered_result_mode=1")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int primary key, b int, c int, d int, key(b))")
	runTestData(t, tk, "TestOrderedResultMode")
}

func TestOrderedResultModeOnDML(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set tidb_enable_ordered_result_mode=1")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int primary key, b int, c int, key(b))")
	runTestData(t, tk, "TestOrderedResultModeOnDML")
}

func TestOrderedResultModeOnSubQuery(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set tidb_cost_model_version=2")
	tk.MustExec("set tidb_enable_ordered_result_mode=1")
	tk.MustExec("drop table if exists t1")
	tk.MustExec("drop table if exists t2")
	tk.MustExec("create table t1 (a int primary key, b int, c int, d int, key(b))")
	tk.MustExec("create table t2 (a int primary key, b int, c int, d int, key(b))")
	runTestData(t, tk, "TestOrderedResultModeOnSubQuery")
}

func TestOrderedResultModeOnJoin(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set tidb_cost_model_version=2")
	tk.MustExec("set tidb_enable_ordered_result_mode=1")
	tk.MustExec("drop table if exists t1")
	tk.MustExec("drop table if exists t2")
	tk.MustExec("create table t1 (a int primary key, b int, c int, d int, key(b))")
	tk.MustExec("create table t2 (a int primary key, b int, c int, d int, key(b))")
	tk.MustExec("set @@tidb_enable_outer_join_reorder=true")
	runTestData(t, tk, "TestOrderedResultModeOnJoin")
}

func TestOrderedResultModeOnOtherOperators(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set tidb_cost_model_version=2")
	tk.MustExec("set tidb_enable_ordered_result_mode=1")
	tk.MustExec("drop table if exists t1")
	tk.MustExec("drop table if exists t2")
	tk.MustExec("create table t1 (a int primary key, b int, c int, d int, unique key(b))")
	tk.MustExec("create table t2 (a int primary key, b int, c int, d int, unique key(b))")
	runTestData(t, tk, "TestOrderedResultModeOnOtherOperators")
}

func TestOrderedResultModeOnPartitionTable(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec(fmt.Sprintf(`set tidb_partition_prune_mode='%v'`, variable.DefTiDBPartitionPruneMode))
	tk.MustExec("set tidb_enable_ordered_result_mode=1")
	tk.MustExec("drop table if exists thash")
	tk.MustExec("drop table if exists trange")
	tk.MustExec("create table thash (a int primary key, b int, c int, d int) partition by hash(a) partitions 4")
	tk.MustExec(`create table trange (a int primary key, b int, c int, d int) partition by range(a) (
					partition p0 values less than (100),
					partition p1 values less than (200),
					partition p2 values less than (300),
					partition p3 values less than (400))`)
	tk.MustExec(`analyze table thash`)
	tk.MustExec(`analyze table trange`)
	tk.MustQuery("select @@tidb_partition_prune_mode").Check(testkit.Rows("dynamic"))
	runTestData(t, tk, "TestOrderedResultModeOnPartitionTable")
}
