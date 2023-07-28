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

package partition

import (
	"strings"
	"testing"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/testkit"
	"github.com/pingcap/tidb/testkit/testdata"
	"github.com/stretchr/testify/require"
)

func TestListPartitionPushDown(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("create database list_push_down")
	tk.MustExec("use list_push_down")
	tk.MustExec("set tidb_cost_model_version=2")
	tk.MustExec("drop table if exists tlist")
	tk.MustExec(`set tidb_enable_list_partition = 1`)
	tk.MustExec(`create table tlist (a int) partition by list (a) (
    partition p0 values in (0, 1, 2),
    partition p1 values in (3, 4, 5))`)
	tk.MustExec(`create table tcollist (a int) partition by list columns(a) (
    partition p0 values in (0, 1, 2),
    partition p1 values in (3, 4, 5))`)
	tk.MustExec("set @@tidb_partition_prune_mode = 'static'")

	var input []string
	var output []struct {
		SQL  string
		Plan []string
	}
	integrationPartitionSuiteData := getIntegrationPartitionSuiteData()
	integrationPartitionSuiteData.LoadTestCases(t, &input, &output)
	for i, tt := range input {
		testdata.OnRecord(func() {
			output[i].SQL = tt
			output[i].Plan = testdata.ConvertRowsToStrings(tk.MustQuery(tt).Rows())
		})
		tk.MustQuery(tt).Check(testkit.Rows(output[i].Plan...))
	}
}

func TestListColVariousTypes(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("create database list_col_partition_types")
	tk.MustExec("use list_col_partition_types")
	tk.MustExec("drop table if exists tlist")
	tk.MustExec(`set tidb_enable_list_partition = 1`)

	tk.MustExec(`create table tint (a int) partition by list columns(a) (partition p0 values in (0, 1), partition p1 values in (2, 3))`)
	tk.MustExec(`create table tdate (a date) partition by list columns(a) (partition p0 values in ('2000-01-01', '2000-01-02'), partition p1 values in ('2000-01-03', '2000-01-04'))`)
	tk.MustExec(`create table tstring (a varchar(32)) partition by list columns(a) (partition p0 values in ('a', 'b'), partition p1 values in ('c', 'd'))`)

	err := tk.ExecToErr(`create table tdouble (a double) partition by list columns(a) (partition p0 values in (0, 1), partition p1 values in (2, 3))`)
	require.Error(t, err)
	require.Contains(t, err.Error(), "not allowed")

	err = tk.ExecToErr(`create table tdecimal (a decimal(30, 10)) partition by list columns(a) (partition p0 values in (0, 1), partition p1 values in (2, 3))`)
	require.Error(t, err)
	require.Contains(t, err.Error(), "not allowed")

	tk.MustExec(`insert into tint values (0), (1), (2), (3)`)
	tk.MustExec(`insert into tdate values ('2000-01-01'), ('2000-01-02'), ('2000-01-03'), ('2000-01-04')`)
	tk.MustExec(`insert into tstring values ('a'), ('b'), ('c'), ('d')`)
	tk.MustExec(`analyze table tint`)
	tk.MustExec(`analyze table tdate`)
	tk.MustExec(`analyze table tstring`)

	var input []string
	var output []struct {
		SQL     string
		Results []string
	}
	integrationPartitionSuiteData := getIntegrationPartitionSuiteData()
	integrationPartitionSuiteData.LoadTestCases(t, &input, &output)
	for i, tt := range input {
		testdata.OnRecord(func() {
			output[i].SQL = tt
			output[i].Results = testdata.ConvertRowsToStrings(tk.MustQuery(tt).Rows())
		})
		tk.MustQuery(tt).Check(testkit.Rows(output[i].Results...))
	}
}

func TestListPartitionPruning(t *testing.T) {
	failpoint.Enable("github.com/pingcap/tidb/planner/core/forceDynamicPrune", `return(true)`)
	defer failpoint.Disable("github.com/pingcap/tidb/planner/core/forceDynamicPrune")

	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("create database list_partition_pruning")
	tk.MustExec("use list_partition_pruning")
	tk.MustExec("drop table if exists tlist")
	tk.MustExec(`set tidb_enable_list_partition = 1`)
	tk.MustExec(`create table tlist (a int) partition by list (a) (
    partition p0 values in (0, 1, 2),
    partition p1 values in (3, 4, 5),
    partition p2 values in (6, 7, 8),
    partition p3 values in (9, 10, 11))`)
	tk.MustExec(`create table tcollist (a int) partition by list columns(a) (
    partition p0 values in (0, 1, 2),
    partition p1 values in (3, 4, 5),
    partition p2 values in (6, 7, 8),
    partition p3 values in (9, 10, 11))`)
	tk.MustExec(`analyze table tlist`)
	tk.MustExec(`analyze table tcollist`)

	var input []string
	var output []struct {
		SQL         string
		DynamicPlan []string
		StaticPlan  []string
	}
	integrationPartitionSuiteData := getIntegrationPartitionSuiteData()
	integrationPartitionSuiteData.LoadTestCases(t, &input, &output)
	for i, tt := range input {
		testdata.OnRecord(func() {
			output[i].SQL = tt
			tk.MustExec("set @@tidb_partition_prune_mode = 'dynamic'")
			output[i].DynamicPlan = testdata.ConvertRowsToStrings(tk.MustQuery(tt).Rows())
			tk.MustExec("set @@tidb_partition_prune_mode = 'static'")
			output[i].StaticPlan = testdata.ConvertRowsToStrings(tk.MustQuery(tt).Rows())
		})
		tk.MustExec("set @@tidb_partition_prune_mode = 'dynamic'")
		tk.MustQuery(tt).Check(testkit.Rows(output[i].DynamicPlan...))
		tk.MustExec("set @@tidb_partition_prune_mode = 'static'")
		tk.MustQuery(tt).Check(testkit.Rows(output[i].StaticPlan...))
	}
}

func TestListPartitionFunctions(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("create database list_partition_pruning")
	tk.MustExec("use list_partition_pruning")
	tk.MustExec("set tidb_enable_list_partition = 1")
	tk.MustExec("set @@tidb_partition_prune_mode = 'static'")

	var input []string
	var output []struct {
		SQL     string
		Results []string
	}
	integrationPartitionSuiteData := getIntegrationPartitionSuiteData()
	integrationPartitionSuiteData.LoadTestCases(t, &input, &output)
	for i, tt := range input {
		testdata.OnRecord(func() {
			output[i].SQL = tt
			output[i].Results = nil
			if strings.Contains(tt, "select") {
				output[i].Results = testdata.ConvertRowsToStrings(tk.MustQuery(tt).Sort().Rows())
			}
		})

		if strings.Contains(tt, "select") {
			tk.MustQuery(tt).Sort().Check(testkit.Rows(output[i].Results...))
		} else {
			tk.MustExec(tt)
		}
	}
}

func TestEstimationForTopNPushToDynamicPartition(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set tidb_cost_model_version=2")
	tk.MustExec("drop table if exists tlist")
	tk.MustExec(`set tidb_enable_list_partition = 1`)
	tk.MustExec(`create table trange (a int, b int, c int, index ia(a), primary key (b) clustered)
    partition by range(b) (
    partition p1 values less than(100),
    partition p2 values less than(200),
    partition p3 values less than maxvalue);`)
	tk.MustExec(`create table tlist (a int, b int, c int, index ia(a), primary key (b) clustered)
    partition by list (b) (
    partition p0 values in (0, 1, 2),
    partition p1 values in (3, 4, 5));`)
	tk.MustExec(`create table thash (a int, b int, c int, index ia(a), primary key (b) clustered)
    partition by hash(b) partitions 4;`)
	tk.MustExec(`create table t (a int, b int, c int, index ia(a), primary key (b) clustered);`)
	tk.MustExec(`analyze table trange;`)
	tk.MustExec(`analyze table tlist;`)
	tk.MustExec(`analyze table thash;`)
	tk.MustExec(`analyze table t;`)

	var input []string
	var output []struct {
		SQL  string
		Plan []string
	}
	integrationPartitionSuiteData := getIntegrationPartitionSuiteData()
	integrationPartitionSuiteData.LoadTestCases(t, &input, &output)
	for i, tt := range input {
		testdata.OnRecord(func() {
			output[i].SQL = tt
			output[i].Plan = testdata.ConvertRowsToStrings(tk.MustQuery(tt).Rows())
		})
		tk.MustQuery(tt).Check(testkit.Rows(output[i].Plan...))
	}
}

func TestPartitionTableExplain(t *testing.T) {
	failpoint.Enable("github.com/pingcap/tidb/planner/core/forceDynamicPrune", `return(true)`)
	defer failpoint.Disable("github.com/pingcap/tidb/planner/core/forceDynamicPrune")

	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec(`create table t (a int primary key, b int, key (b)) partition by hash(a) (partition P0, partition p1, partition P2)`)
	tk.MustExec(`create table t2 (a int, b int)`)
	tk.MustExec(`insert into t values (1,1),(2,2),(3,3)`)
	tk.MustExec(`insert into t2 values (1,1),(2,2),(3,3)`)
	tk.MustExec(`analyze table t, t2`)

	var input []string
	var output []struct {
		SQL         string
		DynamicPlan []string
		StaticPlan  []string
	}
	integrationPartitionSuiteData := getIntegrationPartitionSuiteData()
	integrationPartitionSuiteData.LoadTestCases(t, &input, &output)
	for i, tt := range input {
		testdata.OnRecord(func() {
			output[i].SQL = tt
			tk.MustExec("set @@tidb_partition_prune_mode = 'dynamic'")
			output[i].DynamicPlan = testdata.ConvertRowsToStrings(tk.MustQuery(tt).Rows())
			tk.MustExec("set @@tidb_partition_prune_mode = 'static'")
			output[i].StaticPlan = testdata.ConvertRowsToStrings(tk.MustQuery(tt).Rows())
		})

		tk.MustExec("set @@tidb_partition_prune_mode = 'dynamic'")
		tk.MustQuery(tt).Check(testkit.Rows(output[i].DynamicPlan...))
		tk.MustExec("set @@tidb_partition_prune_mode = 'static'")
		tk.MustQuery(tt).Check(testkit.Rows(output[i].StaticPlan...))
	}
}

func TestBatchPointGetTablePartition(t *testing.T) {
	failpoint.Enable("github.com/pingcap/tidb/planner/core/forceDynamicPrune", `return(true)`)
	defer failpoint.Disable("github.com/pingcap/tidb/planner/core/forceDynamicPrune")

	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1,t2,t3,t4,t5,t6")

	tk.MustExec("create table t1(a int, b int, primary key(a,b) nonclustered) partition by hash(b) partitions 2")
	tk.MustExec("insert into t1 values(1,1),(1,2),(2,1),(2,2)")

	tk.MustExec("create table t2(a int, b int, primary key(a,b) nonclustered) partition by range(b) (partition p0 values less than (2), partition p1 values less than maxvalue)")
	tk.MustExec("insert into t2 values(1,1),(1,2),(2,1),(2,2)")

	tk.MustExec("create table t3(a int, b int, primary key(a,b)) partition by hash(b) partitions 2")
	tk.MustExec("insert into t3 values(1,1),(1,2),(2,1),(2,2)")

	tk.MustExec("create table t4(a int, b int, primary key(a,b)) partition by range(b) (partition p0 values less than (2), partition p1 values less than maxvalue)")
	tk.MustExec("insert into t4 values(1,1),(1,2),(2,1),(2,2)")

	tk.MustExec("create table t5(a int, b int, primary key(a)) partition by hash(a) partitions 2")
	tk.MustExec("insert into t5 values(1,0),(2,0),(3,0),(4,0)")

	tk.MustExec("create table t6(a int, b int, primary key(a)) partition by range(a) (partition p0 values less than (3), partition p1 values less than maxvalue)")
	tk.MustExec("insert into t6 values(1,0),(2,0),(3,0),(4,0)")

	tk.MustExec(`analyze table t1, t2, t3, t4, t5, t6`)

	var input []string
	var output []struct {
		SQL         string
		DynamicPlan []string
		StaticPlan  []string
		Result      []string
	}

	integrationPartitionSuiteData := getIntegrationPartitionSuiteData()
	integrationPartitionSuiteData.LoadTestCases(t, &input, &output)
	for i, tt := range input {
		testdata.OnRecord(func() {
			output[i].SQL = tt
			tk.MustExec("set @@tidb_partition_prune_mode = 'dynamic'")
			output[i].DynamicPlan = testdata.ConvertRowsToStrings(tk.MustQuery("explain format = 'brief' " + tt).Rows())
			dynamicRes := testdata.ConvertRowsToStrings(tk.MustQuery(tt).Sort().Rows())
			tk.MustExec("set @@tidb_partition_prune_mode = 'static'")
			output[i].StaticPlan = testdata.ConvertRowsToStrings(tk.MustQuery("explain format = 'brief' " + tt).Rows())
			staticRes := testdata.ConvertRowsToStrings(tk.MustQuery(tt).Sort().Rows())

			require.Equal(t, dynamicRes, staticRes)
			output[i].Result = staticRes
		})

		tk.MustExec("set @@tidb_partition_prune_mode = 'dynamic'")
		tk.MustQuery("explain format = 'brief' " + tt).Check(testkit.Rows(output[i].DynamicPlan...))
		tk.MustQuery(tt).Sort().Check(testkit.Rows(output[i].Result...))
		tk.MustExec("set @@tidb_partition_prune_mode = 'static'")
		tk.MustQuery("explain format = 'brief' " + tt).Check(testkit.Rows(output[i].StaticPlan...))
		tk.MustQuery(tt).Sort().Check(testkit.Rows(output[i].Result...))
	}
}
