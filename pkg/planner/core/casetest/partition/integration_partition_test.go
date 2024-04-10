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
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testdata"
	"github.com/stretchr/testify/require"
)

func TestListPartitionPruning(t *testing.T) {
	failpoint.Enable("github.com/pingcap/tidb/pkg/planner/core/forceDynamicPrune", `return(true)`)
	defer failpoint.Disable("github.com/pingcap/tidb/pkg/planner/core/forceDynamicPrune")

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

func TestPartitionTableExplain(t *testing.T) {
	failpoint.Enable("github.com/pingcap/tidb/pkg/planner/core/forceDynamicPrune", `return(true)`)
	defer failpoint.Disable("github.com/pingcap/tidb/pkg/planner/core/forceDynamicPrune")

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
	failpoint.Enable("github.com/pingcap/tidb/pkg/planner/core/forceDynamicPrune", `return(true)`)
	defer failpoint.Disable("github.com/pingcap/tidb/pkg/planner/core/forceDynamicPrune")

	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustExec("create table thash1(a int, b int, primary key(a,b) nonclustered) partition by hash(b) partitions 2")
	tk.MustExec("insert into thash1 values(1,1),(1,2),(2,1),(2,2)")

	tk.MustExec("create table trange1(a int, b int, primary key(a,b) nonclustered) partition by range(b) (partition p0 values less than (2), partition p1 values less than maxvalue)")
	tk.MustExec("insert into trange1 values(1,1),(1,2),(2,1),(2,2)")

	tk.MustExec("create table tlist1(a int, b int, primary key(a,b) nonclustered) partition by list(b) (partition p0 values in (0, 1), partition p1 values in (2, 3))")
	tk.MustExec("insert into tlist1 values(1,1),(1,2),(2,1),(2,2)")

	tk.MustExec("create table thash2(a int, b int, primary key(a,b)) partition by hash(b) partitions 2")
	tk.MustExec("insert into thash2 values(1,1),(1,2),(2,1),(2,2)")

	tk.MustExec("create table trange2(a int, b int, primary key(a,b)) partition by range(b) (partition p0 values less than (2), partition p1 values less than maxvalue)")
	tk.MustExec("insert into trange2 values(1,1),(1,2),(2,1),(2,2)")

	tk.MustExec("create table tlist2(a int, b int, primary key(a,b)) partition by list(b) (partition p0 values in (0, 1), partition p1 values in (2, 3))")
	tk.MustExec("insert into tlist2 values(1,1),(1,2),(2,1),(2,2)")

	tk.MustExec("create table thash3(a int, b int, primary key(a)) partition by hash(a) partitions 2")
	tk.MustExec("insert into thash3 values(1,0),(2,0),(3,0),(4,0)")

	tk.MustExec("create table trange3(a int, b int, primary key(a)) partition by range(a) (partition p0 values less than (3), partition p1 values less than maxvalue)")
	tk.MustExec("insert into trange3 values(1,0),(2,0),(3,0),(4,0)")

	tk.MustExec("create table tlist3(a int, b int, primary key(a)) partition by list(a) (partition p0 values in (0, 1, 2), partition p1 values in (3, 4, 5))")
	tk.MustExec("insert into tlist3 values(1,0),(2,0),(3,0),(4,0)")

	tk.MustExec("create table issue45889(a int) partition by list(a) (partition p0 values in (0, 1), partition p1 values in (2, 3))")
	tk.MustExec("insert into issue45889 values (0),(0),(1),(1),(2),(2),(3),(3)")

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
			dynamicQuery := tk.MustQuery(tt)
			if !strings.Contains(tt, "order by") {
				dynamicQuery = dynamicQuery.Sort()
			}
			dynamicRes := testdata.ConvertRowsToStrings(dynamicQuery.Rows())
			tk.MustExec("set @@tidb_partition_prune_mode = 'static'")
			output[i].StaticPlan = testdata.ConvertRowsToStrings(tk.MustQuery("explain format = 'brief' " + tt).Rows())
			staticQuery := tk.MustQuery(tt)
			if !strings.Contains(tt, "order by") {
				staticQuery = staticQuery.Sort()
			}
			staticRes := testdata.ConvertRowsToStrings(staticQuery.Rows())
			require.Equal(t, dynamicRes, staticRes)
			output[i].Result = staticRes
		})

		tk.MustExec("set @@tidb_partition_prune_mode = 'dynamic'")
		tk.MustQuery("explain format = 'brief' " + tt).Check(testkit.Rows(output[i].DynamicPlan...))
		if strings.Contains(tt, "order by") {
			tk.MustQuery(tt).Check(testkit.Rows(output[i].Result...))
		} else {
			tk.MustQuery(tt).Sort().Check(testkit.Rows(output[i].Result...))
		}
		tk.MustExec("set @@tidb_partition_prune_mode = 'static'")
		tk.MustQuery("explain format = 'brief' " + tt).Check(testkit.Rows(output[i].StaticPlan...))
		if strings.Contains(tt, "order by") {
			tk.MustQuery(tt).Check(testkit.Rows(output[i].Result...))
		} else {
			tk.MustQuery(tt).Sort().Check(testkit.Rows(output[i].Result...))
		}
	}
}

func TestBatchPointGetPartitionForAccessObject(t *testing.T) {
	failpoint.Enable("github.com/pingcap/tidb/pkg/planner/core/forceDynamicPrune", `return(true)`)
	defer failpoint.Disable("github.com/pingcap/tidb/pkg/planner/core/forceDynamicPrune")

	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t1(a int, b int, UNIQUE KEY (b)) PARTITION BY HASH(b) PARTITIONS 4")
	tk.MustExec("insert into t1 values(1, 1), (2, 2), (3, 3), (4, 4)")

	tk.MustExec("CREATE TABLE t2 (id int primary key, name_id int) PARTITION BY LIST(id) (" +
		"partition p0 values IN (1, 2), " +
		"partition p1 values IN (3, 4), " +
		"partition p3 values IN (5))")
	tk.MustExec("insert into t2 values(1, 1), (2, 2), (3, 3), (4, 4)")

	tk.MustExec("CREATE TABLE t3 (id int primary key, name_id int) PARTITION BY LIST COLUMNS(id) (" +
		"partition p0 values IN (1, 2), " +
		"partition p1 values IN (3, 4), " +
		"partition p3 values IN (5))")
	tk.MustExec("insert into t3 values(1, 1), (2, 2), (3, 3), (4, 4)")

	tk.MustExec("CREATE TABLE t4 (id int, name_id int, unique key(id, name_id)) PARTITION BY LIST COLUMNS(id, name_id) (" +
		"partition p0 values IN ((1, 1),(2, 2)), " +
		"partition p1 values IN ((3, 3),(4, 4)), " +
		"partition p3 values IN ((5, 5)))")
	tk.MustExec("insert into t4 values(1, 1), (2, 2), (3, 3), (4, 4)")

	tk.MustExec("CREATE TABLE t5 (id int, name varchar(10), unique key(id, name)) PARTITION BY LIST COLUMNS(id, name) (" +
		"partition p0 values IN ((1,'a'),(2,'b')), " +
		"partition p1 values IN ((3,'c'),(4,'d')), " +
		"partition p3 values IN ((5,'e')))")
	tk.MustExec("insert into t5 values(1, 'a'), (2, 'b'), (3, 'c'), (4, 'd')")

	tk.MustExec("set @@tidb_partition_prune_mode = 'dynamic'")

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
