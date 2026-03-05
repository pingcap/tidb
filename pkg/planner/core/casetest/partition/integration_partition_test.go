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

	"github.com/pingcap/tidb/pkg/config/kerneltype"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testdata"
	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
	"github.com/stretchr/testify/require"
)

func testListPartitionPruning(t *testing.T, testKit *testkit.TestKit, cascades, caller string) {
	testKit.MustExec("create database list_partition_pruning")
	testKit.MustExec("use list_partition_pruning")
	testKit.MustExec("drop table if exists tlist")
	testKit.MustExec(`create table tlist (a int, b int) partition by list (a) (
    partition p0 values in (0, 1, 2),
    partition p1 values in (3, 4, 5),
    partition p2 values in (6, 7, 8),
    partition p3 values in (9, 10, 11),
    partition p4 values in (-1))`)
	testKit.MustExec(`create table tcollist (a int, b int) partition by list columns(a) (
    partition p0 values in (0, 1, 2),
    partition p1 values in (3, 4, 5),
    partition p2 values in (6, 7, 8),
    partition p3 values in (9, 10, 11),
    partition p4 values in (-1))`)
	testKit.MustExec(`analyze table tlist`)
	testKit.MustExec(`analyze table tcollist`)

	var input []string
	var output []struct {
		SQL         string
		DynamicPlan []string
		StaticPlan  []string
	}
	integrationPartitionSuiteData := getIntegrationPartitionSuiteData()
	integrationPartitionSuiteData.LoadTestCases(t, &input, &output, cascades, caller)
	for i, tt := range input {
		testdata.OnRecord(func() {
			output[i].SQL = tt
			testKit.MustExec("set @@tidb_partition_prune_mode = 'dynamic'")
			output[i].DynamicPlan = testdata.ConvertRowsToStrings(testKit.MustQuery(tt).Rows())
			testKit.MustExec("set @@tidb_partition_prune_mode = 'static'")
			output[i].StaticPlan = testdata.ConvertRowsToStrings(testKit.MustQuery(tt).Rows())
		})
		testKit.MustExec("set @@tidb_partition_prune_mode = 'dynamic'")
		testKit.MustQuery(tt).Check(testkit.Rows(output[i].DynamicPlan...))
		testKit.MustExec("set @@tidb_partition_prune_mode = 'static'")
		testKit.MustQuery(tt).Check(testkit.Rows(output[i].StaticPlan...))
	}
}

func TestListPartitionPruning(t *testing.T) {
	if kerneltype.IsNextGen() {
		t.Skip("Please run TestListPartitionPruningForNextGen under the next-gen mode")
	}
	testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/planner/core/forceDynamicPrune", `return(true)`)
	testkit.RunTestUnderCascades(t, testListPartitionPruning)
}

func TestListPartitionPruningForNextGen(t *testing.T) {
	if kerneltype.IsClassic() {
		t.Skip("Please run TestListPartitionPruning under the non next-gen mode")
	}
	testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/planner/core/forceDynamicPrune", `return(true)`)
	testkit.RunTestUnderCascades(t, testListPartitionPruning)
}

func TestPartitionTableExplain(t *testing.T) {
	testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/planner/core/forceDynamicPrune", `return(true)`)

	testkit.RunTestUnderCascades(t, func(t *testing.T, testKit *testkit.TestKit, cascades, caller string) {
		testKit.MustExec("use test")
		testKit.MustExec(`create table t (a int primary key, b int, key (b)) partition by hash(a) (partition P0, partition p1, partition P2)`)
		testKit.MustExec(`create table t2 (a int, b int)`)
		testKit.MustExec(`insert into t values (1,1),(2,2),(3,3)`)
		testKit.MustExec(`insert into t2 values (1,1),(2,2),(3,3)`)
		testKit.MustExec(`analyze table t, t2 all columns`)

		var input []string
		var output []struct {
			SQL         string
			DynamicPlan []string
			StaticPlan  []string
		}
		integrationPartitionSuiteData := getIntegrationPartitionSuiteData()
		integrationPartitionSuiteData.LoadTestCases(t, &input, &output, cascades, caller)
		for i, tt := range input {
			testdata.OnRecord(func() {
				output[i].SQL = tt
				testKit.MustExec("set @@tidb_partition_prune_mode = 'dynamic'")
				output[i].DynamicPlan = testdata.ConvertRowsToStrings(testKit.MustQuery(tt).Rows())
				testKit.MustExec("set @@tidb_partition_prune_mode = 'static'")
				output[i].StaticPlan = testdata.ConvertRowsToStrings(testKit.MustQuery(tt).Rows())
			})

			testKit.MustExec("set @@tidb_partition_prune_mode = 'dynamic'")
			testKit.MustQuery(tt).Check(testkit.Rows(output[i].DynamicPlan...))
			testKit.MustExec("set @@tidb_partition_prune_mode = 'static'")
			testKit.MustQuery(tt).Check(testkit.Rows(output[i].StaticPlan...))
		}
	})
}

func TestBatchPointGetTablePartition(t *testing.T) {
	testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/planner/core/forceDynamicPrune", `return(true)`)

	testkit.RunTestUnderCascades(t, func(t *testing.T, testKit *testkit.TestKit, cascades, caller string) {
		testKit.MustExec("use test")

		testKit.MustExec("create table thash1(a int, b int, primary key(a,b) nonclustered) partition by hash(b) partitions 2")
		testKit.MustExec("insert into thash1 values(1,1),(1,2),(2,1),(2,2)")

		testKit.MustExec("create table trange1(a int, b int, primary key(a,b) nonclustered) partition by range(b) (partition p0 values less than (2), partition p1 values less than maxvalue)")
		testKit.MustExec("insert into trange1 values(1,1),(1,2),(2,1),(2,2)")

		testKit.MustExec("create table tlist1(a int, b int, primary key(a,b) nonclustered) partition by list(b) (partition p0 values in (0, 1), partition p1 values in (2, 3))")
		testKit.MustExec("insert into tlist1 values(1,1),(1,2),(2,1),(2,2)")

		testKit.MustExec("create table thash2(a int, b int, primary key(a,b)) partition by hash(b) partitions 2")
		testKit.MustExec("insert into thash2 values(1,1),(1,2),(2,1),(2,2)")

		testKit.MustExec("create table trange2(a int, b int, primary key(a,b)) partition by range(b) (partition p0 values less than (2), partition p1 values less than maxvalue)")
		testKit.MustExec("insert into trange2 values(1,1),(1,2),(2,1),(2,2)")

		testKit.MustExec("create table tlist2(a int, b int, primary key(a,b)) partition by list(b) (partition p0 values in (0, 1), partition p1 values in (2, 3))")
		testKit.MustExec("insert into tlist2 values(1,1),(1,2),(2,1),(2,2)")

		testKit.MustExec("create table thash3(a int, b int, primary key(a)) partition by hash(a) partitions 2")
		testKit.MustExec("insert into thash3 values(1,0),(2,0),(3,0),(4,0)")

		testKit.MustExec("create table trange3(a int, b int, primary key(a)) partition by range(a) (partition p0 values less than (3), partition p1 values less than maxvalue)")
		testKit.MustExec("insert into trange3 values(1,0),(2,0),(3,0),(4,0)")

		testKit.MustExec("create table tlist3(a int, b int, primary key(a)) partition by list(a) (partition p0 values in (0, 1, 2), partition p1 values in (3, 4, 5))")
		testKit.MustExec("insert into tlist3 values(1,0),(2,0),(3,0),(4,0)")

		testKit.MustExec("create table issue45889(a int) partition by list(a) (partition p0 values in (0, 1), partition p1 values in (2, 3))")
		testKit.MustExec("insert into issue45889 values (0),(0),(1),(1),(2),(2),(3),(3)")

		var input []string
		var output []struct {
			SQL         string
			DynamicPlan []string
			StaticPlan  []string
			Result      []string
		}

		integrationPartitionSuiteData := getIntegrationPartitionSuiteData()
		integrationPartitionSuiteData.LoadTestCases(t, &input, &output, cascades, caller)
		for i, tt := range input {
			testdata.OnRecord(func() {
				output[i].SQL = tt
				testKit.MustExec("set @@tidb_partition_prune_mode = 'dynamic'")
				output[i].DynamicPlan = testdata.ConvertRowsToStrings(testKit.MustQuery("explain format = 'brief' " + tt).Rows())
				dynamicQuery := testKit.MustQuery(tt)
				if !strings.Contains(tt, "order by") {
					dynamicQuery = dynamicQuery.Sort()
				}
				dynamicRes := testdata.ConvertRowsToStrings(dynamicQuery.Rows())
				testKit.MustExec("set @@tidb_partition_prune_mode = 'static'")
				output[i].StaticPlan = testdata.ConvertRowsToStrings(testKit.MustQuery("explain format = 'brief' " + tt).Rows())
				staticQuery := testKit.MustQuery(tt)
				if !strings.Contains(tt, "order by") {
					staticQuery = staticQuery.Sort()
				}
				staticRes := testdata.ConvertRowsToStrings(staticQuery.Rows())
				require.Equal(t, dynamicRes, staticRes)
				output[i].Result = staticRes
			})

			testKit.MustExec("set @@tidb_partition_prune_mode = 'dynamic'")
			testKit.MustQuery("explain format = 'brief' " + tt).Check(testkit.Rows(output[i].DynamicPlan...))
			if strings.Contains(tt, "order by") {
				testKit.MustQuery(tt).Check(testkit.Rows(output[i].Result...))
			} else {
				testKit.MustQuery(tt).Sort().Check(testkit.Rows(output[i].Result...))
			}
			testKit.MustExec("set @@tidb_partition_prune_mode = 'static'")
			testKit.MustQuery("explain format = 'brief' " + tt).Check(testkit.Rows(output[i].StaticPlan...))
			if strings.Contains(tt, "order by") {
				testKit.MustQuery(tt).Check(testkit.Rows(output[i].Result...))
			} else {
				testKit.MustQuery(tt).Sort().Check(testkit.Rows(output[i].Result...))
			}
		}
	})
}

func TestBatchPointGetPartitionForAccessObject(t *testing.T) {
	testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/planner/core/forceDynamicPrune", `return(true)`)

	testkit.RunTestUnderCascades(t, func(t *testing.T, testKit *testkit.TestKit, cascades, caller string) {
		testKit.MustExec("use test")
		testKit.MustExec("create table t1(a int, b int, UNIQUE KEY (b)) PARTITION BY HASH(b) PARTITIONS 4")
		testKit.MustExec("insert into t1 values(1, 1), (2, 2), (3, 3), (4, 4)")

		testKit.MustExec("CREATE TABLE t2 (id int primary key, name_id int) PARTITION BY LIST(id) (" +
			"partition p0 values IN (1, 2), " +
			"partition p1 values IN (3, 4), " +
			"partition p3 values IN (5))")
		testKit.MustExec("insert into t2 values(1, 1), (2, 2), (3, 3), (4, 4)")

		testKit.MustExec("CREATE TABLE t3 (id int primary key, name_id int) PARTITION BY LIST COLUMNS(id) (" +
			"partition p0 values IN (1, 2), " +
			"partition p1 values IN (3, 4), " +
			"partition p3 values IN (5))")
		testKit.MustExec("insert into t3 values(1, 1), (2, 2), (3, 3), (4, 4)")

		testKit.MustExec("CREATE TABLE t4 (id int, name_id int, unique key(id, name_id)) PARTITION BY LIST COLUMNS(id, name_id) (" +
			"partition p0 values IN ((1, 1),(2, 2)), " +
			"partition p1 values IN ((3, 3),(4, 4)), " +
			"partition p3 values IN ((5, 5)))")
		testKit.MustExec("insert into t4 values(1, 1), (2, 2), (3, 3), (4, 4)")

		testKit.MustExec("CREATE TABLE t5 (id int, name varchar(10), unique key(id, name)) PARTITION BY LIST COLUMNS(id, name) (" +
			"partition p0 values IN ((1,'a'),(2,'b')), " +
			"partition p1 values IN ((3,'c'),(4,'d')), " +
			"partition p3 values IN ((5,'e')))")
		testKit.MustExec("insert into t5 values(1, 'a'), (2, 'b'), (3, 'c'), (4, 'd')")

		testKit.MustExec("set @@tidb_partition_prune_mode = 'dynamic'")

		var input []string
		var output []struct {
			SQL  string
			Plan []string
		}

		integrationPartitionSuiteData := getIntegrationPartitionSuiteData()
		integrationPartitionSuiteData.LoadTestCases(t, &input, &output, cascades, caller)
		for i, tt := range input {
			testdata.OnRecord(func() {
				output[i].SQL = tt
				output[i].Plan = testdata.ConvertRowsToStrings(testKit.MustQuery(tt).Rows())
			})
			testKit.MustQuery(tt).Check(testkit.Rows(output[i].Plan...))
		}
	})
}

// Issue 58475
func TestGeneratedColumnWithPartition(t *testing.T) {
	testkit.RunTestUnderCascades(t, func(t *testing.T, testKit *testkit.TestKit, cascades, caller string) {
		testKit.MustExec("use test")

		testKit.MustExec(`
		CREATE TABLE tp (
			id int,
			c1 int,
			c2 int GENERATED ALWAYS AS (c1) VIRTUAL,
			KEY idx (id)
		) PARTITION BY RANGE (id)
		(PARTITION p0 VALUES LESS THAN (0),
		PARTITION p1 VALUES LESS THAN (10000))
	`)
		testKit.MustExec(`INSERT INTO tp (id, c1) VALUES (0, 1)`)
		testKit.MustExec(`select /*+ FORCE_INDEX(tp, idx) */id from tp where c2 = 2 group by id having id in (0)`)
	})
}

func TestPartitionPruneWithPredicateSimplification(t *testing.T) {
	testkit.RunTestUnderCascades(t, func(t *testing.T, testKit *testkit.TestKit, cascades, caller string) {
		testKit.MustExec("use test")
		testKit.MustExec(` CREATE TABLE tla842d94a (
       col_1 varchar(188) CHARACTER SET gbk COLLATE gbk_bin NOT NULL,
       col_2 double NOT NULL,
       PRIMARY KEY (col_1,col_2) /*T![clustered_index] NONCLUSTERED */,
       UNIQUE KEY idx_2 (col_1,col_2),
       UNIQUE KEY idx_3 (col_1,col_2),
       KEY idx_4 (col_1,col_2) /*T![global_index] GLOBAL */
     ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci
     PARTITION BY RANGE COLUMNS(col_1)
     (PARTITION p0 VALUES LESS THAN ('E恘l57'),
      PARTITION p1 VALUES LESS THAN ('MboOU0'),
      PARTITION p2 VALUES LESS THAN ('Q&h髑UDZ娻躸(襲!籂35'),
      PARTITION p3 VALUES LESS THAN ('f獟@'),
      PARTITION p4 VALUES LESS THAN ('~W噽纓'));`)
		testKit.MustQuery(`explain format='brief' SELECT /*+ set_var(tidb_partition_prune_mode="static") */
    1,
    char(tla842d94a.col_2, tla842d94a.col_2 using utf8mb4) AS col_383,
    tla842d94a.col_2 AS col_384
FROM tla842d94a
WHERE tla842d94a.col_1 IN ('與P)凥i5', 'AI禡=Ymm滕籔湾$IUKiF3撔')
AND char(tla842d94a.col_2, tla842d94a.col_2 using utf8mb4) IN ('9eQ)6nzji', 'bF!pOc~')
AND NOT (tla842d94a.col_2 <> 3496.9237290113774)
ORDER BY char(tla842d94a.col_2, tla842d94a.col_2 using utf8mb4), tla842d94a.col_2;
`).Check(testkit.Rows(
			`Projection 0.00 root  Column#5, Column#6, test.tla842d94a.col_2`,
			`└─Sort 0.00 root  Column#7, test.tla842d94a.col_2`,
			`  └─Projection 0.00 root  Column#5, Column#6, test.tla842d94a.col_2, char_func(cast(test.tla842d94a.col_2, bigint(22) BINARY), cast(test.tla842d94a.col_2, bigint(22) BINARY), utf8mb4)->Column#7`,
			`    └─Projection 0.00 root  1->Column#5, char_func(cast(test.tla842d94a.col_2, bigint(22) BINARY), cast(test.tla842d94a.col_2, bigint(22) BINARY), utf8mb4)->Column#6, test.tla842d94a.col_2`,
			`      └─TableDual 0.00 root  rows:0`))
		testKit.MustQuery(`explain format='brief' SELECT
    1,
    char(tla842d94a.col_2, tla842d94a.col_2 using utf8mb4) AS col_383,
    tla842d94a.col_2 AS col_384
FROM tla842d94a
WHERE tla842d94a.col_1 IN ('與P)凥i5', 'AI禡=Ymm滕籔湾$IUKiF3撔')
AND char(tla842d94a.col_2, tla842d94a.col_2 using utf8mb4) IN ('9eQ)6nzji', 'bF!pOc~')
AND NOT (tla842d94a.col_2 <> 3496.9237290113774)
ORDER BY char(tla842d94a.col_2, tla842d94a.col_2 using utf8mb4), tla842d94a.col_2;
`).Check(testkit.Rows(
			`Projection 0.00 root  Column#5, Column#6, test.tla842d94a.col_2`,
			`└─Sort 0.00 root  Column#7, test.tla842d94a.col_2`,
			`  └─Projection 0.00 root  Column#5, Column#6, test.tla842d94a.col_2, char_func(cast(test.tla842d94a.col_2, bigint(22) BINARY), cast(test.tla842d94a.col_2, bigint(22) BINARY), utf8mb4)->Column#7`,
			`    └─Projection 0.00 root  1->Column#5, char_func(cast(test.tla842d94a.col_2, bigint(22) BINARY), cast(test.tla842d94a.col_2, bigint(22) BINARY), utf8mb4)->Column#6, test.tla842d94a.col_2`,
			`      └─TableDual 0.00 root  rows:0`))
	})
}
