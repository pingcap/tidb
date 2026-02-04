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
	"bytes"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/planner/util/coretestsdk"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testdata"
	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
	"github.com/stretchr/testify/require"
)

func TestHashPartitionPruner(t *testing.T) {
	testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/planner/core/forceDynamicPrune", `return(true)`)
	testkit.RunTestUnderCascades(t, func(t *testing.T, tk *testkit.TestKit, cascades, caller string) {
		tk.MustExec("create database test_partition")
		tk.MustExec("use test_partition")
		tk.MustExec("drop table if exists t1, t2;")
		tk.Session().GetSessionVars().EnableClusteredIndex = vardef.ClusteredIndexDefModeIntOnly
		tk.MustExec("create table t2(id int, a int, b int, primary key(id, a)) partition by hash(id + a) partitions 10;")
		tk.MustExec("create table t1(id int primary key, a int, b int) partition by hash(id) partitions 10;")
		tk.MustExec("create table t3(id int, a int, b int, primary key(id, a)) partition by hash(id) partitions 10;")
		tk.MustExec("create table t4(d datetime, a int, b int, primary key(d, a)) partition by hash(year(d)) partitions 10;")
		tk.MustExec("create table t5(d date, a int, b int, primary key(d, a)) partition by hash(month(d)) partitions 10;")
		tk.MustExec("create table t6(a int, b int) partition by hash(a) partitions 3;")
		tk.MustExec("create table t7(a int, b int) partition by hash(a + b) partitions 10;")
		tk.MustExec("create table t8(a int, b int) partition by hash(a) partitions 6;")
		tk.MustExec("create table t9(a bit(1) default null, b int(11) default null) partition by hash(a) partitions 3;") //issue #22619
		tk.MustExec("create table t10(a bigint unsigned) partition BY hash (a);")
		tk.MustExec("create table t11(a int, b int) partition by hash(a + a + a + b) partitions 5")

		var input []string
		var output []struct {
			SQL    string
			Result []string
		}
		partitionPrunerData := getPartitionPrunerData()
		partitionPrunerData.LoadTestCases(t, &input, &output, cascades, caller)
		for i, tt := range input {
			testdata.OnRecord(func() {
				output[i].SQL = tt
				output[i].Result = testdata.ConvertRowsToStrings(tk.MustQuery(tt).Rows())
			})
			tk.MustQuery(tt).Check(testkit.Rows(output[i].Result...))
		}
	})
}

type testTablePartitionInfo struct {
	Table      string
	Partitions string
}

// getPartitionInfoFromPlan uses to extract table partition information from the plan tree string. Here is an example, the plan is like below:
//
//	"Projection_7 80.00 root  test_partition.t1.id, test_partition.t1.a, test_partition.t1.b, test_partition.t2.id, test_partition.t2.a, test_partition.t2.b",
//	"└─HashJoin_9 80.00 root  CARTESIAN inner join",
//	"  ├─TableReader_12(Build) 8.00 root partition:p1 data:Selection_11",
//	"  │ └─Selection_11 8.00 cop[tikv]  1, eq(test_partition.t2.b, 6), in(test_partition.t2.a, 6, 7, 8)",
//	"  │   └─TableFullScan_10 10000.00 cop[tikv] table:t2 keep order:false, stats:pseudo",
//	"  └─TableReader_15(Probe) 10.00 root partition:p0 data:Selection_14",
//	"    └─Selection_14 10.00 cop[tikv]  1, eq(test_partition.t1.a, 5)",
//	"      └─TableFullScan_13 10000.00 cop[tikv] table:t1 keep order:false, stats:pseudo"
//
// The return table partition info is: t1: p0; t2: p1
func getPartitionInfoFromPlan(plan []string) string {
	infos := make([]testTablePartitionInfo, 0, 2)
	info := testTablePartitionInfo{}
	for _, row := range plan {
		partitions := coretestsdk.GetFieldValue("partition:", row)
		if partitions != "" {
			info.Partitions = partitions
			continue
		}
		tbl := coretestsdk.GetFieldValue("table:", row)
		if tbl != "" {
			info.Table = tbl
			infos = append(infos, info)
		}
	}
	sort.Slice(infos, func(i, j int) bool {
		if infos[i].Table != infos[j].Table {
			return infos[i].Table < infos[j].Table
		}
		return infos[i].Partitions < infos[j].Partitions
	})
	buf := bytes.NewBuffer(nil)
	for i, info := range infos {
		if i > 0 {
			buf.WriteString("; ")
		}
		buf.WriteString(fmt.Sprintf("%v: %v", info.Table, info.Partitions))
	}
	return buf.String()
}

func checkPrunePartitionInfo(c *testing.T, query string, infos1 string, plan []string) {
	infos2 := getPartitionInfoFromPlan(plan)
	comment := fmt.Sprintf("the query is: %v, the plan is:\n%v", query, strings.Join(plan, "\n"))
	require.Equal(c, infos1, infos2, comment)
}

func TestListColumnsPartitionPruner(t *testing.T) {
	testkit.RunTestUnderCascades(t, func(t *testing.T, tk *testkit.TestKit, cascades, caller string) {
		failpoint.Enable("github.com/pingcap/tidb/pkg/planner/core/forceDynamicPrune", `return(true)`)
		defer failpoint.Disable("github.com/pingcap/tidb/pkg/planner/core/forceDynamicPrune")
		store := tk.Session().GetStore()

		tk.MustExec("drop database if exists test_partition;")
		tk.MustExec("create database test_partition")
		tk.MustExec("use test_partition")
		tk.MustExec("create table t1 (id int, a int, b int) partition by list columns (b,a) (partition p0 values in ((1,1),(2,2),(3,3),(4,4),(5,5)), partition p1 values in ((6,6),(7,7),(8,8),(9,9),(10,10),(null,10)));")
		tk.MustExec("create table t2 (id int, a int, b int) partition by list columns (id,a,b) (partition p0 values in ((1,1,1),(2,2,2),(3,3,3),(4,4,4),(5,5,5)), partition p1 values in ((6,6,6),(7,7,7),(8,8,8),(9,9,9),(10,10,10),(null,null,null)));")
		tk.MustExec("insert into t1 (id,a,b) values (1,1,1),(2,2,2),(3,3,3),(4,4,4),(5,5,5),(6,6,6),(7,7,7),(8,8,8),(9,9,9),(10,10,10),(null,10,null)")
		tk.MustExec("insert into t2 (id,a,b) values (1,1,1),(2,2,2),(3,3,3),(4,4,4),(5,5,5),(6,6,6),(7,7,7),(8,8,8),(9,9,9),(10,10,10),(null,null,null)")

		// tk1 use to test partition table with index.
		tk1 := testkit.NewTestKit(t, store)
		tk1.MustExec("drop database if exists test_partition_1;")
		tk1.MustExec(`set @@session.tidb_regard_null_as_point=false`)
		tk1.MustExec("create database test_partition_1")
		tk1.MustExec("use test_partition_1")
		tk1.MustExec("create table t1 (id int, a int, b int, unique key (a,b,id)) partition by list columns (b,a) (partition p0 values in ((1,1),(2,2),(3,3),(4,4),(5,5)), partition p1 values in ((6,6),(7,7),(8,8),(9,9),(10,10),(null,10)));")
		tk1.MustExec("create table t2 (id int, a int, b int, unique key (a,b,id)) partition by list columns (id,a,b) (partition p0 values in ((1,1,1),(2,2,2),(3,3,3),(4,4,4),(5,5,5)), partition p1 values in ((6,6,6),(7,7,7),(8,8,8),(9,9,9),(10,10,10),(null,null,null)));")
		tk1.MustExec("insert into t1 (id,a,b) values (1,1,1),(2,2,2),(3,3,3),(4,4,4),(5,5,5),(6,6,6),(7,7,7),(8,8,8),(9,9,9),(10,10,10),(null,10,null)")
		tk1.MustExec("insert into t2 (id,a,b) values (1,1,1),(2,2,2),(3,3,3),(4,4,4),(5,5,5),(6,6,6),(7,7,7),(8,8,8),(9,9,9),(10,10,10),(null,null,null)")

		// tk2 use to compare the result with normal table.
		tk2 := testkit.NewTestKit(t, store)
		tk2.MustExec("drop database if exists test_partition_2;")
		tk2.MustExec(`set @@session.tidb_regard_null_as_point=false`)
		tk2.MustExec("create database test_partition_2")
		tk2.MustExec("use test_partition_2")
		tk2.MustExec("create table t1 (id int, a int, b int)")
		tk2.MustExec("create table t2 (id int, a int, b int)")
		tk2.MustExec("insert into t1 (id,a,b) values (1,1,1),(2,2,2),(3,3,3),(4,4,4),(5,5,5),(6,6,6),(7,7,7),(8,8,8),(9,9,9),(10,10,10),(null,10,null)")
		tk2.MustExec("insert into t2 (id,a,b) values (1,1,1),(2,2,2),(3,3,3),(4,4,4),(5,5,5),(6,6,6),(7,7,7),(8,8,8),(9,9,9),(10,10,10),(null,null,null)")

		// Default RPC encoding may cause statistics explain result differ and then the test unstable.
		tk1.MustExec("set @@tidb_enable_chunk_rpc = on")
		cascadesVal := "off"
		if cascades == "on" {
			cascadesVal = "on"
		}
		tk1.MustExec(fmt.Sprintf("set @@tidb_enable_cascades_planner = %s", cascadesVal))
		tk2.MustExec(fmt.Sprintf("set @@tidb_enable_cascades_planner = %s", cascadesVal))

		var input []struct {
			SQL    string
			Pruner string
		}
		var output []struct {
			SQL       string
			Result    []string
			Plan      []string
			IndexPlan []string
		}
		partitionPrunerData := getPartitionPrunerData()
		partitionPrunerData.LoadTestCases(t, &input, &output, cascades, caller)
		valid := false
		for i, tt := range input {
			// Test for table without index.
			plan := tk.MustQuery("explain format = 'brief' " + tt.SQL)
			planTree := testdata.ConvertRowsToStrings(plan.Rows())
			// Test for table with index.
			indexPlan := tk1.MustQuery("explain format = 'brief' " + tt.SQL)
			indexPlanTree := testdata.ConvertRowsToStrings(indexPlan.Rows())
			testdata.OnRecord(func() {
				output[i].SQL = tt.SQL
				output[i].Result = testdata.ConvertRowsToStrings(tk.MustQuery(tt.SQL).Sort().Rows())
				// Test for table without index.
				output[i].Plan = planTree
				// Test for table with index.
				output[i].IndexPlan = indexPlanTree
			})
			// compare the plan.
			plan.Check(testkit.Rows(output[i].Plan...))
			indexPlan.Check(testkit.Rows(output[i].IndexPlan...))

			// compare the pruner information.
			checkPrunePartitionInfo(t, tt.SQL, tt.Pruner, planTree)
			checkPrunePartitionInfo(t, tt.SQL, tt.Pruner, indexPlanTree)

			// compare the result.
			result := tk.MustQuery(tt.SQL).Sort()
			idxResult := tk1.MustQuery(tt.SQL)
			result.Check(idxResult.Sort().Rows())
			result.Check(testkit.Rows(output[i].Result...))

			// If the query doesn't specified the partition, compare the result with normal table
			if !strings.Contains(tt.SQL, "partition(") {
				result.Check(tk2.MustQuery(tt.SQL).Sort().Rows())
				valid = true
			}
		}
		require.True(t, valid)
	})
}

func TestPointGetIntHandleNotFirst(t *testing.T) {
	testkit.RunTestUnderCascades(t, func(t *testing.T, tk *testkit.TestKit, cascades, caller string) {
		tk.MustExec("use test")
		tk.MustExec(`create table t (
		c int,
		a int not null,
		b int,
		primary key  (a) /*T![clustered_index] clustered */
	  )`)
		tk.MustExec(`insert into t values(1, 13, 1)`)
		tk.MustQuery("select * from t WHERE `a` BETWEEN 13 AND 13").Check(testkit.Rows("1 13 1"))
		tk.MustExec(`alter table t
	  partition by range (a)
	  (partition p0 values less than (10),
	   partition p1 values less than (maxvalue))`)

		tk.MustQuery("select * from t WHERE a BETWEEN 13 AND 13").Check(testkit.Rows("1 13 1"))
		tk.MustQuery(`select * from t`).Check(testkit.Rows("1 13 1"))
	})
}

type ExtractTestCase struct {
	TimeUnit    string
	ColumnTypes []string
	PruneResult []string // cmpOps + BETWEEN pRange[1] [, pRange[2]]
	NoFspResult string
}

// TODO: test LIST/HASH pruning?
func TestRangeDatePruningExtract(t *testing.T) {
	for _, colType := range []string{"DATE", "DATETIME", "DATETIME(1)", "DATETIME(6)"} {
		extractTestCases := []ExtractTestCase{
			{
				"YEAR",
				[]string{"DATE", "DATETIME"},
				[]string{"p2", "p0,p1,p2", "p2,p3,pMax", "p0,p1,p2", "p2,p3,pMax", "p2,p3"},
				"",
			}, {
				"QUARTER",
				[]string{"DATE", "DATETIME"},
				[]string{"p2", "all", "all", "all", "all", "all"},
				"",
			}, {
				"YEAR_MONTH",
				[]string{"DATE", "DATETIME"},
				[]string{"p2", "p0,p1,p2", "p2,p3,pMax", "p0,p1,p2", "p2,p3,pMax", "p2,p3"},
				"",
			}, {

				"MONTH",
				[]string{"DATE", "DATETIME"},
				[]string{"p2", "all", "all", "all", "all", "all"},
				"",
			}, {
				"WEEK",
				[]string{},
				[]string{},
				"",
			}, {
				"DAY",
				[]string{"DATE", "DATETIME"},
				[]string{"p2", "all", "all", "all", "all", "all"},
				"",
			}, {
				"DAY_HOUR",
				[]string{"DATETIME"},
				[]string{"p2", "all", "all", "all", "all", "all"},
				"",
			}, {
				"DAY_MINUTE",
				[]string{"DATETIME"},
				[]string{"p2", "all", "all", "all", "all", "all"},
				"",
			}, {
				"DAY_SECOND",
				[]string{"DATETIME"},
				[]string{"p2", "all", "all", "all", "all", "all"},
				"",
			}, {
				"DAY_MICROSECOND",
				[]string{"DATETIME"},
				[]string{"p2", "all", "all", "all", "all", "all"},
				// Would also be affected by FSP truncation, but since the partition definitions
				// in this test are increasing for each partition, this will not be noticed
				"",
			}, {
				"HOUR",
				[]string{"DATETIME"},
				[]string{"p2", "all", "all", "all", "all", "all"},
				"",
			}, {
				"HOUR_MINUTE",
				[]string{"DATETIME"},
				[]string{"p2", "all", "all", "all", "all", "all"},
				"",
			}, {
				"HOUR_SECOND",
				[]string{"DATETIME"},
				[]string{"p2", "all", "all", "all", "all", "all"},
				"",
			}, {
				"HOUR_MICROSECOND",
				[]string{"DATETIME"},
				// If no fsp is given, the partitioning expression still records
				// the fsp, but evaluation will truncate it, so that is why
				// the pruning will give p1, which is actually correct!
				[]string{"p2", "all", "all", "all", "all", "all"},
				"p1",
			}, {
				"MINUTE",
				[]string{"DATETIME"},
				[]string{"p2", "all", "all", "all", "all", "all"},
				"",
			}, {
				"MINUTE_SECOND",
				[]string{"DATETIME"},
				[]string{"p2", "all", "all", "all", "all", "all"},
				"",
			}, {
				"MINUTE_MICROSECOND",
				[]string{"DATETIME"},
				[]string{"p2", "all", "all", "all", "all", "all"},
				"p1",
			}, {
				"SECOND",
				[]string{"DATETIME"},
				[]string{"p2", "all", "all", "all", "all", "all"},
				"",
			}, {
				"SECOND_MICROSECOND",
				[]string{"DATETIME"},
				[]string{"p2", "all", "all", "all", "all", "all"},
				"p1",
			}, {
				"MICROSECOND",
				[]string{"DATETIME"},
				[]string{"p2", "all", "all", "all", "all", "all"},
				"p1",
			},
		}
		runExtractTestCases(t, colType, extractTestCases)
	}
}

func runExtractTestCases(t *testing.T, colType string, extractTestCases []ExtractTestCase) {
	testkit.RunTestUnderCascades(t, func(t *testing.T, tk *testkit.TestKit, cascades, caller string) {
		tk.MustExec("use test")

		// Loop over different datatypes, DATE, DATETIME(fsp), TIMESTAMP(fsp)
		pRanges := []string{
			"1990-01-01 00:00:00.000000",
			"1991-04-02 01:01:01.100000",
			"1992-08-03 02:02:02.200000",
			"1993-12-31 23:59:59.999999",
		}
		cmpOps := []string{"=", "<", ">", "<=", ">="}
		for _, tc := range extractTestCases {
			found := false
			hasFsp := strings.HasSuffix(colType, ")")
			for _, cType := range tc.ColumnTypes {
				end := strings.TrimPrefix(colType, cType)

				if end == "" || end[:1] == "(" {
					found = true
				}
			}
			pRangesStrings := make([]string, 0, len(pRanges))
			partDefs := ""
			for i, pString := range pRanges {
				r := tk.MustQuery(`SELECT EXTRACT(` + tc.TimeUnit + ` FROM '` + pString + `')`)
				pRangesStrings = append(pRangesStrings, r.Rows()[0][0].(string))
				if i > 0 {
					partDefs += ", "
				}
				partDefs += "PARTITION p" +
					strconv.Itoa(i) +
					" VALUES LESS THAN (" +
					pRangesStrings[i] + ")"
			}
			tk.MustExec(`drop table if exists t`)
			createSQL := `create table t (d ` + colType + `, f varchar(255)) partition by range (EXTRACT(` + tc.TimeUnit + ` FROM d)) (` + partDefs + `, partition pMax values less than (maxvalue))`
			if !found {
				tk.MustContainErrMsg(createSQL, `[ddl:1486]Constant, random or timezone-dependent expressions in (sub)partitioning function are not allowed`)
				continue
			}
			tk.MustExec(createSQL)
			for i, op := range cmpOps {
				res := tk.MustQuery(`explain select * from t where d ` + op + ` '` + pRanges[1] + `'`)
				parts := strings.TrimPrefix(res.Rows()[0][3].(string), "partition:")
				require.Greater(t, len(tc.PruneResult), i, "PruneResults does not include enough values, colType %s, EXTRACT %s, op %s", colType, tc.TimeUnit, op)
				expects := tc.PruneResult[i]
				if i == 0 && !hasFsp && tc.NoFspResult != "" {
					expects = tc.NoFspResult
				}
				require.Equal(t, expects, parts, "colType %s, EXTRACT %s, op %s", colType, tc.TimeUnit, op)
			}
			res := tk.MustQuery(`explain select * from t where d between '` + pRanges[1] + `' and '` + pRanges[2] + `'`)
			parts := strings.TrimPrefix(res.Rows()[0][3].(string), "partition:")
			require.Equal(t, tc.PruneResult[len(cmpOps)], parts, "colType %s, EXTRACT %s, BETWEEN", colType, tc.TimeUnit)
		}
	})
}

func TestRangeTimePruningExtract(t *testing.T) {
	for _, colType := range []string{"TIME", "TIME(1)", "TIME(6)", "TIMESTAMP", "TIMESTAMP(1)", "TIMESTAMP(6)"} {
		extractTestCases := []ExtractTestCase{
			{
				"YEAR",
				[]string{"DATE", "DATETIME"},
				[]string{},
				"",
			}, {
				"QUARTER",
				[]string{"DATE", "DATETIME"},
				[]string{},
				"",
			}, {
				"YEAR_MONTH",
				[]string{"DATE", "DATETIME"},
				[]string{},
				"",
			}, {
				"MONTH",
				[]string{"DATE", "DATETIME"},
				[]string{},
				"",
			}, {
				"WEEK",
				[]string{"DATE", "DATETIME"},
				[]string{},
				"",
			}, {
				"DAY",
				[]string{"DATE", "DATETIME"},
				[]string{},
				"",
			}, {
				"DAY_HOUR",
				[]string{"DATETIME"},
				[]string{},
				"",
			}, {
				"DAY_MINUTE",
				[]string{"DATETIME"},
				[]string{},
				"",
			}, {
				"DAY_SECOND",
				[]string{"DATETIME"},
				[]string{},
				"",
			}, {
				"DAY_MICROSECOND",
				[]string{"DATETIME"},
				[]string{},
				"",
			}, {
				"HOUR",
				[]string{"TIME"},
				[]string{"p2", "p0,p1,p2", "p2,p3,pMax", "p0,p1,p2", "p2,p3,pMax", "p2,p3"},
				"",
			}, {
				"HOUR_MINUTE",
				[]string{"TIME"},
				[]string{"p2", "p0,p1,p2", "p2,p3,pMax", "p0,p1,p2", "p2,p3,pMax", "p2,p3"},
				"",
			}, {
				"HOUR_SECOND",
				[]string{"TIME"},
				[]string{"p2", "p0,p1,p2", "p2,p3,pMax", "p0,p1,p2", "p2,p3,pMax", "p2,p3"},
				"",
			}, {
				"HOUR_MICROSECOND",
				[]string{"TIME"},
				[]string{"p2", "p0,p1,p2", "p2,p3,pMax", "p0,p1,p2", "p2,p3,pMax", "p2,p3"},
				"",
			}, {
				"MINUTE",
				[]string{"TIME"},
				[]string{"p2", "all", "all", "all", "all", "all"},
				"",
			}, {
				"MINUTE_SECOND",
				[]string{"TIME"},
				[]string{"p2", "all", "all", "all", "all", "all"},
				"",
			}, {
				"MINUTE_MICROSECOND",
				[]string{"TIME"},
				[]string{"p2", "all", "all", "all", "all", "all"},
				"",
			}, {
				"SECOND",
				[]string{"TIME"},
				[]string{"p2", "all", "all", "all", "all", "all"},
				"",
			}, {
				"SECOND_MICROSECOND",
				[]string{"TIME"},
				[]string{"p2", "all", "all", "all", "all", "all"},
				"",
			}, {
				"MICROSECOND",
				[]string{"TIME"},
				[]string{"p2", "all", "all", "all", "all", "all"},
				"",
			},
		}
		runExtractTestCases(t, colType, extractTestCases)
	}
}

func TestPartitionPrunerRegression(t *testing.T) {
	testkit.RunTestUnderCascades(t, func(t *testing.T, tk *testkit.TestKit, cascades, caller string) {
		tk.MustExec("use test")
		tk.MustExec("drop table if exists t")
		tk.MustExec("CREATE TABLE `t` (" +
			"`a` varchar(150) NOT NULL," +
			"`b` varchar(100) NOT NULL," +
			"`c` int NOT NULL DEFAULT '0'" +
			",PRIMARY KEY (`a`,`b`) /*T![clustered_index] CLUSTERED */" +
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci" +
			" PARTITION BY LIST COLUMNS(`b`)" +
			"(PARTITION `p0` VALUES IN ('0')," +
			"PARTITION `p1` VALUES IN ('1')," +
			"PARTITION `p2` VALUES IN ('2'))")

		tk.MustExec("insert into t values ('a','1',1),('b','1',1),('b', '2', 2)")
		tk.MustExec("set @@session.tidb_partition_prune_mode = 'static'")
		tk.MustQuery("select /* issue:59827 */ * from t where a = 'b' and b IN('1','2')").Sort().Check(testkit.Rows("b 1 1", "b 2 2"))
		tk.MustQuery("select /* issue:59827 */ * from t where a = 'b' and (b IN ('1','2'))").Sort().Check(testkit.Rows("b 1 1", "b 2 2"))
		tk.MustQuery("select /* issue:59827 */ * from t where a = 'b' and (b = '2' or b = '1')").Sort().Check(testkit.Rows("b 1 1", "b 2 2"))
		tk.MustExec("set @@session.tidb_partition_prune_mode = 'dynamic'")
		tk.MustQuery("select /* issue:59827 */ * from t where a = 'b' and b IN('1','2')").Sort().Check(testkit.Rows("b 1 1", "b 2 2"))
		tk.MustQuery("select /* issue:59827 */ * from t where a = 'b' and (b IN ('1','2'))").Sort().Check(testkit.Rows("b 1 1", "b 2 2"))
		tk.MustQuery("select /* issue:59827 */ * from t where a = 'b' and (b = '2' or b = '1')").Sort().Check(testkit.Rows("b 1 1", "b 2 2"))

		tk.MustQuery("select /* issue:59827 */ * from t where a = 'b' and b = '2'").Check(testkit.Rows("b 2 2"))
		tk.MustQuery("select /* issue:59827 */ * from t where a = 'b' and b = '1'").Check(testkit.Rows("b 1 1"))
		tk.MustQuery("select /* issue:59827 */ * from t where a = 'b' and (b = '2')").Check(testkit.Rows("b 2 2"))
		tk.MustQuery("select /* issue:59827 */ * from t where a = 'b' and (b = '1')").Check(testkit.Rows("b 1 1"))
		tk.MustQuery("select /* issue:59827 */ * from t where a = 'b' and b = ('2')").Check(testkit.Rows("b 2 2"))
		tk.MustQuery("select /* issue:59827 */ * from t where a = 'b' and b = ('1')").Check(testkit.Rows("b 1 1"))
		tk.MustQuery("explain select /* issue:59827 */ * from t where a = 'b' and b = '2'").CheckContain("partition:p2")
		tk.MustQuery("explain select /* issue:59827 */ * from t where a = 'b' and (b = '2')").CheckContain("partition:p2")

		tk.MustExec("PREPARE stmt FROM 'select * from t where a = ? and b = ?'")
		tk.MustExec("SET @a = 'b', @b = '2'")
		tk.MustQuery("EXECUTE stmt USING @a, @b").Check(testkit.Rows("b 2 2"))
		tk.MustExec("SET @a = 'a', @b = '1'")
		tk.MustQuery("EXECUTE stmt USING @a, @b").Check(testkit.Rows("a 1 1"))
		tk.MustExec("DEALLOCATE PREPARE stmt")
		tk.MustExec(`PREPARE stmt FROM "select * from t where a = 'b' and b = ?"`)
		tk.MustExec("SET @b = '2'")
		tk.MustQuery("EXECUTE stmt USING @b").Check(testkit.Rows("b 2 2"))
		tk.MustExec("SET @b = '1'")
		tk.MustQuery("EXECUTE stmt USING @b").Check(testkit.Rows("b 1 1"))
		tk.MustExec("DEALLOCATE PREPARE stmt")

		tk.MustExec("PREPARE stmt FROM 'select * from t where a = ? and (b = ?)'")
		tk.MustExec("SET @a = 'b', @b = '2'")
		tk.MustQuery("EXECUTE stmt USING @a, @b").Check(testkit.Rows("b 2 2"))
		tk.MustExec("SET @a = 'a', @b = '1'")
		tk.MustQuery("EXECUTE stmt USING @a, @b").Check(testkit.Rows("a 1 1"))
		tk.MustExec("DEALLOCATE PREPARE stmt")
		tk.MustExec(`PREPARE stmt FROM "select * from t where a = 'b' and b = (?)"`)
		tk.MustExec("SET @b = '2'")
		tk.MustQuery("EXECUTE stmt USING @b").Check(testkit.Rows("b 2 2"))
		tk.MustExec("SET @b = '1'")
		tk.MustQuery("EXECUTE stmt USING @b").Check(testkit.Rows("b 1 1"))
		tk.MustExec("DEALLOCATE PREPARE stmt")

		tk.MustExec("drop table if exists t")
		tk.MustExec("CREATE TABLE `t` (" +
			"`a` varchar(150) NOT NULL," +
			"`b` varchar(100) NOT NULL," +
			"`c` int NOT NULL DEFAULT '0'" +
			",PRIMARY KEY (`b`) /*T![clustered_index] CLUSTERED */" +
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci" +
			" PARTITION BY KEY(`b`) PARTITIONS 13")

		tk.MustExec("insert into t values ('a','3',3),('b','1',1),('b', '2', 2),('xX','xX',10),('Yy','Yy',11)")
		tk.MustExec("set @@session.tidb_partition_prune_mode = 'static'")
		tk.MustQuery("select /* issue:59827 */ * from t where (b IN ('Xx','yY'))").Sort().Check(testkit.Rows("Yy Yy 11", "xX xX 10"))
		tk.MustQuery("select /* issue:59827 */ * from t where b IN('1','2')").Sort().Check(testkit.Rows("b 1 1", "b 2 2"))
		tk.MustQuery("select /* issue:59827 */ * from t where (b IN ('1','2'))").Sort().Check(testkit.Rows("b 1 1", "b 2 2"))
		tk.MustQuery("select /* issue:59827 */ * from t where b = '2' or b = '1'").Sort().Check(testkit.Rows("b 1 1", "b 2 2"))
		tk.MustQuery("select /* issue:59827 */ * from t where (b = '2' or b = '1')").Sort().Check(testkit.Rows("b 1 1", "b 2 2"))
		tk.MustExec("set @@session.tidb_partition_prune_mode = 'dynamic'")
		tk.MustQuery("select /* issue:59827 */ * from t where (b IN ('Xx','yY'))").Sort().Check(testkit.Rows("Yy Yy 11", "xX xX 10"))
		tk.MustQuery("select /* issue:59827 */ * from t where b IN('1','2')").Sort().Check(testkit.Rows("b 1 1", "b 2 2"))
		tk.MustQuery("select /* issue:59827 */ * from t where (b IN ('1','2'))").Sort().Check(testkit.Rows("b 1 1", "b 2 2"))
		tk.MustQuery("select /* issue:59827 */ * from t where b = '2' or b = '1'").Sort().Check(testkit.Rows("b 1 1", "b 2 2"))
		tk.MustQuery("select /* issue:59827 */ * from t where (b = '2' or b = '1')").Sort().Check(testkit.Rows("b 1 1", "b 2 2"))

		tk.MustExec("drop table if exists t")
		tk.MustExec("CREATE TABLE `t` (" +
			"`a` varchar(150) COLLATE utf8mb4_general_ci NOT NULL," +
			"`b` varchar(100) COLLATE utf8mb4_general_ci NOT NULL," +
			"`c` int NOT NULL DEFAULT '0'," +
			"PRIMARY KEY (`a`,`b`) /*T![clustered_index] CLUSTERED */" +
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci" +
			" PARTITION BY RANGE COLUMNS(`b`)" +
			"(PARTITION `p0` VALUES LESS THAN ('1')," +
			"PARTITION `p1` VALUES LESS THAN ('2')," +
			"PARTITION `p2` VALUES LESS THAN ('3'))")

		tk.MustExec("insert into t values ('a','1',1),('b','1',1),('b', '2', 2)")
		tk.MustQuery("select /* issue:59827 */ * from t where a = 'b' and b = '2'").Check(testkit.Rows("b 2 2"))
		tk.MustQuery("select /* issue:59827 */ * from t where a = 'b' and (b = '2')").Check(testkit.Rows("b 2 2"))
		tk.MustQuery("explain select /* issue:59827 */ * from t where a = 'a' and b = '2'").CheckContain("partition:p2")
		tk.MustQuery("explain select /* issue:59827 */ * from t where a = 'a' and (b = '2')").CheckContain("partition:p2")

		tk.MustExec("drop table if exists t")
		tk.MustExec("create table t (" +
			"a varchar(291)," +
			"b int," +
			"primary key(a)) partition by list columns (a)" +
			"(partition p0 values in ('', '1'))")

		tk.MustExec("insert into t values ('', 1)")
		tk.MustQuery("explain select /* issue:61134 */ * from t where a in ('')").CheckContain("Point_Get")
		tk.MustQuery("select /* issue:61134 */ * from t where a in ('')").Check(testkit.Rows(" 1"))

		testCaseChar := []struct {
			partitionBy string
			partD       string
			partY       string
			partNull    string
		}{
			{
				" PARTITION BY RANGE COLUMNS (a) (PARTITION pNULL VALUES LESS THAN (''), PARTITION p0 VALUES LESS THAN ('M'), PARTITION p1 VALUES LESS THAN (MAXVALUE))",
				"p0",
				"p1",
				"pNULL",
			}, {
				" PARTITION BY LIST COLUMNS (a) (PARTITION p0 VALUES IN ('D'), PARTITION p1 VALUES IN ('Y'), PARTITION pNULL VALUES IN (NULL))",
				"p0",
				"p1",
				"pNULL",
			}, {
				" PARTITION BY KEY (a) PARTITIONS 2",
				"p0",
				"p1",
				"p1",
			},
		}
		for _, tc := range testCaseChar {
			tk.MustExec("drop table if exists t")
			tk.MustExec(`CREATE TABLE t (a varchar(9), unique index (a))` + tc.partitionBy)
			tk.MustExec(`insert into t values ('Y'),('D'),(NULL)`)
			tk.MustQuery(`select /* issue:61176 */ a from t where a <=> 'D'`).Check(testkit.Rows("D"))
			tk.MustQuery(`select /* issue:61176 */ a from t where a <=> 'Y'`).Check(testkit.Rows("Y"))
			tk.MustQuery(`select /* issue:61176 */ a from t where a <=> NULL`).Check(testkit.Rows("<nil>"))
			tk.MustQuery(`explain format=brief select /* issue:61176 */ a from t where a <=> 'D'`).MultiCheckContain([]string{"Point_Get", "partition:" + tc.partD})
			tk.MustQuery(`explain format=brief select /* issue:61176 */ a from t where a <=> 'Y'`).MultiCheckContain([]string{"Point_Get", "partition:" + tc.partY})
			tk.MustQuery(`explain format=brief select /* issue:61176 */ a from t where a <=> NULL`).MultiCheckContain([]string{"IndexRangeScan", "partition:" + tc.partNull})
			tk.MustExec(`drop table t`)
			tk.MustExec(`CREATE TABLE t (a varchar(9) PRIMARY KEY)` + tc.partitionBy)
			tk.MustExec(`insert into t values ('Y'),('D')`)
			tk.MustContainErrMsg(`insert into t values (NULL)`, "[table:1048]Column 'a' cannot be null")
			tk.MustQuery(`select /* issue:61176 */ a from t where a <=> 'D'`).Check(testkit.Rows("D"))
			tk.MustQuery(`select /* issue:61176 */ a from t where a <=> 'Y'`).Check(testkit.Rows("Y"))
			tk.MustQuery(`select /* issue:61176 */ a from t where a <=> NULL`).Check(testkit.Rows())
			tk.MustQuery(`explain format=brief select /* issue:61176 */ a from t where a <=> 'D'`).MultiCheckContain([]string{"Point_Get", "partition:" + tc.partD})
			tk.MustQuery(`explain format=brief select /* issue:61176 */ a from t where a <=> 'Y'`).MultiCheckContain([]string{"Point_Get", "partition:" + tc.partY})
			tk.MustQuery(`explain format=brief select /* issue:61176 */ a from t where a <=> NULL`).MultiCheckContain([]string{"TableRangeScan", "partition:" + tc.partNull})
			tk.MustExec(`drop table t`)
		}

		testCaseInt := []struct {
			partitionBy string
			part1       string
			part5       string
			partNull    string
		}{
			{
				" PARTITION BY RANGE (a) (PARTITION pNULL VALUES LESS THAN (0), PARTITION p0 VALUES LESS THAN (5), PARTITION p1 VALUES LESS THAN (MAXVALUE))",
				"p0",
				"p1",
				"pNULL",
			}, {
				" PARTITION BY LIST (a) (PARTITION p0 VALUES IN (1), PARTITION p1 VALUES IN (5), PARTITION pNULL VALUES IN (NULL))",
				"p0",
				"p1",
				"pNULL",
			}, {
				" PARTITION BY KEY (a) PARTITIONS 3",
				"p2",
				"p1",
				"p1",
			}, {
				" PARTITION BY HASH (a) PARTITIONS 3",
				"p1",
				"p2",
				"p0",
			},
		}
		for _, tc := range testCaseInt {
			tk.MustExec("drop table if exists t")
			tk.MustExec(`CREATE TABLE t (a int, unique index (a))` + tc.partitionBy)
			tk.MustExec(`insert into t values (1),(5),(NULL)`)
			tk.MustQuery(`select /* issue:61176 */ a from t where a <=> 1`).Check(testkit.Rows("1"))
			tk.MustQuery(`select /* issue:61176 */ a from t where a <=> 5`).Check(testkit.Rows("5"))
			tk.MustQuery(`select /* issue:61176 */ a from t where a <=> NULL`).Check(testkit.Rows("<nil>"))
			tk.MustQuery(`explain format=brief select /* issue:61176 */ a from t where a <=> 1`).MultiCheckContain([]string{"Point_Get", "partition:" + tc.part1})
			tk.MustQuery(`explain format=brief select /* issue:61176 */ a from t where a <=> 5`).MultiCheckContain([]string{"Point_Get", "partition:" + tc.part5})
			tk.MustQuery(`explain format=brief select /* issue:61176 */ a from t where a <=> NULL`).MultiCheckContain([]string{"IndexRangeScan", "partition:" + tc.partNull})
			tk.MustExec(`drop table t`)
			tk.MustExec(`CREATE TABLE t (a int PRIMARY KEY)` + tc.partitionBy)
			tk.MustExec(`insert into t values (1),(5)`)
			tk.MustContainErrMsg(`insert into t values (NULL)`, "[table:1048]Column 'a' cannot be null")
			tk.MustQuery(`select /* issue:61176 */ a from t where a <=> 1`).Check(testkit.Rows("1"))
			tk.MustQuery(`select /* issue:61176 */ a from t where a <=> 5`).Check(testkit.Rows("5"))
			tk.MustQuery(`select /* issue:61176 */ a from t where a <=> NULL`).Check(testkit.Rows())
			tk.MustQuery(`explain format=brief select /* issue:61176 */ a from t where a <=> 1`).MultiCheckContain([]string{"Point_Get", "partition:" + tc.part1})
			tk.MustQuery(`explain format=brief select /* issue:61176 */ a from t where a <=> 5`).MultiCheckContain([]string{"Point_Get", "partition:" + tc.part5})
			// TODO: Also do this for RANGE COLUMNS? Why is this different?!?
			tk.MustQuery(`explain format=brief select /* issue:61176 */ a from t where a <=> NULL`).CheckContain("TableDual")
			tk.MustExec(`drop table t`)
		}
	})
}

func TestCast(t *testing.T) {
	testkit.RunTestUnderCascades(t, func(t *testing.T, tk *testkit.TestKit, cascades, caller string) {
		tk.MustExec("use test")
		tk.MustExec(`CREATE TABLE t4365b15e (
  col_18 tinyblob DEFAULT NULL,
  col_19 smallint unsigned NOT NULL DEFAULT '14683',
  col_20 char(39) COLLATE utf8_bin DEFAULT '',
  col_21 time NOT NULL DEFAULT '01:08:07',
  col_22 char(153) COLLATE utf8_unicode_ci NOT NULL DEFAULT '-仂',
  UNIQUE KEY idx_11 (col_22,col_21) /*T![global_index] GLOBAL */,
  UNIQUE KEY idx_12 (col_22) /*T![global_index] GLOBAL */
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci
PARTITION BY RANGE COLUMNS(col_22)
(PARTITION p0 VALUES LESS THAN ('腅tD蠊KV吵d啗s'),
 PARTITION p1 VALUES LESS THAN (MAXVALUE));`)
		tk.MustExec(`INSERT INTO t4365b15e VALUES(x'4a5955476d',32987,'uQKDBD99zce','09:35:47','y瘇=r賜莶)2g壅T敤q屉'),(x'4a5955476d',25115,'uQKDBD99zce','09:35:47','@j-yDA'),(x'406b6271',22117,'c^E@*ybYRTqNkAgTFC5','19:31:21','綑鞗'),(x'2d5e',4433,'d8G_Ic3Y*!rom**','09:38:32','1g0餪Pc诡Y&4y槄(Ou稠9嘪'),(x'69595e6467682b',48233,'nP)$V','14:32:31','埼dR鿱'),(x'6f5f2b5e7a3346235a64787954784975566f',57918,'o^8g*wCsmo@FBXmOS','09:35:45','鰻e=r+'),(x'4a5955476d',65016,'uQKDBD99zce','09:35:47','w层'),(x'6169262d4b243d792a4775',58947,'uQKDBD99zce','09:35:47','wqT8擟t獱Ip'),(x'6d6e6a40415e48395e423634',20706,'MMz1zdUDgztb@','22:11:26','rUN'),(x'4a517a6b3066716f426823',41270,'v67T4$LO7uy0','19:28:09','!鞤鏈鋎2p擛)nX4QI'),(x'36697124377042514a2521516c232862',23768,'c~~J','00:39:02','m'),(x'4a5955476d',61509,'uQKDBD99zce','09:35:47','鐩UQBb籹JQ57N韧S仂Fw'),(x'4d6f78586d6e4a68426e5e2d71',41755,'wICNk7+@HQ&j)&ojFL','20:10:51','0GIUbF賕#B'),(x'475a2444684c4a41',24780,'cPIPfD9Pd','20:30:43','YO5(e秬v鼾Bk扣竷6I韤$Zp'),(x'4a5955476d',51992,'uQKDBD99zce','09:35:47','N!SwEUsn堶I鐱'),(x'4a5955476d',65535,'uQKDBD99zce','09:35:47','Z5NL畏閻蚵-'),(x'7751514d',25824,'LO9TOd^CI_MtE7LR^z','22:42:19','@睾掘~坒cq(o6X鈞'),(x'38423771215333455e2d3842566b7641',49763,'yCyNZ$M8Q','12:52:23','嵌&)oW佞u蒧48嗡8帽TdRf*'),(x'4a5955476d',65217,'uQKDBD99zce','09:35:47','%qnr'),(x'4a5955476d',37678,'uQKDBD99zce','09:35:47','陂c隊n杭S鶔瓱p5');`)
		tk.MustExec(`CREATE TABLE tf460485d (
  col_57 tinytext COLLATE utf8_unicode_ci DEFAULT NULL,
  col_58 varchar(477) COLLATE utf8_general_ci NOT NULL DEFAULT '覽',
  col_59 tinyint unsigned NOT NULL,
  col_60 tinyint(1) NOT NULL DEFAULT '0',
  col_61 tinyint(1) NOT NULL,
  col_62 date DEFAULT '2031-12-15',
  col_63 smallint unsigned DEFAULT '28366',
  col_64 varbinary(389) NOT NULL,
  UNIQUE KEY idx_17 (col_58),
  PRIMARY KEY (col_58) /*T![clustered_index] NONCLUSTERED */ /*T![global_index] GLOBAL */,
  UNIQUE KEY idx_19 (col_58,col_59,col_61) /*T![global_index] GLOBAL */,
  UNIQUE KEY idx_20 (col_58,col_63,col_61)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_general_ci
PARTITION BY RANGE COLUMNS(col_58)
(PARTITION p0 VALUES LESS THAN ('$0Wc櫙'),
 PARTITION p1 VALUES LESS THAN ('襑蒢$堀$hKiV'),
 PARTITION p2 VALUES LESS THAN (MAXVALUE));`)
		tk.MustExec(`INSERT INTO tf460485d VALUES('1','R獾',127,1,0,'1999-06-26',NULL,x'6f416e515e4e'),('-9Wd+h9FNWgtZ','~x',82,0,0,'2035-10-28',NULL,x''),('=I','UtEvX殃h瞉CpWOE',171,1,0,'1973-03-18',858,x'524228455f71'),('0ATpI@W+8uF','xq烱O',146,1,0,'1986-12-17',26667,x'76664d59257456233951366f506e'),('1','dT啛鋌PeJh喅AL',33,1,0,'2022-05-06',5881,x'296748734d6f7634525f655f26556a7161'),('1','Y掿麶絊磵夬$楯%悒也贒R',255,1,0,'2014-11-06',10258,x'6f6a2a624a414e445a686a32237a7a23536664'),('0ATpI@W+8uF','D7Xa',203,1,0,'1981-05-12',26055,x'695a45617445344c5a233872436d'),('1','-Jw瞲80z灧UsY觱艵X礋',156,1,0,'2007-09-27',31907,x'6b74'),('9Zv(^6PEL5%r','cm痵e缓R崻*駌Z*TuM',175,0,1,'2031-10-08',32717,x'6f462d634f4b795e58454a423246646b356666'),('0ATpI@W+8uF','(3攓i痈XDQ許WL薹F錓',119,0,1,'1981-02-19',56610,x'6c2163514c5026'),('cCfZwARDfmRnhT4V8D+','$4#Y助怔s',230,0,0,'2020-06-23',12937,x'5455554d744a5a75412a43365f41387a5456'),('s(O04','fQjXE#L櫘UOa膸',157,0,0,'2010-12-19',28999,x'26494a73216342313d344d514d'),('LR5^o$*2','跪熝葌0O冾湂~(D5犣Y@*',81,0,0,'2013-05-30',15558,x'7a78625e4c4f6b752d472854'),('6*Y$H62VB)','襾X_梼乵',25,0,1,'2024-03-29',40076,x'67576c505a21457a576b264f64523d5e4149'),('VH5P+UikL(','e坲V',31,1,1,'2005-05-07',1799,x'785e'),('0ATpI@W+8uF','%yr',61,0,1,'1976-07-20',65535,x'2d6566576a'),('!hJ7J!C#','d讦Il8鬏挻#R蜃顧',241,1,0,'1975-08-22',46070,x'37665f6f4f6d797a3243'),('DEhuX+rH(Tqnww','P溃挋%T',224,1,0,'2006-04-13',60626,x'5e2a712a4c234479436b796a455274525459'),('~6','1H罥詟蝮%n6',127,0,0,'2024-06-12',24793,x'3d3126474a5236284a'),('0ATpI@W+8uF','yyR寓卖=淙Q!',148,1,0,'1991-07-05',32767,x''),('0ATpI@W+8uF','澪',105,0,0,'1981-02-15',NULL,x'5f406767'),('0ATpI@W+8uF','9騞G眡gMJP+v+鋜襑鲯I',171,1,1,'1993-06-26',47054,x'74466e576550'),('tZ@aRBI8&','%酬dV遇5',81,0,0,'1975-09-16',48645,x'2366546f7573756b267832');`)
		tk.MustQuery(`SELECT SUM(t4365b15e.col_19) AS r0, tf460485d.col_60 FROM tf460485d JOIN t4365b15e ON tf460485d.col_60=t4365b15e.col_22 GROUP BY tf460485d.col_60 HAVING tf460485d.col_60 IN (1);`).Check(testkit.Rows("53196 1"))
	})
}
