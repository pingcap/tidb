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
	"github.com/stretchr/testify/require"
)

func TestHashPartitionPruner(t *testing.T) {
	failpoint.Enable("github.com/pingcap/tidb/pkg/planner/core/forceDynamicPrune", `return(true)`)
	defer failpoint.Disable("github.com/pingcap/tidb/pkg/planner/core/forceDynamicPrune")
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
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
	partitionPrunerData.LoadTestCases(t, &input, &output)
	for i, tt := range input {
		testdata.OnRecord(func() {
			output[i].SQL = tt
			output[i].Result = testdata.ConvertRowsToStrings(tk.MustQuery(tt).Rows())
		})
		tk.MustQuery(tt).Check(testkit.Rows(output[i].Result...))
	}
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
	failpoint.Enable("github.com/pingcap/tidb/pkg/planner/core/forceDynamicPrune", `return(true)`)
	defer failpoint.Disable("github.com/pingcap/tidb/pkg/planner/core/forceDynamicPrune")
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("set tidb_cost_model_version=2")
	tk.MustExec("drop database if exists test_partition;")
	tk.MustExec("create database test_partition")
	tk.MustExec("use test_partition")
	tk.MustExec("create table t1 (id int, a int, b int) partition by list columns (b,a) (partition p0 values in ((1,1),(2,2),(3,3),(4,4),(5,5)), partition p1 values in ((6,6),(7,7),(8,8),(9,9),(10,10),(null,10)));")
	tk.MustExec("create table t2 (id int, a int, b int) partition by list columns (id,a,b) (partition p0 values in ((1,1,1),(2,2,2),(3,3,3),(4,4,4),(5,5,5)), partition p1 values in ((6,6,6),(7,7,7),(8,8,8),(9,9,9),(10,10,10),(null,null,null)));")
	tk.MustExec("insert into t1 (id,a,b) values (1,1,1),(2,2,2),(3,3,3),(4,4,4),(5,5,5),(6,6,6),(7,7,7),(8,8,8),(9,9,9),(10,10,10),(null,10,null)")
	tk.MustExec("insert into t2 (id,a,b) values (1,1,1),(2,2,2),(3,3,3),(4,4,4),(5,5,5),(6,6,6),(7,7,7),(8,8,8),(9,9,9),(10,10,10),(null,null,null)")

	// tk1 use to test partition table with index.
	tk1 := testkit.NewTestKit(t, store)
	tk1.MustExec("set tidb_cost_model_version=2")
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
	tk2.MustExec("set tidb_cost_model_version=2")
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
	partitionPrunerData.LoadTestCases(t, &input, &output)
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
}

func TestPointGetIntHandleNotFirst(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
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
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
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
