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

package scalarsubquery

import (
	"fmt"
	"slices"
	"testing"

	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testdata"
	"github.com/stretchr/testify/require"
)

func TestExplainNonEvaledSubquery(t *testing.T) {
	testkit.RunTestUnderCascades(t, func(t *testing.T, testKit *testkit.TestKit, cascades, caller string) {
		var (
			input []struct {
				SQL              string
				IsExplainAnalyze bool
				HasErr           bool
			}
			output []struct {
				SQL   string
				Plan  []string
				Error string
			}
		)
		planSuiteData := GetPlanSuiteData()
		planSuiteData.LoadTestCases(t, &input, &output, cascades, caller)

		testKit.MustExec("use test")
		testKit.MustExec("create table t1(a int, b int, c int)")
		testKit.MustExec("create table t2(a int, b int, c int)")
		testKit.MustExec("create table t3(a varchar(5), b varchar(5), c varchar(5))")
		testKit.MustExec("set @@tidb_opt_enable_non_eval_scalar_subquery=true")

		cutExecutionInfoFromExplainAnalyzeOutput := func(rows [][]any) [][]any {
			// The columns are id, estRows, actRows, task type, access object, execution info, operator info, memory, disk
			// We need to cut the unstable output of execution info, memory and disk.
			for i := range rows {
				rows[i] = rows[i][:6] // cut the final memory and disk.
				rows[i] = slices.Delete(rows[i], 5, 6)
			}
			return rows
		}

		for i, ts := range input {
			testdata.OnRecord(func() {
				output[i].SQL = ts.SQL
				if ts.HasErr {
					err := testKit.ExecToErr(ts.SQL)
					require.NotNil(t, err, fmt.Sprintf("Failed at case #%d", i))
					output[i].Error = err.Error()
					output[i].Plan = nil
				} else {
					rows := testKit.MustQuery(ts.SQL).Rows()
					if ts.IsExplainAnalyze {
						rows = cutExecutionInfoFromExplainAnalyzeOutput(rows)
					}
					output[i].Plan = testdata.ConvertRowsToStrings(rows)
					output[i].Error = ""
				}
			})
			if ts.HasErr {
				err := testKit.ExecToErr(ts.SQL)
				require.NotNil(t, err, fmt.Sprintf("Failed at case #%d", i))
			} else {
				rows := testKit.MustQuery(ts.SQL).Rows()
				if ts.IsExplainAnalyze {
					rows = cutExecutionInfoFromExplainAnalyzeOutput(rows)
				}
				require.Equal(t,
					testdata.ConvertRowsToStrings(testkit.Rows(output[i].Plan...)),
					testdata.ConvertRowsToStrings(rows),
					fmt.Sprintf("Failed at case #%d, SQL: %v", i, ts.SQL),
				)
			}
		}
	})
}

func TestSubqueryInExplainAnalyze(t *testing.T) {
	testkit.RunTestUnderCascades(t, func(t *testing.T, testKit *testkit.TestKit, cascades, caller string) {
		// Setup test tables with more comprehensive data
		testKit.MustExec("use test")
		testKit.MustExec("drop table if exists t1, t2, t3, t4")
		testKit.MustExec("create table t1(a int, b int, c int, primary key(a))")
		testKit.MustExec("create table t2(a int, b int, c int, primary key(a))")
		testKit.MustExec("create table t3(a int, b int, c int, primary key(a))")
		testKit.MustExec("create table t4(a int, b int, c int, primary key(a))")

		// Insert test data for better execution plan analysis
		testKit.MustExec("insert into t1 values (1,10,100), (2,20,200), (3,30,300), (4,40,400), (5,50,500)")
		testKit.MustExec("insert into t2 values (1,11,111), (2,22,222), (3,33,333), (4,44,444), (5,55,555)")
		testKit.MustExec("insert into t3 values (1,12,121), (2,23,232), (3,34,343), (4,45,454), (5,56,565)")
		testKit.MustExec("insert into t4 values (1,13,131), (2,24,242), (3,35,353), (4,46,464), (5,57,575)")

		// Test 1: Subquery in WHERE clause
		sql := "explain analyze select * from t1 where a = (select a from t2 limit 1)"
		rows := [][]any{
			{"Point_Get_25", "root", "handle:1"},
			{"ScalarSubQuery_21", "root", "Output: ScalarQueryCol#7"},
			{"└─MaxOneRow_9", "root", ""},
			{"  └─Limit_12", "root", "offset:0, count:1"},
			{"    └─TableReader_18", "root", "data:Limit_17"},
			{"      └─Limit_17", "cop[tikv]", "offset:0, count:1"},
			{"        └─TableFullScan_16", "cop[tikv]", "keep order:false, stats:pseudo"},
		}
		testKit.MustQuery(sql).CheckAt([]int{0, 3, 6}, rows)

		// Test 2: Subquery in projection columns
		sql = "explain analyze select a, (select max(b) from t2 where t2.a ) as max_b, (select count(*) from t3 where t3.a) as count_t3 from t1"
		rows = [][]any{
			{"Projection_74", "root", "test.t1.a, (ScalarQueryCol#14)->Column#15, (ScalarQueryCol#22)->Column#23"},
			{"└─TableReader_77", "root", "data:TableFullScan_76"},
			{"  └─TableFullScan_76", "cop[tikv]", "keep order:false, stats:pseudo"},
			{"ScalarSubQuery_47", "root", "Output: ScalarQueryCol#14"},
			{"└─MaxOneRow_14", "root", ""},
			{"  └─StreamAgg_22", "root", "funcs:max(test.t2.b)->Column#13"},
			{"    └─TopN_37", "root", "test.t2.b:desc, offset:0, count:1"},
			{"      └─TableReader_45", "root", "data:TopN_44"},
			{"        └─TopN_44", "cop[tikv]", "test.t2.b:desc, offset:0, count:1"},
			{"          └─Selection_31", "cop[tikv]", "not(isnull(test.t2.b))"},
			{"            └─TableRangeScan_30", "cop[tikv]", "range:[-inf,0), (0,+inf], keep order:false, stats:pseudo"},
			{"ScalarSubQuery_73", "root", "Output: ScalarQueryCol#22"},
			{"└─MaxOneRow_53", "root", ""},
			{"  └─HashAgg_63", "root", "funcs:count(Column#20)->Column#19"},
			{"    └─TableReader_64", "root", "data:HashAgg_56"},
			{"      └─HashAgg_56", "cop[tikv]", "funcs:count(1)->Column#20"},
			{"        └─TableRangeScan_62", "cop[tikv]", "range:[-inf,0), (0,+inf], keep order:false, stats:pseudo"},
		}
		testKit.MustQuery(sql).CheckAt([]int{0, 3, 6}, rows)

		// Test 3: Nested subqueries
		sql = "explain analyze select * from t1 where a = (select a from t2 where b = (select b from t3 where c = (select c from t4 where a = 1)))"
		rows = [][]any{
			{"TableDual_44", "root", "rows:0"},
			{"ScalarSubQuery_17", "root", "Output: ScalarQueryCol#13"},
			{"└─MaxOneRow_11", "root", ""},
			{"  └─Projection_12", "root", "test.t4.c"},
			{"    └─Point_Get_15", "root", "handle:1"},
			{"ScalarSubQuery_30", "root", "Output: ScalarQueryCol#14"},
			{"└─MaxOneRow_20", "root", ""},
			{"  └─TableReader_29", "root", "data:Projection_23"},
			{"    └─Projection_23", "cop[tikv]", "test.t3.b"},
			{"      └─Selection_28", "cop[tikv]", "eq(test.t3.c, 131<-(ScalarQueryCol#13))"},
			{"        └─TableFullScan_27", "cop[tikv]", "keep order:false, stats:pseudo"},
			{"ScalarSubQuery_39", "root", "Output: ScalarQueryCol#15"},
			{"└─MaxOneRow_34", "root", ""},
			{"  └─TableDual_37", "root", "rows:0"},
		}
		testKit.MustQuery(sql).CheckAt([]int{0, 3, 6}, rows)

		// Test 4: CTE with nested subqueries
		sql = "explain analyze with cte1 as (select a, b from t1 where c > (select avg(c) from t2)), cte2 as (select a, b from t2 where a in (select a from t3 where b > 20)) select cte1.*, cte2.* from cte1 join cte2 on cte1.a = cte2.a"
		rows = [][]any{
			{"MergeJoin_84", "root", "inner join, left key:test.t2.a, right key:test.t3.a"},
			{"├─TableReader_117(Build)", "root", "data:Selection_116"},
			{"│ └─Selection_116", "cop[tikv]", "gt(test.t3.b, 20)"},
			{"│   └─TableFullScan_115", "cop[tikv]", "keep order:true, stats:pseudo"},
			{"└─MergeJoin_100(Probe)", "root", "inner join, left key:test.t1.a, right key:test.t2.a"},
			{"  ├─TableReader_111(Build)", "root", "data:TableFullScan_110"},
			{"  │ └─TableFullScan_110", "cop[tikv]", "keep order:true, stats:pseudo"},
			{"  └─TableReader_109(Probe)", "root", "data:Selection_108"},
			{"    └─Selection_108", "cop[tikv]", "gt(test.t1.c, 333)"},
			{"      └─TableFullScan_107", "cop[tikv]", "keep order:true, stats:pseudo"},
			{"ScalarSubQuery_27", "root", "Output: ScalarQueryCol#12"},
			{"└─MaxOneRow_7", "root", ""},
			{"  └─HashAgg_17", "root", "funcs:avg(Column#8, Column#9)->Column#7"},
			{"    └─TableReader_18", "root", "data:HashAgg_10"},
			{"      └─HashAgg_10", "cop[tikv]", "funcs:count(test.t2.c)->Column#8, funcs:sum(test.t2.c)->Column#9"},
			{"        └─TableFullScan_16", "cop[tikv]", "keep order:false, stats:pseudo"},
			{"ScalarSubQuery_64", "root", "Output: ScalarQueryCol#32"},
			{"└─MaxOneRow_44", "root", ""},
			{"  └─HashAgg_54", "root", "funcs:avg(Column#28, Column#29)->Column#27"},
			{"    └─TableReader_55", "root", "data:HashAgg_47"},
			{"      └─HashAgg_47", "cop[tikv]", "funcs:count(test.t2.c)->Column#28, funcs:sum(test.t2.c)->Column#29"},
			{"        └─TableFullScan_53", "cop[tikv]", "keep order:false, stats:pseudo"},
		}
		testKit.MustQuery(sql).CheckAt([]int{0, 3, 6}, rows)

		// Test 5: UNION with subqueries
		sql = "explain analyze select * from t1 where a in (select a from t2 union all select a from t3 where b > (select avg(b) from t4))"
		rows = [][]any{
			{"HashJoin_55", "root", "inner join, equal:[eq(test.t1.a, Column#19)]"},
			{"├─HashAgg_69(Build)", "root", "group by:Column#19, funcs:firstrow(Column#19)->Column#19"},
			{"│ └─Union_73", "root", ""},
			{"│   ├─TableReader_78", "root", "data:TableFullScan_77"},
			{"│   │ └─TableFullScan_77", "cop[tikv]", "keep order:false, stats:pseudo"},
			{"│   └─TableReader_87", "root", "data:Projection_81"},
			{"│     └─Projection_81", "cop[tikv]", "test.t3.a->Column#19"},
			{"│       └─Selection_86", "cop[tikv]", "gt(test.t3.b, 35)"},
			{"│         └─TableFullScan_85", "cop[tikv]", "keep order:false, stats:pseudo"},
			{"└─TableReader_58(Probe)", "root", "data:TableFullScan_57"},
			{"  └─TableFullScan_57", "cop[tikv]", "keep order:false, stats:pseudo"},
			{"ScalarSubQuery_31", "root", "Output: ScalarQueryCol#18"},
			{"└─MaxOneRow_11", "root", ""},
			{"  └─HashAgg_21", "root", "funcs:avg(Column#14, Column#15)->Column#13"},
			{"    └─TableReader_22", "root", "data:HashAgg_14"},
			{"      └─HashAgg_14", "cop[tikv]", "funcs:count(test.t4.b)->Column#14, funcs:sum(test.t4.b)->Column#15"},
			{"        └─TableFullScan_20", "cop[tikv]", "keep order:false, stats:pseudo"},
		}
		testKit.MustQuery(sql).CheckAt([]int{0, 3, 6}, rows)
	})
}
