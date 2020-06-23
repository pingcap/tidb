// Copyright 2020 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package executor_test

import (
	"fmt"
	"strings"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/util/testkit"
)

func checkApplyPlan(c *C, tk *testkit.TestKit, sql, plan string) {
	formatPlan := func(plan string) string {
		lines := strings.Split(plan, "\n")
		for i, j := 0, 0; j < len(lines); j++ {
			// remove all spaces and '|'
			line := strings.ReplaceAll(lines[j], " ", "")
			line = strings.ReplaceAll(line, "|", "")
			if line == "" {
				continue
			}
			lines[i] = line
			i++
		}
		return strings.Join(lines, "\n")
	}
	getPlan := func(sql string) string {
		rows := tk.MustQuery("explain " + sql).Rows()
		lines := make([]string, 0, 8)
		for _, r := range rows {
			line := fmt.Sprintf("%v", r)
			line = line[1 : len(line)-1] // remove '[' and ']'
			lines = append(lines, line)
		}
		return strings.Join(lines, "\n")
	}
	c.Assert(formatPlan(plan), Equals, formatPlan(getPlan(sql)))
}

func (s *testSuite) TestParallelApply(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int, b int)")
	tk.MustExec("insert into t values (0, 0), (1, 1), (2, 2), (3, 3), (4, 4), (5, 5), (6, 6), (7, 7), (8, 8), (9, 9), (null, null)")

	q1 := "select t1.b from t t1 where t1.b > (select max(b) from t t2 where t1.a > t2.a)"
	p1Seria := `| Projection_15                        | 9990.00  | root      |               | test.t.b                                                                           |
| └─Apply_17                           | 9990.00  | root      |               | cache: ON, CARTESIAN inner join, other cond:gt(test.t.b, Column#7) |
|   ├─TableReader_20(Build)            | 9990.00  | root      |               | data:Selection_19                                                                  |
|   │ └─Selection_19                   | 9990.00  | cop[tikv] |               | not(isnull(test.t.b))                                                              |
|   │   └─TableFullScan_18             | 10000.00 | cop[tikv] | table:t1      | keep order:false, stats:pseudo                                                     |
|   └─Selection_21(Probe)              | 0.80     | root      |               | not(isnull(Column#7))                                                              |
|     └─StreamAgg_26                   | 1.00     | root      |               | funcs:max(test.t.b)->Column#7                                                      |
|       └─TopN_27                      | 1.00     | root      |               | test.t.b:desc, offset:0, count:1                                                   |
|         └─TableReader_36             | 1.00     | root      |               | data:TopN_35                                                                       |
|           └─TopN_35                  | 1.00     | cop[tikv] |               | test.t.b:desc, offset:0, count:1                                                   |
|             └─Selection_34           | 7992.00  | cop[tikv] |               | gt(test.t.a, test.t.a), not(isnull(test.t.b))                                      |
|               └─TableFullScan_33     | 10000.00 | cop[tikv] | table:t2      | keep order:false, stats:pseudo                                                     |`
	checkApplyPlan(c, tk, q1, p1Seria)
	tk.MustQuery(q1).Sort().Check(testkit.Rows("1", "2", "3", "4", "5", "6", "7", "8", "9"))
	tk.MustExec("set tidb_enable_parallel_apply=true")
	p1Paral := `| Projection_15                        | 9990.00  | root      |               | test.t.b                                                                           |
| └─Apply_17                           | 9990.00  | root      |               | cache: ON, concurrency: 4, CARTESIAN inner join, other cond:gt(test.t.b, Column#7) |
|   ├─TableReader_20(Build)            | 9990.00  | root      |               | data:Selection_19                                                                  |
|   │ └─Selection_19                   | 9990.00  | cop[tikv] |               | not(isnull(test.t.b))                                                              |
|   │   └─TableFullScan_18             | 10000.00 | cop[tikv] | table:t1      | keep order:false, stats:pseudo                                                     |
|   └─Selection_21(Probe)              | 0.80     | root      |               | not(isnull(Column#7))                                                              |
|     └─StreamAgg_26                   | 1.00     | root      |               | funcs:max(test.t.b)->Column#7                                                      |
|       └─TopN_27                      | 1.00     | root      |               | test.t.b:desc, offset:0, count:1                                                   |
|         └─TableReader_36             | 1.00     | root      |               | data:TopN_35                                                                       |
|           └─TopN_35                  | 1.00     | cop[tikv] |               | test.t.b:desc, offset:0, count:1                                                   |
|             └─Selection_34           | 7992.00  | cop[tikv] |               | gt(test.t.a, test.t.a), not(isnull(test.t.b))                                      |
|               └─TableFullScan_33     | 10000.00 | cop[tikv] | table:t2      | keep order:false, stats:pseudo                                                     |`
	checkApplyPlan(c, tk, q1, p1Paral)
	tk.MustQuery(q1).Sort().Check(testkit.Rows("1", "2", "3", "4", "5", "6", "7", "8", "9"))

	// Apply_45 is in the inner side of Apply_28
	q2 := "select * from t t0 where t0.b <= (select max(t1.b) from t t1 where t1.b > (select max(b) from t t2 where t1.a > t2.a and t0.a > t2.a));"
	p2 := `| Projection_26                                | 9990.00  | root      |               | test.t.a, test.t.b                                                    |
| └─Apply_28                                   | 9990.00  | root      |               | cache: ON, concurrency: 4, CARTESIAN inner join, other cond:le(test.t.b, Column#11)   |
|   ├─TableReader_31(Build)                    | 9990.00  | root      |               | data:Selection_30                                                     |
|   │ └─Selection_30                           | 9990.00  | cop[tikv] |               | not(isnull(test.t.b))                                                 |
|   │   └─TableFullScan_29                     | 10000.00 | cop[tikv] | table:t0      | keep order:false, stats:pseudo                                        |
|   └─Selection_32(Probe)                      | 0.80     | root      |               | not(isnull(Column#11))                                                |
|     └─StreamAgg_37                           | 1.00     | root      |               | funcs:max(test.t.b)->Column#11                                        |
|       └─TopN_40                              | 1.00     | root      |               | test.t.b:desc, offset:0, count:1                                      |
|         └─Apply_45                           | 9990.00  | root      |               | cache: ON, CARTESIAN inner join, other cond:gt(test.t.b, Column#10)   |
|           ├─TableReader_48(Build)            | 9990.00  | root      |               | data:Selection_47                                                     |
|           │ └─Selection_47                   | 9990.00  | cop[tikv] |               | not(isnull(test.t.b))                                                 |
|           │   └─TableFullScan_46             | 10000.00 | cop[tikv] | table:t1      | keep order:false, stats:pseudo                                        |
|           └─Selection_49(Probe)              | 0.80     | root      |               | not(isnull(Column#10))                                                |
|             └─StreamAgg_54                   | 1.00     | root      |               | funcs:max(test.t.b)->Column#10                                        |
|               └─TopN_55                      | 1.00     | root      |               | test.t.b:desc, offset:0, count:1                                      |
|                 └─TableReader_64             | 1.00     | root      |               | data:TopN_63                                                          |
|                   └─TopN_63                  | 1.00     | cop[tikv] |               | test.t.b:desc, offset:0, count:1                                      |
|                     └─Selection_62           | 7992.00  | cop[tikv] |               | gt(test.t.a, test.t.a), gt(test.t.a, test.t.a), not(isnull(test.t.b)) |
|                       └─TableFullScan_61     | 10000.00 | cop[tikv] | table:t2      | keep order:false, stats:pseudo                                        |`
	checkApplyPlan(c, tk, q2, p2) // only the outside apply can be parallel
	tk.MustQuery(q2).Sort().Check(testkit.Rows("1 1", "2 2", "3 3", "4 4", "5 5", "6 6", "7 7", "8 8", "9 9"))
}
