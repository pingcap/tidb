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

func checkApplyPlan(c *C, tk *testkit.TestKit, sql string, enabled bool) {
	results := tk.MustQuery("explain analyze " + sql)
	first := true
	for _, row := range results.Rows() {
		line := fmt.Sprintf("%v", row)
		if strings.Contains(line, "Apply") {
			if enabled && first {
				c.Assert(strings.Contains(line, "Concurrency"), IsTrue)
				first = false
			} else {
				c.Assert(strings.Contains(line, "Concurrency"), IsFalse)
			}
		}
	}
	return
}

func (s *testSuite) TestParallelApply(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int, b int)")
	tk.MustExec("insert into t values (0, 0), (1, 1), (2, 2), (3, 3), (4, 4), (5, 5), (6, 6), (7, 7), (8, 8), (9, 9), (null, null)")

	q1 := "select t1.b from t t1 where t1.b > (select max(b) from t t2 where t1.a > t2.a)"
	checkApplyPlan(c, tk, q1, false)
	tk.MustQuery(q1).Sort().Check(testkit.Rows("1", "2", "3", "4", "5", "6", "7", "8", "9"))
	tk.MustExec("set tidb_enable_parallel_apply=true")
	checkApplyPlan(c, tk, q1, true)
	tk.MustQuery(q1).Sort().Check(testkit.Rows("1", "2", "3", "4", "5", "6", "7", "8", "9"))

	q2 := "select * from t t0 where t0.b <= (select max(t1.b) from t t1 where t1.b > (select max(b) from t t2 where t1.a > t2.a and t0.a > t2.a));"
	checkApplyPlan(c, tk, q2, true) // only the outside apply can be parallel
	tk.MustQuery(q2).Sort().Check(testkit.Rows("1 1", "2 2", "3 3", "4 4", "5 5", "6 6", "7 7", "8 8", "9 9"))
}
