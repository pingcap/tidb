// Copyright 2017 PingCAP, Inc.
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
	"github.com/pingcap/tidb/plan"
	"github.com/pingcap/tidb/util/testkit"
	"github.com/pingcap/tidb/util/testleak"
)

func checkMergeAndRun(tk *testkit.TestKit, c *C, sql string) *testkit.Result {
	explainedSql := "explain " + sql
	result := tk.MustQuery(explainedSql)
	resultStr := fmt.Sprintf("%v", result.Rows())
	if !strings.ContainsAny(resultStr, "MergeJoin") {
		c.Error("Expected MergeJoin in plan.")
	}
	return tk.MustQuery(sql)
}

func (s *testSuite) TestMergeJoin(c *C) {
	defer func() {
		s.cleanEnv(c)
		testleak.AfterTest(c)()
	}()
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")

	tk.MustExec("drop table if exists t")
	tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t(c1 int, c2 int)")
	tk.MustExec("create table t1(c1 int, c2 int)")
	tk.MustExec("insert into t values(1,1),(2,2)")
	tk.MustExec("insert into t1 values(2,3),(4,4)")

	result := checkMergeAndRun(tk, c, "select /*+ TIDB_SMJ(t) */ * from t left outer join t1 on t.c1 = t1.c1 where t.c1 = 1 or t1.c2 > 20")
	result.Check(testkit.Rows("1 1 <nil> <nil>"))
	result = checkMergeAndRun(tk, c, "select /*+ TIDB_SMJ(t) */ * from t1 right outer join t on t.c1 = t1.c1 where t.c1 = 1 or t1.c2 > 20")
	result.Check(testkit.Rows("<nil> <nil> 1 1"))
	result = checkMergeAndRun(tk, c, "select /*+ TIDB_SMJ(t) */ * from t right outer join t1 on t.c1 = t1.c1 where t.c1 = 1 or t1.c2 > 20")
	result.Check(testkit.Rows())
	result = checkMergeAndRun(tk, c, "select /*+ TIDB_SMJ(t) */ * from t left outer join t1 on t.c1 = t1.c1 where t1.c1 = 3 or false")
	result.Check(testkit.Rows())
	result = checkMergeAndRun(tk, c, "select /*+ TIDB_SMJ(t) */ * from t left outer join t1 on t.c1 = t1.c1 and t.c1 != 1 order by t1.c1")
	result.Check(testkit.Rows("1 1 <nil> <nil>", "2 2 2 3"))

	tk.MustExec("drop table if exists t1")
	tk.MustExec("drop table if exists t2")
	tk.MustExec("drop table if exists t3")

	tk.MustExec("create table t1 (c1 int, c2 int)")
	tk.MustExec("create table t2 (c1 int, c2 int)")
	tk.MustExec("create table t3 (c1 int, c2 int)")

	tk.MustExec("insert into t1 values (1,1), (2,2), (3,3)")
	tk.MustExec("insert into t2 values (1,1), (3,3), (5,5)")
	tk.MustExec("insert into t3 values (1,1), (5,5), (9,9)")

	result = tk.MustQuery("select /*+ TIDB_SMJ(t1,t2,t3) */ * from t1 left join t2 on t1.c1 = t2.c1 right join t3 on t2.c1 = t3.c1 order by t1.c1, t1.c2, t2.c1, t2.c2, t3.c1, t3.c2;")
	result.Check(testkit.Rows("<nil> <nil> <nil> <nil> 5 5", "<nil> <nil> <nil> <nil> 9 9", "1 1 1 1 1 1"))

	tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t1 (c1 int)")
	tk.MustExec("insert into t1 values (1), (1), (1)")
	result = tk.MustQuery("select/*+ TIDB_SMJ(t) */  * from t1 a join t1 b on a.c1 = b.c1;")
	result.Check(testkit.Rows("1 1", "1 1", "1 1", "1 1", "1 1", "1 1", "1 1", "1 1", "1 1"))

	tk.MustExec("drop table if exists t")
	tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t(c1 int, index k(c1))")
	tk.MustExec("create table t1(c1 int)")
	tk.MustExec("insert into t values (1),(2),(3),(4),(5),(6),(7)")
	tk.MustExec("insert into t1 values (1),(2),(3),(4),(5),(6),(7)")
	fmt.Println("2")
	result = tk.MustQuery("select /*+ TIDB_SMJ(a,b) */ a.c1 from t a , t1 b where a.c1 = b.c1 order by a.c1;")
	result.Check(testkit.Rows("1", "2", "3", "4", "5", "6", "7"))
	fmt.Println("3")
	result = tk.MustQuery("select /*+ TIDB_SMJ(a, b) */ a.c1 from t a , (select * from t1 limit 3) b where a.c1 = b.c1 order by b.c1;")
	result.Check(testkit.Rows("1", "2", "3"))

	plan.AllowCartesianProduct = false
	_, err := tk.Exec("select /*+ TIDB_SMJ(t,t1) */ * from t, t1")
	c.Check(plan.ErrCartesianProductUnsupported.Equal(err), IsTrue)
	_, err = tk.Exec("select /*+ TIDB_SMJ(t,t1) */ * from t left join t1 on 1")
	c.Check(plan.ErrCartesianProductUnsupported.Equal(err), IsTrue)
	_, err = tk.Exec("select /*+ TIDB_SMJ(t,t1) */ * from t right join t1 on 1")
	c.Check(plan.ErrCartesianProductUnsupported.Equal(err), IsTrue)
	plan.AllowCartesianProduct = true
	tk.MustExec("drop table if exists t")
	tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t(c1 int)")
	tk.MustExec("create table t1(c1 int unsigned)")
	tk.MustExec("insert into t values (1)")
	tk.MustExec("insert into t1 values (1)")
	fmt.Println("4")
	result = tk.MustQuery("select /*+ TIDB_SMJ(t,t1) */ t.c1 from t , t1 where t.c1 = t1.c1")
	result.Check(testkit.Rows("1"))
}
