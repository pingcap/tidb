// Copyright 2019 PingCAP, Inc.
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
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/util/testkit"
)

func (s *testSuite2) TestWindowFunctions(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int, b int, c int)")
	tk.MustExec("set @@tidb_enable_window_function = 1")
	defer func() {
		tk.MustExec("set @@tidb_enable_window_function = 0")
	}()
	tk.MustExec("insert into t values (1,2,3),(4,3,2),(2,3,4)")
	result := tk.MustQuery("select count(a) over () from t")
	result.Check(testkit.Rows("3", "3", "3"))
	result = tk.MustQuery("select sum(a) over () + count(a) over () from t")
	result.Check(testkit.Rows("10", "10", "10"))
	result = tk.MustQuery("select sum(a) over (partition by a) from t")
	result.Check(testkit.Rows("1", "2", "4"))
	result = tk.MustQuery("select 1 + sum(a) over (), count(a) over () from t")
	result.Check(testkit.Rows("8 3", "8 3", "8 3"))
	result = tk.MustQuery("select sum(t1.a) over() from t t1, t t2")
	result.Check(testkit.Rows("21", "21", "21", "21", "21", "21", "21", "21", "21"))
	result = tk.MustQuery("select _tidb_rowid, sum(t.a) over() from t")
	result.Check(testkit.Rows("1 7", "2 7", "3 7"))

	result = tk.MustQuery("select a, row_number() over() from t")
	result.Check(testkit.Rows("1 1", "4 2", "2 3"))
	result = tk.MustQuery("select a, row_number() over(partition by a) from t")
	result.Check(testkit.Rows("1 1", "2 1", "4 1"))

	result = tk.MustQuery("select a, sum(a) over(rows between unbounded preceding and 1 following) from t")
	result.Check(testkit.Rows("1 5", "4 7", "2 7"))
	result = tk.MustQuery("select a, sum(a) over(rows between 1 preceding and 1 following) from t")
	result.Check(testkit.Rows("1 5", "4 7", "2 6"))
	result = tk.MustQuery("select a, sum(a) over(rows between unbounded preceding and 1 preceding) from t")
	result.Check(testkit.Rows("1 <nil>", "4 1", "2 5"))

	tk.MustExec("drop table t")
	tk.MustExec("create table t(a int, b date)")
	tk.MustExec("insert into t values (null,null),(1,20190201),(2,20190202),(3,20190203),(5,20190205)")
	result = tk.MustQuery("select a, sum(a) over(order by a range between 1 preceding and 2 following) from t")
	result.Check(testkit.Rows("<nil> <nil>", "1 6", "2 6", "3 10", "5 5"))
	result = tk.MustQuery("select a, sum(a) over(order by a desc range between 1 preceding and 2 following) from t")
	result.Check(testkit.Rows("5 8", "3 6", "2 6", "1 3", "<nil> <nil>"))
	result = tk.MustQuery("select a, b, sum(a) over(order by b range between interval 1 day preceding and interval 2 day following) from t")
	result.Check(testkit.Rows("<nil> <nil> <nil>", "1 2019-02-01 6", "2 2019-02-02 6", "3 2019-02-03 10", "5 2019-02-05 5"))
	result = tk.MustQuery("select a, b, sum(a) over(order by b desc range between interval 1 day preceding and interval 2 day following) from t")
	result.Check(testkit.Rows("5 2019-02-05 8", "3 2019-02-03 6", "2 2019-02-02 6", "1 2019-02-01 3", "<nil> <nil> <nil>"))

	tk.MustExec("drop table t")
	tk.MustExec("create table t(a int, b int)")
	tk.MustExec("insert into t values (1,1),(1,2),(2,1),(2,2)")
	result = tk.MustQuery("select a, b, rank() over() from t")
	result.Check(testkit.Rows("1 1 1", "1 2 1", "2 1 1", "2 2 1"))
	result = tk.MustQuery("select a, b, rank() over(order by a) from t")
	result.Check(testkit.Rows("1 1 1", "1 2 1", "2 1 3", "2 2 3"))
	result = tk.MustQuery("select a, b, rank() over(order by a, b) from t")
	result.Check(testkit.Rows("1 1 1", "1 2 2", "2 1 3", "2 2 4"))

	result = tk.MustQuery("select a, b, dense_rank() over() from t")
	result.Check(testkit.Rows("1 1 1", "1 2 1", "2 1 1", "2 2 1"))
	result = tk.MustQuery("select a, b, dense_rank() over(order by a) from t")
	result.Check(testkit.Rows("1 1 1", "1 2 1", "2 1 2", "2 2 2"))
	result = tk.MustQuery("select a, b, dense_rank() over(order by a, b) from t")
	result.Check(testkit.Rows("1 1 1", "1 2 2", "2 1 3", "2 2 4"))

	result = tk.MustQuery("select row_number() over(rows between 1 preceding and 1 following) from t")
	result.Check(testkit.Rows("1", "2", "3", "4"))
	result = tk.MustQuery("show warnings")
	result.Check(testkit.Rows("Note 3599 Window function 'row_number' ignores the frame clause of window '<unnamed window>' and aggregates over the whole partition"))

	result = tk.MustQuery("select a, sum(a) over() from t")
	result.Check(testkit.Rows("1 6", "1 6", "2 6", "2 6"))
	result = tk.MustQuery("select a, sum(a) over(order by a) from t")
	result.Check(testkit.Rows("1 2", "1 2", "2 6", "2 6"))
	result = tk.MustQuery("select a, sum(a) over(order by a, b) from t")
	result.Check(testkit.Rows("1 1", "1 2", "2 4", "2 6"))

	result = tk.MustQuery("select a, first_value(a) over(), last_value(a) over() from t")
	result.Check(testkit.Rows("1 1 2", "1 1 2", "2 1 2", "2 1 2"))
	result = tk.MustQuery("select a, first_value(a) over(rows between 1 preceding and 1 following), last_value(a) over(rows between 1 preceding and 1 following) from t")
	result.Check(testkit.Rows("1 1 1", "1 1 2", "2 1 2", "2 2 2"))
	result = tk.MustQuery("select a, first_value(a) over(rows between 1 following and 1 following), last_value(a) over(rows between 1 following and 1 following) from t")
	result.Check(testkit.Rows("1 1 1", "1 2 2", "2 2 2", "2 <nil> <nil>"))
	result = tk.MustQuery("select a, first_value(rand(0)) over(), last_value(rand(0)) over() from t")
	result.Check(testkit.Rows("1 0.9451961492941164 0.05434383959970039", "1 0.9451961492941164 0.05434383959970039",
		"2 0.9451961492941164 0.05434383959970039", "2 0.9451961492941164 0.05434383959970039"))

	result = tk.MustQuery("select a, b, cume_dist() over() from t")
	result.Check(testkit.Rows("1 1 1", "1 2 1", "2 1 1", "2 2 1"))
	result = tk.MustQuery("select a, b, cume_dist() over(order by a) from t")
	result.Check(testkit.Rows("1 1 0.5", "1 2 0.5", "2 1 1", "2 2 1"))
	result = tk.MustQuery("select a, b, cume_dist() over(order by a, b) from t")
	result.Check(testkit.Rows("1 1 0.25", "1 2 0.5", "2 1 0.75", "2 2 1"))

	result = tk.MustQuery("select a, nth_value(a, null) over() from t")
	result.Check(testkit.Rows("1 <nil>", "1 <nil>", "2 <nil>", "2 <nil>"))
	result = tk.MustQuery("select a, nth_value(a, 1) over() from t")
	result.Check(testkit.Rows("1 1", "1 1", "2 1", "2 1"))
	result = tk.MustQuery("select a, nth_value(a, 4) over() from t")
	result.Check(testkit.Rows("1 2", "1 2", "2 2", "2 2"))
	result = tk.MustQuery("select a, nth_value(a, 5) over() from t")
	result.Check(testkit.Rows("1 <nil>", "1 <nil>", "2 <nil>", "2 <nil>"))

	result = tk.MustQuery("select ntile(3) over() from t")
	result.Check(testkit.Rows("1", "1", "2", "3"))
	result = tk.MustQuery("select ntile(2) over() from t")
	result.Check(testkit.Rows("1", "1", "2", "2"))
	result = tk.MustQuery("select ntile(null) over() from t")
	result.Check(testkit.Rows("<nil>", "<nil>", "<nil>", "<nil>"))

	result = tk.MustQuery("select a, percent_rank() over() from t")
	result.Check(testkit.Rows("1 0", "1 0", "2 0", "2 0"))
	result = tk.MustQuery("select a, percent_rank() over(order by a) from t")
	result.Check(testkit.Rows("1 0", "1 0", "2 0.6666666666666666", "2 0.6666666666666666"))
	result = tk.MustQuery("select a, b, percent_rank() over(order by a, b) from t")
	result.Check(testkit.Rows("1 1 0", "1 2 0.3333333333333333", "2 1 0.6666666666666666", "2 2 1"))

	result = tk.MustQuery("select a, lead(a) over (), lag(a) over() from t")
	result.Check(testkit.Rows("1 1 <nil>", "1 2 1", "2 2 1", "2 <nil> 2"))
	result = tk.MustQuery("select a, lead(a, 0) over(), lag(a, 0) over() from t")
	result.Check(testkit.Rows("1 1 1", "1 1 1", "2 2 2", "2 2 2"))
	result = tk.MustQuery("select a, lead(a, 1, a) over(), lag(a, 1, a) over() from t")
	result.Check(testkit.Rows("1 1 1", "1 2 1", "2 2 1", "2 2 2"))
	result = tk.MustQuery("select a, lead(a, 1, 'lead') over(), lag(a, 1, 'lag') over() from t")
	result.Check(testkit.Rows("1 1 lag", "1 2 1", "2 2 1", "2 lead 2"))

	result = tk.MustQuery("SELECT CUME_DIST() OVER (ORDER BY null);")
	result.Check(testkit.Rows("1"))
}
