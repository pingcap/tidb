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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package executor_test

import (
	"fmt"
	"testing"

	"github.com/pingcap/tidb/testkit"
)

func TestWindowFunctions(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("set @@tidb_window_concurrency = 1")
	tk.MustExec("set @@tidb_enable_pipelined_window_function = 0")
	defer func() {
		tk.MustExec("set @@tidb_enable_pipelined_window_function=1;")
	}()
	doTestWindowFunctions(tk)
}

func TestWindowParallelFunctions(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("set @@tidb_window_concurrency = 4")
	tk.MustExec("set @@tidb_enable_pipelined_window_function = 0")
	defer func() {
		tk.MustExec("set @@tidb_enable_pipelined_window_function=1;")
	}()
	doTestWindowFunctions(tk)
}

func TestPipelinedWindowFunctions(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("set @@tidb_window_concurrency = 1")
	doTestWindowFunctions(tk)
}

func TestPipelinedWindowParallelFunctions(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("set @@tidb_window_concurrency = 4")
	doTestWindowFunctions(tk)
}

func doTestWindowFunctions(tk *testkit.TestKit) {
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int, b int, c int)")
	tk.MustExec("set @@tidb_enable_window_function = 1")
	defer func() {
		tk.MustExec("set @@tidb_enable_window_function = 0")
	}()
	tk.MustExec("insert into t values (1,2,3),(4,3,2),(2,3,4)")
	tk.MustQuery("select count(a) over () from t").
		Check(testkit.Rows("3", "3", "3"))
	tk.MustQuery("select sum(a) over () + count(a) over () from t").
		Check(testkit.Rows("10", "10", "10"))
	tk.MustQuery("select sum(a) over (partition by a) from t").Sort().
		Check(testkit.Rows("1", "2", "4"))
	tk.MustQuery("select 1 + sum(a) over (), count(a) over () from t").
		Check(testkit.Rows("8 3", "8 3", "8 3"))
	tk.MustQuery("select sum(t1.a) over() from t t1, t t2").
		Check(testkit.Rows("21", "21", "21", "21", "21", "21", "21", "21", "21"))
	tk.MustQuery("select _tidb_rowid, sum(t.a) over() from t").
		Check(testkit.Rows("1 7", "2 7", "3 7"))

	tk.MustQuery("select a, row_number() over() from t").
		Check(testkit.Rows("1 1", "4 2", "2 3"))
	tk.MustQuery("select a, row_number() over(partition by a) from t").Sort().
		Check(testkit.Rows("1 1", "2 1", "4 1"))

	tk.MustQuery("select a, sum(a) over(rows between unbounded preceding and 1 following) from t").
		Check(testkit.Rows("1 5", "4 7", "2 7"))
	tk.MustQuery("select a, sum(a) over(rows between 1 preceding and 1 following) from t").
		Check(testkit.Rows("1 5", "4 7", "2 6"))
	tk.MustQuery("select a, sum(a) over(rows between unbounded preceding and 1 preceding) from t").
		Check(testkit.Rows("1 <nil>", "4 1", "2 5"))

	tk.MustExec("drop table t")
	tk.MustExec("create table t(a int, b date)")
	tk.MustExec("insert into t values (null,null),(1,20190201),(2,20190202),(3,20190203),(5,20190205)")
	tk.MustQuery("select a, sum(a) over(order by a range between 1 preceding and 2 following) from t").
		Check(testkit.Rows("<nil> <nil>", "1 6", "2 6", "3 10", "5 5"))
	tk.MustQuery("select a, sum(a) over(order by a desc range between 1 preceding and 2 following) from t").
		Check(testkit.Rows("5 8", "3 6", "2 6", "1 3", "<nil> <nil>"))
	tk.MustQuery("select a, b, sum(a) over(order by b range between interval 1 day preceding and interval 2 day following) from t").
		Check(testkit.Rows("<nil> <nil> <nil>", "1 2019-02-01 6", "2 2019-02-02 6", "3 2019-02-03 10", "5 2019-02-05 5"))
	tk.MustQuery("select a, b, sum(a) over(order by b desc range between interval 1 day preceding and interval 2 day following) from t").
		Check(testkit.Rows("5 2019-02-05 8", "3 2019-02-03 6", "2 2019-02-02 6", "1 2019-02-01 3", "<nil> <nil> <nil>"))

	tk.MustExec("drop table t")
	tk.MustExec("CREATE TABLE t (id INTEGER, sex CHAR(1))")
	tk.MustExec("insert into t values (1, 'M'), (2, 'F'), (3, 'F'), (4, 'F'), (5, 'M'), (10, NULL), (11, NULL)")
	tk.MustQuery("SELECT sex, id, RANK() OVER (PARTITION BY sex ORDER BY id DESC) FROM t").Sort().
		Check(testkit.Rows("<nil> 10 2", "<nil> 11 1", "F 2 3", "F 3 2", "F 4 1", "M 1 2", "M 5 1"))

	tk.MustExec("drop table t")
	tk.MustExec("create table t(a int, b int)")
	tk.MustExec("insert into t values (1,1),(1,2),(2,1),(2,2)")
	tk.MustQuery("select a, b, rank() over() from t").
		Check(testkit.Rows("1 1 1", "1 2 1", "2 1 1", "2 2 1"))
	tk.MustQuery("select a, b, rank() over(order by a) from t").
		Check(testkit.Rows("1 1 1", "1 2 1", "2 1 3", "2 2 3"))
	tk.MustQuery("select a, b, rank() over(order by a, b) from t").
		Check(testkit.Rows("1 1 1", "1 2 2", "2 1 3", "2 2 4"))

	tk.MustQuery("select a, b, dense_rank() over() from t").
		Check(testkit.Rows("1 1 1", "1 2 1", "2 1 1", "2 2 1"))
	tk.MustQuery("select a, b, dense_rank() over(order by a) from t").
		Check(testkit.Rows("1 1 1", "1 2 1", "2 1 2", "2 2 2"))
	tk.MustQuery("select a, b, dense_rank() over(order by a, b) from t").
		Check(testkit.Rows("1 1 1", "1 2 2", "2 1 3", "2 2 4"))

	tk.MustQuery("select row_number() over(rows between 1 preceding and 1 following) from t").
		Check(testkit.Rows("1", "2", "3", "4"))
	tk.MustQuery("show warnings").
		Check(testkit.Rows("Note 3599 Window function 'row_number' ignores the frame clause of window '<unnamed window>' and aggregates over the whole partition"))

	tk.MustQuery("select a, sum(a) over() from t").
		Check(testkit.Rows("1 6", "1 6", "2 6", "2 6"))
	tk.MustQuery("select a, sum(a) over(order by a) from t").
		Check(testkit.Rows("1 2", "1 2", "2 6", "2 6"))
	tk.MustQuery("select a, sum(a) over(order by a, b) from t").
		Check(testkit.Rows("1 1", "1 2", "2 4", "2 6"))

	tk.MustQuery("select a, first_value(a) over(), last_value(a) over() from t").
		Check(testkit.Rows("1 1 2", "1 1 2", "2 1 2", "2 1 2"))
	tk.MustQuery("select a, first_value(a) over(rows between 1 preceding and 1 following), last_value(a) over(rows between 1 preceding and 1 following) from t").
		Check(testkit.Rows("1 1 1", "1 1 2", "2 1 2", "2 2 2"))
	tk.MustQuery("select a, first_value(a) over(rows between 1 following and 1 following), last_value(a) over(rows between 1 following and 1 following) from t").
		Check(testkit.Rows("1 1 1", "1 2 2", "2 2 2", "2 <nil> <nil>"))
	tk.MustQuery("select a, first_value(rand(0)) over(), last_value(rand(0)) over() from t").
		Check(testkit.Rows("1 0.15522042769493574 0.33109208227236947", "1 0.15522042769493574 0.33109208227236947",
			"2 0.15522042769493574 0.33109208227236947", "2 0.15522042769493574 0.33109208227236947"))

	tk.MustQuery("select a, b, cume_dist() over() from t").
		Check(testkit.Rows("1 1 1", "1 2 1", "2 1 1", "2 2 1"))
	tk.MustQuery("select a, b, cume_dist() over(order by a) from t").
		Check(testkit.Rows("1 1 0.5", "1 2 0.5", "2 1 1", "2 2 1"))
	tk.MustQuery("select a, b, cume_dist() over(order by a, b) from t").
		Check(testkit.Rows("1 1 0.25", "1 2 0.5", "2 1 0.75", "2 2 1"))

	tk.MustQuery("select a, nth_value(a, null) over() from t").
		Check(testkit.Rows("1 <nil>", "1 <nil>", "2 <nil>", "2 <nil>"))
	tk.MustQuery("select a, nth_value(a, 1) over() from t").
		Check(testkit.Rows("1 1", "1 1", "2 1", "2 1"))
	tk.MustQuery("select a, nth_value(a, 4) over() from t").
		Check(testkit.Rows("1 2", "1 2", "2 2", "2 2"))
	tk.MustQuery("select a, nth_value(a, 5) over() from t").
		Check(testkit.Rows("1 <nil>", "1 <nil>", "2 <nil>", "2 <nil>"))

	tk.MustQuery("select ntile(3) over() from t").
		Check(testkit.Rows("1", "1", "2", "3"))
	tk.MustQuery("select ntile(2) over() from t").
		Check(testkit.Rows("1", "1", "2", "2"))
	tk.MustQuery("select ntile(null) over() from t").
		Check(testkit.Rows("<nil>", "<nil>", "<nil>", "<nil>"))

	tk.MustQuery("select a, percent_rank() over() from t").
		Check(testkit.Rows("1 0", "1 0", "2 0", "2 0"))
	tk.MustQuery("select a, percent_rank() over(order by a) from t").
		Check(testkit.Rows("1 0", "1 0", "2 0.6666666666666666", "2 0.6666666666666666"))
	tk.MustQuery("select a, b, percent_rank() over(order by a, b) from t").
		Check(testkit.Rows("1 1 0", "1 2 0.3333333333333333", "2 1 0.6666666666666666", "2 2 1"))

	tk.MustQuery("select a, lead(a) over (), lag(a) over() from t").
		Check(testkit.Rows("1 1 <nil>", "1 2 1", "2 2 1", "2 <nil> 2"))
	tk.MustQuery("select a, lead(a, 0) over(), lag(a, 0) over() from t").
		Check(testkit.Rows("1 1 1", "1 1 1", "2 2 2", "2 2 2"))
	tk.MustQuery("select a, lead(a, 1, a) over(), lag(a, 1, a) over() from t").
		Check(testkit.Rows("1 1 1", "1 2 1", "2 2 1", "2 2 2"))
	tk.MustQuery("select a, lead(a, 1, 'lead') over(), lag(a, 1, 'lag') over() from t").
		Check(testkit.Rows("1 1 lag", "1 2 1", "2 2 1", "2 lead 2"))

	tk.MustQuery("SELECT CUME_DIST() OVER (ORDER BY null);").
		Check(testkit.Rows("1"))

	tk.MustQuery("select lead(a) over(partition by null) from t").Sort().Check(testkit.Rows("1", "2", "2", "<nil>"))

	tk.MustExec("create table issue10494(a INT, b CHAR(1), c DATETIME, d BLOB)")
	tk.MustExec("insert into issue10494 VALUES (1,'x','2010-01-01','blob'), (2, 'y', '2011-01-01', ''), (3, 'y', '2012-01-01', ''), (4, 't', '2012-01-01', 'blob'), (5, null, '2013-01-01', null)")
	tk.MustQuery("SELECT a, b, c, SUM(a) OVER (RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM issue10494 order by a;").Check(
		testkit.Rows(
			"1 x 2010-01-01 00:00:00 15",
			"2 y 2011-01-01 00:00:00 15",
			"3 y 2012-01-01 00:00:00 15",
			"4 t 2012-01-01 00:00:00 15",
			"5 <nil> 2013-01-01 00:00:00 15",
		),
	)

	tk.MustExec("CREATE TABLE td_dec (id DECIMAL(10,2), sex CHAR(1));")
	tk.MustExec("insert into td_dec value (2.0, 'F'), (NULL, 'F'), (1.0, 'F')")
	tk.MustQuery("SELECT id, FIRST_VALUE(id) OVER w FROM td_dec WINDOW w AS (ORDER BY id);").Check(
		testkit.Rows("<nil> <nil>", "1.00 <nil>", "2.00 <nil>"),
	)

	tk.MustQuery("select sum(a) over w, sum(b) over w from t window w as (order by a)").
		Check(testkit.Rows("2 3", "2 3", "6 6", "6 6"))
	tk.MustQuery("select row_number() over w, sum(b) over w from t window w as (order by a)").
		Check(testkit.Rows("1 3", "2 3", "3 6", "4 6"))
	tk.MustQuery("select row_number() over w, sum(b) over w from t window w as (rows between 1 preceding and 1 following)").
		Check(testkit.Rows("1 3", "2 4", "3 5", "4 3"))

	tk.Session().GetSessionVars().MaxChunkSize = 1
	tk.MustQuery("select a, row_number() over (partition by a) from t").Sort().
		Check(testkit.Rows("1 1", "1 2", "2 1", "2 2"))
}

func TestWindowFunctionsDataReference(t *testing.T) {
	// see https://github.com/pingcap/tidb/issues/11614
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b int)")
	tk.MustExec("insert into t values (2,1),(2,2),(2,3)")

	tk.Session().GetSessionVars().MaxChunkSize = 2
	tk.MustQuery("select a, b, rank() over (partition by a order by b) from t").
		Check(testkit.Rows("2 1 1", "2 2 2", "2 3 3"))
	tk.MustQuery("select a, b, PERCENT_RANK() over (partition by a order by b) from t").
		Check(testkit.Rows("2 1 0", "2 2 0.5", "2 3 1"))
	tk.MustQuery("select a, b, CUME_DIST() over (partition by a order by b) from t").
		Check(testkit.Rows("2 1 0.3333333333333333", "2 2 0.6666666666666666", "2 3 1"))

	// see https://github.com/pingcap/tidb/issues/12415
	tk.MustQuery("select b, first_value(b) over (order by b RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) from t").
		Check(testkit.Rows("1 1", "2 1", "3 1"))
	tk.MustQuery("select b, first_value(b) over (order by b ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) from t").
		Check(testkit.Rows("1 1", "2 1", "3 1"))
}

func TestSlidingWindowFunctions(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test;")
	tk.MustExec("set @@tidb_enable_pipelined_window_function=0;")
	defer func() {
		tk.MustExec("set @@tidb_enable_pipelined_window_function=1;")
	}()
	idTypes := []string{"FLOAT", "DOUBLE"}
	useHighPrecisions := []string{"ON", "OFF"}
	for _, idType := range idTypes {
		for _, useHighPrecision := range useHighPrecisions {
			tk.MustExec("drop table if exists t;")
			tk.MustExec(fmt.Sprintf("CREATE TABLE t (id %s, sex CHAR(1));", idType))
			tk.MustExec(fmt.Sprintf("SET SESSION windowing_use_high_precision = %s;", useHighPrecision))
			baseTestSlidingWindowFunctions(tk)
		}
	}
}

func TestPipelinedSlidingWindowFunctions(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test;")
	idTypes := []string{"FLOAT", "DOUBLE"}
	useHighPrecisions := []string{"ON", "OFF"}
	for _, idType := range idTypes {
		for _, useHighPrecision := range useHighPrecisions {
			tk.MustExec("drop table if exists t;")
			tk.MustExec(fmt.Sprintf("CREATE TABLE t (id %s, sex CHAR(1));", idType))
			tk.MustExec(fmt.Sprintf("SET SESSION windowing_use_high_precision = %s;", useHighPrecision))
			baseTestSlidingWindowFunctions(tk)
		}
	}
}

func baseTestSlidingWindowFunctions(tk *testkit.TestKit) {
	tk.MustExec("insert into t values (1,'M')")
	tk.MustExec("insert into t values (2,'F')")
	tk.MustExec("insert into t values (3,'F')")
	tk.MustExec("insert into t values (4,'F')")
	tk.MustExec("insert into t values (5,'M')")
	tk.MustExec("insert into t values (10,null)")
	tk.MustExec("insert into t values (11,null)")

	tk.MustExec("PREPARE p FROM 'SELECT sex, COUNT(id) OVER (ORDER BY id ROWS BETWEEN ? PRECEDING and ? PRECEDING) FROM t';")
	tk.MustExec("SET @p1= 1;")
	tk.MustExec("SET @p2= 2;")
	tk.MustQuery("EXECUTE p USING @p1, @p2;").
		Check(testkit.Rows("M 0", "F 0", "F 0", "F 0", "M 0", "<nil> 0", "<nil> 0"))
	tk.MustQuery("EXECUTE p USING @p2, @p1;").
		Check(testkit.Rows("M 0", "F 1", "F 2", "F 2", "M 2", "<nil> 2", "<nil> 2"))
	tk.MustExec("DROP PREPARE p;")
	tk.MustExec("PREPARE p FROM 'SELECT sex, COUNT(id) OVER (ORDER BY id ROWS BETWEEN ? FOLLOWING and ? FOLLOWING) FROM t';")
	tk.MustExec("SET @p1= 1;")
	tk.MustExec("SET @p2= 2;")
	tk.MustQuery("EXECUTE p USING @p2, @p1;").
		Check(testkit.Rows("M 0", "F 0", "F 0", "F 0", "M 0", "<nil> 0", "<nil> 0"))
	tk.MustQuery("EXECUTE p USING @p1, @p2;").
		Check(testkit.Rows("M 2", "F 2", "F 2", "F 2", "M 2", "<nil> 1", "<nil> 0"))
	tk.MustExec("DROP PREPARE p;")

	// COUNT ROWS
	tk.MustQuery("SELECT sex, COUNT(id) OVER (ORDER BY id ROWS BETWEEN 1 FOLLOWING and 2 FOLLOWING) FROM t;").
		Check(testkit.Rows("M 2", "F 2", "F 2", "F 2", "M 2", "<nil> 1", "<nil> 0"))
	tk.MustQuery("SELECT sex, COUNT(id) OVER (ORDER BY id ROWS BETWEEN 3 FOLLOWING and 1 FOLLOWING) FROM t;").
		Check(testkit.Rows("M 0", "F 0", "F 0", "F 0", "M 0", "<nil> 0", "<nil> 0"))
	tk.MustQuery("SELECT sex, COUNT(id) OVER (ORDER BY id ROWS BETWEEN 2 PRECEDING and 1 PRECEDING) FROM t;").
		Check(testkit.Rows("M 0", "F 1", "F 2", "F 2", "M 2", "<nil> 2", "<nil> 2"))
	tk.MustQuery("SELECT sex, COUNT(id) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING and 3 PRECEDING) FROM t;").
		Check(testkit.Rows("M 0", "F 0", "F 0", "F 0", "M 0", "<nil> 0", "<nil> 0"))

	// COUNT RANGE
	tk.MustQuery("SELECT sex, COUNT(id) OVER (ORDER BY id RANGE BETWEEN 1 FOLLOWING and 2 FOLLOWING) FROM t;").
		Check(testkit.Rows("M 2", "F 2", "F 2", "F 1", "M 0", "<nil> 1", "<nil> 0"))
	tk.MustQuery("SELECT sex, COUNT(id) OVER (ORDER BY id RANGE BETWEEN 3 FOLLOWING and 1 FOLLOWING) FROM t;").
		Check(testkit.Rows("M 0", "F 0", "F 0", "F 0", "M 0", "<nil> 0", "<nil> 0"))
	tk.MustQuery("SELECT sex, COUNT(id) OVER (ORDER BY id RANGE BETWEEN 2 PRECEDING and 1 PRECEDING) FROM t;").
		Check(testkit.Rows("M 0", "F 1", "F 2", "F 2", "M 2", "<nil> 0", "<nil> 1"))
	tk.MustQuery("SELECT sex, COUNT(id) OVER (ORDER BY id RANGE BETWEEN 1 PRECEDING and 3 PRECEDING) FROM t;").
		Check(testkit.Rows("M 0", "F 0", "F 0", "F 0", "M 0", "<nil> 0", "<nil> 0"))

	// SUM ROWS
	tk.MustQuery("SELECT sex, SUM(id) OVER (ORDER BY id ROWS BETWEEN 1 FOLLOWING and 2 FOLLOWING) FROM t;").
		Check(testkit.Rows("M 5", "F 7", "F 9", "F 15", "M 21", "<nil> 11", "<nil> <nil>"))
	tk.MustQuery("SELECT sex, SUM(id) OVER (ORDER BY id ROWS BETWEEN 3 FOLLOWING and 1 FOLLOWING) FROM t;").
		Check(testkit.Rows("M <nil>", "F <nil>", "F <nil>", "F <nil>", "M <nil>", "<nil> <nil>", "<nil> <nil>"))
	tk.MustQuery("SELECT sex, SUM(id) OVER (ORDER BY id ROWS BETWEEN 2 PRECEDING and 1 PRECEDING) FROM t;").
		Check(testkit.Rows("M <nil>", "F 1", "F 3", "F 5", "M 7", "<nil> 9", "<nil> 15"))
	tk.MustQuery("SELECT sex, SUM(id) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING and 3 PRECEDING) FROM t;").
		Check(testkit.Rows("M <nil>", "F <nil>", "F <nil>", "F <nil>", "M <nil>", "<nil> <nil>", "<nil> <nil>"))
	tk.MustQuery("SELECT sex, SUM(id) OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING and 1 FOLLOWING) FROM t;").
		Check(testkit.Rows("M 3", "F 6", "F 10", "F 15", "M 25", "<nil> 36", "<nil> 36"))

	// SUM RANGE
	tk.MustQuery("SELECT sex, SUM(id) OVER (ORDER BY id RANGE BETWEEN 1 FOLLOWING and 2 FOLLOWING) FROM t;").
		Check(testkit.Rows("M 5", "F 7", "F 9", "F 5", "M <nil>", "<nil> 11", "<nil> <nil>"))
	tk.MustQuery("SELECT sex, SUM(id) OVER (ORDER BY id RANGE BETWEEN 3 FOLLOWING and 1 FOLLOWING) FROM t;").
		Check(testkit.Rows("M <nil>", "F <nil>", "F <nil>", "F <nil>", "M <nil>", "<nil> <nil>", "<nil> <nil>"))
	tk.MustQuery("SELECT sex, SUM(id) OVER (ORDER BY id RANGE BETWEEN 2 PRECEDING and 1 PRECEDING) FROM t;").
		Check(testkit.Rows("M <nil>", "F 1", "F 3", "F 5", "M 7", "<nil> <nil>", "<nil> 10"))
	tk.MustQuery("SELECT sex, SUM(id) OVER (ORDER BY id RANGE BETWEEN 1 PRECEDING and 2 FOLLOWING) FROM t;").
		Check(testkit.Rows("M 6", "F 10", "F 14", "F 12", "M 9", "<nil> 21", "<nil> 21"))
	tk.MustQuery("SELECT sex, SUM(id) OVER (ORDER BY id DESC RANGE BETWEEN 1 PRECEDING and 2 FOLLOWING) FROM t;").
		Check(testkit.Rows("<nil> 21", "<nil> 21", "M 12", "F 14", "F 10", "F 6", "M 3"))

	// AVG ROWS
	tk.MustQuery("SELECT sex, AVG(id) OVER (ORDER BY id ROWS BETWEEN 1 FOLLOWING and 2 FOLLOWING) FROM t;").
		Check(testkit.Rows("M 2.5", "F 3.5", "F 4.5", "F 7.5", "M 10.5", "<nil> 11", "<nil> <nil>"))
	tk.MustQuery("SELECT sex, AVG(id) OVER (ORDER BY id ROWS BETWEEN 3 FOLLOWING and 1 FOLLOWING) FROM t;").
		Check(testkit.Rows("M <nil>", "F <nil>", "F <nil>", "F <nil>", "M <nil>", "<nil> <nil>", "<nil> <nil>"))
	tk.MustQuery("SELECT sex, AVG(id) OVER (ORDER BY id ROWS BETWEEN 2 PRECEDING and 1 PRECEDING) FROM t;").
		Check(testkit.Rows("M <nil>", "F 1", "F 1.5", "F 2.5", "M 3.5", "<nil> 4.5", "<nil> 7.5"))
	tk.MustQuery("SELECT sex, AVG(id) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING and 3 PRECEDING) FROM t;").
		Check(testkit.Rows("M <nil>", "F <nil>", "F <nil>", "F <nil>", "M <nil>", "<nil> <nil>", "<nil> <nil>"))
	tk.MustQuery("SELECT sex, AVG(id) OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING and 1 FOLLOWING) FROM t;").
		Check(testkit.Rows("M 1.5", "F 2", "F 2.5", "F 3", "M 4.166666666666667", "<nil> 5.142857142857143", "<nil> 5.142857142857143"))

	// AVG RANGE
	tk.MustQuery("SELECT sex, AVG(id) OVER (ORDER BY id RANGE BETWEEN 1 FOLLOWING and 2 FOLLOWING) FROM t;").
		Check(testkit.Rows("M 2.5", "F 3.5", "F 4.5", "F 5", "M <nil>", "<nil> 11", "<nil> <nil>"))
	tk.MustQuery("SELECT sex, AVG(id) OVER (ORDER BY id RANGE BETWEEN 3 FOLLOWING and 1 FOLLOWING) FROM t;").
		Check(testkit.Rows("M <nil>", "F <nil>", "F <nil>", "F <nil>", "M <nil>", "<nil> <nil>", "<nil> <nil>"))
	tk.MustQuery("SELECT sex, AVG(id) OVER (ORDER BY id RANGE BETWEEN 2 PRECEDING and 1 PRECEDING) FROM t;").
		Check(testkit.Rows("M <nil>", "F 1", "F 1.5", "F 2.5", "M 3.5", "<nil> <nil>", "<nil> 10"))
	tk.MustQuery("SELECT sex, AVG(id) OVER (ORDER BY id RANGE BETWEEN 1 PRECEDING and 2 FOLLOWING) FROM t;").
		Check(testkit.Rows("M 2", "F 2.5", "F 3.5", "F 4", "M 4.5", "<nil> 10.5", "<nil> 10.5"))
	tk.MustQuery("SELECT sex, AVG(id) OVER (ORDER BY id DESC RANGE BETWEEN 1 PRECEDING and 2 FOLLOWING) FROM t;").
		Check(testkit.Rows("<nil> 10.5", "<nil> 10.5", "M 4", "F 3.5", "F 2.5", "F 2", "M 1.5"))

	// BIT_XOR ROWS
	tk.MustQuery("SELECT sex, BIT_XOR(id) OVER (ORDER BY id ROWS BETWEEN 1 FOLLOWING and 2 FOLLOWING) FROM t;").
		Check(testkit.Rows("M 1", "F 7", "F 1", "F 15", "M 1", "<nil> 11", "<nil> 0"))
	tk.MustQuery("SELECT sex, BIT_XOR(id) OVER (ORDER BY id ROWS BETWEEN 3 FOLLOWING and 1 FOLLOWING) FROM t;").
		Check(testkit.Rows("M 0", "F 0", "F 0", "F 0", "M 0", "<nil> 0", "<nil> 0"))
	tk.MustQuery("SELECT sex, BIT_XOR(id) OVER (ORDER BY id ROWS BETWEEN 2 PRECEDING and 1 PRECEDING) FROM t;").
		Check(testkit.Rows("M 0", "F 1", "F 3", "F 1", "M 7", "<nil> 1", "<nil> 15"))
	tk.MustQuery("SELECT sex, BIT_XOR(id) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING and 3 PRECEDING) FROM t;").
		Check(testkit.Rows("M 0", "F 0", "F 0", "F 0", "M 0", "<nil> 0", "<nil> 0"))
	tk.MustQuery("SELECT sex, BIT_XOR(id) OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING and 1 FOLLOWING) FROM t;").
		Check(testkit.Rows("M 3", "F 0", "F 4", "F 1", "M 11", "<nil> 0", "<nil> 0"))

	// BIT_XOR RANGE
	tk.MustQuery("SELECT sex, BIT_XOR(id) OVER (ORDER BY id RANGE BETWEEN 1 FOLLOWING and 2 FOLLOWING) FROM t;").
		Check(testkit.Rows("M 1", "F 7", "F 1", "F 5", "M 0", "<nil> 11", "<nil> 0"))
	tk.MustQuery("SELECT sex, BIT_XOR(id) OVER (ORDER BY id RANGE BETWEEN 3 FOLLOWING and 1 FOLLOWING) FROM t;").
		Check(testkit.Rows("M 0", "F 0", "F 0", "F 0", "M 0", "<nil> 0", "<nil> 0"))
	tk.MustQuery("SELECT sex, BIT_XOR(id) OVER (ORDER BY id RANGE BETWEEN 2 PRECEDING and 1 PRECEDING) FROM t;").
		Check(testkit.Rows("M 0", "F 1", "F 3", "F 1", "M 7", "<nil> 0", "<nil> 10"))
	tk.MustQuery("SELECT sex, BIT_XOR(id) OVER (ORDER BY id RANGE BETWEEN 1 PRECEDING and 2 FOLLOWING) FROM t;").
		Check(testkit.Rows("M 0", "F 4", "F 0", "F 2", "M 1", "<nil> 1", "<nil> 1"))
	tk.MustQuery("SELECT sex, BIT_XOR(id) OVER (ORDER BY id DESC RANGE BETWEEN 1 PRECEDING and 2 FOLLOWING) FROM t;").
		Check(testkit.Rows("<nil> 1", "<nil> 1", "M 2", "F 0", "F 4", "F 0", "M 3"))

	// MIN ROWS
	tk.MustQuery("SELECT sex, MIN(id) OVER (ORDER BY id ROWS BETWEEN 1 FOLLOWING and 2 FOLLOWING) FROM t;").
		Check(testkit.Rows("M 2", "F 3", "F 4", "F 5", "M 10", "<nil> 11", "<nil> <nil>"))
	tk.MustQuery("SELECT sex, MIN(id) OVER (ORDER BY id ROWS BETWEEN 3 FOLLOWING and 1 FOLLOWING) FROM t;").
		Check(testkit.Rows("M <nil>", "F <nil>", "F <nil>", "F <nil>", "M <nil>", "<nil> <nil>", "<nil> <nil>"))
	tk.MustQuery("SELECT sex, MIN(id) OVER (ORDER BY id ROWS BETWEEN 2 PRECEDING and 1 PRECEDING) FROM t;").
		Check(testkit.Rows("M <nil>", "F 1", "F 1", "F 2", "M 3", "<nil> 4", "<nil> 5"))
	tk.MustQuery("SELECT sex, MIN(id) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING and 3 PRECEDING) FROM t;").
		Check(testkit.Rows("M <nil>", "F <nil>", "F <nil>", "F <nil>", "M <nil>", "<nil> <nil>", "<nil> <nil>"))
	tk.MustQuery("SELECT sex, MIN(id) OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING and 1 FOLLOWING) FROM t;").
		Check(testkit.Rows("M 1", "F 1", "F 1", "F 1", "M 1", "<nil> 1", "<nil> 1"))

	// MIN RANGE
	tk.MustQuery("SELECT sex, MIN(id) OVER (ORDER BY id RANGE BETWEEN 1 FOLLOWING and 2 FOLLOWING) FROM t;").
		Check(testkit.Rows("M 2", "F 3", "F 4", "F 5", "M <nil>", "<nil> 11", "<nil> <nil>"))
	tk.MustQuery("SELECT sex, MIN(id) OVER (ORDER BY id RANGE BETWEEN 3 FOLLOWING and 1 FOLLOWING) FROM t;").
		Check(testkit.Rows("M <nil>", "F <nil>", "F <nil>", "F <nil>", "M <nil>", "<nil> <nil>", "<nil> <nil>"))
	tk.MustQuery("SELECT sex, MIN(id) OVER (ORDER BY id RANGE BETWEEN 2 PRECEDING and 1 PRECEDING) FROM t;").
		Check(testkit.Rows("M <nil>", "F 1", "F 1", "F 2", "M 3", "<nil> <nil>", "<nil> 10"))
	tk.MustQuery("SELECT sex, MIN(id) OVER (ORDER BY id RANGE BETWEEN 1 PRECEDING and 2 FOLLOWING) FROM t;").
		Check(testkit.Rows("M 1", "F 1", "F 2", "F 3", "M 4", "<nil> 10", "<nil> 10"))
	tk.MustQuery("SELECT sex, MIN(id) OVER (ORDER BY id DESC RANGE BETWEEN 1 PRECEDING and 2 FOLLOWING) FROM t;").
		Check(testkit.Rows("<nil> 10", "<nil> 10", "M 3", "F 2", "F 1", "F 1", "M 1"))

	// MAX ROWS
	tk.MustQuery("SELECT sex, MAX(id) OVER (ORDER BY id ROWS BETWEEN 1 FOLLOWING and 2 FOLLOWING) FROM t;").
		Check(testkit.Rows("M 3", "F 4", "F 5", "F 10", "M 11", "<nil> 11", "<nil> <nil>"))
	tk.MustQuery("SELECT sex, MAX(id) OVER (ORDER BY id ROWS BETWEEN 3 FOLLOWING and 1 FOLLOWING) FROM t;").
		Check(testkit.Rows("M <nil>", "F <nil>", "F <nil>", "F <nil>", "M <nil>", "<nil> <nil>", "<nil> <nil>"))
	tk.MustQuery("SELECT sex, MAX(id) OVER (ORDER BY id ROWS BETWEEN 2 PRECEDING and 1 PRECEDING) FROM t;").
		Check(testkit.Rows("M <nil>", "F 1", "F 2", "F 3", "M 4", "<nil> 5", "<nil> 10"))
	tk.MustQuery("SELECT sex, MAX(id) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING and 3 PRECEDING) FROM t;").
		Check(testkit.Rows("M <nil>", "F <nil>", "F <nil>", "F <nil>", "M <nil>", "<nil> <nil>", "<nil> <nil>"))
	tk.MustQuery("SELECT sex, MAX(id) OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING and 1 FOLLOWING) FROM t;").
		Check(testkit.Rows("M 2", "F 3", "F 4", "F 5", "M 10", "<nil> 11", "<nil> 11"))

	// MAX RANGE
	tk.MustQuery("SELECT sex, MAX(id) OVER (ORDER BY id RANGE BETWEEN 1 FOLLOWING and 2 FOLLOWING) FROM t;").
		Check(testkit.Rows("M 3", "F 4", "F 5", "F 5", "M <nil>", "<nil> 11", "<nil> <nil>"))
	tk.MustQuery("SELECT sex, MAX(id) OVER (ORDER BY id RANGE BETWEEN 3 FOLLOWING and 1 FOLLOWING) FROM t;").
		Check(testkit.Rows("M <nil>", "F <nil>", "F <nil>", "F <nil>", "M <nil>", "<nil> <nil>", "<nil> <nil>"))
	tk.MustQuery("SELECT sex, MAX(id) OVER (ORDER BY id RANGE BETWEEN 2 PRECEDING and 1 PRECEDING) FROM t;").
		Check(testkit.Rows("M <nil>", "F 1", "F 2", "F 3", "M 4", "<nil> <nil>", "<nil> 10"))
	tk.MustQuery("SELECT sex, MAX(id) OVER (ORDER BY id RANGE BETWEEN 1 PRECEDING and 2 FOLLOWING) FROM t;").
		Check(testkit.Rows("M 3", "F 4", "F 5", "F 5", "M 5", "<nil> 11", "<nil> 11"))
	tk.MustQuery("SELECT sex, MAX(id) OVER (ORDER BY id DESC RANGE BETWEEN 1 PRECEDING and 2 FOLLOWING) FROM t;").
		Check(testkit.Rows("<nil> 11", "<nil> 11", "M 5", "F 5", "F 4", "F 3", "M 2"))
}

func TestIssue24264(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists tbl_2")
	tk.MustExec("create table tbl_2 ( col_10 char(65) collate utf8mb4_unicode_ci not null , col_11 bigint not null , col_12 datetime not null , col_13 bigint unsigned default 327695751717730004 , col_14 timestamp default '2010-11-18' not null , primary key idx_5 ( col_11,col_13 ) /*T![clustered_index] clustered */ , unique key idx_6 ( col_10,col_11,col_13 ) , unique key idx_7 ( col_14,col_12,col_13 ) )")
	tk.MustExec("insert into tbl_2 values ( 'RmF',-5353757041350034197,'1996-01-22',1866803697729291364,'1996-09-11' )")
	tk.MustExec("insert into tbl_2 values ( 'xEOGaB',-6602924241498980347,'2019-02-22',8297270320597030697,'1972-04-04' )")
	tk.MustExec("insert into tbl_2 values ( 'dvUztqgTPAhLdzgEsV',3316448219481769821,'2034-09-12',937089564901142512,'2030-12-04' )")
	tk.MustExec("insert into tbl_2 values ( 'mNoyfbT',-6027094365061219400,'2035-10-10',1752804734961508175,'1992-08-09' )")
	tk.MustExec("insert into tbl_2 values ( 'BDPJMhLYXuKB',6823702503458376955,'2015-04-09',737914379167848827,'2026-04-29' )")
	tk.MustExec("insert into tbl_2 values ( 'WPiaVfPstGohvHd',1308183537252932688,'2020-05-03',5364104746649397703,'1979-01-28' )")
	tk.MustExec("insert into tbl_2 values ( 'lrm',4642935044097656317,'1973-04-29',149081313305673035,'2013-02-03' )")
	tk.MustExec("insert into tbl_2 values ( '',-7361040853169906422,'2024-10-22',6308270832310351889,'1981-02-01' )")
	tk.MustExec("insert into tbl_2 values ( 'uDANahGcLwpSssabD',2235074865448210231,'1992-10-10',7140606140672586593,'1992-11-25' )")
	tk.MustExec("insert into tbl_2 values ( 'TDH',-1911014243756021618,'2013-01-26',2022218243939205750,'1982-04-04' )")
	tk.MustQuery("select   lead(col_13,1,NULL) over w from tbl_2 window w as (order by col_13)").Check(testkit.Rows(
		"737914379167848827",
		"937089564901142512",
		"1752804734961508175",
		"1866803697729291364",
		"2022218243939205750",
		"5364104746649397703",
		"6308270832310351889",
		"7140606140672586593",
		"8297270320597030697",
		"<nil>"))
}

func TestIssue29947(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec(`drop table if exists t_tir89b, t_vejdy`)

	tk.MustExec("CREATE TABLE `t_tir89b` (`c_3pcik` int(11) DEFAULT NULL,`c_0b6nxb` text DEFAULT NULL,`c_qytrlc` double NOT NULL,`c_sroc_c` int(11) DEFAULT NULL,PRIMARY KEY (`c_qytrlc`) /*T![clustered_index] NONCLUSTERED */	) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;")

	tk.MustExec(`INSERT INTO t_tir89b VALUES (66,'cjd1o',87.77,NULL),(134217728,'d_unpd',76.66,NULL),(50,'_13gs',1.46,32),(49,'xclvsc',64.7,48),(7,'1an13',70.86,7),(29,NULL,6.26,6),(8,'hc485b',47.44,2),(84,'d_nlmd',99.3,76),(14,'lbny1c',61.1,47),(45,'9r5bid',25.37,95),(49,'jbz5r',72.99,49),(18,'uode3d',7.21,992),(-8945040,'ftrtib',47.47,20),(29,'algrj',6.28,24),(96,NULL,67.83,24),(5,'s1gfz',89.18,78),(74,'ggqbl',83.89,68),(61,'5n1q7',26.92,6),(10,'4gflb',33.84,28),(48,'xoe0cd',84.71,77),(6,'xkh6i',53.83,19),(5,NULL,89.1,46),(49,'4q6nx',31.5,384),(1,'pgs1',66.8,77),(19,'lltflc',33.49,63),(87,'vd4htc',39.92,-5367008),(47,NULL,28.3,10),(29,'15jqfc',100.11,64),(45,'ii6pm',52.41,61),(0,NULL,85.27,19),(104,'ikpxnb',40.66,955),(40,'gzryzd',36.23,42),(18,'7UPNE',84.27,14),(32,NULL,84.8,53),(51,'2c5lfb',18.98,74),(97,NULL,22.89,6),(70,'guyzyc',96.29,89),(34,'dvdoqb',53.82,1),(94,'6eop6b',81.77,90),(42,'p7vsnd',62.54,NULL);`)

	tk.MustExec("CREATE TABLE `t_vejdy` (`c_iovir` int(11) NOT NULL,`c_r_mw3d` double DEFAULT NULL,`c_uxhghb` int(11) DEFAULT NULL,`c_rb7otb` int(11) NOT NULL,`c_dplyac` int(11) DEFAULT NULL,`c_lmcqed` double DEFAULT NULL,`c_ayaoed` text DEFAULT NULL,`c__zbqr` int(11) DEFAULT NULL,PRIMARY KEY (`c_iovir`,`c_rb7otb`) /*T![clustered_index] NONCLUSTERED */,KEY `t_e1ejcd` (`c_uxhghb`),KEY `t_o6ui_b` (`c_iovir`,`c_r_mw3d`,`c_uxhghb`,`c_rb7otb`,`c_dplyac`,`c_lmcqed`)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;")

	tk.MustExec(`INSERT INTO t_vejdy VALUES (49,100.11,68,57,44,17.93,NULL,84),(38,56.91,78,30,0,53.28,'cjd1o',2),(6,NULL,NULL,88,81,93.47,'0jftkb',54),(73,91.51,31,82,3,38.12,'buesob',40),(7,26.73,7,78,9,NULL,'fd5kgd',49),(80,70.57,4,47,43,25.59,'glpoq',44),(79,94.16,15,0,0,79.55,'0ok94d',56),(58,NULL,50,69,2,65.46,'sm6rj',29),(41472,6.51,70,1080,100,43.18,'fofk4c',43),(0,6.2,57,97,2,56.17,'zqpzq',56),(72,76.66,97,88,95,75.47,'hikxqb',34),(27,1.11,134217728,57,25,NULL,'4gflb',0),(64,NULL,47,69,6,72.5,'w7jmhd',45),(-134217679,88.74,33,82,85,59.89,NULL,26),(59,97.98,37,28,33,61.1,'xioxdd',45),(6,47.31,0,0,-19,38.77,'uxmdlc',17),(82,28.62,36,70,39,11.79,'zzi8cc',2),(33,37.3,55,86,69,60.56,'mn_xx',0),(7,NULL,80,0,17,59.79,'5n1q7',97),(88,50.81,15,30,63,25.37,'ordwed',29),(48,4.32,90,48,38,84.62,'lclx',32),(10,NULL,95,75,1,21.64,NULL,85),(62,NULL,0,30,10,NULL,'7bacud',5),(50,38.81,6,0,6,64.28,'gpibn',57),(1,46.8,21,32,46,33.38,NULL,6),(29,NULL,38,7,91,31.5,'pdzdl',24),(54,6.26,1,85,22,75.63,'gl4_7',29),(1,90.37,63,63,6,61.2,'wvw23b',86),(47,NULL,82,73,0,95.79,'uipcf',NULL),(46,48.1,37,6,1,52.33,'gthpic',0),(41,75.1,7,44,5,84.16,'fe_e5',58),(43,87.71,81,32,28,91.98,'9e5nvc',66),(20,58.21,88,75,92,43.64,'kagroc',66),(91,52.75,22,14,80,NULL,'\'_YN6MD\'',6),(72,94.83,0,49,5,57.82,NULL,23),(7,100.11,0,92,13,6.28,NULL,0);`)

	tk.MustExec("begin")
	tk.MustExec("delete from t_tir89b where t_tir89b.c_3pcik >= t_tir89b.c_sroc_c;")
	result := tk.MustQuery("select * from (select count(*) over (partition by ref_0.c_0b6nxb order by ref_0.c_3pcik) as c0 from t_tir89b as ref_0) as subq_0 where subq_0.c0 <> 1;")
	result.Check(testkit.Rows("2", "3"))
	tk.MustExec("commit")
}
