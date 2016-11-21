// Copyright 2015 PingCAP, Inc.
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

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/executor"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/util/testkit"
	"github.com/pingcap/tidb/util/testleak"
	"github.com/pingcap/tidb/util/types"
)

type MockExec struct {
	fields    []*ast.ResultField
	Rows      []*executor.Row
	curRowIdx int
}

func (m *MockExec) Schema() expression.Schema {
	return nil
}

func (m *MockExec) Fields() []*ast.ResultField {
	return m.fields
}

func (m *MockExec) Next() (*executor.Row, error) {
	if m.curRowIdx >= len(m.Rows) {
		return nil, nil
	}
	r := m.Rows[m.curRowIdx]
	m.curRowIdx++
	if len(m.fields) > 0 {
		for i, d := range r.Data {
			m.fields[i].Expr.SetValue(d.GetValue())
		}
	}
	return r, nil
}

func (m *MockExec) Close() error {
	m.curRowIdx = 0
	return nil
}

func (s *testSuite) TestAggregation(c *C) {
	defer func() {
		s.cleanEnv(c)
		testleak.AfterTest(c)()
	}()
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (c int, d int)")
	tk.MustExec("insert t values (NULL, 1)")
	tk.MustExec("insert t values (1, 1)")
	tk.MustExec("insert t values (1, 2)")
	tk.MustExec("insert t values (1, 3)")
	tk.MustExec("insert t values (1, 1)")
	tk.MustExec("insert t values (3, 2)")
	tk.MustExec("insert t values (4, 3)")
	result := tk.MustQuery("select count(*) from t group by d")
	result.Check(testkit.Rows("3", "2", "2"))
	result = tk.MustQuery("select count(*) from t having 1 = 0")
	result.Check(testkit.Rows())
	result = tk.MustQuery("select - c, c as d from t group by c having null not between c and avg(distinct d) - d")
	result.Check(testkit.Rows())
	result = tk.MustQuery("select - c as c from t group by c having t.c > 5")
	result.Check(testkit.Rows())
	// TODO: This query is reported error in resolver.
	//result := tk.MustQuery("select t1.c from t t1, t t2 group by c having c > 5")
	//result.Check(testkit.Rows())
	result = tk.MustQuery("select count(*) from (select d, c from t) k where d != 0 group by d")
	result.Check(testkit.Rows("3", "2", "2"))
	result = tk.MustQuery("select c as a from t group by d having a < 0")
	result.Check(testkit.Rows())
	result = tk.MustQuery("select c as a from t group by d having sum(a) = 2")
	result.Check(testkit.Rows("1"))
	result = tk.MustQuery("select count(distinct c) from t group by d")
	result.Check(testkit.Rows("1", "2", "2"))
	result = tk.MustQuery("select sum(c) from t group by d")
	result.Check(testkit.Rows("2", "4", "5"))
	result = tk.MustQuery("select sum(c), sum(c+1), sum(c), sum(c+1) from t group by d")
	result.Check(testkit.Rows("2 4 2 4", "4 6 4 6", "5 7 5 7"))
	result = tk.MustQuery("select count(distinct c,d), count(c,d) from t")
	result.Check(testkit.Rows("5 6"))
	result = tk.MustQuery("select d*2 as ee, sum(c) from t group by ee")
	result.Check(testkit.Rows("2 2", "4 4", "6 5"))
	result = tk.MustQuery("select sum(distinct c) from t group by d")
	result.Check(testkit.Rows("1", "4", "5"))
	result = tk.MustQuery("select min(c) from t group by d")
	result.Check(testkit.Rows("1", "1", "1"))
	result = tk.MustQuery("select max(c) from t group by d")
	result.Check(testkit.Rows("1", "3", "4"))
	result = tk.MustQuery("select avg(c) from t group by d")
	result.Check(testkit.Rows("1.0000", "2.0000", "2.5000"))
	result = tk.MustQuery("select d, d + 1 from t group by d")
	result.Check(testkit.Rows("1 2", "2 3", "3 4"))
	result = tk.MustQuery("select count(*) from t")
	result.Check(testkit.Rows("7"))
	result = tk.MustQuery("select count(distinct d) from t")
	result.Check(testkit.Rows("3"))
	result = tk.MustQuery("select count(*) from t group by d having sum(c) > 3")
	result.Check(testkit.Rows("2", "2"))
	result = tk.MustQuery("select max(c) from t group by d having sum(c) > 3 order by avg(c) desc")
	result.Check(testkit.Rows("4", "3"))
	result = tk.MustQuery("select sum(-1) from t a left outer join t b on not null is null")
	result.Check(testkit.Rows("-7"))
	result = tk.MustQuery("select count(*), b.d from t a left join t b on a.c = b.d group by b.d order by b.d")
	result.Check(testkit.Rows("2 <nil>", "12 1", "2 3"))
	result = tk.MustQuery("select count(b.d), b.d from t a left join t b on a.c = b.d group by b.d order by b.d")
	result.Check(testkit.Rows("0 <nil>", "12 1", "2 3"))
	result = tk.MustQuery("select count(b.d), b.d from t b right join t a on a.c = b.d group by b.d order by b.d")
	result.Check(testkit.Rows("0 <nil>", "12 1", "2 3"))
	result = tk.MustQuery("select count(*), b.d from t b right join t a on a.c = b.d group by b.d order by b.d")
	result.Check(testkit.Rows("2 <nil>", "12 1", "2 3"))
	result = tk.MustQuery("select max(case when b.d is null then 10 else b.c end), b.d from t b right join t a on a.c = b.d group by b.d order by b.d")
	result.Check(testkit.Rows("10 <nil>", "1 1", "4 3"))
	result = tk.MustQuery("select count(*) from t a , t b")
	result.Check(testkit.Rows("49"))
	result = tk.MustQuery("select count(*) from t a , t b, t c")
	result.Check(testkit.Rows("343"))
	result = tk.MustQuery("select count(*) from t a , t b where a.c = b.d")
	result.Check(testkit.Rows("14"))
	result = tk.MustQuery("select count(a.d), sum(b.c) from t a , t b where a.c = b.d")
	result.Check(testkit.Rows("14 13"))
	result = tk.MustQuery("select count(*) from t a , t b, t c where a.c = b.d and b.d = c.d")
	result.Check(testkit.Rows("40"))
	result = tk.MustQuery("select count(*), a.c from t a , t b, t c where a.c = b.d and b.d = c.d group by c.d order by a.c")
	result.Check(testkit.Rows("36 1", "4 3"))
	result = tk.MustQuery("select count(a.c), c.d from t a , t b, t c where a.c = b.d and b.d = c.d group by c.d order by c.d")
	result.Check(testkit.Rows("36 1", "4 3"))
	result = tk.MustQuery("select count(*) from t a , t b where a.c = b.d and a.c + b.d = 2")
	result.Check(testkit.Rows("12"))
	result = tk.MustQuery("select count(*) from t a join t b having sum(a.c) < 0")
	result.Check(testkit.Rows())
	result = tk.MustQuery("select count(*) from t a join t b where a.c < 0")
	result.Check(testkit.Rows("0"))
	result = tk.MustQuery("select sum(b.c), count(b.d), a.c from t a left join t b on a.c = b.d group by b.d order by b.d")
	result.Check(testkit.Rows("<nil> 0 4", "8 12 1", "5 2 3"))
	// This two cases prove that having always resolve name from field list firstly.
	result = tk.MustQuery("select 1-d as d from t having d < 0 order by d desc")
	result.Check(testkit.Rows("-1", "-1", "-2", "-2"))
	result = tk.MustQuery("select 1-d as d from t having d + 1 < 0 order by d + 1")
	result.Check(testkit.Rows("-2", "-2"))
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (c int, d int)")
	tk.MustExec("insert t values (1, -1)")
	tk.MustExec("insert t values (1, 0)")
	tk.MustExec("insert t values (1, 1)")
	result = tk.MustQuery("select d, d*d as d from t having d = -1")
	result.Check(testkit.Rows())
	result = tk.MustQuery("select d*d as d from t group by d having d = -1")
	result.Check(testkit.Rows("1"))
	result = tk.MustQuery("select d, 1-d as d, c as d from t order by d")
	result.Check(testkit.Rows("1 0 1", "0 1 1", "-1 2 1"))
	result = tk.MustQuery("select d, 1-d as d, c as d from t order by d+1")
	result.Check(testkit.Rows("-1 2 1", "0 1 1", "1 0 1"))
	result = tk.MustQuery("select d, 1-d as d, c as d from t group by d")
	result.Check(testkit.Rows("-1 2 1", "0 1 1", "1 0 1"))
	result = tk.MustQuery("select d as d1, t.d as d1, 1-d as d1, c as d1 from t having d1 < 10")
	result.Check(testkit.Rows("-1 -1 2 1", "0 0 1 1", "1 1 0 1"))
	result = tk.MustQuery("select d*d as d1, c as d1 from t group by d1")
	result.Check(testkit.Rows("1 1", "0 1"))
	result = tk.MustQuery("select d*d as d1, c as d1 from t group by 2")
	result.Check(testkit.Rows("1 1"))
	result = tk.MustQuery("select * from t group by 2")
	result.Check(testkit.Rows("1 -1", "1 0", "1 1"))
	result = tk.MustQuery("select * , sum(d) from t group by 1")
	result.Check(testkit.Rows("1 -1 0"))
	result = tk.MustQuery("select sum(d), t.* from t group by 2")
	result.Check(testkit.Rows("0 1 -1"))
	result = tk.MustQuery("select d as d, c as d from t group by d + 1")
	result.Check(testkit.Rows("-1 1", "0 1", "1 1"))
	result = tk.MustQuery("select c as d, c as d from t group by d")
	result.Check(testkit.Rows("1 1", "1 1", "1 1"))
	_, err := tk.Exec("select d as d, c as d from t group by d")
	c.Assert(err, NotNil)
	_, err = tk.Exec("select t.d, c as d from t group by d")
	c.Assert(err, NotNil)
	result = tk.MustQuery("select *, c+1 as d from t group by 3")
	result.Check(testkit.Rows("1 -1 2"))
	tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t1(a float, b int default 3)")
	tk.MustExec("insert into t1 (a) values (2), (11), (8)")
	result = tk.MustQuery("select min(a), min(case when 1=1 then a else NULL end), min(case when 1!=1 then NULL else a end) from t1 where b=3 group by b")
	result.Check(testkit.Rows("2 2 2"))
	// The following cases use streamed aggregation.
	tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t1(a int, index(a))")
	tk.MustExec("insert into t1 (a) values (1),(2),(3),(4),(5)")
	result = tk.MustQuery("select count(a) from t1 where a < 3")
	result.Check(testkit.Rows("2"))
	tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t1(a int, b int, index(a))")
	result = tk.MustQuery("select sum(b) from (select * from t1) t group by a")
	result.Check(testkit.Rows())
	result = tk.MustQuery("select sum(b) from (select * from t1) t")
	result.Check(testkit.Rows("<nil>"))
	tk.MustExec("insert into t1 (a, b) values (1, 1),(2, 2),(3, 3),(1, 4),(3, 5)")
	result = tk.MustQuery("select avg(b) from (select * from t1) t group by a")
	result.Check(testkit.Rows("2.5000", "2.0000", "4.0000"))
	result = tk.MustQuery("select sum(b) from (select * from t1) t group by a")
	result.Check(testkit.Rows("5", "2", "8"))
	result = tk.MustQuery("select count(b) from (select * from t1) t group by a")
	result.Check(testkit.Rows("2", "1", "2"))
	result = tk.MustQuery("select max(b) from (select * from t1) t group by a")
	result.Check(testkit.Rows("4", "2", "5"))
	result = tk.MustQuery("select min(b) from (select * from t1) t group by a")
	result.Check(testkit.Rows("1", "2", "3"))
	tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t1(a int, b int, index(a,b))")
	tk.MustExec("insert into t1 (a, b) values (1, 1),(2, 2),(3, 3),(1, 4), (1,1),(3, 5), (2,2), (3,5), (3,3)")
	result = tk.MustQuery("select avg(distinct b) from (select * from t1) t group by a")
	result.Check(testkit.Rows("2.5000", "2.0000", "4.0000"))
	result = tk.MustQuery("select sum(distinct b) from (select * from t1) t group by a")
	result.Check(testkit.Rows("5", "2", "8"))
	result = tk.MustQuery("select count(distinct b) from (select * from t1) t group by a")
	result.Check(testkit.Rows("2", "1", "2"))
	result = tk.MustQuery("select max(distinct b) from (select * from t1) t group by a")
	result.Check(testkit.Rows("4", "2", "5"))
	result = tk.MustQuery("select min(distinct b) from (select * from t1) t group by a")
	result.Check(testkit.Rows("1", "2", "3"))
	tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t1(a int, b int, index(b, a))")
	tk.MustExec("insert into t1 (a, b) values (1, 1),(2, 2),(3, 3),(1, 4), (1,1),(3, 5), (2,2), (3,5), (3,3)")
	result = tk.MustQuery("select avg(distinct b) from (select * from t1) t group by a")
	result.Check(testkit.Rows("2.5000", "2.0000", "4.0000"))
	result = tk.MustQuery("select sum(distinct b) from (select * from t1) t group by a")
	result.Check(testkit.Rows("5", "2", "8"))
	result = tk.MustQuery("select count(distinct b) from (select * from t1) t group by a")
	result.Check(testkit.Rows("2", "1", "2"))
	result = tk.MustQuery("select max(distinct b) from (select * from t1) t group by a")
	result.Check(testkit.Rows("4", "2", "5"))
	result = tk.MustQuery("select min(distinct b) from (select * from t1) t group by a")
	result.Check(testkit.Rows("1", "2", "3"))
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (id int primary key, ds date)")
	tk.MustExec("insert into t (id, ds) values (1, \"1991-09-05\"),(2,\"1991-09-05\"), (3, \"1991-09-06\"),(0,\"1991-09-06\")")
	result = tk.MustQuery("select sum(id), ds from t group by ds order by id")
	result.Check(testkit.Rows("3 1991-09-06", "3 1991-09-05"))
	tk.MustExec("drop table if exists t1")
	tk.MustExec("drop table if exists t2")
	tk.MustExec("create table t1 (col0 int, col1 int)")
	tk.MustExec("create table t2 (col0 int, col1 int)")
	tk.MustExec("insert into t1 values(83, 0), (26, 0), (43, 81)")
	tk.MustExec("insert into t2 values(22, 2), (3, 12), (38, 98)")
	result = tk.MustQuery("SELECT COALESCE ( + 1, cor0.col0 ) + - CAST( NULL AS DECIMAL ) FROM t2, t1 AS cor0, t2 AS cor1 GROUP BY cor0.col1")
	result.Check(testkit.Rows("<nil>", "<nil>"))
}

func (s *testSuite) TestStreamAgg(c *C) {
	col := &expression.Column{
		Index: 1,
	}
	gbyCol := &expression.Column{
		Index: 0,
	}
	sumAgg := expression.NewAggFunction(ast.AggFuncSum, []expression.Expression{col}, false)
	cntAgg := expression.NewAggFunction(ast.AggFuncCount, []expression.Expression{col}, false)
	avgAgg := expression.NewAggFunction(ast.AggFuncAvg, []expression.Expression{col}, false)
	maxAgg := expression.NewAggFunction(ast.AggFuncMax, []expression.Expression{col}, false)
	cases := []struct {
		aggFunc expression.AggregationFunction
		result  string
		input   [][]interface{}
		result1 []string
	}{
		{
			sumAgg,
			"<nil>",
			[][]interface{}{
				{0, 1}, {0, nil}, {1, 2}, {1, 3},
			},
			[]string{
				"1", "5",
			},
		},
		{
			cntAgg,
			"0",
			[][]interface{}{
				{0, 1}, {0, nil}, {1, 2}, {1, 3},
			},
			[]string{
				"1", "2",
			},
		},
		{
			avgAgg,
			"<nil>",
			[][]interface{}{
				{0, 1}, {0, nil}, {1, 2}, {1, 3},
			},
			[]string{
				"1.0000", "2.5000",
			},
		},
		{
			maxAgg,
			"<nil>",
			[][]interface{}{
				{0, 1}, {0, nil}, {1, 2}, {1, 3},
			},
			[]string{
				"1", "3",
			},
		},
	}
	for _, ca := range cases {
		mock := &MockExec{}
		e := &executor.StreamAggExec{
			AggFuncs: []expression.AggregationFunction{ca.aggFunc},
			Src:      mock,
		}
		row, err := e.Next()
		c.Check(err, IsNil)
		c.Check(row, NotNil)
		c.Assert(fmt.Sprintf("%v", row.Data[0].GetValue()), Equals, ca.result)
		e.GroupByItems = append(e.GroupByItems, gbyCol)
		e.Close()
		row, err = e.Next()
		c.Check(err, IsNil)
		c.Check(row, IsNil)
		e.Close()
		for _, input := range ca.input {
			data := types.MakeDatums(input...)
			mock.Rows = append(mock.Rows, &executor.Row{Data: data})
		}
		for _, res := range ca.result1 {
			row, err = e.Next()
			c.Check(err, IsNil)
			c.Check(row, NotNil)
			c.Assert(fmt.Sprintf("%v", row.Data[0].GetValue()), Equals, res)
		}
	}
}
