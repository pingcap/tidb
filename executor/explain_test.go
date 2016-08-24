// Copyright 2016 PingCAP, Inc.
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
	"github.com/pingcap/tidb/executor"
	"github.com/pingcap/tidb/util/testkit"
	"github.com/pingcap/tidb/util/testleak"
)

func (s *testSuite) TestExplain(c *C) {
	executor.ExplainTestMode = true
	defer func() {
		testleak.AfterTest(c)()
		executor.ExplainTestMode = false
	}()
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1, t2")
	tk.MustExec("create table t1 (c1 int primary key, c2 int, index c2 (c2))")
	tk.MustExec("create table t2 (c1 int unique, c2 int)")

	cases := []struct {
		sql    string
		result string
	}{
		{
			"select * from t1",
			`{"type":"Projection","child":{"type":"TableScan","db":"test","table":"t1","desc":false,"keep order":false,"access condition":null,"limit":0},"exprs":["test.t1.c1","test.t1.c2"]}`,
		},
		{
			"select * from t1 order by c2",
			`{"type":"Projection","child":{"type":"IndexScan","db":"test","table":"t1","index":"c2","ranges":"[[\u003cnil\u003e,+inf]]","desc":false,"out of order":false,"double read":false,"access condition":null,"limit":0},"exprs":["test.t1.c1","test.t1.c2"]}`,
		},
		{
			"select * from t2 order by c2",
			`{"type":"Sort","child":{"type":"Projection","child":{"type":"TableScan","db":"test","table":"t2","desc":false,"keep order":false,"access condition":null,"limit":0},"exprs":["test.t2.c1","test.t2.c2"]},"exprs":[{"Expr":"t2.c2","Desc":false}]}`,
		},
		{
			"select * from t1 where t1.c1 > 0",
			`{"type":"Projection","child":{"type":"TableScan","db":"test","table":"t1","desc":false,"keep order":false,"access condition":["\u003e(test.t1.c1,0,)"],"limit":0},"exprs":["test.t1.c1","test.t1.c2"]}`,
		},
		{
			"select * from t1 where t1.c2 = 1",
			`{"type":"Projection","child":{"type":"IndexScan","db":"test","table":"t1","index":"c2","ranges":"[[1,1]]","desc":false,"out of order":true,"double read":false,"access condition":["=(test.t1.c2,1,)"],"limit":0},"exprs":["test.t1.c1","test.t1.c2"]}`,
		},
		{
			"select * from t1 left join t2 on t1.c2 = t2.c1 where t1.c1 > 1",
			`{"type":"Projection","child":{"type":"LeftJoin","leftPlan":{"type":"TableScan","db":"test","table":"t1","desc":false,"keep order":false,"access condition":["\u003e(test.t1.c1,1,)"],"limit":0},"rightPlan":{"type":"TableScan","db":"test","table":"t2","desc":false,"keep order":false,"access condition":null,"limit":0},"eqCond":["=(test.t1.c2,test.t2.c1,)"],"leftCond":null,"rightCond":null,"otherCond":null},"exprs":["test.t1.c1","test.t1.c2","test.t2.c1","test.t2.c2"]}`,
		},
		{
			"update t1 set t1.c2 = 2 where t1.c1 = 1",
			`{"id":"Update_3","children":[{"type":"TableScan","db":"test","table":"t1","desc":false,"keep order":false,"access condition":["=(test.t1.c1,1,)"],"limit":0}]}`,
		},
		{
			"delete from t1 where t1.c2 = 1",
			`{"id":"Delete_3","children":[{"type":"IndexScan","db":"test","table":"t1","index":"c2","ranges":"[[1,1]]","desc":false,"out of order":true,"double read":false,"access condition":["=(test.t1.c2,1,)"],"limit":0}]}`,
		},
	}
	for _, ca := range cases {
		result := tk.MustQuery("explain " + ca.sql)
		result.Check(testkit.Rows("EXPLAIN " + ca.result))
	}
}
