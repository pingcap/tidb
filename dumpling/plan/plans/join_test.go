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

package plans_test

import (
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb"
	"github.com/pingcap/tidb/column"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/field"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/parser/opcode"
	"github.com/pingcap/tidb/plan/plans"
	"github.com/pingcap/tidb/rset/rsets"
	"github.com/pingcap/tidb/util/mock"
	"github.com/pingcap/tidb/util/testkit"
	"github.com/pingcap/tidb/util/types"
)

type testJoinSuit struct {
	cols []*column.Col
}

var _ = Suite(&testJoinSuit{})

func (s *testJoinSuit) TestJoin(c *C) {
	s.cols = []*column.Col{
		{
			ColumnInfo: model.ColumnInfo{
				ID:           0,
				Name:         model.NewCIStr("id"),
				Offset:       0,
				DefaultValue: 0,
				FieldType:    *types.NewFieldType(mysql.TypeLonglong),
			},
		},
		{
			ColumnInfo: model.ColumnInfo{
				ID:           1,
				Name:         model.NewCIStr("name"),
				Offset:       1,
				DefaultValue: nil,
				FieldType:    *types.NewFieldType(mysql.TypeVarchar),
			},
		},
	}

	var testData1 = []*testRowData{
		{1, []interface{}{10, "10"}},
		{2, []interface{}{10, "20"}},
		{3, []interface{}{10, "30"}},
		{4, []interface{}{40, "40"}},
		{6, []interface{}{60, "60"}},
	}

	var testData2 = []*testRowData{
		{1, []interface{}{10, "10"}},
		{2, []interface{}{10, "20"}},
		{3, []interface{}{10, "30"}},
		{4, []interface{}{40, "40"}},
		{6, []interface{}{60, "60"}},
	}

	tblPlan1 := &testTablePlan{testData1, []string{"id", "name"}, 0}
	tblPlan2 := &testTablePlan{testData2, []string{"id", "name"}, 0}

	joinPlan := &plans.JoinPlan{
		Left:   tblPlan1,
		Right:  tblPlan2,
		Type:   "CROSS",
		Fields: []*field.ResultField{},
		On:     expression.Value{Val: true},
	}
	rset := rsets.Recordset{
		Plan: joinPlan,
		Ctx:  mock.NewContext()}

	rset.Do(func(data []interface{}) (bool, error) {
		return true, nil
	})
	tblPlan1.Close()
	tblPlan2.Close()
	rset.Plan = &plans.JoinPlan{
		Left:   tblPlan1,
		Right:  tblPlan2,
		Type:   "LEFT",
		Fields: []*field.ResultField{},
		On:     expression.Value{Val: true},
	}
	rset.Do(func(data []interface{}) (bool, error) {
		return true, nil
	})
	tblPlan1.Close()
	tblPlan2.Close()
	joinPlan = &plans.JoinPlan{
		Left:   tblPlan1,
		Right:  tblPlan2,
		Type:   "RIGHT",
		Fields: []*field.ResultField{},
		On:     expression.Value{Val: true},
	}

	expr := &expression.BinaryOperation{
		Op: opcode.LT,
		L: &expression.Ident{
			CIStr: model.NewCIStr("id"),
		},
		R: expression.Value{
			Val: 100,
		},
	}
	np, _, err := joinPlan.Filter(nil, expr)
	c.Assert(np, NotNil)
	c.Assert(err, IsNil)
	rset.Plan = joinPlan
	rset.Do(func(data []interface{}) (bool, error) {
		return true, nil
	})
	tblPlan1.Close()
	tblPlan2.Close()
	rset.Plan = &plans.JoinPlan{
		Left:   tblPlan1,
		Right:  tblPlan2,
		Type:   "FULL",
		Fields: []*field.ResultField{},
		On:     expression.Value{Val: true},
	}

	rset.Do(func(data []interface{}) (bool, error) {
		return true, nil
	})
}

func (s *testJoinSuit) TestJoinWithoutIndex(c *C) {
	store, err := tidb.NewStore(tidb.EngineGoLevelDBMemory)
	c.Assert(err, IsNil)
	tk := testkit.NewTestKit(c, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1;")
	tk.MustExec("drop table if exists t2;")
	tk.MustExec("drop table if exists t3;")

	tk.MustExec("create table t1 (c1 int, c2 int, index(c1), index(c2));")
	tk.MustExec("create table t2 (c1 int, c2 int, index(c1), index(c2));")
	tk.MustExec("create table t3 (c1 int, c2 int, index(c1), index(c2));")

	tk.MustExec("insert into t1 values (1,1), (2,2), (3,3);")
	tk.MustExec("insert into t2 values (1,1), (3,3), (5,5);")
	tk.MustExec("insert into t3 values (1,1), (5,5), (9,9);")

	testcases := []struct {
		sql    string
		result [][]interface{}
	}{
		{
			"select * from t1, t2 order by t1.c1, t1.c2, t2.c1, t2.c2",
			[][]interface{}{
				{1, 1, 1, 1},
				{1, 1, 3, 3},
				{1, 1, 5, 5},
				{2, 2, 1, 1},
				{2, 2, 3, 3},
				{2, 2, 5, 5},
				{3, 3, 1, 1},
				{3, 3, 3, 3},
				{3, 3, 5, 5},
			},
		},
		{
			"select * from t1 cross join t2 on t1.c1 = t2.c1 order by t1.c1, t1.c2, t2.c1, t2.c2",
			[][]interface{}{
				{1, 1, 1, 1},
				{3, 3, 3, 3},
			},
		},
		{
			"select * from t1 left join t2 on t1.c1 = t2.c1 order by t1.c1, t1.c2, t2.c1, t2.c2;",
			[][]interface{}{
				{1, 1, 1, 1},
				{2, 2, nil, nil},
				{3, 3, 3, 3},
			},
		},
		{
			"select * from t1 right join t2 on t1.c1 = t2.c1 order by t1.c1, t1.c2, t2.c1, t2.c2;",
			[][]interface{}{
				{nil, nil, 5, 5},
				{1, 1, 1, 1},
				{3, 3, 3, 3},
			},
		},
		{
			"select * from t1 left join (t2, t3) on t1.c1 = t2.c1 order by t1.c1, t1.c2, t2.c1, t2.c2, t3.c1, t3.c2;",
			[][]interface{}{
				{1, 1, 1, 1, 1, 1},
				{1, 1, 1, 1, 5, 5},
				{1, 1, 1, 1, 9, 9},
				{2, 2, nil, nil, nil, nil},
				{3, 3, 3, 3, 1, 1},
				{3, 3, 3, 3, 5, 5},
				{3, 3, 3, 3, 9, 9},
			},
		},
		{
			"select * from t1 left join t2 on t1.c1 = t2.c1 right join t3 on t2.c1 = t3.c1 order by t1.c1, t1.c2, t2.c1, t2.c2, t3.c1, t3.c2;",
			[][]interface{}{
				{nil, nil, nil, nil, 5, 5},
				{nil, nil, nil, nil, 9, 9},
				{1, 1, 1, 1, 1, 1},
			},
		},
		{
			"select * from t1 left join t2 on t1.c1 = t2.c1 right join t3 on t2.c1 = t3.c1 order by t1.c1, t1.c2, t2.c1, t2.c2, t3.c1, t3.c2;",
			[][]interface{}{
				{nil, nil, nil, nil, 5, 5},
				{nil, nil, nil, nil, 9, 9},
				{1, 1, 1, 1, 1, 1},
			},
		},
		{
			"select * from t1, t2 right join t3 on t2.c1 = t3.c1 order by t1.c1, t1.c2, t2.c1, t2.c2, t3.c1, t3.c2;",
			[][]interface{}{
				{1, 1, nil, nil, 9, 9},
				{1, 1, 1, 1, 1, 1},
				{1, 1, 5, 5, 5, 5},
				{2, 2, nil, nil, 9, 9},
				{2, 2, 1, 1, 1, 1},
				{2, 2, 5, 5, 5, 5},
				{3, 3, nil, nil, 9, 9},
				{3, 3, 1, 1, 1, 1},
				{3, 3, 5, 5, 5, 5},
			},
		},
		{
			"select * from t1, (select * from t2) as t2 order by t1.c1, t1.c2, t2.c1, t2.c2;",
			[][]interface{}{
				{1, 1, 1, 1},
				{1, 1, 3, 3},
				{1, 1, 5, 5},
				{2, 2, 1, 1},
				{2, 2, 3, 3},
				{2, 2, 5, 5},
				{3, 3, 1, 1},
				{3, 3, 3, 3},
				{3, 3, 5, 5},
			},
		},
		{
			"select * from t1 left join (select * from t2) as t2 on t1.c1 = t2.c1 order by t1.c1, t1.c2, t2.c1, t2.c2;",
			[][]interface{}{
				{1, 1, 1, 1},
				{2, 2, nil, nil},
				{3, 3, 3, 3},
			},
		},
	}
	for _, v := range testcases {
		tk.MustQuery(v.sql).Check(v.result)
	}
}
