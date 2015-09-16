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

package rsets

import (
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/expression/expressions"
	"github.com/pingcap/tidb/field"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/parser/opcode"
	"github.com/pingcap/tidb/plan/plans"
)

var _ = Suite(&testGroupByRsetSuite{})

type testGroupByRsetSuite struct {
	r *GroupByRset
}

func (s *testGroupByRsetSuite) SetUpSuite(c *C) {
	names := []string{"id", "name"}
	tblPlan := newTestTablePlan(testData, names)
	resultFields := tblPlan.GetFields()

	fields := make([]*field.Field, len(resultFields))
	for i, resultField := range resultFields {
		name := resultField.Name
		fields[i] = &field.Field{Expr: &expressions.Ident{CIStr: model.NewCIStr(name)}, Name: name}
	}

	selectList := &plans.SelectList{
		HiddenFieldOffset: len(resultFields),
		ResultFields:      resultFields,
		Fields:            fields,
	}

	by := []expression.Expression{&expressions.Ident{CIStr: model.NewCIStr("name")}}

	s.r = &GroupByRset{Src: tblPlan, SelectList: selectList, By: by, OuterQuery: &plans.OuterQuery{}}
}

func (s *testGroupByRsetSuite) TestGroupByRsetPlan(c *C) {
	// `select id, name from t group by name`
	p, err := s.r.Plan(nil)
	c.Assert(err, IsNil)

	_, ok := p.(*plans.GroupByDefaultPlan)
	c.Assert(ok, IsTrue)

	// `select id, name from t group by abc`
	s.r.By[0] = expressions.Value{Val: "abc"}

	_, err = s.r.Plan(nil)
	c.Assert(err, IsNil)

	// `select id, name from t group by 1`
	s.r.By[0] = expressions.Value{Val: int64(1)}

	_, err = s.r.Plan(nil)
	c.Assert(err, IsNil)

	s.r.By[0] = expressions.Value{Val: uint64(1)}

	_, err = s.r.Plan(nil)
	c.Assert(err, IsNil)

	// `select id, name from t group by 0`
	s.r.By[0] = expressions.Value{Val: int64(0)}

	_, err = s.r.Plan(nil)
	c.Assert(err, NotNil)

	fldExpr, err := expressions.NewCall("count", []expression.Expression{expressions.Value{Val: 1}}, false)
	c.Assert(err, IsNil)

	// `select count(1) as a, name from t group by 1`
	fld := &field.Field{Expr: fldExpr, Name: "a"}
	s.r.SelectList.Fields[0] = fld

	s.r.By[0] = expressions.Value{Val: int64(1)}

	_, err = s.r.Plan(nil)
	c.Assert(err, NotNil)

	// check ambiguous field, like `select id as name, name from t group by name`
	s.r.By[0] = &expressions.Ident{CIStr: model.NewCIStr("name")}

	fldx := &field.Field{Expr: &expressions.Ident{CIStr: model.NewCIStr("id")}, Name: "name"}
	fldy := &field.Field{Expr: &expressions.Ident{CIStr: model.NewCIStr("name")}, Name: "name"}
	s.r.SelectList.Fields = []*field.Field{fldx, fldy}

	s.r.SelectList.ResultFields[0].Name = "name"
	s.r.SelectList.ResultFields[1].Name = "name"

	p, err = s.r.Plan(nil)
	c.Assert(err, NotNil)

	// check aggregate function reference, like `select count(1) as a, name from t group by a + 1`
	expr := expressions.NewBinaryOperation(opcode.Plus, &expressions.Ident{CIStr: model.NewCIStr("a")}, expressions.Value{Val: 1})

	s.r.By[0] = expr

	s.r.SelectList.Fields[0] = fld
	s.r.SelectList.ResultFields[0].Col.Name = model.NewCIStr("count(id)")
	s.r.SelectList.ResultFields[0].Name = "a"

	_, err = s.r.Plan(nil)
	c.Assert(err, NotNil)

	// check contain aggregate function, like `select count(1) as a, name from t group by count(1)`
	s.r.By[0] = fldExpr

	_, err = s.r.Plan(nil)
	c.Assert(err, NotNil)
}

func (s *testGroupByRsetSuite) TestGroupByHasAmbiguousField(c *C) {
	fld := &field.Field{Expr: expressions.Value{Val: 1}}

	// check `1`
	fields := []*field.Field{fld}
	indices := []int{0}

	ret := s.r.HasAmbiguousField(indices, fields)
	c.Assert(ret, IsFalse)

	// check `c1 as c2, c1 as c2`
	fld = &field.Field{Expr: &expressions.Ident{CIStr: model.NewCIStr("c1")}, Name: "c2"}
	fields = []*field.Field{fld, fld}
	indices = []int{0, 1}

	ret = s.r.HasAmbiguousField(indices, fields)
	c.Assert(ret, IsFalse)

	// check `c1+c2 as c2, c1+c3 as c2`
	exprx := expressions.NewBinaryOperation(opcode.Plus, expressions.Value{Val: "c1"},
		expressions.Value{Val: "c2"})
	expry := expressions.NewBinaryOperation(opcode.Plus, expressions.Value{Val: "c1"},
		expressions.Value{Val: "c3"})

	fldx := &field.Field{Expr: exprx, Name: "c2"}
	fldy := &field.Field{Expr: expry, Name: "c2"}
	fields = []*field.Field{fldx, fldy}
	indices = []int{0, 1}

	ret = s.r.HasAmbiguousField(indices, fields)
	c.Assert(ret, IsFalse)

	// check `c1 as c2, c3 as c2`
	fldx = &field.Field{Expr: &expressions.Ident{CIStr: model.NewCIStr("c1")}, Name: "c2"}
	fldy = &field.Field{Expr: &expressions.Ident{CIStr: model.NewCIStr("c3")}, Name: "c2"}
	fields = []*field.Field{fldx, fldy}
	indices = []int{0, 1}

	ret = s.r.HasAmbiguousField(indices, fields)
	c.Assert(ret, IsTrue)
}

func (s *testGroupByRsetSuite) TestGroupByRsetString(c *C) {
	str := s.r.String()
	c.Assert(len(str), Greater, 0)
}
