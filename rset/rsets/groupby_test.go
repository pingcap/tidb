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
		fields[i] = &field.Field{Expr: &expression.Ident{CIStr: model.NewCIStr(name)}}
	}

	selectList := &plans.SelectList{
		HiddenFieldOffset: len(resultFields),
		ResultFields:      resultFields,
		Fields:            fields,
	}

	by := []expression.Expression{&expression.Ident{CIStr: model.NewCIStr("name")}}

	s.r = &GroupByRset{Src: tblPlan, SelectList: selectList, By: by}
}

func resetAggFields(selectList *plans.SelectList) {
	fields := selectList.Fields
	selectList.AggFields = GetAggFields(fields)
}

func (s *testGroupByRsetSuite) TestGroupByRsetPlan(c *C) {
	// `select id, name from t group by name`
	p, err := s.r.Plan(nil)
	c.Assert(err, IsNil)

	_, ok := p.(*plans.GroupByDefaultPlan)
	c.Assert(ok, IsTrue)

	// `select id, name from t group by abc`
	s.r.By[0] = expression.Value{Val: "abc"}

	_, err = s.r.Plan(nil)
	c.Assert(err, IsNil)

	// `select id, name from t group by 1`
	s.r.By[0] = expression.Value{Val: int64(1)}

	_, err = s.r.Plan(nil)
	c.Assert(err, IsNil)

	s.r.By[0] = expression.Value{Val: uint64(1)}

	_, err = s.r.Plan(nil)
	c.Assert(err, IsNil)

	// `select id, name from t group by 0`
	s.r.By[0] = expression.Value{Val: int64(0)}

	_, err = s.r.Plan(nil)
	c.Assert(err, NotNil)

	fldExpr, err := expression.NewCall("count", []expression.Expression{expression.Value{Val: 1}}, false)
	c.Assert(err, IsNil)

	// `select count(1) as a, name from t group by 1`
	fld := &field.Field{Expr: fldExpr, AsName: "a"}
	s.r.SelectList.Fields[0] = fld

	resetAggFields(s.r.SelectList)

	s.r.By[0] = expression.Value{Val: int64(1)}

	_, err = s.r.Plan(nil)
	c.Assert(err, NotNil)

	// check ambiguous field, like `select id as name, name from t group by name`
	s.r.By[0] = &expression.Ident{CIStr: model.NewCIStr("name")}

	fldx := &field.Field{Expr: &expression.Ident{CIStr: model.NewCIStr("id")}, AsName: "name"}
	fldy := &field.Field{Expr: &expression.Ident{CIStr: model.NewCIStr("name")}, AsName: "name"}
	s.r.SelectList.Fields = []*field.Field{fldx, fldy}

	s.r.SelectList.ResultFields[0].Name = "name"
	s.r.SelectList.ResultFields[1].Name = "name"

	p, err = s.r.Plan(nil)
	c.Assert(err, NotNil)

	// check aggregate function reference, like `select count(1) as a, name from t group by a + 1`
	expr := expression.NewBinaryOperation(opcode.Plus, &expression.Ident{CIStr: model.NewCIStr("a")}, expression.Value{Val: 1})

	s.r.By[0] = expr

	s.r.SelectList.Fields[0] = fld
	s.r.SelectList.ResultFields[0].Col.Name = model.NewCIStr("count(id)")
	s.r.SelectList.ResultFields[0].Name = "a"

	resetAggFields(s.r.SelectList)

	_, err = s.r.Plan(nil)
	c.Assert(err, NotNil)

	// check contain aggregate function, like `select count(1) as a, name from t group by count(1)`
	s.r.By[0] = fldExpr

	_, err = s.r.Plan(nil)
	c.Assert(err, NotNil)
}

func (s *testGroupByRsetSuite) TestGroupByRsetString(c *C) {
	str := s.r.String()
	c.Assert(len(str), Greater, 0)
}
