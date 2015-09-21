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
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/parser/opcode"
	"github.com/pingcap/tidb/plan/plans"
)

var _ = Suite(&testWhereRsetSuite{})

type testWhereRsetSuite struct {
	r *WhereRset
}

func (s *testWhereRsetSuite) SetUpSuite(c *C) {
	names := []string{"id", "name"}
	tblPlan := newTestTablePlan(testData, names)

	expr := expression.NewBinaryOperation(opcode.Plus, &expression.Ident{CIStr: model.NewCIStr("id")}, expression.Value{Val: 1})

	s.r = &WhereRset{Src: tblPlan, Expr: expr}
}

func (s *testWhereRsetSuite) TestShowRsetPlan(c *C) {
	// `select id, name from t where id + 1`
	p, err := s.r.Plan(nil)
	c.Assert(err, IsNil)

	_, ok := p.(*plans.FilterDefaultPlan)
	c.Assert(ok, IsTrue)

	// `select id, name from t where 1`
	s.r.Expr = expression.Value{Val: int64(1)}

	_, err = s.r.Plan(nil)
	c.Assert(err, IsNil)

	// `select id, name from t where 0`
	s.r.Expr = expression.Value{Val: int64(0)}

	_, err = s.r.Plan(nil)
	c.Assert(err, IsNil)

	// `select id, name from t where null`
	s.r.Expr = expression.Value{}

	_, err = s.r.Plan(nil)
	c.Assert(err, IsNil)

	// `select id, name from t where id < 10`
	expr := expression.NewBinaryOperation(opcode.LT, &expression.Ident{CIStr: model.NewCIStr("id")}, expression.Value{Val: 10})

	s.r.Expr = expr

	_, err = s.r.Plan(nil)
	c.Assert(err, IsNil)

	src := s.r.Src.(*testTablePlan)
	src.SetFilter(true)

	_, err = s.r.Plan(nil)
	c.Assert(err, IsNil)

	// `select id, name from t where null && 1`
	src.SetFilter(false)

	expr = expression.NewBinaryOperation(opcode.AndAnd, expression.Value{}, expression.Value{Val: 1})

	s.r.Expr = expr

	_, err = s.r.Plan(nil)
	c.Assert(err, IsNil)

	// `select id, name from t where id && 1`
	expr = expression.NewBinaryOperation(opcode.AndAnd, &expression.Ident{CIStr: model.NewCIStr("id")}, expression.Value{Val: 1})

	s.r.Expr = expr

	_, err = s.r.Plan(nil)
	c.Assert(err, IsNil)

	// `select id, name from t where id && (id < 10)`
	exprx := expression.NewBinaryOperation(opcode.LT, &expression.Ident{CIStr: model.NewCIStr("id")}, expression.Value{Val: 10})
	expr = expression.NewBinaryOperation(opcode.AndAnd, &expression.Ident{CIStr: model.NewCIStr("id")}, exprx)

	s.r.Expr = expr

	_, err = s.r.Plan(nil)
	c.Assert(err, IsNil)

	src.SetFilter(true)

	_, err = s.r.Plan(nil)
	c.Assert(err, IsNil)

	// `select id, name from t where abc`
	src.SetFilter(false)

	expr = &expression.Ident{CIStr: model.NewCIStr("abc")}

	s.r.Expr = expr

	_, err = s.r.Plan(nil)
	c.Assert(err, IsNil)

	src.SetFilter(true)

	_, err = s.r.Plan(nil)
	c.Assert(err, IsNil)

	// `select id, name from t where 1 is null`
	src.SetFilter(false)

	exprx = expression.Value{Val: 1}
	expr = &expression.IsNull{Expr: exprx}

	s.r.Expr = expr

	_, err = s.r.Plan(nil)
	c.Assert(err, IsNil)

	src.SetFilter(true)

	_, err = s.r.Plan(nil)
	c.Assert(err, IsNil)

	// `select id, name from t where id is null`
	src.SetFilter(false)

	exprx = &expression.Ident{CIStr: model.NewCIStr("id")}
	expr = &expression.IsNull{Expr: exprx}

	s.r.Expr = expr

	_, err = s.r.Plan(nil)
	c.Assert(err, IsNil)

	src.SetFilter(true)

	_, err = s.r.Plan(nil)
	c.Assert(err, IsNil)

	// `select id, name from t where +id`
	src.SetFilter(false)

	exprx = &expression.Ident{CIStr: model.NewCIStr("id")}
	expr = expression.NewUnaryOperation(opcode.Plus, exprx)

	s.r.Expr = expr

	_, err = s.r.Plan(nil)
	c.Assert(err, IsNil)

	src.SetFilter(true)

	_, err = s.r.Plan(nil)
	c.Assert(err, IsNil)

	// `select id, name from t where id in (id)`
	expr = &expression.PatternIn{Expr: exprx, List: []expression.Expression{exprx}}
	s.r.Expr = expr

	_, err = s.r.Plan(nil)
	c.Assert(err, IsNil)

	// `select id, name from t where id like '%s'`
	expry := expression.Value{Val: "%s"}
	expr = &expression.PatternLike{Expr: exprx, Pattern: expry}
	s.r.Expr = expr

	_, err = s.r.Plan(nil)
	c.Assert(err, IsNil)

	// `select id, name from t where id is true`
	expr = &expression.IsTruth{Expr: exprx}
	s.r.Expr = expr

	_, err = s.r.Plan(nil)
	c.Assert(err, IsNil)
}
