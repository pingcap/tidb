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

package expression_test

import (
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/parser/opcode"
	"github.com/pingcap/tidb/util/types"
)

var _ = Suite(&testVisitorSuite{})

type testVisitorSuite struct {
}

func (s *testVisitorSuite) TestBase(c *C) {
	val := expression.Value{Val: 1}
	type TestVisitor struct {
		expression.BaseVisitor
	}
	visitor := &TestVisitor{}
	visitor.BaseVisitor.V = visitor
	var exp expression.Expression
	exp = &expression.Between{Expr: val, Left: val, Right: val}
	exp.Accept(visitor)
	exp = expression.NewBinaryOperation(opcode.And, val, val)
	exp.Accept(visitor)
	exp, _ = expression.NewCall("avg", []expression.Expression{val}, true)
	exp.Accept(visitor)
	rows := [][]interface{}{{1}}
	sq := newMockSubQuery(rows, []string{"a"})
	exp = expression.NewCompareSubQuery(opcode.EQ, val, sq, true)
	exp.Accept(visitor)
	exp = &expression.Default{Name: "a"}
	exp.Accept(visitor)
	exp = expression.NewExistsSubQuery(sq)
	exp.Accept(visitor)

	when := &expression.WhenClause{Expr: val, Result: val}
	exp = &expression.FunctionCase{
		WhenClauses: []*expression.WhenClause{when},
		Value:       val,
		ElseClause:  val,
	}
	exp.Accept(visitor)
	exp = &expression.FunctionCast{Expr: val, Tp: types.NewFieldType(mysql.TypeLong), FunctionType: expression.ConvertFunction}
	exp.Accept(visitor)
	exp = &expression.FunctionConvert{Expr: val, Charset: "utf8"}
	exp.Accept(visitor)
	exp = &expression.FunctionSubstring{StrExpr: expression.Value{Val: "string"}, Pos: expression.Value{Val: 0}, Len: val}
	exp.Accept(visitor)
	exp = &expression.IsNull{Expr: val}
	exp.Accept(visitor)
	exp = &expression.IsTruth{Expr: val}
	exp.Accept(visitor)
	exp = &expression.ParamMarker{Expr: val}
	exp.Accept(visitor)
	exp = &expression.PatternIn{Expr: val, List: []expression.Expression{val}}
	exp.Accept(visitor)
	exp = &expression.PatternLike{Expr: val, Pattern: val}
	exp.Accept(visitor)
	exp = &expression.PatternRegexp{Expr: val, Pattern: val}
	exp.Accept(visitor)
	exp = &expression.PExpr{Expr: val}
	exp.Accept(visitor)
	exp = &expression.Position{Name: "a"}
	exp.Accept(visitor)
	exp = &expression.Row{Values: []expression.Expression{val}}
	exp.Accept(visitor)
	exp = &expression.UnaryOperation{V: val}
	exp.Accept(visitor)
	exp = &expression.Values{CIStr: model.NewCIStr("a")}
	exp.Accept(visitor)
	exp = &expression.Variable{Name: "a"}
	exp.Accept(visitor)
	exp = &expression.Extract{Unit: "SECOND", Date: expression.Value{Val: nil}}
	exp.Accept(visitor)
}
