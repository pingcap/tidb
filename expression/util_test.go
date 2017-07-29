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

package expression

import (
	"github.com/pingcap/check"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/util/mock"
	"github.com/pingcap/tidb/util/testleak"
	"github.com/pingcap/tidb/util/types"
)

var _ = check.Suite(&testUtilSuite{})

type testUtilSuite struct {
}

func (s *testUtilSuite) TestDistinct(c *check.C) {
	defer testleak.AfterTest(c)()
	dc := createDistinctChecker()
	tests := []struct {
		vals   []interface{}
		expect bool
	}{
		{[]interface{}{1, 1}, true},
		{[]interface{}{1, 1}, false},
		{[]interface{}{1, 2}, true},
		{[]interface{}{1, 2}, false},
		{[]interface{}{1, nil}, true},
		{[]interface{}{1, nil}, false},
	}
	for _, tt := range tests {
		d, err := dc.Check(types.MakeDatums(tt.vals...))
		c.Assert(err, check.IsNil)
		c.Assert(d, check.Equals, tt.expect)
	}
}

func (s *testUtilSuite) TestSubstituteCorCol2Constant(c *check.C) {
	defer testleak.AfterTest(c)()
	ctx := mock.NewContext()
	corCol1 := &CorrelatedColumn{Data: &One.Value}
	corCol1.RetType = types.NewFieldType(mysql.TypeLonglong)
	corCol2 := &CorrelatedColumn{Data: &One.Value}
	corCol2.RetType = types.NewFieldType(mysql.TypeLonglong)
	cast := NewCastFunc(types.NewFieldType(mysql.TypeLonglong), corCol1, ctx)
	plus := newFunction(ast.Plus, cast, corCol2)
	plus2 := newFunction(ast.Plus, plus, One)
	ans1 := &Constant{Value: types.NewIntDatum(3), RetType: types.NewFieldType(mysql.TypeLonglong)}
	ret, err := SubstituteCorCol2Constant(plus2)
	c.Assert(err, check.IsNil)
	c.Assert(ret.Equal(ans1, ctx), check.IsTrue)
	col1 := &Column{Index: 1}
	ret, err = SubstituteCorCol2Constant(col1)
	c.Assert(err, check.IsNil)
	ans2 := col1
	c.Assert(ret.Equal(ans2, ctx), check.IsTrue)
	plus3 := newFunction(ast.Plus, plus2, col1)
	ret, err = SubstituteCorCol2Constant(plus3)
	c.Assert(err, check.IsNil)
	ans3 := newFunction(ast.Plus, ans1, col1)
	c.Assert(ret.Equal(ans3, ctx), check.IsTrue)
}

func (s *testUtilSuite) TestPushDownNot(c *check.C) {
	defer testleak.AfterTest(c)()
	ctx := mock.NewContext()
	col := &Column{Index: 1, RetType: types.NewFieldType(mysql.TypeLonglong)}
	// !((a=1||a=1)&&a=1)
	eqFunc := newFunction(ast.EQ, col, One)
	orFunc := newFunction(ast.LogicOr, eqFunc, eqFunc)
	andFunc := newFunction(ast.LogicAnd, orFunc, eqFunc)
	notFunc := newFunction(ast.UnaryNot, andFunc)
	// (a!=1&&a!=1)||a=1
	neFunc := newFunction(ast.NE, col, One)
	andFunc2 := newFunction(ast.LogicAnd, neFunc, neFunc)
	orFunc2 := newFunction(ast.LogicOr, andFunc2, neFunc)
	ret := PushDownNot(notFunc, false, ctx)
	c.Assert(ret.Equal(orFunc2, ctx), check.IsTrue)
}
