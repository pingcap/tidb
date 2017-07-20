// Copyright 2017 PingCAP, Inc.
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
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/util/testleak"
	"github.com/pingcap/tidb/util/types"
)

func (s *testEvaluatorSuite) TestNewCastFunc(c *C) {
	defer testleak.AfterTest(c)()

	res := NewCastFunc(types.NewFieldType(mysql.TypeJSON), &Constant{RetType: types.NewFieldType(mysql.TypeLonglong), Value: types.NewIntDatum(1)}, s.ctx)
	c.Assert(res.FuncName.O, Equals, "cast")
	c.Assert(res.RetType.Tp, Equals, mysql.TypeJSON)
	_, ok := res.Function.(*builtinCastSig)
	c.Assert(ok, IsTrue)
}

func (s *testEvaluatorSuite) TestNewValuesFunc(c *C) {
	defer testleak.AfterTest(c)()

	res := NewValuesFunc(0, types.NewFieldType(mysql.TypeLonglong), s.ctx)
	c.Assert(res.FuncName.O, Equals, "values")
	c.Assert(res.RetType.Tp, Equals, mysql.TypeLonglong)
	_, ok := res.Function.(*builtinValuesSig)
	c.Assert(ok, IsTrue)
}

func (s *testEvaluatorSuite) TestEvaluateExprWithNull(c *C) {
	defer testleak.AfterTest(c)()

	col0 := &Column{RetType: types.NewFieldType(mysql.TypeLonglong), FromID: "DataSource_0", Position: 0, ColName: model.NewCIStr("col0")}
	col1 := &Column{RetType: types.NewFieldType(mysql.TypeLonglong), FromID: "DataSource_0", Position: 1, ColName: model.NewCIStr("col1")}
	ifnullInner := newFunction(ast.Ifnull, col1, One)
	ifnullOuter := newFunction(ast.Ifnull, col0, ifnullInner)

	// ifnull(null, ifnull(col1, 1))
	schema := &Schema{Columns: []*Column{col0}}
	res, err := EvaluateExprWithNull(s.ctx, schema, ifnullOuter)
	c.Assert(err, IsNil)
	c.Assert(res.String(), Equals, "ifnull(<nil>, ifnull(col1, 1))")

	schema.Columns = append(schema.Columns, col1)
	// ifnull(null, ifnull(null, 1))
	res, err = EvaluateExprWithNull(s.ctx, schema, ifnullOuter)
	c.Assert(err, IsNil)
	c.Assert(res.Equal(One, s.ctx), IsTrue)
}

func (s *testEvaluatorSuite) TestConstant(c *C) {
	defer testleak.AfterTest(c)()

	c.Assert(Zero.IsCorrelated(), IsFalse)
	c.Assert(Zero.Decorrelate(nil).Equal(Zero, s.ctx), IsTrue)
	c.Assert(Zero.HashCode(), DeepEquals, []byte{0x8, 0x0})
	c.Assert(Zero.Equal(One, s.ctx), IsFalse)
	res, err := Zero.MarshalJSON()
	c.Assert(err, IsNil)
	c.Assert(res, DeepEquals, []byte{0x22, 0x30, 0x22})
}
