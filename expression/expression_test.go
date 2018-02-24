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
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/testleak"
)

func (s *testEvaluatorSuite) TestNewValuesFunc(c *C) {
	defer testleak.AfterTest(c)()

	res := NewValuesFunc(s.ctx, 0, types.NewFieldType(mysql.TypeLonglong))
	c.Assert(res.FuncName.O, Equals, "values")
	c.Assert(res.RetType.Tp, Equals, mysql.TypeLonglong)
	_, ok := res.Function.(*builtinValuesIntSig)
	c.Assert(ok, IsTrue)
}

func (s *testEvaluatorSuite) TestEvaluateExprWithNull(c *C) {
	defer testleak.AfterTest(c)()

	col0 := &Column{RetType: types.NewFieldType(mysql.TypeLonglong), FromID: 0, Position: 0, ColName: model.NewCIStr("col0")}
	col1 := &Column{RetType: types.NewFieldType(mysql.TypeLonglong), FromID: 0, Position: 1, ColName: model.NewCIStr("col1")}
	ifnullInner := newFunction(ast.Ifnull, col1, One)
	ifnullOuter := newFunction(ast.Ifnull, col0, ifnullInner)

	// ifnull(null, ifnull(col1, 1))
	schema := NewSchema(col0)
	res := EvaluateExprWithNull(s.ctx, schema, ifnullOuter)
	c.Assert(res.String(), Equals, "ifnull(<nil>, ifnull(col1, 1))")

	schema.Columns = append(schema.Columns, col1)
	// ifnull(null, ifnull(null, 1))
	res = EvaluateExprWithNull(s.ctx, schema, ifnullOuter)
	c.Assert(res.Equal(s.ctx, One), IsTrue)
}

func (s *testEvaluatorSuite) TestConstant(c *C) {
	defer testleak.AfterTest(c)()

	sc := &stmtctx.StatementContext{TimeZone: time.Local}
	c.Assert(Zero.IsCorrelated(), IsFalse)
	c.Assert(Zero.Decorrelate(nil).Equal(s.ctx, Zero), IsTrue)
	c.Assert(Zero.HashCode(sc), DeepEquals, []byte{0x8, 0x0})
	c.Assert(Zero.Equal(s.ctx, One), IsFalse)
	res, err := Zero.MarshalJSON()
	c.Assert(err, IsNil)
	c.Assert(res, DeepEquals, []byte{0x22, 0x30, 0x22})
}

func (s *testEvaluatorSuite) TestIsBinaryLiteral(c *C) {
	col := &Column{RetType: types.NewFieldType(mysql.TypeEnum)}
	c.Assert(IsBinaryLiteral(col), IsFalse)
	col.RetType.Tp = mysql.TypeSet
	c.Assert(IsBinaryLiteral(col), IsFalse)
	col.RetType.Tp = mysql.TypeBit
	c.Assert(IsBinaryLiteral(col), IsFalse)
	col.RetType.Tp = mysql.TypeDuration
	c.Assert(IsBinaryLiteral(col), IsFalse)

	con := &Constant{RetType: types.NewFieldType(mysql.TypeVarString), Value: types.NewBinaryLiteralDatum([]byte{byte(0), byte(1)})}
	c.Assert(IsBinaryLiteral(con), IsTrue)
	con.Value = types.NewIntDatum(1)
	c.Assert(IsBinaryLiteral(con), IsFalse)
}
