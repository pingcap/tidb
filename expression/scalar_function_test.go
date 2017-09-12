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

func (s *testEvaluatorSuite) TestScalarFunction(c *C) {
	defer testleak.AfterTest(c)()

	a := &Column{
		FromID:   0,
		Position: 1,
		TblName:  model.NewCIStr("fei"),
		ColName:  model.NewCIStr("han"),
		RetType:  types.NewFieldType(mysql.TypeDouble),
	}
	sf := newFunction(ast.LT, a, One)
	res, err := sf.MarshalJSON()
	c.Assert(err, IsNil)
	c.Assert(res, DeepEquals, []byte{0x22, 0x6c, 0x74, 0x28, 0x66, 0x65, 0x69, 0x2e, 0x68, 0x61, 0x6e, 0x2c, 0x20, 0x31, 0x29, 0x22})
	c.Assert(sf.IsCorrelated(), IsFalse)
	c.Assert(sf.Decorrelate(nil).Equal(sf, s.ctx), IsTrue)
	c.Assert(sf.HashCode(), DeepEquals, []byte{0x2, 0x8, 0x2, 0x4, 0x6c, 0x74, 0x2, 0x20, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1, 0x2, 0x12, 0x5, 0xbf, 0xf0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0})

	sf = NewValuesFunc(0, types.NewFieldType(mysql.TypeLonglong), s.ctx)
	newSf, ok := sf.Clone().(*ScalarFunction)
	c.Assert(ok, IsTrue)
	c.Assert(newSf.FuncName.O, Equals, "values")
	c.Assert(newSf.RetType.Tp, Equals, mysql.TypeLonglong)
	_, ok = newSf.Function.(*builtinValuesIntSig)
	c.Assert(ok, IsTrue)
}

func (s *testEvaluatorSuite) TestScalarFuncs2Exprs(c *C) {
	defer testleak.AfterTest(c)()
	a := &Column{
		FromID:   0,
		Position: 1,
		RetType:  types.NewFieldType(mysql.TypeDouble),
	}
	sf0, _ := newFunction(ast.LT, a.Clone(), Zero).(*ScalarFunction)
	sf1, _ := newFunction(ast.LT, a.Clone(), One).(*ScalarFunction)

	funcs := []*ScalarFunction{sf0, sf1}
	exprs := ScalarFuncs2Exprs(funcs)
	for i := range exprs {
		c.Assert(exprs[i].Equal(funcs[i], s.ctx), IsTrue)
	}
}
