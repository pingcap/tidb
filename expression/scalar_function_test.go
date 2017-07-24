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
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/util/testleak"
	"github.com/pingcap/tidb/util/types"
)

func (s *testEvaluatorSuite) TestScalarFunction(c *C) {
	defer testleak.AfterTest(c)()

	sf := newFunction(ast.LT, Zero, One)
	res, err := sf.MarshalJSON()
	c.Assert(err, IsNil)
	c.Assert(res, DeepEquals, []byte{0x22, 0x6c, 0x74, 0x28, 0x30, 0x2c, 0x20, 0x31, 0x29, 0x22})
	c.Assert(sf.IsCorrelated(), IsFalse)
	c.Assert(sf.Decorrelate(nil).Equal(sf, s.ctx), IsTrue)
	c.Assert(sf.HashCode(), DeepEquals, []byte{0x2, 0x8, 0x2, 0x4, 0x6c, 0x74, 0x2, 0x4, 0x8, 0x0, 0x2, 0x4, 0x8, 0x2})

	sf = NewValuesFunc(0, types.NewFieldType(mysql.TypeLonglong), s.ctx)
	newSf, ok := sf.Clone().(*ScalarFunction)
	c.Assert(ok, IsTrue)
	c.Assert(newSf.FuncName.O, Equals, "values")
	c.Assert(newSf.RetType.Tp, Equals, mysql.TypeLonglong)
	_, ok = newSf.Function.(*builtinValuesSig)
	c.Assert(ok, IsTrue)
}

func (s *testEvaluatorSuite) TestScalarFuncs2Exprs(c *C) {
	defer testleak.AfterTest(c)()
	sf0, _ := newFunction(ast.LT, Zero, Zero).(*ScalarFunction)
	sf1, _ := newFunction(ast.LT, One, One).(*ScalarFunction)

	funcs := []*ScalarFunction{sf0, sf1}
	exprs := ScalarFuncs2Exprs(funcs)
	for i := range exprs {
		c.Assert(exprs[i].Equal(funcs[i], s.ctx), IsTrue)
	}
}
