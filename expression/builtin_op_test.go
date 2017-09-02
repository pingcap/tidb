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
	"math"

	"github.com/juju/errors"
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/util/testleak"
	"github.com/pingcap/tidb/util/testutil"
	"github.com/pingcap/tidb/util/types"
)

func (s *testEvaluatorSuite) TestUnary(c *C) {
	defer testleak.AfterTest(c)()
	cases := []struct {
		args     interface{}
		expected interface{}
		overflow bool
		getErr   bool
	}{
		{uint64(9223372036854775809), "-9223372036854775809", true, false},
		{uint64(9223372036854775810), "-9223372036854775810", true, false},
		{uint64(9223372036854775808), int64(-9223372036854775808), false, false},
		{int64(math.MinInt64), "9223372036854775808", true, false}, // --9223372036854775808
	}
	sc := s.ctx.GetSessionVars().StmtCtx
	origin := sc.InSelectStmt
	sc.InSelectStmt = true
	defer func() {
		sc.InSelectStmt = origin
	}()

	for _, t := range cases {
		f, err := newFunctionForTest(s.ctx, ast.UnaryMinus, primitiveValsToConstants([]interface{}{t.args})...)
		c.Assert(err, IsNil)
		d, err := f.Eval(nil)
		if t.getErr == false {
			c.Assert(err, IsNil)
			if !t.overflow {
				c.Assert(d.GetValue(), Equals, t.expected)
			} else {
				c.Assert(d.GetMysqlDecimal().String(), Equals, t.expected)
			}
		} else {
			c.Assert(err, NotNil)
		}
	}

	f, err := funcs[ast.UnaryMinus].getFunction(s.ctx, []Expression{Zero})
	c.Assert(err, IsNil)
	c.Assert(f.canBeFolded(), IsTrue)
}

func (s *testEvaluatorSuite) TestLogicAnd(c *C) {
	defer testleak.AfterTest(c)()

	sc := s.ctx.GetSessionVars().StmtCtx
	origin := sc.IgnoreTruncate
	defer func() {
		sc.IgnoreTruncate = origin
	}()
	sc.IgnoreTruncate = true

	cases := []struct {
		args     []interface{}
		expected int64
		isNil    bool
		getErr   bool
	}{
		{[]interface{}{1, 1}, 1, false, false},
		{[]interface{}{1, 0}, 0, false, false},
		{[]interface{}{0, 1}, 0, false, false},
		{[]interface{}{0, 0}, 0, false, false},
		{[]interface{}{2, -1}, 1, false, false},
		{[]interface{}{"a", "1"}, 0, false, false},
		{[]interface{}{"1a", "1"}, 1, false, false},
		{[]interface{}{0, nil}, 0, false, false},
		{[]interface{}{nil, 0}, 0, false, false},
		{[]interface{}{nil, 1}, 0, true, false},

		{[]interface{}{errors.New("must error"), 1}, 0, false, true},
	}

	for _, t := range cases {
		f, err := newFunctionForTest(s.ctx, ast.LogicAnd, primitiveValsToConstants(t.args)...)
		c.Assert(err, IsNil)
		d, err := f.Eval(nil)
		if t.getErr {
			c.Assert(err, NotNil)
		} else {
			c.Assert(err, IsNil)
			if t.isNil {
				c.Assert(d.Kind(), Equals, types.KindNull)
			} else {
				c.Assert(d.GetInt64(), Equals, t.expected)
			}
		}
	}

	// Test incorrect parameter count.
	_, err := newFunctionForTest(s.ctx, ast.LogicAnd, Zero)
	c.Assert(err, NotNil)

	f, err := funcs[ast.LogicAnd].getFunction(s.ctx, []Expression{Zero, Zero})
	c.Assert(err, IsNil)
	c.Assert(f.canBeFolded(), IsTrue)
}

func (s *testEvaluatorSuite) TestLeftShift(c *C) {
	defer testleak.AfterTest(c)()

	cases := []struct {
		args     []interface{}
		expected uint64
		isNil    bool
		getErr   bool
	}{
		{[]interface{}{123, 2}, uint64(492), false, false},
		{[]interface{}{-123, 2}, uint64(18446744073709551124), false, false},
		{[]interface{}{nil, 1}, 0, true, false},

		{[]interface{}{errors.New("must error"), 1}, 0, false, true},
	}

	for _, t := range cases {
		f, err := newFunctionForTest(s.ctx, ast.LeftShift, primitiveValsToConstants(t.args)...)
		c.Assert(err, IsNil)
		d, err := f.Eval(nil)
		if t.getErr {
			c.Assert(err, NotNil)
		} else {
			c.Assert(err, IsNil)
			if t.isNil {
				c.Assert(d.Kind(), Equals, types.KindNull)
			} else {
				c.Assert(d.GetUint64(), Equals, t.expected)
			}
		}
	}
}

func (s *testEvaluatorSuite) TestRightShift(c *C) {
	defer testleak.AfterTest(c)()

	cases := []struct {
		args     []interface{}
		expected uint64
		isNil    bool
		getErr   bool
	}{
		{[]interface{}{123, 2}, uint64(30), false, false},
		{[]interface{}{-123, 2}, uint64(4611686018427387873), false, false},
		{[]interface{}{nil, 1}, 0, true, false},

		{[]interface{}{errors.New("must error"), 1}, 0, false, true},
	}

	for _, t := range cases {
		f, err := newFunctionForTest(s.ctx, ast.RightShift, primitiveValsToConstants(t.args)...)
		c.Assert(err, IsNil)
		d, err := f.Eval(nil)
		if t.getErr {
			c.Assert(err, NotNil)
		} else {
			c.Assert(err, IsNil)
			if t.isNil {
				c.Assert(d.Kind(), Equals, types.KindNull)
			} else {
				c.Assert(d.GetUint64(), Equals, t.expected)
			}
		}
	}

	// Test incorrect parameter count.
	_, err := newFunctionForTest(s.ctx, ast.RightShift, Zero)
	c.Assert(err, NotNil)

	f, err := funcs[ast.RightShift].getFunction(s.ctx, []Expression{Zero, Zero})
	c.Assert(err, IsNil)
	c.Assert(f.canBeFolded(), IsTrue)
}

func (s *testEvaluatorSuite) TestBitXor(c *C) {
	defer testleak.AfterTest(c)()

	cases := []struct {
		args     []interface{}
		expected uint64
		isNil    bool
		getErr   bool
	}{
		{[]interface{}{123, 321}, uint64(314), false, false},
		{[]interface{}{-123, 321}, uint64(18446744073709551300), false, false},
		{[]interface{}{nil, 1}, 0, true, false},

		{[]interface{}{errors.New("must error"), 1}, 0, false, true},
	}

	for _, t := range cases {
		f, err := newFunctionForTest(s.ctx, ast.Xor, primitiveValsToConstants(t.args)...)
		c.Assert(err, IsNil)
		d, err := f.Eval(nil)
		if t.getErr {
			c.Assert(err, NotNil)
		} else {
			c.Assert(err, IsNil)
			if t.isNil {
				c.Assert(d.Kind(), Equals, types.KindNull)
			} else {
				c.Assert(d.GetUint64(), Equals, t.expected)
			}
		}
	}

	// Test incorrect parameter count.
	_, err := newFunctionForTest(s.ctx, ast.Xor, Zero)
	c.Assert(err, NotNil)

	f, err := funcs[ast.Xor].getFunction(s.ctx, []Expression{Zero, Zero})
	c.Assert(err, IsNil)
	c.Assert(f.canBeFolded(), IsTrue)
}

func (s *testEvaluatorSuite) TestBitOr(c *C) {
	defer testleak.AfterTest(c)()

	sc := s.ctx.GetSessionVars().StmtCtx
	origin := sc.IgnoreTruncate
	defer func() {
		sc.IgnoreTruncate = origin
	}()
	sc.IgnoreTruncate = true

	cases := []struct {
		args     []interface{}
		expected uint64
		isNil    bool
		getErr   bool
	}{
		{[]interface{}{123, 321}, uint64(379), false, false},
		{[]interface{}{-123, 321}, uint64(18446744073709551557), false, false},
		{[]interface{}{nil, 1}, 0, true, false},

		{[]interface{}{errors.New("must error"), 1}, 0, false, true},
	}

	for _, t := range cases {
		f, err := newFunctionForTest(s.ctx, ast.Or, primitiveValsToConstants(t.args)...)
		c.Assert(err, IsNil)
		d, err := f.Eval(nil)
		if t.getErr {
			c.Assert(err, NotNil)
		} else {
			c.Assert(err, IsNil)
			if t.isNil {
				c.Assert(d.Kind(), Equals, types.KindNull)
			} else {
				c.Assert(d.GetUint64(), Equals, t.expected)
			}
		}
	}

	// Test incorrect parameter count.
	_, err := newFunctionForTest(s.ctx, ast.Or, Zero)
	c.Assert(err, NotNil)

	f, err := funcs[ast.Or].getFunction(s.ctx, []Expression{Zero, Zero})
	c.Assert(err, IsNil)
	c.Assert(f.canBeFolded(), IsTrue)
}

func (s *testEvaluatorSuite) TestLogicOr(c *C) {
	defer testleak.AfterTest(c)()

	sc := s.ctx.GetSessionVars().StmtCtx
	origin := sc.IgnoreTruncate
	defer func() {
		sc.IgnoreTruncate = origin
	}()
	sc.IgnoreTruncate = true

	cases := []struct {
		args     []interface{}
		expected int64
		isNil    bool
		getErr   bool
	}{
		{[]interface{}{1, 1}, 1, false, false},
		{[]interface{}{1, 0}, 1, false, false},
		{[]interface{}{0, 1}, 1, false, false},
		{[]interface{}{0, 0}, 0, false, false},
		{[]interface{}{2, -1}, 1, false, false},
		{[]interface{}{"a", "1"}, 1, false, false},
		{[]interface{}{"1a", "1"}, 1, false, false},
		{[]interface{}{1, nil}, 1, false, false},
		{[]interface{}{nil, 1}, 1, false, false},
		{[]interface{}{nil, 0}, 0, true, false},

		{[]interface{}{errors.New("must error"), 1}, 0, false, true},
	}

	for _, t := range cases {
		f, err := newFunctionForTest(s.ctx, ast.LogicOr, primitiveValsToConstants(t.args)...)
		c.Assert(err, IsNil)
		d, err := f.Eval(nil)
		if t.getErr {
			c.Assert(err, NotNil)
		} else {
			c.Assert(err, IsNil)
			if t.isNil {
				c.Assert(d.Kind(), Equals, types.KindNull)
			} else {
				c.Assert(d.GetInt64(), Equals, t.expected)
			}
		}
	}

	// Test incorrect parameter count.
	_, err := newFunctionForTest(s.ctx, ast.LogicOr, Zero)
	c.Assert(err, NotNil)

	f, err := funcs[ast.LogicOr].getFunction(s.ctx, []Expression{Zero, Zero})
	c.Assert(err, IsNil)
	c.Assert(f.canBeFolded(), IsTrue)
}

func (s *testEvaluatorSuite) TestBitAnd(c *C) {
	defer testleak.AfterTest(c)()

	cases := []struct {
		args     []interface{}
		expected int64
		isNil    bool
		getErr   bool
	}{
		{[]interface{}{123, 321}, 65, false, false},
		{[]interface{}{-123, 321}, 257, false, false},
		{[]interface{}{nil, 1}, 0, true, false},

		{[]interface{}{errors.New("must error"), 1}, 0, false, true},
	}

	for _, t := range cases {
		f, err := newFunctionForTest(s.ctx, ast.And, primitiveValsToConstants(t.args)...)
		c.Assert(err, IsNil)
		d, err := f.Eval(nil)
		if t.getErr {
			c.Assert(err, NotNil)
		} else {
			c.Assert(err, IsNil)
			if t.isNil {
				c.Assert(d.Kind(), Equals, types.KindNull)
			} else {
				c.Assert(d.GetInt64(), Equals, t.expected)
			}
		}
	}

	// Test incorrect parameter count.
	_, err := newFunctionForTest(s.ctx, ast.And, Zero)
	c.Assert(err, NotNil)

	f, err := funcs[ast.And].getFunction(s.ctx, []Expression{Zero, Zero})
	c.Assert(err, IsNil)
	c.Assert(f.canBeFolded(), IsTrue)
}

func (s *testEvaluatorSuite) TestBitNeg(c *C) {
	defer testleak.AfterTest(c)()

	sc := s.ctx.GetSessionVars().StmtCtx
	origin := sc.IgnoreTruncate
	defer func() {
		sc.IgnoreTruncate = origin
	}()
	sc.IgnoreTruncate = true

	cases := []struct {
		args     []interface{}
		expected uint64
		isNil    bool
		getErr   bool
	}{
		{[]interface{}{123}, uint64(18446744073709551492), false, false},
		{[]interface{}{-123}, uint64(122), false, false},
		{[]interface{}{nil}, 0, true, false},

		{[]interface{}{errors.New("must error")}, 0, false, true},
	}

	for _, t := range cases {
		f, err := newFunctionForTest(s.ctx, ast.BitNeg, primitiveValsToConstants(t.args)...)
		c.Assert(err, IsNil)
		d, err := f.Eval(nil)
		if t.getErr {
			c.Assert(err, NotNil)
		} else {
			c.Assert(err, IsNil)
			if t.isNil {
				c.Assert(d.Kind(), Equals, types.KindNull)
			} else {
				c.Assert(d.GetUint64(), Equals, t.expected)
			}
		}
	}

	// Test incorrect parameter count.
	_, err := newFunctionForTest(s.ctx, ast.BitNeg, Zero, Zero)
	c.Assert(err, NotNil)

	f, err := funcs[ast.BitNeg].getFunction(s.ctx, []Expression{Zero})
	c.Assert(err, IsNil)
	c.Assert(f.canBeFolded(), IsTrue)
}

func (s *testEvaluatorSuite) TestUnaryNot(c *C) {
	defer testleak.AfterTest(c)()

	sc := s.ctx.GetSessionVars().StmtCtx
	origin := sc.IgnoreTruncate
	defer func() {
		sc.IgnoreTruncate = origin
	}()
	sc.IgnoreTruncate = true

	cases := []struct {
		args     []interface{}
		expected int64
		isNil    bool
		getErr   bool
	}{
		{[]interface{}{1}, 0, false, false},
		{[]interface{}{0}, 1, false, false},
		{[]interface{}{123}, 0, false, false},
		{[]interface{}{-123}, 0, false, false},
		{[]interface{}{"123"}, 0, false, false},
		{[]interface{}{nil}, 0, true, false},

		{[]interface{}{errors.New("must error")}, 0, false, true},
	}

	for _, t := range cases {
		f, err := newFunctionForTest(s.ctx, ast.UnaryNot, primitiveValsToConstants(t.args)...)
		c.Assert(err, IsNil)
		d, err := f.Eval(nil)
		if t.getErr {
			c.Assert(err, NotNil)
		} else {
			c.Assert(err, IsNil)
			if t.isNil {
				c.Assert(d.Kind(), Equals, types.KindNull)
			} else {
				c.Assert(d.GetInt64(), Equals, t.expected)
			}
		}
	}

	// Test incorrect parameter count.
	_, err := newFunctionForTest(s.ctx, ast.UnaryNot, Zero, Zero)
	c.Assert(err, NotNil)

	f, err := funcs[ast.UnaryNot].getFunction(s.ctx, []Expression{Zero})
	c.Assert(err, IsNil)
	c.Assert(f.canBeFolded(), IsTrue)
}

func (s *testEvaluatorSuite) TestIsTrueOrFalse(c *C) {
	defer testleak.AfterTest(c)()
	sc := s.ctx.GetSessionVars().StmtCtx
	origin := sc.IgnoreTruncate
	defer func() {
		sc.IgnoreTruncate = origin
	}()
	sc.IgnoreTruncate = true

	testCases := []struct {
		args    []interface{}
		isTrue  interface{}
		isFalse interface{}
	}{
		{
			args:    []interface{}{-12},
			isTrue:  1,
			isFalse: 0,
		},
		{
			args:    []interface{}{12},
			isTrue:  1,
			isFalse: 0,
		},
		{
			args:    []interface{}{0},
			isTrue:  0,
			isFalse: 1,
		},
		{
			args:    []interface{}{float64(0)},
			isTrue:  0,
			isFalse: 1,
		},
		{
			args:    []interface{}{"aaa"},
			isTrue:  0,
			isFalse: 1,
		},
		{
			args:    []interface{}{""},
			isTrue:  0,
			isFalse: 1,
		},
		{
			args:    []interface{}{nil},
			isTrue:  0,
			isFalse: 0,
		},
	}

	for _, tc := range testCases {
		isTrueSig, err := funcs[ast.IsTruth].getFunction(s.ctx, datumsToConstants(types.MakeDatums(tc.args...)))
		c.Assert(err, IsNil)
		c.Assert(isTrueSig, NotNil)
		c.Assert(isTrueSig.canBeFolded(), IsTrue)

		isTrue, err := isTrueSig.eval(nil)
		c.Assert(err, IsNil)
		c.Assert(isTrue, testutil.DatumEquals, types.NewDatum(tc.isTrue))
	}

	for _, tc := range testCases {
		isFalseSig, err := funcs[ast.IsFalsity].getFunction(s.ctx, datumsToConstants(types.MakeDatums(tc.args...)))
		c.Assert(err, IsNil)
		c.Assert(isFalseSig, NotNil)
		c.Assert(isFalseSig.canBeFolded(), IsTrue)

		isFalse, err := isFalseSig.eval(nil)
		c.Assert(err, IsNil)
		c.Assert(isFalse, testutil.DatumEquals, types.NewDatum(tc.isFalse))
	}
}
