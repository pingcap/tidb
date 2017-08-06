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
	"errors"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/util/testleak"
	"github.com/pingcap/tidb/util/testutil"
	"github.com/pingcap/tidb/util/types"
)

func (s *testEvaluatorSuite) TestCaseWhen(c *C) {
	defer testleak.AfterTest(c)()
	tbl := []struct {
		Arg []interface{}
		Ret interface{}
	}{
		{[]interface{}{true, 1, true, 2, 3}, 1},
		{[]interface{}{false, 1, true, 2, 3}, 2},
		{[]interface{}{nil, 1, true, 2, 3}, 2},
		{[]interface{}{false, 1, false, 2, 3}, 3},
		{[]interface{}{nil, 1, nil, 2, 3}, 3},
		{[]interface{}{false, 1, nil, 2, 3}, 3},
		{[]interface{}{nil, 1, false, 2, 3}, 3},
	}
	fc := funcs[ast.Case]
	for _, t := range tbl {
		f, err := fc.getFunction(datumsToConstants(types.MakeDatums(t.Arg...)), s.ctx)
		c.Assert(err, IsNil)
		d, err := f.eval(nil)
		c.Assert(err, IsNil)
		c.Assert(d, testutil.DatumEquals, types.NewDatum(t.Ret))
	}
	f, err := fc.getFunction(datumsToConstants(types.MakeDatums(errors.New("can't convert string to bool"), 1, true)), s.ctx)
	c.Assert(err, IsNil)
	_, err = f.eval(nil)
	c.Assert(err, NotNil)
}

func (s *testEvaluatorSuite) TestIf(c *C) {
	defer testleak.AfterTest(c)()
	tbl := []struct {
		Arg1 interface{}
		Arg2 interface{}
		Arg3 interface{}
		Ret  interface{}
	}{
		{1, 1, 2, 1},
		{nil, 1, 2, 2},
		{0, 1, 2, 2},
	}

	fc := funcs[ast.If]
	for _, t := range tbl {
		f, err := fc.getFunction(datumsToConstants(types.MakeDatums(t.Arg1, t.Arg2, t.Arg3)), s.ctx)
		c.Assert(err, IsNil)
		d, err := f.eval(nil)
		c.Assert(err, IsNil)
		c.Assert(d, testutil.DatumEquals, types.NewDatum(t.Ret))
	}
	f, err := fc.getFunction(datumsToConstants(types.MakeDatums(errors.New("must error"), 1, 2)), s.ctx)
	c.Assert(err, IsNil)
	_, err = f.eval(nil)
	c.Assert(err, NotNil)
}

func (s *testEvaluatorSuite) TestIfNull(c *C) {
	defer testleak.AfterTest(c)()
	tbl := []struct {
		arg1     interface{}
		arg2     interface{}
		expected interface{}
		isNil    bool
		getErr   bool
	}{
		{1, 2, int64(1), false, false},
		{nil, 2, int64(2), false, false},
		{nil, nil, nil, true, false},
		{tm, nil, tm, false, false},
		{nil, duration, duration, false, false},
		{nil, types.NewDecFromFloatForTest(123.123), types.NewDecFromFloatForTest(123.123), false, false},
		{nil, types.Bit{Value: 1, Width: 8}, "\x01", false, false},
		{nil, types.Hex{Value: 1}, "\x01", false, false},
		{nil, types.Set{Value: 1, Name: "abc"}, "abc", false, false},
		{"abc", nil, "abc", false, false},
	}

	for _, t := range tbl {
		f, err := newFunctionForTest(s.ctx, ast.Ifnull, primitiveValsToConstants([]interface{}{t.arg1, t.arg2})...)
		c.Assert(err, IsNil)
		d, err := f.Eval(nil)
		if t.getErr {
			c.Assert(err, NotNil)
		} else {
			c.Assert(err, IsNil)
			if t.isNil {
				c.Assert(d.Kind(), Equals, types.KindNull)
			} else {
				c.Assert(d.GetValue(), DeepEquals, t.expected)
			}
		}
	}

	f, err := funcs[ast.Ifnull].getFunction([]Expression{Zero, Zero}, s.ctx)
	c.Assert(err, IsNil)
	c.Assert(f.isDeterministic(), IsTrue)

	f, err = funcs[ast.Ifnull].getFunction([]Expression{Zero}, s.ctx)
	c.Assert(err, NotNil)
}

func (s *testEvaluatorSuite) TestNullIf(c *C) {
	defer testleak.AfterTest(c)()
	tbl := []struct {
		Arg1 interface{}
		Arg2 interface{}
		Ret  interface{}
	}{
		{1, 1, nil},
		{nil, 2, nil},
		{1, nil, 1},
		{1, 2, 1},
	}

	for _, t := range tbl {
		fc := funcs[ast.Nullif]
		f, err := fc.getFunction(datumsToConstants(types.MakeDatums(t.Arg1, t.Arg2)), s.ctx)
		c.Assert(err, IsNil)
		d, err := f.eval(nil)
		c.Assert(err, IsNil)
		c.Assert(d, testutil.DatumEquals, types.NewDatum(t.Ret))
	}
}
