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

package builtin

import (
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/util/types"
)

func (s *testBuiltinSuite) TestGroupBy(c *C) {
	tbl := []struct {
		F string
		// args for every Eval round
		RoundArgs []interface{}
		Distinct  bool
		Ret       interface{}
	}{
		{"avg", []interface{}{1, 1}, false, 1},
		{"avg", []interface{}{1, 2}, true, 1.5},
		{"avg", []interface{}{1.0, 2.0}, true, 1.5},
		{"avg", []interface{}{1, 1}, true, 1},
		{"avg", []interface{}{nil, nil}, true, nil},
		{"count", []interface{}{1, 1}, false, 2},
		{"count", []interface{}{1, 1}, true, 1},
		{"count", []interface{}{nil, nil}, true, 0},
		{"count", []interface{}{nil, nil}, false, 0},
		{"count", []interface{}{nil, 1}, true, 1},
		{"max", []interface{}{1, 2, 3}, false, 3},
		{"max", []interface{}{nil, 2}, false, 2},
		{"max", []interface{}{nil, nil}, false, nil},
		{"min", []interface{}{1, 2, 3}, false, 1},
		{"min", []interface{}{nil, 1}, false, 1},
		{"min", []interface{}{nil, nil}, false, nil},
		{"min", []interface{}{3, 2, 1}, false, 1},
		{"sum", []interface{}{1, 1, 1, 1, 1}, false, 5},
		{"sum", []interface{}{float64(1.0), float64(1.0)}, false, 2.0},
		{"sum", []interface{}{1, 1, 1, 1, 1}, true, 1},
		{"sum", []interface{}{1, mysql.NewDecimalFromInt(1, 0)}, false, 2},
		{"sum", []interface{}{nil, nil}, false, nil},
		{"sum", []interface{}{nil, nil}, true, nil},
	}

	for _, t := range tbl {
		f, ok := Funcs[t.F]
		c.Assert(ok, IsTrue)

		m := map[interface{}]interface{}{}

		m[ExprEvalFn] = new(Func)
		m[ExprAggDistinct] = CreateAggregateDistinct(t.F, t.Distinct)

		for _, arg := range t.RoundArgs {
			args := []interface{}{arg}

			_, err := f.F(args, m)
			c.Assert(err, IsNil)
		}

		m[ExprAggDone] = struct{}{}
		v, err := f.F(nil, m)
		c.Assert(err, IsNil)
		switch v.(type) {
		case nil:
			c.Assert(t.Ret, IsNil)
		default:
			// we can not check decimal type directly, but we can convert all to float64
			f, err := types.ToFloat64(v)
			c.Assert(err, IsNil)

			ret, err := types.ToFloat64(t.Ret)
			c.Assert(err, IsNil)

			c.Assert(f, Equals, ret)
		}
	}
}

func (s *testBuiltinSuite) TestGroupConcat(c *C) {
	tbl := []struct {
		RoundArgs []interface{}
		Distinct  bool
		Ret       interface{}
	}{
		{[]interface{}{1, nil, 1}, false, "1,1"},
		{[]interface{}{nil, nil, nil}, false, nil},
		{[]interface{}{1, nil, 1}, true, "1"},
	}

	f := builtinGroupConcat

	for _, t := range tbl {
		// create a call and use dummy args.

		m := map[interface{}]interface{}{}
		m[ExprEvalFn] = new(Func)
		m[ExprAggDistinct] = CreateAggregateDistinct("group_concat", t.Distinct)

		for _, arg := range t.RoundArgs {
			args := []interface{}{arg}

			_, err := f(args, m)
			c.Assert(err, IsNil)
		}

		m[ExprAggDone] = struct{}{}
		v, err := f(nil, m)
		c.Assert(err, IsNil)
		c.Assert(v, DeepEquals, t.Ret)
	}
}

func (s *testBuiltinSuite) TestGroupByEmpty(c *C) {
	m := map[interface{}]interface{}{}
	m[ExprEvalArgAggEmpty] = struct{}{}

	v, err := builtinSum(nil, m)
	c.Assert(err, IsNil)
	c.Assert(v, IsNil)

	v, err = builtinAvg(nil, m)
	c.Assert(err, IsNil)
	c.Assert(v, IsNil)

	v, err = builtinCount(nil, m)
	c.Assert(err, IsNil)
	c.Assert(v, Equals, int64(0))

	v, err = builtinMax(nil, m)
	c.Assert(err, IsNil)
	c.Assert(v, IsNil)

	v, err = builtinMin(nil, m)
	c.Assert(err, IsNil)
	c.Assert(v, IsNil)

	v, err = builtinGroupConcat(nil, m)
	c.Assert(err, IsNil)
	c.Assert(v, IsNil)
}
