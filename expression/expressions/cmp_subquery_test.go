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

package expressions

import (
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/parser/opcode"
	"github.com/pingcap/tidb/util/types"
)

var _ = Suite(&testCompSubQuerySuite{})

type testCompSubQuerySuite struct {
}

func (s *testCompSubQuerySuite) TestCompSubQuery(c *C) {
	tbl := []struct {
		lhs    interface{}
		op     opcode.Op
		rhs    []interface{}
		all    bool
		result interface{} // 0 for false, 1 for true, nil for nil.
	}{
		// Test any subquery.
		{nil, opcode.EQ, []interface{}{1, 2}, false, nil},
		{0, opcode.EQ, []interface{}{1, 2}, false, 0},
		{0, opcode.EQ, []interface{}{1, 2, nil}, false, nil},
		{1, opcode.EQ, []interface{}{1, 1}, false, 1},
		{1, opcode.EQ, []interface{}{1, 1, nil}, false, 1},
		{nil, opcode.NE, []interface{}{1, 2}, false, nil},
		{1, opcode.NE, []interface{}{1, 2}, false, 1},
		{1, opcode.NE, []interface{}{1, 2, nil}, false, 1},
		{1, opcode.NE, []interface{}{1, 1}, false, 0},
		{1, opcode.NE, []interface{}{1, 1, nil}, false, nil},
		{nil, opcode.GT, []interface{}{1, 2}, false, nil},
		{1, opcode.GT, []interface{}{1, 2}, false, 0},
		{1, opcode.GT, []interface{}{1, 2, nil}, false, nil},
		{2, opcode.GT, []interface{}{1, 2}, false, 1},
		{2, opcode.GT, []interface{}{1, 2, nil}, false, 1},
		{3, opcode.GT, []interface{}{1, 2}, false, 1},
		{3, opcode.GT, []interface{}{1, 2, nil}, false, 1},
		{nil, opcode.GE, []interface{}{1, 2}, false, nil},
		{0, opcode.GE, []interface{}{1, 2}, false, 0},
		{0, opcode.GE, []interface{}{1, 2, nil}, false, nil},
		{1, opcode.GE, []interface{}{1, 2}, false, 1},
		{1, opcode.GE, []interface{}{1, 2, nil}, false, 1},
		{2, opcode.GE, []interface{}{1, 2}, false, 1},
		{3, opcode.GE, []interface{}{1, 2}, false, 1},
		{nil, opcode.LT, []interface{}{1, 2}, false, nil},
		{0, opcode.LT, []interface{}{1, 2}, false, 1},
		{0, opcode.LT, []interface{}{1, 2, nil}, false, 1},
		{1, opcode.LT, []interface{}{1, 2}, false, 1},
		{2, opcode.LT, []interface{}{1, 2}, false, 0},
		{2, opcode.LT, []interface{}{1, 2, nil}, false, nil},
		{3, opcode.LT, []interface{}{1, 2}, false, 0},
		{nil, opcode.LE, []interface{}{1, 2}, false, nil},
		{0, opcode.LE, []interface{}{1, 2}, false, 1},
		{0, opcode.LE, []interface{}{1, 2, nil}, false, 1},
		{1, opcode.LE, []interface{}{1, 2}, false, 1},
		{2, opcode.LE, []interface{}{1, 2}, false, 1},
		{3, opcode.LE, []interface{}{1, 2}, false, 0},
		{3, opcode.LE, []interface{}{1, 2, nil}, false, nil},

		// Test all subquery.
		{nil, opcode.EQ, []interface{}{1, 2}, true, nil},
		{0, opcode.EQ, []interface{}{1, 2}, true, 0},
		{0, opcode.EQ, []interface{}{1, 2, nil}, true, 0},
		{1, opcode.EQ, []interface{}{1, 2}, true, 0},
		{1, opcode.EQ, []interface{}{1, 2, nil}, true, 0},
		{1, opcode.EQ, []interface{}{1, 1}, true, 1},
		{1, opcode.EQ, []interface{}{1, 1, nil}, true, nil},
		{nil, opcode.NE, []interface{}{1, 2}, true, nil},
		{0, opcode.NE, []interface{}{1, 2}, true, 1},
		{1, opcode.NE, []interface{}{1, 2, nil}, true, 0},
		{1, opcode.NE, []interface{}{1, 1}, true, 0},
		{1, opcode.NE, []interface{}{1, 1, nil}, true, 0},
		{nil, opcode.GT, []interface{}{1, 2}, true, nil},
		{1, opcode.GT, []interface{}{1, 2}, true, 0},
		{1, opcode.GT, []interface{}{1, 2, nil}, true, 0},
		{2, opcode.GT, []interface{}{1, 2}, true, 0},
		{2, opcode.GT, []interface{}{1, 2, nil}, true, 0},
		{3, opcode.GT, []interface{}{1, 2}, true, 1},
		{3, opcode.GT, []interface{}{1, 2, nil}, true, nil},
		{nil, opcode.GE, []interface{}{1, 2}, true, nil},
		{0, opcode.GE, []interface{}{1, 2}, true, 0},
		{0, opcode.GE, []interface{}{1, 2, nil}, true, 0},
		{1, opcode.GE, []interface{}{1, 2}, true, 0},
		{1, opcode.GE, []interface{}{1, 2, nil}, true, 0},
		{2, opcode.GE, []interface{}{1, 2}, true, 1},
		{3, opcode.GE, []interface{}{1, 2}, true, 1},
		{3, opcode.GE, []interface{}{1, 2, nil}, true, nil},
		{nil, opcode.LT, []interface{}{1, 2}, true, nil},
		{0, opcode.LT, []interface{}{1, 2}, true, 1},
		{0, opcode.LT, []interface{}{1, 2, nil}, true, nil},
		{1, opcode.LT, []interface{}{1, 2}, true, 0},
		{2, opcode.LT, []interface{}{1, 2}, true, 0},
		{2, opcode.LT, []interface{}{1, 2, nil}, true, 0},
		{3, opcode.LT, []interface{}{1, 2}, true, 0},
		{nil, opcode.LE, []interface{}{1, 2}, true, nil},
		{0, opcode.LE, []interface{}{1, 2}, true, 1},
		{0, opcode.LE, []interface{}{1, 2, nil}, true, nil},
		{1, opcode.LE, []interface{}{1, 2}, true, 1},
		{2, opcode.LE, []interface{}{1, 2}, true, 0},
		{3, opcode.LE, []interface{}{1, 2}, true, 0},
		{3, opcode.LE, []interface{}{1, 2, nil}, true, 0},
	}

	for _, t := range tbl {
		lhs := convert(t.lhs)

		rhs := make([][]interface{}, 0, len(t.rhs))
		for _, v := range t.rhs {
			rhs = append(rhs, []interface{}{convert(v)})
		}

		sq := newMockSubQuery(rhs, []string{"c"})
		expr := NewCompareSubQuery(t.op, Value{lhs}, sq, t.all)

		c.Assert(expr.IsStatic(), IsFalse)

		str := expr.String()
		c.Assert(len(str), Greater, 0)

		v, err := expr.Eval(nil, nil)
		c.Assert(err, IsNil)

		switch x := t.result.(type) {
		case nil:
			c.Assert(v, IsNil)
		case int:
			val, err := types.ToBool(v)
			c.Assert(err, IsNil)
			c.Assert(val, Equals, int64(x))
		}
	}

	// Test error.
	sq := newMockSubQuery([][]interface{}{{1, 2}}, []string{"c1", "c2"})
	expr := NewCompareSubQuery(opcode.EQ, Value{1}, sq, true)

	_, err := expr.Eval(nil, nil)
	c.Assert(err, NotNil)

	expr = NewCompareSubQuery(opcode.EQ, Value{1}, sq, false)

	_, err = expr.Eval(nil, nil)
	c.Assert(err, NotNil)
}
