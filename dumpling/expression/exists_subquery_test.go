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
	"github.com/pingcap/tidb/expression/subquery"
	"github.com/pingcap/tidb/parser/opcode"
	"github.com/pingcap/tidb/util/mock"
	"github.com/pingcap/tidb/util/mock/mocks"
	"github.com/pingcap/tidb/util/types"
)

var _ = Suite(&testExistsSubQuerySuite{})

type testExistsSubQuerySuite struct {
}

func (s *testExistsSubQuerySuite) TestExistsSubQuery(c *C) {
	// Test exists subquery.
	tbl := []struct {
		in     []interface{}
		result int64 // 0 for false, 1 for true.
	}{
		{[]interface{}{1}, 1},
		{[]interface{}{nil}, 1},
		{[]interface{}{}, 0},
	}

	ctx := mock.NewContext()
	for _, t := range tbl {
		in := make([][]interface{}, 0, len(t.in))
		for _, v := range t.in {
			in = append(in, []interface{}{convert(v)})
		}

		sq := newMockSubQuery(in, []string{"c"})
		expr := expression.NewExistsSubQuery(sq)

		c.Assert(expr.IsStatic(), IsFalse)

		exprc := expr.Clone()
		c.Assert(exprc, NotNil)

		str := exprc.String()
		c.Assert(len(str), Greater, 0)

		v, err := exprc.Eval(ctx, nil)
		c.Assert(err, IsNil)

		val, err := types.ToBool(v)
		c.Assert(err, IsNil)
		c.Assert(val, Equals, t.result)

		// test cache
		v, err = exprc.Eval(ctx, nil)
		c.Assert(err, IsNil)

		val, err = types.ToBool(v)
		c.Assert(err, IsNil)
		c.Assert(val, Equals, t.result)
	}

	// Test not exists subquery.
	tbl = []struct {
		in     []interface{}
		result int64 // 0 for false, 1 for true.
	}{
		{[]interface{}{1}, 0},
		{[]interface{}{nil}, 0},
		{[]interface{}{}, 1},
	}
	for _, t := range tbl {
		in := make([][]interface{}, 0, len(t.in))
		for _, v := range t.in {
			in = append(in, []interface{}{convert(v)})
		}

		sq := newMockSubQuery(in, []string{"c"})
		es := expression.NewExistsSubQuery(sq)

		c.Assert(es.IsStatic(), IsFalse)

		str := es.String()
		c.Assert(len(str), Greater, 0)

		expr := expression.NewUnaryOperation(opcode.Not, es)

		exprc := expr.Clone()
		c.Assert(exprc, NotNil)

		str = exprc.String()
		c.Assert(len(str), Greater, 0)

		v, err := exprc.Eval(ctx, nil)
		c.Assert(err, IsNil)

		val, err := types.ToBool(v)
		c.Assert(err, IsNil)
		c.Assert(val, Equals, t.result)
	}
}

func newMockSubQuery(rows [][]interface{}, fields []string) *subquery.SubQuery {
	r := mocks.NewRecordset(rows, fields, len(fields))
	ms := &mocks.Statement{Rset: r}
	ms.P = mocks.NewPlan(ms.Rset)
	return &subquery.SubQuery{Stmt: ms}
}
