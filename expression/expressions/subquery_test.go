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
	"github.com/pingcap/tidb/util/mock"
)

var _ = Suite(&testSubQuerySuite{})

type testSubQuerySuite struct {
}

func (s *testSubQuerySuite) TestSubQuery(c *C) {
	e := &SubQuery{
		Value: 1,
	}

	ctx := mock.NewContext()
	c.Assert(e.IsStatic(), IsFalse)

	str := e.String()
	c.Assert(str, Equals, "")

	ec := e.Clone()

	v, err := ec.Eval(ctx, nil)
	c.Assert(err, IsNil)
	c.Assert(v, Equals, 1)

	e2, ok := ec.(*SubQuery)
	c.Assert(ok, IsTrue)

	e2 = newMockSubQuery([][]interface{}{{1}}, []string{"id"})

	vv, err := e2.Eval(ctx, nil)
	c.Assert(err, IsNil)
	c.Assert(vv, Equals, 1)

	e2.Value = nil
	vv, err = e2.Eval(ctx, nil)
	c.Assert(err, IsNil)
	c.Assert(vv, Equals, 1)

	e2 = newMockSubQuery([][]interface{}{{1, 2}}, []string{"id", "name"})

	vv, err = e2.Eval(ctx, nil)
	c.Assert(err, IsNil)
	c.Assert(vv, DeepEquals, []interface{}{1, 2})

	e2 = newMockSubQuery([][]interface{}{{1}, {2}}, []string{"id"})

	_, err = e2.Eval(ctx, nil)
	c.Assert(err, NotNil)

	str = e2.String()
	c.Assert(len(str), Greater, 0)

	e2 = newMockSubQuery([][]interface{}{{1, 2}}, []string{"id", "name"})

	count, err := e2.ColumnCount(nil)
	c.Assert(err, IsNil)
	c.Assert(count, Equals, 2)

	count, err = e2.ColumnCount(nil)
	c.Assert(err, IsNil)
	c.Assert(count, Equals, 2)

	e3 := newMockSubQuery([][]interface{}{{1, 2}}, []string{"id", "name"})

	e2.push(ctx)
	e3.push(ctx)

	c.Assert(e2.UseOuterQuery, IsFalse)
	c.Assert(e3.UseOuterQuery, IsFalse)
	SetOuterQueryUsed(ctx)
	c.Assert(e2.UseOuterQuery, IsTrue)
	c.Assert(e3.UseOuterQuery, IsTrue)
	err = e2.pop(ctx)
	c.Assert(err, NotNil)

	err = e3.pop(ctx)
	SetOuterQueryUsed(ctx)

	err = e2.pop(ctx)
	c.Assert(err, IsNil)

	err = e2.pop(ctx)
	c.Assert(err, NotNil)

	SetOuterQueryUsed(ctx)

	c.Assert(len(subQueryStackKey.String()), Greater, 0)
}

func newMockSubQuery(rows [][]interface{}, fields []string) *SubQuery {
	r := &mockRecordset{
		rows:   rows,
		fields: fields,
		offset: len(fields),
	}
	ms := &mockStatement{rset: r}
	ms.plan = newMockPlan(ms.rset)
	return &SubQuery{Stmt: ms}
}
