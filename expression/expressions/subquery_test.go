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

import . "github.com/pingcap/check"

var _ = Suite(&testSubQuerySuite{})

type testSubQuerySuite struct {
}

func (s *testSubQuerySuite) TestSubQuery(c *C) {
	e := &SubQuery{
		Value: 1,
	}

	c.Assert(e.IsStatic(), IsFalse)

	str := e.String()
	c.Assert(str, Equals, "")

	ec, err := e.Clone()
	c.Assert(err, IsNil)

	v, err := ec.Eval(nil, nil)
	c.Assert(err, IsNil)
	c.Assert(v, Equals, 1)

	e2, ok := ec.(*SubQuery)
	c.Assert(ok, IsTrue)

	e2 = newMockSubQuery([][]interface{}{{1}}, []string{"id"})

	vv, err := e2.Eval(nil, nil)
	c.Assert(err, IsNil)
	c.Assert(vv, Equals, 1)

	e2.Value = nil
	vv, err = e2.Eval(nil, nil)
	c.Assert(err, IsNil)
	c.Assert(vv, Equals, 1)

	e2 = newMockSubQuery([][]interface{}{{1, 2}}, []string{"id", "name"})

	vv, err = e2.Eval(nil, nil)
	c.Assert(err, IsNil)
	c.Assert(vv, DeepEquals, []interface{}{1, 2})

	e2 = newMockSubQuery([][]interface{}{{1}, {2}}, []string{"id"})

	_, err = e2.Eval(nil, nil)
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
