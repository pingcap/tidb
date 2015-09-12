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
	"errors"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/expression"
)

var _ = Suite(&testRowSuite{})

type testRowSuite struct {
}

func (s *testRowSuite) TestRow(c *C) {
	r := Row{Values: []expression.Expression{Value{1}, Value{2}}}
	c.Assert(r.IsStatic(), IsTrue)

	_, err := r.Clone()
	c.Assert(err, IsNil)

	c.Assert(r.String(), Equals, "ROW(1, 2)")

	_, err = r.Eval(nil, nil)
	c.Assert(err, IsNil)

	expr := mockExpr{}
	expr.isStatic = false
	expr.err = errors.New("must error")
	r = Row{Values: []expression.Expression{Value{1}, expr}}
	c.Assert(r.IsStatic(), IsFalse)

	_, err = r.Clone()
	c.Assert(err, NotNil)

	_, err = r.Eval(nil, nil)
	c.Assert(err, NotNil)
}
