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
	. "github.com/pingcap/check"
)

var _ = Suite(&testPositionSuite{})

type testPositionSuite struct {
}

func (s *testPositionSuite) TestPosition(c *C) {
	e := &Position{
		N: 1,
	}

	c.Assert(e.IsStatic(), IsFalse)

	str := e.String()
	c.Assert(len(str), Greater, 0)

	ec := e.Clone()

	e2, ok := ec.(*Position)
	c.Assert(ok, IsTrue)

	e2.Name = "name"
	str = e2.String()
	c.Assert(len(str), Greater, 0)

	m := map[interface{}]interface{}{}
	_, err := e2.Eval(nil, m)
	c.Assert(err, NotNil)

	m[ExprEvalPositionFunc] = 1
	_, err = e2.Eval(nil, m)
	c.Assert(err, NotNil)

	f := func(int) (interface{}, error) {
		return 0, nil
	}

	m[ExprEvalPositionFunc] = f
	v, err := e2.Eval(nil, m)
	c.Assert(err, IsNil)
	c.Assert(v, Equals, 0)
}
