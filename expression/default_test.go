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

var _ = Suite(&testDefaultSuite{})

type testDefaultSuite struct {
}

func (s *testDefaultSuite) TestDefault(c *C) {
	e := Default{}

	c.Assert(e.String(), Equals, "default")
	c.Assert(e.Clone(), NotNil)

	c.Assert(e.IsStatic(), IsFalse)

	m := map[interface{}]interface{}{}

	_, err := e.Eval(nil, m)
	c.Assert(err, NotNil)

	m[ExprEvalDefaultName] = "id"

	_, err = e.Eval(nil, m)
	c.Assert(err, NotNil)

	m["id"] = 1

	v, err := e.Eval(nil, m)
	c.Assert(err, IsNil)
	c.Assert(v, Equals, 1)

	e.Name = "id"
	v, err = e.Eval(nil, m)
	c.Assert(err, IsNil)
	c.Assert(v, Equals, 1)

	c.Assert(len(e.String()), Greater, 0)
}
