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

var _ = Suite(&testParamMarkerSuite{})

type testParamMarkerSuite struct {
}

func (t *testParamMarkerSuite) TestParamMarker(c *C) {
	e := &ParamMarker{
		Expr: Value{1},
	}

	v, err := e.Eval(nil, nil)
	c.Assert(err, IsNil)
	c.Assert(v, Equals, 1)

	c.Assert(e.IsStatic(), IsFalse)

	str := e.String()
	c.Assert(len(str), Greater, 0)

	ec := e.Clone()

	e2, ok := ec.(*ParamMarker)
	c.Assert(ok, IsTrue)

	e2.Expr = nil

	vv, err := e2.Eval(nil, nil)
	c.Assert(err, IsNil)
	c.Assert(vv, IsNil)

	str = e2.String()
	c.Assert(str, Equals, "?")

	c.Assert(e2.Clone(), NotNil)
}
