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

var _ = Suite(&testTrimSuite{})

type testTrimSuite struct {
}

func (s *testTrimSuite) TestTrim(c *C) {
	tbl := []struct {
		str    interface{}
		remstr interface{}
		dir    int
		result interface{}
	}{
		{"  bar   ", nil, TrimBothDefault, "bar"},
		{"xxxbarxxx", "x", TrimLeading, "barxxx"},
		{"xxxbarxxx", "x", TrimBoth, "bar"},
		{"barxxyz", "xyz", TrimTrailing, "barx"},
		{nil, "xyz", TrimBoth, nil},
		{1, 2, TrimBoth, "1"},
		{"  \t\rbar\n   ", nil, TrimBothDefault, "bar"},
	}
	for _, v := range tbl {
		f := FunctionTrim{
			Str:       &Value{Val: v.str},
			Direction: v.dir,
		}
		if v.remstr != nil {
			f.RemStr = &Value{Val: v.remstr}
		}
		c.Assert(f.IsStatic(), Equals, true)

		fs := f.String()
		c.Assert(len(fs), Greater, 0)

		f1 := f.Clone()

		r, err := f.Eval(nil, nil)
		c.Assert(err, IsNil)
		c.Assert(r, Equals, v.result)

		r1, err := f1.Eval(nil, nil)
		c.Assert(err, IsNil)
		c.Assert(r, Equals, r1)
	}
}
