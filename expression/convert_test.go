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

var _ = Suite(&testConvertSuite{})

type testConvertSuite struct {
}

func (*testConvertSuite) TestConvert(c *C) {
	tbl := []struct {
		str    string
		cs     string
		result string
	}{
		{"haha", "utf8", "haha"},
		{"haha", "ascii", "haha"},
	}
	for _, v := range tbl {
		f := FunctionConvert{
			Expr:    &Value{Val: v.str},
			Charset: v.cs,
		}

		c.Assert(f.IsStatic(), Equals, true)

		fs := f.String()
		c.Assert(len(fs), Greater, 0)

		f1 := f.Clone()
		c.Assert(f1, NotNil)

		r, err := f.Eval(nil, nil)
		c.Assert(err, IsNil)
		s, ok := r.(string)
		c.Assert(ok, Equals, true)
		c.Assert(s, Equals, v.result)

		r1, err := f1.Eval(nil, nil)
		c.Assert(err, IsNil)
		s1, ok := r1.(string)
		c.Assert(ok, Equals, true)
		c.Assert(s, Equals, s1)
	}

	// Test case for error
	errTbl := []struct {
		str    interface{}
		cs     string
		result string
	}{
		{"haha", "wrongcharset", "haha"},
	}
	for _, v := range errTbl {
		f := FunctionConvert{
			Expr:    &Value{Val: v.str},
			Charset: v.cs,
		}

		_, err := f.Eval(nil, nil)
		c.Assert(err, NotNil)
	}
}
