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

package stringutil

import (
	"testing"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/util/testleak"
)

func TestT(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testStringUtilSuite{})

type testStringUtilSuite struct {
}

func (s *testStringUtilSuite) TestRemoveUselessBackslash(c *C) {
	defer testleak.AfterTest(c)()
	table := []struct {
		str    string
		expect string
	}{
		{"xxxx", "xxxx"},
		{`\x01`, `x01`},
		{`\b01`, `\b01`},
		{`\B01`, `B01`},
		{`'\'a\''`, `'\'a\''`},
	}

	for _, t := range table {
		x := RemoveUselessBackslash(t.str)
		c.Assert(x, Equals, t.expect)
	}
}

func (s *testStringUtilSuite) TestReverse(c *C) {
	defer testleak.AfterTest(c)()
	table := []struct {
		str    string
		expect string
	}{
		{"zxcf", "fcxz"},
		{"abc", "cba"},
		{"Hello, 世界", "界世 ,olleH"},
		{"", ""},
	}

	for _, t := range table {
		x := Reverse(t.str)
		c.Assert(x, Equals, t.expect)
	}
}
