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

func (s *testStringUtilSuite) TestUnquote(c *C) {
	defer testleak.AfterTest(c)()
	table := []struct {
		str    string
		expect string
		ok     bool
	}{
		{"", "", false},
		{"'", "", false},
		{`'abc"`, "", false},
		{`abcdef`, "", false},
		{`abcdea`, "", false},
		{"```", "", false},
		{"'abc'def'", "", false},

		{`"abcdef"`, `abcdef`, true},
		{`"abc'def"`, `abc'def`, true},
		{`"汉字测试"`, `汉字测试`, true},
		{`"☺"`, "☺", true},
		{`"\xFF"`, "\xFF", true},
		{`"\U00010111"`, "\U00010111", true},
		{`"\U0001011111"`, "\U0001011111", true},
		{`"\a\b\f\n\r\t\v\\\""`, "\a\b\f\n\r\t\v\\\"", true},

		{`'abcdef'`, `abcdef`, true},
		{`'"'`, "\"", true},
		{`'\a\b\f\n\r\t\v\\\''`, "\a\b\f\n\r\t\v\\'", true},
		{`' '`, " ", true},

		{"``", ``, true},
		{"`a`", `a`, true},
		{"`abc`", `abc`, true},
		{"`☺`", `☺`, true},
		{"`hello world`", `hello world`, true},
		{"`\\xFF`", `\xFF`, true},
	}

	for _, t := range table {
		x, err := Unquote(t.str)
		c.Assert(x, Equals, t.expect)
		comment := Commentf("source %v", t.str)
		if t.ok {
			c.Assert(err, IsNil, comment)
		} else {
			c.Assert(err, NotNil, comment)
		}
	}
}

func (s *testStringUtilSuite) TestUnquoteChar(c *C) {
	defer testleak.AfterTest(c)()
	table := []struct {
		str    string
		expect string
		ok     bool
	}{
		{"", "", false},
		{"'", "", false},
		{`'abc"`, "", false},
		{`abcdef`, "", false},
		{`abcdea`, "", false},
		{"```", "", false},
		{"'abc'def'", "", false},
		{`'abc\n\'`, "", false},
		{`"abc\0"`, "", false},
		{`"\098"`, "", false},
		{`"\777"`, "", false},
		{`"\汉字"`, "", false},

		{`"abcdef"`, `abcdef`, true},
		{`"abc'def"`, `abc'def`, true},
		{`"汉字测试"`, `汉字测试`, true},
		{`"☺"`, "☺", true},
		{`"\u0011"`, "\u0011", true},
		{`"\xFF"`, "\xFF", true},
		{`"\U00010111"`, "\U00010111", true},
		{`"\U0001011111"`, "\U0001011111", true},
		{`"\a\b\f\n\r\t\v\\\""`, "\a\b\f\n\r\t\v\\\"", true},
		{`"\066"`, "\066", true},

		{`'abcdef'`, `abcdef`, true},
		{`'"'`, "\"", true},
		{`'\a\b\f\n\r\t\v\\\''`, "\a\b\f\n\r\t\v\\'", true},
		{`' '`, " ", true},

		{"``", ``, true},
		{"`a`", `a`, true},
		{"`abc`", `abc`, true},
		{"`☺`", `☺`, true},
		{"`hello world`", `hello world`, true},
		{"`\\xFF`", `\xFF`, true},
	}

	for _, t := range table {
		x, err := Unquote(t.str)
		c.Assert(x, Equals, t.expect)
		comment := Commentf("source %v", t.str)
		if t.ok {
			c.Assert(err, IsNil, comment)
		} else {
			c.Assert(err, NotNil, comment)
		}
	}
}
