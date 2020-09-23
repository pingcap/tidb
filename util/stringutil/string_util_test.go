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
	CustomVerboseFlag = true
	TestingT(t)
}

var _ = Suite(&testStringUtilSuite{})

type testStringUtilSuite struct {
}

func (s *testStringUtilSuite) TestUnquote(c *C) {
	defer testleak.AfterTest(c)()
	table := []struct {
		str    string
		expect string
		ok     bool
	}{
		{``, ``, false},
		{`'`, ``, false},
		{`'abc"`, ``, false},
		{`abcdea`, ``, false},
		{`'abc'def'`, ``, false},
		{`"abc\"`, ``, false},

		{`"abcdef"`, `abcdef`, true},
		{`"abc'def"`, `abc'def`, true},
		{`"\a汉字测试"`, `a汉字测试`, true},
		{`"☺"`, `☺`, true},
		{`"\xFF"`, `xFF`, true},
		{`"\U00010111"`, `U00010111`, true},
		{`"\U0001011111"`, `U0001011111`, true},
		{`"\a\b\f\n\r\t\v\\\""`, "a\bf\n\r\tv\\\"", true},
		{`"\Z\%\_"`, "\032" + `\%\_`, true},
		{`"abc\0"`, "abc\000", true},
		{`"abc\"abc"`, `abc"abc`, true},

		{`'abcdef'`, `abcdef`, true},
		{`'"'`, "\"", true},
		{`'\a\b\f\n\r\t\v\\\''`, "a\bf\n\r\tv\\'", true},
		{`' '`, ` `, true},
		{"'\\a汉字'", "a汉字", true},
		{"'\\a\x90'", "a\x90", true},
		{"\"\\a\x18èàø»\x05\"", "a\x18èàø»\x05", true},
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

func (s *testStringUtilSuite) TestPatternMatch(c *C) {
	defer testleak.AfterTest(c)()
	tbl := []struct {
		pattern string
		input   string
		escape  byte
		match   bool
	}{
		{``, `a`, '\\', false},
		{`a`, `a`, '\\', true},
		{`a`, `b`, '\\', false},
		{`aA`, `aA`, '\\', true},
		{`_`, `a`, '\\', true},
		{`_`, `ab`, '\\', false},
		{`__`, `b`, '\\', false},
		{`%`, `abcd`, '\\', true},
		{`%`, ``, '\\', true},
		{`%b`, `AAA`, '\\', false},
		{`%a%`, `BBB`, '\\', false},
		{`a%`, `BBB`, '\\', false},
		{`\%a`, `%a`, '\\', true},
		{`\%a`, `aa`, '\\', false},
		{`\_a`, `_a`, '\\', true},
		{`\_a`, `aa`, '\\', false},
		{`\\_a`, `\xa`, '\\', true},
		{`\a\b`, `\a\b`, '\\', true},
		{`%%_`, `abc`, '\\', true},
		{`%_%_aA`, "aaaA", '\\', true},
		{`+_a`, `_a`, '+', true},
		{`+%a`, `%a`, '+', true},
		{`\%a`, `%a`, '+', false},
		{`++a`, `+a`, '+', true},
		{`++_a`, `+xa`, '+', true},
		// We may reopen these test when like function go back to case insensitive.
		/*
			{"_ab", "AAB", '\\', true},
			{"%a%", "BAB", '\\', true},
			{"%a", "AAA", '\\', true},
			{"b%", "BBB", '\\', true},
		*/
	}
	for _, v := range tbl {
		patChars, patTypes := CompilePattern(v.pattern, v.escape)
		match := DoMatch(v.input, patChars, patTypes)
		c.Assert(match, Equals, v.match, Commentf("%v", v))
	}
}

func (s *testStringUtilSuite) TestCompileLike2Regexp(c *C) {
	defer testleak.AfterTest(c)()
	tbl := []struct {
		pattern string
		regexp  string
	}{
		{``, ``},
		{`a`, `a`},
		{`aA`, `aA`},
		{`_`, `.`},
		{`__`, `..`},
		{`%`, `.*`},
		{`%b`, `.*b`},
		{`%a%`, `.*a.*`},
		{`a%`, `a.*`},
		{`\%a`, `%a`},
		{`\_a`, `_a`},
		{`\\_a`, `\.a`},
		{`\a\b`, `\a\b`},
		{`%%_`, `..*`},
		{`%_%_aA`, "...*aA"},
	}
	for _, v := range tbl {
		result := CompileLike2Regexp(v.pattern)
		c.Assert(result, Equals, v.regexp, Commentf("%v", v))
	}
}

func (s *testStringUtilSuite) TestIsExactMatch(c *C) {
	defer testleak.AfterTest(c)()
	tbl := []struct {
		pattern    string
		escape     byte
		exactMatch bool
	}{
		{``, '\\', true},
		{`_`, '\\', false},
		{`%`, '\\', false},
		{`a`, '\\', true},
		{`a_`, '\\', false},
		{`a%`, '\\', false},
		{`a\_`, '\\', true},
		{`a\%`, '\\', true},
		{`a\\`, '\\', true},
		{`a\\_`, '\\', false},
		{`a+%`, '+', true},
		{`a\%`, '+', false},
		{`a++`, '+', true},
		{`a++_`, '+', false},
	}
	for _, v := range tbl {
		_, patTypes := CompilePattern(v.pattern, v.escape)
		c.Assert(IsExactMatch(patTypes), Equals, v.exactMatch, Commentf("%v", v))
	}
}

func (s *testStringUtilSuite) TestBuildStringFromLabels(c *C) {
	defer testleak.AfterTest(c)()
	testcases := []struct {
		name     string
		labels   map[string]string
		expected string
	}{
		{
			name:     "nil map",
			labels:   nil,
			expected: "",
		},
		{
			name: "one label",
			labels: map[string]string{
				"aaa": "bbb",
			},
			expected: "aaa=bbb",
		},
		{
			name: "two labels",
			labels: map[string]string{
				"aaa": "bbb",
				"ccc": "ddd",
			},
			expected: "aaa=bbb,ccc=ddd",
		},
	}
	for _, testcase := range testcases {
		c.Log(testcase.name)
		c.Assert(BuildStringFromLabels(testcase.labels), Equals, testcase.expected)
	}
}

func BenchmarkDoMatch(b *testing.B) {
	escape := byte('\\')
	tbl := []struct {
		pattern string
		target  string
	}{
		{`a%_%_%_%_b`, `aababab`},
		{`%_%_a%_%_b`, `bbbaaabb`},
		{`a%_%_a%_%_b`, `aaaabbbbbbaaaaaaaaabbbbb`},
	}

	for _, v := range tbl {
		b.Run(v.pattern, func(b *testing.B) {
			patChars, patTypes := CompilePattern(v.pattern, escape)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				match := DoMatch(v.target, patChars, patTypes)
				if !match {
					b.Fatal("Match expected.")
				}
			}
		})
	}
}

func BenchmarkDoMatchNegative(b *testing.B) {
	escape := byte('\\')
	tbl := []struct {
		pattern string
		target  string
	}{
		{`a%a%a%a%a%a%a%a%b`, `aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa`},
	}

	for _, v := range tbl {
		b.Run(v.pattern, func(b *testing.B) {
			patChars, patTypes := CompilePattern(v.pattern, escape)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				match := DoMatch(v.target, patChars, patTypes)
				if match {
					b.Fatal("Unmatch expected.")
				}
			}
		})
	}
}

func BenchmarkBuildStringFromLabels(b *testing.B) {
	cases := []struct {
		name   string
		labels map[string]string
	}{
		{
			name: "normal case",
			labels: map[string]string{
				"aaa": "bbb",
				"foo": "bar",
			},
		},
	}

	for _, testcase := range cases {
		b.Run(testcase.name, func(b *testing.B) {
			b.ResetTimer()
			BuildStringFromLabels(testcase.labels)
		})
	}
}
