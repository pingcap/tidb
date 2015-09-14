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

package parser

import (
	"fmt"
	"unicode"

	. "github.com/pingcap/check"
)

func tok2name(i int) string {
	if i == unicode.ReplacementChar {
		return "<?>"
	}

	if i < 128 {
		return fmt.Sprintf("tok-'%c'", i)
	}

	return fmt.Sprintf("tok-%d", i)
}

func (s *testParserSuite) TestScaner0(c *C) {
	table := []struct {
		src                         string
		tok, line, col, nline, ncol int
		val                         string
	}{
		{"a", identifier, 1, 1, 1, 2, "a"},
		{" a", identifier, 1, 2, 1, 3, "a"},
		{"a ", identifier, 1, 1, 1, 2, "a"},
		{" a ", identifier, 1, 2, 1, 3, "a"},
		{"\na", identifier, 2, 1, 2, 2, "a"},

		{"a\n", identifier, 1, 1, 1, 2, "a"},
		{"\na\n", identifier, 2, 1, 2, 2, "a"},
		{"\n a", identifier, 2, 2, 2, 3, "a"},
		{"a \n", identifier, 1, 1, 1, 2, "a"},
		{"\n a \n", identifier, 2, 2, 2, 3, "a"},

		{"ab", identifier, 1, 1, 1, 3, "ab"},
		{" ab", identifier, 1, 2, 1, 4, "ab"},
		{"ab ", identifier, 1, 1, 1, 3, "ab"},
		{" ab ", identifier, 1, 2, 1, 4, "ab"},
		{"\nab", identifier, 2, 1, 2, 3, "ab"},

		{"ab\n", identifier, 1, 1, 1, 3, "ab"},
		{"\nab\n", identifier, 2, 1, 2, 3, "ab"},
		{"\n ab", identifier, 2, 2, 2, 4, "ab"},
		{"ab \n", identifier, 1, 1, 1, 3, "ab"},
		{"\n ab \n", identifier, 2, 2, 2, 4, "ab"},

		{"c", identifier, 1, 1, 1, 2, "c"},
		{"cR", identifier, 1, 1, 1, 3, "cR"},
		{"cRe", identifier, 1, 1, 1, 4, "cRe"},
		{"cReA", identifier, 1, 1, 1, 5, "cReA"},
		{"cReAt", identifier, 1, 1, 1, 6, "cReAt"},

		{"cReATe", create, 1, 1, 1, 7, "cReATe"},
		{"cReATeD", identifier, 1, 1, 1, 8, "cReATeD"},
		{"2", intLit, 1, 1, 1, 2, "2"},
		{"2.", floatLit, 1, 1, 1, 3, "2."},
		{"2.3", floatLit, 1, 1, 1, 4, "2.3"},
	}

	lval := &yySymType{}
	for _, t := range table {
		l := NewLexer(t.src)
		tok := l.Lex(lval)
		nline, ncol := l.npos()
		val := string(l.val)

		c.Assert(tok, Equals, t.tok)
		c.Assert(l.line, Equals, t.line)
		c.Assert(l.col, Equals, t.col)
		c.Assert(nline, Equals, t.nline)
		c.Assert(ncol, Equals, t.ncol)
		c.Assert(val, Equals, t.val)
	}
}
