// Copyright 2016 PingCAP, Inc.
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

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/util/testleak"
)

var _ = Suite(&testLexerSuite{})

type testLexerSuite struct {
}

func (s *testLexerSuite) TestTokenID(c *C) {
	defer testleak.AfterTest(c)()
	for str, tok := range tokenMap {
		l := NewScanner(str)
		var v yySymType
		tok1 := l.Lex(&v)
		c.Check(tok, Equals, tok1)
	}
}

func (s *testLexerSuite) TestSingleChar(c *C) {
	defer testleak.AfterTest(c)()
	table := []byte{'|', '&', '-', '+', '*', '/', '%', '^', '~', '(', ',', ')'}
	for _, tok := range table {
		l := NewScanner(string(tok))
		var v yySymType
		tok1 := l.Lex(&v)
		c.Check(int(tok), Equals, tok1)
	}
}

type testCaseItem struct {
	str string
	tok int
}

func (s *testLexerSuite) TestSingleCharOther(c *C) {
	defer testleak.AfterTest(c)()
	table := []testCaseItem{
		{"@", at},
		{"AT", identifier},
		{"?", placeholder},
		{"PLACEHOLDER", identifier},
		{"=", eq},
	}
	runTest(c, table)
}

func (s *testLexerSuite) TestSysOrUserVar(c *C) {
	defer testleak.AfterTest(c)()
	table := []testCaseItem{
		{"@a_3cbbc", userVar},
		{"@-3cbbc", at},
		{"@@global.test", sysVar},
		{"@@session.test", sysVar},
		{"@@local.test", sysVar},
		{"@@test", sysVar},
	}
	runTest(c, table)
}

func (s *testLexerSuite) TestUnderscoreCS(c *C) {
	defer testleak.AfterTest(c)()
	var v yySymType
	tok := NewLexer(`_utf8"string"`).Lex(&v)
	c.Check(tok, Equals, underscoreCS)
}

func (s *testLexerSuite) TestLiteral(c *C) {
	defer testleak.AfterTest(c)()
	table := []testCaseItem{
		{`'''a'''`, stringLit},
		{`''a''`, stringLit},
		{`""a""`, stringLit},
		{`\'a\'`, int('\\')},
		{`\"a\"`, int('\\')},
		{"0.2314", floatLit},
		{"132.3e231", floatLit},
		{"23416", intLit},
		{"0", intLit},
		{"0x3c26", hexLit},
		{"0b01", bitLit},
	}
	runTest(c, table)
}

func runTest(c *C, table []testCaseItem) {
	var val yySymType
	for _, v := range table {
		l := NewScanner(v.str)
		tok := l.Lex(&val)
		c.Check(tok, Equals, v.tok)
	}
}

func (s *testLexerSuite) TestComment(c *C) {
	defer testleak.AfterTest(c)()

	table := []testCaseItem{
		{"-- select --\n1", intLit},
		{"/*!40101 SET character_set_client = utf8 */;", int(';')},
		{"/* some comments */ SELECT ", selectKwd},
		{`-- comment continues to the end of line
SELECT`, selectKwd},
		{`# comment continues to the end of line
SELECT`, selectKwd},
		{"#comment\n123", intLit},
		{"--5", int('-')},
	}
	runTest(c, table)
}

func (s *testLexerSuite) TestLexerCompatible(c *C) {
	defer testleak.AfterTest(c)()

	for _, str := range tableCompatible {
		l1 := NewScanner(str)
		l2 := NewLexer(str)
		for {
			var v1, v2 yySymType
			tok1 := l1.Lex(&v1)
			tok2 := l2.Lex(&v2)
			fmt.Println(tok1, tok2, v1, v2)
			c.Assert(tok1, Equals, tok2)
			if tok1 == 0 {
				break
			}
		}
	}
}

func (s *testLexerSuite) TestscanQuotedIdent(c *C) {
	defer testleak.AfterTest(c)()
	l := NewScanner("`fk`")
	l.r.peek()
	tok, pos, lit := scanQuotedIdent(l)
	c.Assert(pos.Offset, Equals, 0)
	c.Assert(tok, Equals, identifier)
	c.Assert(lit, Equals, "fk")
}

func (s *testLexerSuite) TestscanString(c *C) {
	defer testleak.AfterTest(c)()
	table := []struct {
		raw    string
		expect string
	}{
		{`' \n\tTest String'`, " \n\tTest String"},
		{`'a' ' ' 'string'`, "a string"},
		{`'\x\B'`, "xB"},
		{`'\0\'\"\b\n\r\t\\'`, "\000'\"\b\n\r\t\\"},
		{`'\Z'`, string(26)},
		{`'hello'`, "hello"},
		{`'"hello"'`, `"hello"`},
		{`'""hello""'`, `""hello""`},
		{`'hel''lo'`, "hel'lo"},
		{`'\'hello'`, "'hello"},
		{`"hello"`, "hello"},
		{`"'hello'"`, "'hello'"},
		{`"''hello''"`, "''hello''"},
		{`"hel""lo"`, `hel"lo`},
		{`"\"hello"`, `"hello`},
		{`'disappearing\ backslash'`, "disappearing backslash"},
	}

	for _, v := range table {
		l := NewScanner(v.raw)
		tok, pos, lit := l.scan()
		c.Assert(tok, Equals, stringLit)
		c.Assert(pos.Offset, Equals, 0)
		c.Assert(lit, Equals, v.expect)
	}
}

func (s *testLexerSuite) TestIdentifier(c *C) {
	defer testleak.AfterTest(c)()
	table := []string{
		`哈哈`,
		// `5number`,
	}
	l := &Scanner{}
	for _, v := range table {
		l.reset(v)
		tok, _, lit := l.scan()
		c.Assert(tok, Equals, identifier)
		c.Assert(lit, Equals, v)
	}
}
