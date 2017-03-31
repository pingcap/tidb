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
	"unicode"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/mysql"
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
		{".", int('.')},
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
	scanner := NewScanner(`_utf8"string"`)
	tok := scanner.Lex(&v)
	c.Check(tok, Equals, underscoreCS)
	tok = scanner.Lex(&v)
	c.Check(tok, Equals, stringLit)

	scanner.reset("N'string'")
	tok = scanner.Lex(&v)
	c.Check(tok, Equals, underscoreCS)
	tok = scanner.Lex(&v)
	c.Check(tok, Equals, stringLit)
}

func (s *testLexerSuite) TestLiteral(c *C) {
	defer testleak.AfterTest(c)()
	table := []testCaseItem{
		{`'''a'''`, stringLit},
		{`''a''`, stringLit},
		{`""a""`, stringLit},
		{`\'a\'`, int('\\')},
		{`\"a\"`, int('\\')},
		{"0.2314", decLit},
		{"1234567890123456789012345678901234567890", decLit},
		{"132.313", decLit},
		{"132.3e231", floatLit},
		{"132.3e-231", floatLit},
		{"23416", intLit},
		{"123test", identifier},
		{"123" + string(unicode.ReplacementChar) + "xxx", identifier},
		{"0", intLit},
		{"0x3c26", hexLit},
		{"x'13181C76734725455A'", hexLit},
		{"0b01", bitLit},
		{fmt.Sprintf("%c", 0), invalid},
		{fmt.Sprintf("t1%c", 0), identifier},
		{".*", int('.')},
		{".1_t_1_x", int('.')},
		{"N'some text'", underscoreCS},
		{"n'some text'", underscoreCS},
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
		{"/*!40101 SET character_set_client = utf8 */;", set},
		{"/*+ BKA(t1) */", hintBegin},
		{"/* SET character_set_client = utf8 */;", int(';')},
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

func (s *testLexerSuite) TestscanQuotedIdent(c *C) {
	defer testleak.AfterTest(c)()
	l := NewScanner("`fk`")
	l.r.peek()
	tok, pos, lit := scanQuotedIdent(l)
	c.Assert(pos.Offset, Equals, 0)
	c.Assert(tok, Equals, quotedIdentifier)
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
		{`'a' " " "string"`, "a string"},
		{`'\x\B'`, "xB"},
		{`'\0\'\"\b\n\r\t\\'`, "\000'\"\b\n\r\t\\"},
		{`'\Z'`, string(26)},
		{`'\%\_'`, `\%\_`},
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
		{"'한국의中文UTF8およびテキストトラック'", "한국의中文UTF8およびテキストトラック"},
		{"'\\a\x90'", "a\x90"},
		{`"\aèàø»"`, `aèàø»`},
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
	replacementString := string(unicode.ReplacementChar) + "xxx"
	table := [][2]string{
		{`哈哈`, "哈哈"},
		{"`numeric`", "numeric"},
		{"\r\n \r \n \tthere\t \n", "there"},
		{`5number`, `5number`},
		{"1_x", "1_x"},
		{"0_x", "0_x"},
		{replacementString, replacementString},
		{fmt.Sprintf("t1%cxxx", 0), "t1"},
	}
	l := &Scanner{}
	for _, item := range table {
		l.reset(item[0])
		var v yySymType
		tok := l.Lex(&v)
		c.Assert(tok, Equals, identifier)
		c.Assert(v.ident, Equals, item[1])
	}
}

func (s *testLexerSuite) TestSpecialComment(c *C) {
	l := NewScanner("/*!40101 select\n5*/")
	tok, pos, lit := l.scan()
	c.Assert(tok, Equals, identifier)
	c.Assert(lit, Equals, "select")
	c.Assert(pos, Equals, Pos{0, 0, 9})

	tok, pos, lit = l.scan()
	c.Assert(tok, Equals, intLit)
	c.Assert(lit, Equals, "5")
	c.Assert(pos, Equals, Pos{1, 1, 16})
}

func (s *testLexerSuite) TestOptimizerHint(c *C) {
	l := NewScanner("  /*+ BKA(t1) */")
	tokens := []struct {
		tok int
		lit string
		pos int
	}{
		{hintBegin, "", 2},
		{identifier, "BKA", 6},
		{int('('), "(", 9},
		{identifier, "t1", 10},
		{int(')'), ")", 12},
		{hintEnd, "", 14},
	}
	for i := 0; ; i++ {
		tok, pos, lit := l.scan()
		if tok == 0 {
			return
		}
		c.Assert(tok, Equals, tokens[i].tok, Commentf("%d", i))
		c.Assert(lit, Equals, tokens[i].lit, Commentf("%d", i))
		c.Assert(pos.Offset, Equals, tokens[i].pos, Commentf("%d", i))
	}
}

func (s *testLexerSuite) TestInt(c *C) {
	tests := []struct {
		input  string
		expect uint64
	}{
		{"01000001783", 1000001783},
		{"00001783", 1783},
		{"0", 0},
		{"0000", 0},
		{"01", 1},
		{"10", 10},
	}
	scanner := NewScanner("")
	for _, t := range tests {
		var v yySymType
		scanner.reset(t.input)
		tok := scanner.Lex(&v)
		c.Assert(tok, Equals, intLit)
		switch i := v.item.(type) {
		case int64:
			c.Assert(uint64(i), Equals, t.expect)
		case uint64:
			c.Assert(i, Equals, t.expect)
		default:
			c.Fail()
		}
	}
}

func (s *testLexerSuite) TestSQLModeANSIQuotes(c *C) {
	tests := []struct {
		input string
		tok   int
		ident string
	}{
		{`"identifier"`, identifier, "identifier"},
		{"`identifier`", identifier, "identifier"},
		{`"identifier""and"`, identifier, `identifier"and`},
		{`'string''string'`, stringLit, "string'string"},
		{`'string' 'string'`, stringLit, "stringstring"},
		{`"identifier"'and'`, identifier, "identifier"},
		{`'string'"identifier"`, stringLit, "string"},
	}
	scanner := NewScanner("")
	scanner.SetSQLMode(mysql.ModeANSIQuotes)
	for _, t := range tests {
		var v yySymType
		scanner.reset(t.input)
		tok := scanner.Lex(&v)
		c.Assert(tok, Equals, t.tok)
		c.Assert(v.ident, Equals, t.ident)
	}
}
