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
	"github.com/pingcap/parser/mysql"
)

var _ = Suite(&testLexerSuite{})

type testLexerSuite struct {
}

func (s *testLexerSuite) TestTokenID(c *C) {
	for str, tok := range tokenMap {
		l := NewScanner(str)
		var v yySymType
		tok1 := l.Lex(&v)
		c.Check(tok, Equals, tok1)
	}
}

func (s *testLexerSuite) TestSingleChar(c *C) {
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
	table := []testCaseItem{
		{"AT", identifier},
		{"?", paramMarker},
		{"PLACEHOLDER", identifier},
		{"=", eq},
		{".", int('.')},
	}
	runTest(c, table)
}

func (s *testLexerSuite) TestAtLeadingIdentifier(c *C) {
	table := []testCaseItem{
		{"@", singleAtIdentifier},
		{"@''", singleAtIdentifier},
		{"@1", singleAtIdentifier},
		{"@.1_", singleAtIdentifier},
		{"@-1.", singleAtIdentifier},
		{"@~", singleAtIdentifier},
		{"@$", singleAtIdentifier},
		{"@a_3cbbc", singleAtIdentifier},
		{"@`a_3cbbc`", singleAtIdentifier},
		{"@-3cbbc", singleAtIdentifier},
		{"@!3cbbc", singleAtIdentifier},
		{"@@global.test", doubleAtIdentifier},
		{"@@session.test", doubleAtIdentifier},
		{"@@local.test", doubleAtIdentifier},
		{"@@test", doubleAtIdentifier},
		{"@@global.`test`", doubleAtIdentifier},
		{"@@session.`test`", doubleAtIdentifier},
		{"@@local.`test`", doubleAtIdentifier},
		{"@@`test`", doubleAtIdentifier},
	}
	runTest(c, table)
}

func (s *testLexerSuite) TestUnderscoreCS(c *C) {
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
		{"001e-12", floatLit},
		{"23416", intLit},
		{"123test", identifier},
		{"123" + string(unicode.ReplacementChar) + "xxx", identifier},
		{"0", intLit},
		{"0x3c26", hexLit},
		{"x'13181C76734725455A'", hexLit},
		{"0b01", bitLit},
		{fmt.Sprintf("t1%c", 0), identifier},
		{"N'some text'", underscoreCS},
		{"n'some text'", underscoreCS},
		{"\\N", null},
		{".*", int('.')},       // `.`, `*`
		{".1_t_1_x", int('.')}, // `.`, `1_t_1_x`
		{"9e9e", floatLit},     // 9e9e = 9e9 + e
		// Issue #3954
		{".1e23", floatLit}, // `.1e23`
		{".123", decLit},    // `.123`
		{".1*23", decLit},   // `.1`, `*`, `23`
		{".1,23", decLit},   // `.1`, `,`, `23`
		{".1 23", decLit},   // `.1`, `23`
		// TODO: See #3963. The following test cases do not test the ambiguity.
		{".1$23", int('.')},    // `.`, `1$23`
		{".1a23", int('.')},    // `.`, `1a23`
		{".1e23$23", int('.')}, // `.`, `1e23$23`
		{".1e23a23", int('.')}, // `.`, `1e23a23`
		{".1C23", int('.')},    // `.`, `1C23`
		{".1\u0081", int('.')}, // `.`, `1\u0081`
		{".1\uff34", int('.')}, // `.`, `1\uff34`
		{`b''`, bitLit},
		{`b'0101'`, bitLit},
		{`0b0101`, bitLit},
	}
	runTest(c, table)
}

func runTest(c *C, table []testCaseItem) {
	var val yySymType
	for _, v := range table {
		l := NewScanner(v.str)
		tok := l.Lex(&val)
		c.Check(tok, Equals, v.tok, Commentf(v.str))
	}
}

func (s *testLexerSuite) TestComment(c *C) {
	SpecialCommentsController.Register("test")
	table := []testCaseItem{
		{"-- select --\n1", intLit},
		{"/*!40101 SET character_set_client = utf8 */;", set},
		{"/* SET character_set_client = utf8 */;", int(';')},
		{"/* some comments */ SELECT ", selectKwd},
		{`-- comment continues to the end of line
SELECT`, selectKwd},
		{`# comment continues to the end of line
SELECT`, selectKwd},
		{"#comment\n123", intLit},
		{"--5", int('-')},
		{"--\nSELECT", selectKwd},
		{"--\tSELECT", 0},
		{"--\r\nSELECT", selectKwd},
		{"--", 0},

		// The odd behavior of '*/' inside conditional comment is the same as
		// that of MySQL.
		{"/*T![unsupported] '*/0 -- ' */", intLit}, // equivalent to 0
		{"/*T![test] '*/0 -- ' */", stringLit},     // equivalent to '*/0 -- '
	}
	runTest(c, table)
}

func (s *testLexerSuite) TestscanQuotedIdent(c *C) {
	l := NewScanner("`fk`")
	l.r.peek()
	tok, pos, lit := scanQuotedIdent(l)
	c.Assert(pos.Offset, Equals, 0)
	c.Assert(tok, Equals, quotedIdentifier)
	c.Assert(lit, Equals, "fk")
}

func (s *testLexerSuite) TestscanString(c *C) {
	table := []struct {
		raw    string
		expect string
	}{
		{`' \n\tTest String'`, " \n\tTest String"},
		{`'\x\B'`, "xB"},
		{`'\0\'\"\b\n\r\t\\'`, "\000'\"\b\n\r\t\\"},
		{`'\Z'`, "\x1a"},
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
	replacementString := string(unicode.ReplacementChar) + "xxx"
	table := [][2]string{
		{`哈哈`, "哈哈"},
		{"`numeric`", "numeric"},
		{"\r\n \r \n \tthere\t \n", "there"},
		{`5number`, `5number`},
		{"1_x", "1_x"},
		{"0_x", "0_x"},
		{replacementString, replacementString},
		{"9e", "9e"},
		{"0b", "0b"},
		{"0b123", "0b123"},
		{"0b1ab", "0b1ab"},
		{"0B01", "0B01"},
		{"0x", "0x"},
		{"0x7fz3", "0x7fz3"},
		{"023a4", "023a4"},
		{"9eTSs", "9eTSs"},
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
	c.Assert(pos, Equals, Pos{0, 9, 9})

	tok, pos, lit = l.scan()
	c.Assert(tok, Equals, intLit)
	c.Assert(lit, Equals, "5")
	c.Assert(pos, Equals, Pos{1, 1, 16})
}

func (s *testLexerSuite) TestFeatureIDsComment(c *C) {
	SpecialCommentsController.Register("auto_rand")
	l := NewScanner("/*T![auto_rand] auto_random(5) */")
	tok, pos, lit := l.scan()
	c.Assert(tok, Equals, identifier)
	c.Assert(lit, Equals, "auto_random")
	c.Assert(pos, Equals, Pos{0, 16, 16})
	tok, pos, lit = l.scan()
	c.Assert(tok, Equals, int('('))
	tok, pos, lit = l.scan()
	c.Assert(lit, Equals, "5")
	c.Assert(pos, Equals, Pos{0, 28, 28})
	tok, pos, lit = l.scan()
	c.Assert(tok, Equals, int(')'))

	l = NewScanner("/*T![unsupported_feature] unsupported(123) */")
	tok, pos, lit = l.scan()
	c.Assert(tok, Equals, 0)
}

func (s *testLexerSuite) TestOptimizerHint(c *C) {
	l := NewScanner("SELECT /*+ BKA(t1) */ 0;")
	tokens := []struct {
		tok   int
		ident string
		pos   int
	}{
		{selectKwd, "SELECT", 0},
		{hintComment, "/*+ BKA(t1) */", 7},
		{intLit, "0", 22},
		{';', ";", 23},
	}
	for i := 0; ; i++ {
		var sym yySymType
		tok := l.Lex(&sym)
		if tok == 0 {
			return
		}
		c.Assert(tok, Equals, tokens[i].tok, Commentf("%d", i))
		c.Assert(sym.ident, Equals, tokens[i].ident, Commentf("%d", i))
		c.Assert(sym.offset, Equals, tokens[i].pos, Commentf("%d", i))
	}
}

func (s *testLexerSuite) TestOptimizerHintAfterCertainKeywordOnly(c *C) {
	SpecialCommentsController.Register("test")
	tests := []struct {
		input  string
		tokens []int
	}{
		{
			input:  "SELECT /*+ hint */ *",
			tokens: []int{selectKwd, hintComment, '*', 0},
		},
		{
			input:  "UPDATE /*+ hint */",
			tokens: []int{update, hintComment, 0},
		},
		{
			input:  "INSERT /*+ hint */",
			tokens: []int{insert, hintComment, 0},
		},
		{
			input:  "REPLACE /*+ hint */",
			tokens: []int{replace, hintComment, 0},
		},
		{
			input:  "DELETE /*+ hint */",
			tokens: []int{deleteKwd, hintComment, 0},
		},
		{
			input:  "CREATE /*+ hint */",
			tokens: []int{create, hintComment, 0},
		},
		{
			input:  "/*+ hint */ SELECT *",
			tokens: []int{selectKwd, '*', 0},
		},
		{
			input:  "SELECT /* comment */ /*+ hint */ *",
			tokens: []int{selectKwd, hintComment, '*', 0},
		},
		{
			input:  "SELECT * /*+ hint */",
			tokens: []int{selectKwd, '*', 0},
		},
		{
			input:  "SELECT /*T![test] * */ /*+ hint */",
			tokens: []int{selectKwd, '*', 0},
		},
		{
			input:  "SELECT /*T![unsupported] * */ /*+ hint */",
			tokens: []int{selectKwd, hintComment, 0},
		},
		{
			input:  "SELECT /*+ hint1 */ /*+ hint2 */ *",
			tokens: []int{selectKwd, hintComment, '*', 0},
		},
		{
			input:  "SELECT * FROM /*+ hint */",
			tokens: []int{selectKwd, '*', from, 0},
		},
		{
			input:  "`SELECT` /*+ hint */",
			tokens: []int{identifier, 0},
		},
		{
			input:  "'SELECT' /*+ hint */",
			tokens: []int{stringLit, 0},
		},
	}

	for _, tc := range tests {
		scanner := NewScanner(tc.input)
		var sym yySymType
		for i := 0; ; i++ {
			tok := scanner.Lex(&sym)
			c.Assert(tok, Equals, tc.tokens[i], Commentf("input = [%s], i = %d", tc.input, i))
			if tok == 0 {
				break
			}
		}
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
	scanner.reset(`'string' 'string'`)
	var v yySymType
	tok := scanner.Lex(&v)
	c.Assert(tok, Equals, stringLit)
	c.Assert(v.ident, Equals, "string")
	tok = scanner.Lex(&v)
	c.Assert(tok, Equals, stringLit)
	c.Assert(v.ident, Equals, "string")
}

func (s *testLexerSuite) TestIllegal(c *C) {
	table := []testCaseItem{
		{"'", invalid},
		{"'fu", invalid},
		{"'\\n", invalid},
		{"'\\", invalid},
		{fmt.Sprintf("%c", 0), invalid},
		{"`", invalid},
		{`"`, invalid},
		{"@`", invalid},
		{"@'", invalid},
		{`@"`, invalid},
		{"@@`", invalid},
		{"@@global.`", invalid},
	}
	runTest(c, table)
}

func (s *testLexerSuite) TestVersionDigits(c *C) {
	tests := []struct {
		input    string
		min      int
		max      int
		nextChar rune
	}{
		{
			input:    "12345",
			min:      5,
			max:      5,
			nextChar: unicode.ReplacementChar,
		},
		{
			input:    "12345xyz",
			min:      5,
			max:      5,
			nextChar: 'x',
		},
		{
			input:    "1234xyz",
			min:      5,
			max:      5,
			nextChar: '1',
		},
		{
			input:    "123456",
			min:      5,
			max:      5,
			nextChar: '6',
		},
		{
			input:    "1234",
			min:      5,
			max:      5,
			nextChar: '1',
		},
		{
			input:    "",
			min:      5,
			max:      5,
			nextChar: unicode.ReplacementChar,
		},
		{
			input:    "1234567xyz",
			min:      5,
			max:      6,
			nextChar: '7',
		},
		{
			input:    "12345xyz",
			min:      5,
			max:      6,
			nextChar: 'x',
		},
		{
			input:    "12345",
			min:      5,
			max:      6,
			nextChar: unicode.ReplacementChar,
		},
		{
			input:    "1234xyz",
			min:      5,
			max:      6,
			nextChar: '1',
		},
	}

	scanner := NewScanner("")
	for _, t := range tests {
		comment := Commentf("input = %s", t.input)
		scanner.reset(t.input)
		scanner.scanVersionDigits(t.min, t.max)
		nextChar := scanner.r.readByte()
		c.Assert(nextChar, Equals, t.nextChar, comment)
	}
}

func (s *testLexerSuite) TestFeatureIDs(c *C) {
	tests := []struct {
		input      string
		featureIDs []string
		nextChar   rune
	}{
		{
			input:      "[feature]",
			featureIDs: []string{"feature"},
			nextChar:   unicode.ReplacementChar,
		},
		{
			input:      "[feature] xx",
			featureIDs: []string{"feature"},
			nextChar:   ' ',
		},
		{
			input:      "[feature1,feature2]",
			featureIDs: []string{"feature1", "feature2"},
			nextChar:   unicode.ReplacementChar,
		},
		{
			input:      "[feature1,feature2,feature3]",
			featureIDs: []string{"feature1", "feature2", "feature3"},
			nextChar:   unicode.ReplacementChar,
		},
		{
			input:      "[id_en_ti_fier]",
			featureIDs: []string{"id_en_ti_fier"},
			nextChar:   unicode.ReplacementChar,
		},
		{
			input:      "[invalid,    whitespace]",
			featureIDs: nil,
			nextChar:   '[',
		},
		{
			input:      "[unclosed_brac",
			featureIDs: nil,
			nextChar:   '[',
		},
		{
			input:      "unclosed_brac]",
			featureIDs: nil,
			nextChar:   'u',
		},
		{
			input:      "[invalid_comma,]",
			featureIDs: nil,
			nextChar:   '[',
		},
		{
			input:      "[,]",
			featureIDs: nil,
			nextChar:   '[',
		},
		{
			input:      "[]",
			featureIDs: nil,
			nextChar:   '[',
		},
	}
	scanner := NewScanner("")
	for _, t := range tests {
		comment := Commentf("input = %s", t.input)
		scanner.reset(t.input)
		featureIDs := scanner.scanFeatureIDs()
		c.Assert(featureIDs, DeepEquals, t.featureIDs, comment)
		nextChar := scanner.r.readByte()
		c.Assert(nextChar, Equals, t.nextChar, comment)
	}
}
