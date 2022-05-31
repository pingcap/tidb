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
	"testing"
	"unicode"

	"github.com/pingcap/tidb/parser/mysql"
	requires "github.com/stretchr/testify/require"
)

func TestTokenID(t *testing.T) {
	for str, tok := range tokenMap {
		l := NewScanner(str)
		var v yySymType
		tok1 := l.Lex(&v)
		requires.Equal(t, tok1, tok)
	}
}

func TestSingleChar(t *testing.T) {
	table := []byte{'|', '&', '-', '+', '*', '/', '%', '^', '~', '(', ',', ')'}
	for _, tok := range table {
		l := NewScanner(string(tok))
		var v yySymType
		tok1 := l.Lex(&v)
		requires.Equal(t, tok1, int(tok))
	}
}

type testCaseItem struct {
	str string
	tok int
}

type testLiteralValue struct {
	str string
	val interface{}
}

func TestSingleCharOther(t *testing.T) {
	table := []testCaseItem{
		{"AT", identifier},
		{"?", paramMarker},
		{"PLACEHOLDER", identifier},
		{"=", eq},
		{".", int('.')},
	}
	runTest(t, table)
}

func TestAtLeadingIdentifier(t *testing.T) {
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
	runTest(t, table)
}

func TestUnderscoreCS(t *testing.T) {
	var v yySymType
	scanner := NewScanner(`_utf8"string"`)
	tok := scanner.Lex(&v)
	requires.Equal(t, underscoreCS, tok)
	tok = scanner.Lex(&v)
	requires.Equal(t, stringLit, tok)

	scanner.reset("N'string'")
	tok = scanner.Lex(&v)
	requires.Equal(t, underscoreCS, tok)
	tok = scanner.Lex(&v)
	requires.Equal(t, stringLit, tok)
}

func TestLiteral(t *testing.T) {
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
		{".*", int('.')},     // `.`, `*`
		{".1_t_1_x", decLit}, // `.1`, `_t_1_x`
		{"9e9e", floatLit},   // 9e9e = 9e9 + e
		{".1e", invalid},
		// Issue #3954
		{".1e23", floatLit},    // `.1e23`
		{".123", decLit},       // `.123`
		{".1*23", decLit},      // `.1`, `*`, `23`
		{".1,23", decLit},      // `.1`, `,`, `23`
		{".1 23", decLit},      // `.1`, `23`
		{".1$23", decLit},      // `.1`, `$23`
		{".1a23", decLit},      // `.1`, `a23`
		{".1e23$23", floatLit}, // `.1e23`, `$23`
		{".1e23a23", floatLit}, // `.1e23`, `a23`
		{".1C23", decLit},      // `.1`, `C23`
		{".1\u0081", decLit},   // `.1`, `\u0081`
		{".1\uff34", decLit},   // `.1`, `\uff34`
		{`b''`, bitLit},
		{`b'0101'`, bitLit},
		{`0b0101`, bitLit},
	}
	runTest(t, table)
}

func TestLiteralValue(t *testing.T) {
	table := []testLiteralValue{
		{`'''a'''`, `'a'`},
		{`''a''`, ``},
		{`""a""`, ``},
		{`\'a\'`, `\`},
		{`\"a\"`, `\`},
		{"0.2314", "0.2314"},
		{"1234567890123456789012345678901234567890", "1234567890123456789012345678901234567890"},
		{"132.313", "132.313"},
		{"132.3e231", 1.323e+233},
		{"132.3e-231", 1.323e-229},
		{"001e-12", 1e-12},
		{"23416", int64(23416)},
		{"123test", "123test"},
		{"123" + string(unicode.ReplacementChar) + "xxx", "123" + string(unicode.ReplacementChar) + "xxx"},
		{"0", int64(0)},
		{"0x3c26", "[60 38]"},
		{"x'13181C76734725455A'", "[19 24 28 118 115 71 37 69 90]"},
		{"0b01", "[1]"},
		{fmt.Sprintf("t1%c", 0), "t1"},
		{"N'some text'", "utf8"},
		{"n'some text'", "utf8"},
		{"\\N", `\N`},
		{".*", `.`},                   // `.`, `*`
		{".1_t_1_x", "0.1"},           // `.1`, `_t_1_x`
		{"9e9e", float64(9000000000)}, // 9e9e = 9e9 + e
		{".1e", ""},
		// Issue #3954
		{".1e23", float64(10000000000000000000000)}, // `.1e23`
		{".123", "0.123"}, // `.123`
		{".1*23", "0.1"},  // `.1`, `*`, `23`
		{".1,23", "0.1"},  // `.1`, `,`, `23`
		{".1 23", "0.1"},  // `.1`, `23`
		{".1$23", "0.1"},  // `.1`, `$23`
		{".1a23", "0.1"},  // `.1`, `a23`
		{".1e23$23", float64(10000000000000000000000)}, // `.1e23`, `$23`
		{".1e23a23", float64(10000000000000000000000)}, // `.1e23`, `a23`
		{".1C23", "0.1"},    // `.1`, `C23`
		{".1\u0081", "0.1"}, // `.1`, `\u0081`
		{".1\uff34", "0.1"}, // `.1`, `\uff34`
		{`b''`, "[]"},
		{`b'0101'`, "[5]"},
		{`0b0101`, "[5]"},
	}
	runLiteralTest(t, table)
}

func runTest(t *testing.T, table []testCaseItem) {
	var val yySymType
	for _, v := range table {
		l := NewScanner(v.str)
		tok := l.Lex(&val)
		requires.Equal(t, v.tok, tok, v.str)
	}
}

func runLiteralTest(t *testing.T, table []testLiteralValue) {
	for _, v := range table {
		l := NewScanner(v.str)
		val := l.LexLiteral()
		switch val.(type) {
		case int64:
			requires.Equal(t, v.val, val, v.str)
		case float64:
			requires.Equal(t, v.val, val, v.str)
		case string:
			requires.Equal(t, v.val, val, v.str)
		default:
			requires.Equal(t, v.val, fmt.Sprint(val), v.str)
		}
	}
}

func TestComment(t *testing.T) {
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
		{"/*T![unsupported] '*/0 -- ' */", intLit},  // equivalent to 0
		{"/*T![auto_rand] '*/0 -- ' */", stringLit}, // equivalent to '*/0 -- '
	}
	runTest(t, table)
}

func TestScanQuotedIdent(t *testing.T) {
	l := NewScanner("`fk`")
	l.r.peek()
	tok, pos, lit := scanQuotedIdent(l)
	requires.Zero(t, pos.Offset)
	requires.Equal(t, quotedIdentifier, tok)
	requires.Equal(t, "fk", lit)
}

func TestScanString(t *testing.T) {
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
		{"'\\a\x18èàø»\x05'", "a\x18èàø»\x05"},
	}

	for _, v := range table {
		l := NewScanner(v.raw)
		tok, pos, lit := l.scan()
		requires.Zero(t, pos.Offset)
		requires.Equal(t, stringLit, tok)
		requires.Equal(t, v.expect, lit)
	}
}

func TestScanStringWithNoBackslashEscapesMode(t *testing.T) {
	table := []struct {
		raw    string
		expect string
	}{
		{`' \n\tTest String'`, ` \n\tTest String`},
		{`'\x\B'`, `\x\B`},
		{`'\0\\''"\b\n\r\t\'`, `\0\\'"\b\n\r\t\`},
		{`'\Z'`, `\Z`},
		{`'\%\_'`, `\%\_`},
		{`'hello'`, "hello"},
		{`'"hello"'`, `"hello"`},
		{`'""hello""'`, `""hello""`},
		{`'hel''lo'`, "hel'lo"},
		{`'\'hello'`, `\`},
		{`"hello"`, "hello"},
		{`"'hello'"`, "'hello'"},
		{`"''hello''"`, "''hello''"},
		{`"hel""lo"`, `hel"lo`},
		{`"\"hello"`, `\`},
		{"'한국의中文UTF8およびテキストトラック'", "한국의中文UTF8およびテキストトラック"},
	}
	l := NewScanner("")
	l.SetSQLMode(mysql.ModeNoBackslashEscapes)
	for _, v := range table {
		l.reset(v.raw)
		tok, pos, lit := l.scan()
		requires.Zero(t, pos.Offset)
		requires.Equal(t, stringLit, tok)
		requires.Equal(t, v.expect, lit)
	}
}

func TestIdentifier(t *testing.T) {
	table := [][2]string{
		{`哈哈`, "哈哈"},
		{"`numeric`", "numeric"},
		{"\r\n \r \n \tthere\t \n", "there"},
		{`5number`, `5number`},
		{"1_x", "1_x"},
		{"0_x", "0_x"},
		{string(unicode.ReplacementChar) + "xxx", string(unicode.ReplacementChar) + "xxx"},
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
		requires.Equal(t, identifier, tok, item)
		requires.Equal(t, item[1], v.ident, item)
	}
}

func TestSpecialComment(t *testing.T) {
	l := NewScanner("/*!40101 select\n5*/")
	tok, pos, lit := l.scan()
	requires.Equal(t, identifier, tok)
	requires.Equal(t, "select", lit)
	requires.Equal(t, Pos{1, 9, 9}, pos)

	tok, pos, lit = l.scan()
	requires.Equal(t, intLit, tok)
	requires.Equal(t, "5", lit)
	requires.Equal(t, Pos{2, 1, 16}, pos)
}

func TestFeatureIDsComment(t *testing.T) {
	l := NewScanner("/*T![auto_rand] auto_random(5) */")
	tok, pos, lit := l.scan()
	requires.Equal(t, identifier, tok)
	requires.Equal(t, "auto_random", lit)
	requires.Equal(t, Pos{1, 16, 16}, pos)
	tok, pos, _ = l.scan()
	requires.Equal(t, int('('), tok)
	_, pos, lit = l.scan()
	requires.Equal(t, "5", lit)
	requires.Equal(t, Pos{1, 28, 28}, pos)
	tok, pos, _ = l.scan()
	requires.Equal(t, int(')'), tok)

	l = NewScanner("/*T![unsupported_feature] unsupported(123) */")
	tok, pos, _ = l.scan()
	requires.Equal(t, 0, tok)
}

func TestOptimizerHint(t *testing.T) {
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
		requires.Equal(t, tokens[i].tok, tok, i)
		requires.Equal(t, tokens[i].ident, sym.ident, i)
		requires.Equal(t, tokens[i].pos, sym.offset, i)
	}
}

func TestOptimizerHintAfterCertainKeywordOnly(t *testing.T) {
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
			input:  "SELECT /*T![auto_rand] * */ /*+ hint */",
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
			requires.Equalf(t, tc.tokens[i], tok, "input = [%s], i = %d", tc.input, i)
			if tok == 0 {
				break
			}
		}
	}
}

func TestInt(t *testing.T) {
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
	for _, test := range tests {
		var v yySymType
		scanner.reset(test.input)
		tok := scanner.Lex(&v)
		requires.Equal(t, intLit, tok)
		switch i := v.item.(type) {
		case int64:
			requires.Equal(t, test.expect, uint64(i))
		case uint64:
			requires.Equal(t, test.expect, i)
		default:
			t.Fail()
		}
	}
}

func TestSQLModeANSIQuotes(t *testing.T) {
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
	for _, test := range tests {
		var v yySymType
		scanner.reset(test.input)
		tok := scanner.Lex(&v)
		requires.Equal(t, test.tok, tok)
		requires.Equal(t, test.ident, v.ident)
	}
	scanner.reset(`'string' 'string'`)
	var v yySymType
	tok := scanner.Lex(&v)
	requires.Equal(t, stringLit, tok)
	requires.Equal(t, "string", v.ident)
	tok = scanner.Lex(&v)
	requires.Equal(t, stringLit, tok)
	requires.Equal(t, "string", v.ident)
}

func TestIllegal(t *testing.T) {
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
	runTest(t, table)
}

func TestVersionDigits(t *testing.T) {
	tests := []struct {
		input    string
		min      int
		max      int
		nextChar byte
	}{
		{
			input:    "12345",
			min:      5,
			max:      5,
			nextChar: 0,
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
			nextChar: 0,
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
			nextChar: 0,
		},
		{
			input:    "1234xyz",
			min:      5,
			max:      6,
			nextChar: '1',
		},
	}

	scanner := NewScanner("")
	for _, test := range tests {
		scanner.reset(test.input)
		scanner.scanVersionDigits(test.min, test.max)
		nextChar := scanner.r.readByte()
		requires.Equalf(t, test.nextChar, nextChar, "input = %s", test.input)
	}
}

func TestFeatureIDs(t *testing.T) {
	tests := []struct {
		input      string
		featureIDs []string
		nextChar   byte
	}{
		{
			input:      "[feature]",
			featureIDs: []string{"feature"},
			nextChar:   0,
		},
		{
			input:      "[feature] xx",
			featureIDs: []string{"feature"},
			nextChar:   ' ',
		},
		{
			input:      "[feature1,feature2]",
			featureIDs: []string{"feature1", "feature2"},
			nextChar:   0,
		},
		{
			input:      "[feature1,feature2,feature3]",
			featureIDs: []string{"feature1", "feature2", "feature3"},
			nextChar:   0,
		},
		{
			input:      "[id_en_ti_fier]",
			featureIDs: []string{"id_en_ti_fier"},
			nextChar:   0,
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
	for _, test := range tests {
		scanner.reset(test.input)
		featureIDs := scanner.scanFeatureIDs()
		requires.Equalf(t, test.featureIDs, featureIDs, "input = %s", test.input)
		nextChar := scanner.r.readByte()
		requires.Equalf(t, test.nextChar, nextChar, "input = %s", test.input)
	}
}
