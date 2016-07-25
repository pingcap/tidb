// Copyright (c) 2014 The lexer Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package lexer

import (
	"bufio"
	"bytes"
	"flag"
	"go/token"
	"io/ioutil"
	"os"
	"path/filepath"
	"runtime"
	"testing"
)

var (
	devFlag = flag.Bool("dev", false, "enable dev tests/utils/helpers")
	reFlag  = flag.String("re", "re", "regexp for some of the dev tests")
)

func init() {
	flag.Parse()
}

func TestScan(t *testing.T) {
	lexer, err := CompileLexer(
		nil,
		map[string]int{

			`/[ \n]+/`: -1,
			`A`:        11,
		},
		"",
		"test",
	)
	if err != nil {
		t.Fatal(10, err)
	}

	t.Log("lexer\n", lexer)

	s := lexer.Scanner("testsrc", bytes.NewBufferString(
		`
 A 
 B 
 `,
	))
	x, ok := s.Scan()
	if !ok {
		t.Fatal(20, x, s.TokenStart(), "-", s.Position(), s.Token())
	}

	if x != 11 {
		t.Fatal(30, x, 11, s.TokenStart(), "-", s.Position(), s.Token())
	}

	x, ok = s.Scan()
	if ok {
		t.Fatal(40, x, s.TokenStart(), "-", s.Position(), s.Token())
	}

	if x != 'B' {
		t.Fatal(50, x, 'B')
	}

	x, ok = s.Scan()
	if ok {
		t.Fatal(60)
	}

	if x != 0 {
		t.Fatal(70, x, 0)
	}
}

var lex = MustCompileLexer(
	nil,
	map[string]int{

		`/[ \t\n\r]+/`:                   -1, // token separator(s)
		`//\*([^*]|\*+[^*/])*\*+/|//.*/`: -2, // /*...*/, //...

		"identifier":    int(token.IDENT + 1000), // main
		"int_lit":       int(token.INT),          // 12345
		"float_lit":     int(token.FLOAT),        // 123.45
		"imaginary_lit": int(token.IMAG),         // 123.45i
		"char_lit":      int(token.CHAR),         // 'a'
		"string_lit":    int(token.STRING),       // "abc"

		`+`:             int(token.ADD),
		`-`:             int(token.SUB),
		`*`:             int(token.MUL),
		`/`:             int(token.QUO),
		`%`:             int(token.REM),
		`&`:             int(token.AND),
		`|`:             int(token.OR),
		`^`:             int(token.XOR),
		`<<`:            int(token.SHL),
		`>>`:            int(token.SHR),
		`&^`:            int(token.AND_NOT),
		`+=`:            int(token.ADD_ASSIGN),
		`-=`:            int(token.SUB_ASSIGN),
		`*=`:            int(token.MUL_ASSIGN),
		`/=`:            int(token.QUO_ASSIGN),
		`%=`:            int(token.REM_ASSIGN),
		`&=`:            int(token.AND_ASSIGN),
		`|=`:            int(token.OR_ASSIGN),
		`^=`:            int(token.XOR_ASSIGN),
		`<<=`:           int(token.SHL_ASSIGN),
		`>>=`:           int(token.SHR_ASSIGN),
		`&^=`:           int(token.AND_NOT_ASSIGN),
		`&&`:            int(token.LAND),
		`||`:            int(token.LOR),
		`<-`:            int(token.ARROW),
		`++`:            int(token.INC),
		`--`:            int(token.DEC),
		`==`:            int(token.EQL),
		`<`:             int(token.LSS),
		`>`:             int(token.GTR),
		`=`:             int(token.ASSIGN),
		`!`:             int(token.NOT),
		`!=`:            int(token.NEQ),
		`<=`:            int(token.LEQ),
		`>=`:            int(token.GEQ),
		`:=`:            int(token.DEFINE),
		`...`:           int(token.ELLIPSIS),
		`(`:             int(token.LPAREN),
		`[`:             int(token.LBRACK),
		`{`:             int(token.LBRACE),
		`,`:             int(token.COMMA),
		`.`:             int(token.PERIOD),
		`)`:             int(token.RPAREN),
		`]`:             int(token.RBRACK),
		`}`:             int(token.RBRACE),
		`;`:             int(token.SEMICOLON),
		`:`:             int(token.COLON),
		`/break/`:       int(token.BREAK),
		`/case/`:        int(token.CASE),
		`/chan/`:        int(token.CHAN),
		`/const/`:       int(token.CONST),
		`/continue/`:    int(token.CONTINUE),
		`/default/`:     int(token.DEFAULT),
		`/defer/`:       int(token.DEFER),
		`/else/`:        int(token.ELSE),
		`/fallthrough/`: int(token.FALLTHROUGH),
		`/for/`:         int(token.FOR),
		`/func/`:        int(token.FUNC),
		`/go/`:          int(token.GO),
		`/goto/`:        int(token.GOTO),
		`/if/`:          int(token.IF),
		`/import/`:      int(token.IMPORT),
		`/interface/`:   int(token.INTERFACE),
		`/map/`:         int(token.MAP),
		`/package/`:     int(token.PACKAGE),
		`/range/`:       int(token.RANGE),
		`/return/`:      int(token.RETURN),
		`/select/`:      int(token.SELECT),
		`/struct/`:      int(token.STRUCT),
		`/switch/`:      int(token.SWITCH),
		`/type/`:        int(token.TYPE),
		`/var/`:         int(token.VAR),
	},

	`
identifier    = letter { letter | unicode_digit } .
letter        = unicode_letter | "_" .
//decimal_digit = "0" … "9" .
octal_digit   = "0" … "7" .
//hex_digit     = "0" … "9" | "A" … "F" | "a" … "f" .
hex_digit     = "/[0-9a-fA-F]/" .

unicode_char         = "/.|\n/" /* an arbitrary Unicode code point */ .
unicode_digit        = "/\\p{Nd}/" /* a Unicode code point classified as "Digit" */ .
unicode_letter       = "/\\p{L}/" /* a Unicode code point classified as "Letter" */ .

int_lit     = decimal_lit | octal_lit | hex_lit .
//decimal_lit = ( "1" … "9" ) { decimal_digit } .
decimal_lit =  "/[1-9][0-9]*/" .
//octal_lit   = "0" { octal_digit } .
octal_lit   = "/0[0-7]*/" .
//hex_lit     = "0" ( "x" | "X" ) hex_digit { hex_digit } .
hex_lit     = "/0[xX][0-9a-fA-F]+/" .

float_lit = decimals "." [ decimals ] [ exponent ] |
	decimals exponent |
	"." decimals [ exponent ] .
//decimals  = decimal_digit { decimal_digit } .
decimals  = "/[0-9]+/" .
//exponent  = ( "e" | "E" ) [ "+" | "-" ] decimals .
exponent  = "/[eE][-+]?[0-9]+/" .

imaginary_lit = ( decimals | float_lit ) "i" .

char_lit               = "'" ( char_unicode_value | byte_value ) "'" .
char_unicode_value     = unicode_char | char_interpreter_value .
char_interpreter_value = little_u_value | big_u_value | char_escaped_char .
byte_value             = octal_byte_value | hex_byte_value .
octal_byte_value       = "\\" octal_digit octal_digit octal_digit .
hex_byte_value         = "\\" "x" hex_digit hex_digit .
little_u_value         = "\\" "u" hex_digit hex_digit hex_digit hex_digit .
big_u_value            = "\\" "U" hex_digit hex_digit hex_digit hex_digit hex_digit hex_digit hex_digit hex_digit .
char_escaped_char      = "\\" ( "'" | other_escaped_char ) .
//other_escaped_char     = "a" | "b" | "f" | "n" | "r" | "t" | "v" | "\\" .
other_escaped_char     = "/[abfnrtv\\\\]/" .

string_lit               = raw_string_lit | interpreted_string_lit .
raw_string_lit           = "/\x60[^\x60]*\x60/" .
interpreted_string_lit   = "\"" { string_unicode_value | byte_value } "\"" .
string_unicode_value     = string_unicode_char | string_interpreter_value .
string_unicode_char      = "/[^\"\n\r\\\\]/" /* an arbitrary Unicode code point within an interpreted string */ .
string_interpreter_value = little_u_value | big_u_value | string_escaped_char .
string_escaped_char      = "\\" ( "\"" | other_escaped_char ) .
		`,
	"Go",
)

func BenchmarkNFA(b *testing.B) {
	var v visitor
	for i := 0; i < b.N; i++ {
		v = visitor{s: lex.Scanner("test-go-scanner", nil)}
		filepath.Walk(runtime.GOROOT()+"/src", func(pth string, info os.FileInfo, err error) error {
			if err != nil {
				panic(err)
			}
			if !info.IsDir() {
				v.visitFile(pth, info)
			}
			return nil
		})
	}
	b.SetBytes(int64(b.N) * v.size)

}

func TestNFA(t *testing.T) {
	t.Logf("NFA states: %d", len(lex.nfa))
	fn := filepath.Join(runtime.GOROOT(), "src", stdlib, "fmt", "scan.go")
	f, err := ioutil.ReadFile(fn)
	if err != nil {
		t.Fatal(err)
	}

	s, tok := lex.Scanner(fn, bytes.NewBuffer(f)), 0
	for {
		x, ok := s.Scan()
		if x == 0 {
			break
		}

		if !ok {
			t.Fatalf("Scan fail: %d=%q %s-%s = %q", x, string(x), s.TokenStart(), s.Position(), string(s.Token()))
		}

		tok++
	}

	if tok == 0 {
		t.Fatal(10, "no tokens found")
	}

	t.Log(tok, "tokens in", fn)
}

type visitor struct {
	s        *Scanner
	count    int
	tokCount int
	size     int64
}

func (v *visitor) visitFile(path string, f os.FileInfo) {
	ok, err := filepath.Match("*.go", filepath.Base(path))
	if err != nil {
		panic(err)
	}

	if !ok {
		return
	}

	file, err := os.Open(path)
	if err != nil {
		panic(err)
	}

	defer file.Close()
	v.s.Include(path, bufio.NewReader(file))
	for {
		x, ok := v.s.Scan()
		if x == 0 {
			break
		}

		if !ok {
			return
		}

		v.tokCount++
	}

	v.count++
	v.size += f.Size()
}

func TestDevParse(t *testing.T) {
	if !*devFlag {
		return
	}

	var nfa Nfa
	_, _, err := nfa.ParseRE("test", *reFlag)
	if err != nil {
		t.Fatal(10, err)
	}

	t.Log(nfa)
}

// https://github.com/cznic/golex/issues/1
func TestBug1(t *testing.T) {
	data := []string{
		`[ \-\*]`,

		"[-]",
		"[-z]",
		// "[a-]", // invalid
		"[a-z]",

		"[\\-]",
		"[\\-z]",
		"[a\\-]",
		"[a\\-z]",
	}

	for i, s := range data {
		var nfa Nfa
		in, out, err := nfa.ParseRE("test", s)
		if err != nil {
			t.Fatal(i, s, err)
		}

		t.Logf("i: %d, s: %q, in: [%d], out: [%d]\nnfa: %s", i, s, in.Index, out.Index, nfa)
	}
}

func TestBug2(t *testing.T) {
	data := []string{
		`[+-]`,
		`[+\-]`,
		`[\+-]`,
		`[\+\-]`,

		`[+-z]`,
		`[+\-z]`,
		`[\+-z]`,
		`[\+\-z]`,
	}

	for i, s := range data {
		var nfa Nfa
		in, out, err := nfa.ParseRE("test", s)
		if err != nil {
			t.Fatal(i, s, err)
		}

		t.Logf("i: %d, s: %q, in: [%d], out: [%d]\nnfa: %s", i, s, in.Index, out.Index, nfa)
	}
}
