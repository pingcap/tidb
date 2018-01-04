%{
// Copyright (c) 2011 CZ.NIC z.s.p.o. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// blame: jnml, labs.nic.cz

// +build ignore

package main

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"runtime"
	"unicode"
	"unicode/utf8"
)

var (
	src      []byte
	srclen   int
	pos      int
	pos0     int
	line     int
	column   int
	current  byte
	sc       int
)

const (
	SEP = 0xE000 + iota
	PUNCT
	KWD
	IDENT
	STRING
	CHAR
	INT
	FLOAT
	IMAG
)

func getc() byte {
	pos++
	if pos >= srclen {
		current = 0
		return 0
	}

	current = src[pos]
	if current == '\n' {
		line++
		column = 1
	} else {
		column++
	}
	return current
}

func begin(cond int) {
	sc = cond
}

func getRune() rune {
	if rune, size := utf8.DecodeRune(src[pos:]); size != 0 {
		pos += size
		return rune
	}

	return 0
}

func scan() (x int) {
	const (
		INITIAL = iota
		QSTR
		CH
	)

	c := current
%}

%yyt sc
%yyc c
%yyn c = getc()

%x QSTR CH

int_lit         {decimal_lit}|{octal_lit}|{hex_lit}
decimal_lit     [1-9][0-9]*
octal_lit       0[0-7]*
hex_lit         0[xX][0-9a-fA-F]+

float_lit       {decimals}"."{decimals}?{exponent}?|{decimals}{exponent}|"."{decimals}{exponent}?
decimals        [0-9]+
exponent        [eE][-+]?[0-9]+

imaginary_lit   ({decimals}|{float_lit})i

utf8_h2         [\xC2-\xDF]
utf8_h3         [\xE0-\xEF]
utf8_h4         [\xF0-\xF4]
utf8_cont       [\x80-\xBF]
utf8_2          {utf8_h2}{utf8_cont}
utf8_3          {utf8_h3}{utf8_cont}{utf8_cont}
utf8_4          {utf8_h4}{utf8_cont}{utf8_cont}{utf8_cont}
non_ascii       {utf8_2}|{utf8_3}|{utf8_4}

interpreted_string_lit      ({string_unicode_value}|{byte_value})*"\""
string_unicode_value        {string_unicode_char}|{string_interpreter_value}
string_unicode_char         [^"\x80-\xFF\\\n\r]|{non_ascii}
string_interpreter_value    {little_u_value}|{big_u_value}|{string_escaped_char}
little_u_value              "\u"{hex_digit}{hex_digit}{hex_digit}{hex_digit}
hex_digit                   [0-9a-fA-F]
big_u_value                 "\U"{hex_digit}{hex_digit}{hex_digit}{hex_digit}{hex_digit}{hex_digit}{hex_digit}{hex_digit}
string_escaped_char         \\(\"|{other_escaped_char})
other_escaped_char          [abfnrtv\\]
byte_value                  {octal_byte_value}|{hex_byte_value}
octal_byte_value            \\{octal_digit}{octal_digit}{octal_digit}
octal_digit                 [0-7]
hex_byte_value              "\x"{hex_digit}{hex_digit}
char_unicode_value          [^'\x80-\xFF\\\n\r]|{char_interpreter_value}|{non_ascii}
char_interpreter_value      {little_u_value}|{big_u_value}|{char_escaped_char}
char_escaped_char           \\('|{other_escaped_char})

%%

	pos0 = pos

<*>\0	return 0

[ \t\n\r]+  |
\/\/.*      |
\/\*([^*]|\*+[^*/])*\*+\/

"+"     |
"-"     |
"*"     |
"/"     |
"%"     |
"&"     |
"|"     |
"^"     |
"<<"    |
">>"    |
"&^"    |
"+="    |
"-="    |
"*="    |
"/="    |
"%="    |
"&="    |
"|="    |
"^="    |
"<<="   |
">>="   |
"&^="   |
"&&"    |
"||"    |
"<-"    |
"++"    |
"--"    |
"=="    |
"<"     |
">"     |
"="     |
"!"     |
"!="    |
"<="    |
">="    |
":="    |
"..."   |
"("     |
"["     |
"{"     |
","     |
"."     |
")"     |
"]"     |
"}"     |
";"     |
":"     return PUNCT

break       |
case        |
chan        |
const       |
continue    |
default     |
defer       |
else        |
fallthrough |
for         |
func        |
go          |
goto        |
if          |
import      |
interface   |
map         |
package     |
range       |
return      |
select      |
struct      |
switch      |
type        |
var         return KWD

\"	begin(QSTR)

<QSTR>{interpreted_string_lit}
	begin(INITIAL)
	return STRING

`[^`]*`	return STRING

'	begin(CH)

<CH>({char_unicode_value}|{byte_value})'
	begin(INITIAL)
	return CHAR 

{int_lit}       return INT
{float_lit}     return FLOAT
{imaginary_lit} return IMAG

[a-zA-Z_][a-zA-Z_0-9]*
	if current >= '\xC2' && current <= '\xF4' {
		pos--
		column--
		for {
			l, c, runepos := line, column, pos
			rune := getRune()
			if !(rune == '_' || unicode.IsLetter(rune) || unicode.IsDigit(rune)) {
				pos = runepos
				current = getc()
				line, column = l, c
				break
			}
		}
	}
	return IDENT

{non_ascii}
	pos = pos0
	if rune := getRune(); !unicode.IsLetter(rune) {
		panic(fmt.Errorf("expected unicode letter, got %U", rune))
	}

	for {
		l, c, runepos := line, column, pos
		rune := getRune()
		if !(rune == '_' || unicode.IsLetter(rune) || unicode.IsDigit(rune)) {
			pos = runepos
			current = getc()
			line, column = l, c
			break
		}
	}
	return IDENT

%%
	return unicode.ReplacementChar
}

type visitor struct {
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
	src, err = ioutil.ReadAll(file)
	if err != nil {
		panic(err)
	}
    
	defer func() {
		if e := recover(); e != nil {
			e = fmt.Errorf("%s:%d:%d - scan fail(%s)", path, line, column, e)
			fmt.Println(e)
			os.Exit(1)
		}
	}()

	srclen = len(src)
	pos = 0
	line, column = 1, 1
	current = src[0]
loop:
	for {
		switch x := scan(); {
		default:
			v.tokCount++
		case x == 0:
			break loop
		case x == unicode.ReplacementChar:
			break loop
		case x < SEP || x > IMAG:
			panic(fmt.Errorf("%s:%d:%d - scan fail", path, line, column))
		}
	}

	v.count++
	v.size += f.Size()
}


func main() {

	defer func() {
		if e := recover(); e != nil {
			fmt.Println(e)
			os.Exit(1)
		}
	}()

	v := &visitor{}
	if err := filepath.Walk(runtime.GOROOT()+"/src", func(pth string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		if !info.IsDir() {
			v.visitFile(pth, info)
		}
		return nil
	}); err != nil {
		panic(err)
	}

	fmt.Printf("%d .go files, %d bytes, %d tokens\n", v.count, v.size, v.tokCount)
}
