// Copyright 2013 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// This is an example of a goyacc program.
// To build it:
// go tool yacc -p "expr" expr.y (produces y.go)
// go build -o expr y.go
// expr
// > <type an expression>

%{

// This tag will be copied to the generated file to prevent that file
// confusing a future build.

// +build ignore

package main

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"log"
	"math/big"
	"os"
	"unicode/utf8"
)

%}

%union {
	num *big.Rat
}

%type	<num>	expr expr1 expr2 expr3

%token	<num>	NUM

%%

top:
	expr
	{
		if $1.IsInt() {
			fmt.Println($1.Num().String())
		} else {
			fmt.Println($1.String())
		}
	}

expr:
	expr1
|	'+' expr
	{
		$$ = $2
	}
|	'-' expr
	{
		$$.Neg($2)
	}

expr1:
	expr2
|	expr1 '+' expr2
	{
		$$.Add($1, $3)
	}
|	expr1 '-' expr2
	{
		$$.Sub($1, $3)
	}

expr2:
	expr3
|	expr2 '*' expr3
	{
		$$.Mul($1, $3)
	}
|	expr2 '/' expr3
	{
		$$.Quo($1, $3)
	}

expr3:
	NUM
|	'(' expr ')'
	{
		$$ = $2
	}

%%

/*

state 0
	$accept: .top $end 

	NUM  shift 8
	+  shift 4
	-  shift 5
	(  shift 9
	.  error

	expr  goto 2
	expr1  goto 3
	expr2  goto 6
	expr3  goto 7
	top  goto 1

state 1
	$accept:  top.$end 

	$end  accept
	.  error


state 2
	top:  expr.    (1)

	.  reduce 1 (src line 44)


state 3
	expr:  expr1.    (2)
	expr1:  expr1.+ expr2 
	expr1:  expr1.- expr2 

	+  shift 10
	-  shift 11
	.  reduce 2 (src line 54)


state 4
	expr:  +.expr 

	NUM  shift 8
	+  shift 4
	-  shift 5
	(  shift 9
	.  error

	expr  goto 12
	expr1  goto 3
	expr2  goto 6
	expr3  goto 7

state 5
	expr:  -.expr 

	NUM  shift 8
	+  shift 4
	-  shift 5
	(  shift 9
	.  error

	expr  goto 13
	expr1  goto 3
	expr2  goto 6
	expr3  goto 7

state 6
	expr1:  expr2.    (5)
	expr2:  expr2.* expr3 
	expr2:  expr2./ expr3 

	*  shift 14
	/  shift 15
	.  reduce 5 (src line 65)


state 7
	expr2:  expr3.    (8)

	.  reduce 8 (src line 76)


state 8
	expr3:  NUM.    (11)

	.  reduce 11 (src line 87)


state 9
	expr3:  (.expr ) 

	NUM  shift 8
	+  shift 4
	-  shift 5
	(  shift 9
	.  error

	expr  goto 16
	expr1  goto 3
	expr2  goto 6
	expr3  goto 7

state 10
	expr1:  expr1 +.expr2 

	NUM  shift 8
	(  shift 9
	.  error

	expr2  goto 17
	expr3  goto 7

state 11
	expr1:  expr1 -.expr2 

	NUM  shift 8
	(  shift 9
	.  error

	expr2  goto 18
	expr3  goto 7

state 12
	expr:  + expr.    (3)

	.  reduce 3 (src line 56)


state 13
	expr:  - expr.    (4)

	.  reduce 4 (src line 60)


state 14
	expr2:  expr2 *.expr3 

	NUM  shift 8
	(  shift 9
	.  error

	expr3  goto 19

state 15
	expr2:  expr2 /.expr3 

	NUM  shift 8
	(  shift 9
	.  error

	expr3  goto 20

state 16
	expr3:  ( expr.) 

	)  shift 21
	.  error


state 17
	expr1:  expr1 + expr2.    (6)
	expr2:  expr2.* expr3 
	expr2:  expr2./ expr3 

	*  shift 14
	/  shift 15
	.  reduce 6 (src line 67)


state 18
	expr1:  expr1 - expr2.    (7)
	expr2:  expr2.* expr3 
	expr2:  expr2./ expr3 

	*  shift 14
	/  shift 15
	.  reduce 7 (src line 71)


state 19
	expr2:  expr2 * expr3.    (9)

	.  reduce 9 (src line 78)


state 20
	expr2:  expr2 / expr3.    (10)

	.  reduce 10 (src line 82)


state 21
	expr3:  ( expr ).    (12)

	.  reduce 12 (src line 89)


10 terminals, 6 nonterminals
13 grammar rules, 22/2000 states
0 shift/reduce, 0 reduce/reduce conflicts reported
55 working sets used
memory: parser 22/30000
19 extra closures
33 shift entries, 1 exceptions
12 goto entries
11 entries saved by goto default
Optimizer space used: output 25/30000
25 table entries, 2 zero
maximum spread: 10, maximum offset: 15

*/

// The parser expects the lexer to return 0 on EOF.  Give it a name
// for clarity.
const eof = 0

// The parser uses the type <prefix>Lex as a lexer.  It must provide
// the methods Lex(*<prefix>SymType) int and Error(string).
type exprLex struct {
	line []byte
	peek rune
}

// The parser calls this method to get each new token.  This
// implementation returns operators and NUM.
func (x *exprLex) Lex(yylval *exprSymType) int {
	for {
		c := x.next()
		switch c {
		case eof:
			return eof
		case '0', '1', '2', '3', '4', '5', '6', '7', '8', '9':
			return x.num(c, yylval)
		case '+', '-', '*', '/', '(', ')':
			return int(c)

		// Recognize Unicode multiplication and division
		// symbols, returning what the parser expects.
		case '×':
			return '*'
		case '÷':
			return '/'

		case ' ', '\t', '\n', '\r':
		default:
			log.Printf("unrecognized character %q", c)
		}
	}
}

// Lex a number.
func (x *exprLex) num(c rune, yylval *exprSymType) int {
	add := func(b *bytes.Buffer, c rune) {
		if _, err := b.WriteRune(c); err != nil {
			log.Fatalf("WriteRune: %s", err)
		}
	}
	var b bytes.Buffer
	add(&b, c)
	L: for {
		c = x.next()
		switch c {
		case '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', '.', 'e', 'E':
			add(&b, c)
		default:
			break L
		}
	}
	if c != eof {
		x.peek = c
	}
	yylval.num = &big.Rat{}
	_, ok := yylval.num.SetString(b.String())
	if !ok {
		log.Printf("bad number %q", b.String())
		return eof
	}
	return NUM
}

// Return the next rune for the lexer.
func (x *exprLex) next() rune {
	if x.peek != eof {
		r := x.peek
		x.peek = eof
		return r
	}
	if len(x.line) == 0 {
		return eof
	}
	c, size := utf8.DecodeRune(x.line)
	x.line = x.line[size:]
	if c == utf8.RuneError && size == 1 {
		log.Print("invalid utf8")
		return x.next()
	}
	return c
}

// The parser calls this method on a parse error.
func (x *exprLex) Error(s string) {
	log.Printf("parse error: %s", s)
}

func main() {
	in := bufio.NewReader(os.Stdin)
	for {
		if _, err := os.Stdout.WriteString("> "); err != nil {
			log.Fatalf("WriteString: %s", err)
		}
		line, err := in.ReadBytes('\n')
		if err == io.EOF {
			return
		}
		if err != nil {
			log.Fatalf("ReadBytes: %s", err)
		}

		exprParse(&exprLex{line: line})
	}
}

/*

state 0
	$accept(0): . top $end
	top(1): . expr
	expr(2): . expr1
	expr(3): . '+' expr
	expr(4): . '-' expr
	expr1(5): . expr2
	expr1(6): . expr1 '+' expr2
	expr1(7): . expr1 '-' expr2
	expr2(8): . expr3
	expr2(9): . expr2 '*' expr3
	expr2(10): . expr2 '/' expr3
	expr3(11): . NUM
	expr3(12): . '(' expr ')'

state 1
	$accept(0): top . $end

state 2
	expr(2): . expr1
	expr(3): . '+' expr
	expr(4): . '-' expr
	expr1(5): . expr2
	expr1(6): . expr1 '+' expr2
	expr1(7): . expr1 '-' expr2
	expr2(8): . expr3
	expr2(9): . expr2 '*' expr3
	expr2(10): . expr2 '/' expr3
	expr3(11): . NUM
	expr3(12): . '(' expr ')'
	expr3(12): '(' . expr ')'

state 3
	expr3(12): '(' expr . ')'

state 4
	expr3(12): '(' expr ')' .

state 5
	expr3(11): NUM .

state 6
	expr2(8): expr3 .

state 7
	expr1(5): expr2 .
	expr2(9): expr2 . '*' expr3
	expr2(10): expr2 . '/' expr3

state 8
	expr(2): . expr1
	expr(3): . '+' expr
	expr(4): . '-' expr
	expr(4): '-' . expr
	expr1(5): . expr2
	expr1(6): . expr1 '+' expr2
	expr1(7): . expr1 '-' expr2
	expr2(8): . expr3
	expr2(9): . expr2 '*' expr3
	expr2(10): . expr2 '/' expr3
	expr3(11): . NUM
	expr3(12): . '(' expr ')'

state 9
	expr2(10): expr2 '/' . expr3
	expr3(11): . NUM
	expr3(12): . '(' expr ')'

state 10
	expr2(10): expr2 '/' expr3 .

state 11
	expr(4): '-' expr .

state 12
	expr(2): . expr1
	expr(3): . '+' expr
	expr(3): '+' . expr
	expr(4): . '-' expr
	expr1(5): . expr2
	expr1(6): . expr1 '+' expr2
	expr1(7): . expr1 '-' expr2
	expr2(8): . expr3
	expr2(9): . expr2 '*' expr3
	expr2(10): . expr2 '/' expr3
	expr3(11): . NUM
	expr3(12): . '(' expr ')'

state 13
	expr(2): expr1 .
	expr1(6): expr1 . '+' expr2
	expr1(7): expr1 . '-' expr2

state 14
	expr1(7): expr1 '-' . expr2
	expr2(8): . expr3
	expr2(9): . expr2 '*' expr3
	expr2(10): . expr2 '/' expr3
	expr3(11): . NUM
	expr3(12): . '(' expr ')'

state 15
	expr1(6): expr1 '+' . expr2
	expr2(8): . expr3
	expr2(9): . expr2 '*' expr3
	expr2(10): . expr2 '/' expr3
	expr3(11): . NUM
	expr3(12): . '(' expr ')'

state 16
	expr1(6): expr1 '+' expr2 .
	expr2(9): expr2 . '*' expr3
	expr2(10): expr2 . '/' expr3

state 17
	expr2(9): expr2 '*' . expr3
	expr3(11): . NUM
	expr3(12): . '(' expr ')'

state 18
	expr2(9): expr2 '*' expr3 .

state 19
	expr1(7): expr1 '-' expr2 .
	expr2(9): expr2 . '*' expr3
	expr2(10): expr2 . '/' expr3

state 20
	expr(3): '+' expr .

state 21
	top(1): expr .

===============================================================================

state 0
	$accept: .top $end 

	NUM  shift 8
	+  shift 4
	-  shift 5
	(  shift 9
	.  error

	expr  goto 2
	expr1  goto 3
	expr2  goto 6
	expr3  goto 7
	top  goto 1

state 1
	$accept:  top.$end 

	$end  accept
	.  error


state 2
	top:  expr.    (1)

	.  reduce 1 (src line 44)


state 3
	expr:  expr1.    (2)
	expr1:  expr1.+ expr2 
	expr1:  expr1.- expr2 

	+  shift 10
	-  shift 11
	.  reduce 2 (src line 54)


state 4
	expr:  +.expr 

	NUM  shift 8
	+  shift 4
	-  shift 5
	(  shift 9
	.  error

	expr  goto 12
	expr1  goto 3
	expr2  goto 6
	expr3  goto 7

state 5
	expr:  -.expr 

	NUM  shift 8
	+  shift 4
	-  shift 5
	(  shift 9
	.  error

	expr  goto 13
	expr1  goto 3
	expr2  goto 6
	expr3  goto 7

state 6
	expr1:  expr2.    (5)
	expr2:  expr2.* expr3 
	expr2:  expr2./ expr3 

	*  shift 14
	/  shift 15
	.  reduce 5 (src line 65)


state 7
	expr2:  expr3.    (8)

	.  reduce 8 (src line 76)


state 8
	expr3:  NUM.    (11)

	.  reduce 11 (src line 87)


state 9
	expr3:  (.expr ) 

	NUM  shift 8
	+  shift 4
	-  shift 5
	(  shift 9
	.  error

	expr  goto 16
	expr1  goto 3
	expr2  goto 6
	expr3  goto 7

state 10
	expr1:  expr1 +.expr2 

	NUM  shift 8
	(  shift 9
	.  error

	expr2  goto 17
	expr3  goto 7

state 11
	expr1:  expr1 -.expr2 

	NUM  shift 8
	(  shift 9
	.  error

	expr2  goto 18
	expr3  goto 7

state 12
	expr:  + expr.    (3)

	.  reduce 3 (src line 56)


state 13
	expr:  - expr.    (4)

	.  reduce 4 (src line 60)


state 14
	expr2:  expr2 *.expr3 

	NUM  shift 8
	(  shift 9
	.  error

	expr3  goto 19

state 15
	expr2:  expr2 /.expr3 

	NUM  shift 8
	(  shift 9
	.  error

	expr3  goto 20

state 16
	expr3:  ( expr.) 

	)  shift 21
	.  error


state 17
	expr1:  expr1 + expr2.    (6)
	expr2:  expr2.* expr3 
	expr2:  expr2./ expr3 

	*  shift 14
	/  shift 15
	.  reduce 6 (src line 67)


state 18
	expr1:  expr1 - expr2.    (7)
	expr2:  expr2.* expr3 
	expr2:  expr2./ expr3 

	*  shift 14
	/  shift 15
	.  reduce 7 (src line 71)


state 19
	expr2:  expr2 * expr3.    (9)

	.  reduce 9 (src line 78)


state 20
	expr2:  expr2 / expr3.    (10)

	.  reduce 10 (src line 82)


state 21
	expr3:  ( expr ).    (12)

	.  reduce 12 (src line 89)


10 terminals, 6 nonterminals
13 grammar rules, 22/2000 states
0 shift/reduce, 0 reduce/reduce conflicts reported
55 working sets used
memory: parser 22/30000
19 extra closures
33 shift entries, 1 exceptions
12 goto entries
11 entries saved by goto default
Optimizer space used: output 25/30000
25 table entries, 2 zero
maximum spread: 10, maximum offset: 15

*/
