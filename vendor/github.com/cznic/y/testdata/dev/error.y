// http://publib.boulder.ibm.com/infocenter/zvm/v6r1/index.jsp?topic=/com.ibm.zvm.v610.dmsp4/hcsp4c0022.htm

%{

// +build ignore

package main

import (
	"fmt"
	"os"
)

var variables [26]int

%}

%union {
	val int
}

%token <val> INTEGER VARIABLE

%type <val> expression

%left '+' '-'
%left '*' '/'

%%

program:
	program statement '\n'
|	program error '\n'		 { yyerrok() }
|	/* NULL */
;

statement:
	expression			 { fmt.Printf("\n\texpr: %d\n\n", $1) }
|	VARIABLE '=' expression 	 { variables[$1] = $3 }
;

expression:
	INTEGER
|	VARIABLE			 { $$ = variables[$1] }
|	expression '+' expression 	 { $$ = $1 + $3 }
|	expression '-' expression 	 { $$ = $1 - $3 }
|	expression '*' expression 	 { $$ = $1 * $3 }
|	expression '/' expression 	 { $$ = $1 / $3 }
|	'(' expression ')'		 { $$ = $2 }
;

%%

type item struct {
	tok, val int
}

type lx []item

func (lx *lx) Lex(lval *yySymType) int {
	a := *lx
	if len(a) == 0 {
		return -1
	}

	item := a[0]
	*lx = a[1:]
	lval.val = item.val
	return item.tok
}

func (lx *lx) Error(e string) { fmt.Fprintf(os.Stderr, "%s\n", e) }

func main() {
	lx := lx{
		{INTEGER, 42},
		{'+', -1},
		{INTEGER, 314},
		{'\n', -2},

		{INTEGER, 123},
		{'*', -3},
		{INTEGER, 456},
		{'\n', -4},

		{INTEGER, 24},
		{'+', -5},
		{'\n', -6},
		{-1, -7},
	}

	yyDebug = 6
	fmt.Printf("\nresult: %d\n", yyParse(&lx))
}
