/*

This file is a modified excerpt from the GNU Bison Manual examples originally found here:
http://www.gnu.org/software/bison/manual/html_node/Infix-Calc.html#Infix-Calc

The Copyright License for the GNU Bison Manual can be found in the "fdl-1.3" file.

*/

/* Infix notation calculator.  */

%{

// +build ignore

package main

import (
    "bufio"
    "fmt"
    "math"
    "os"
)

%}

%union{
    value float64
}

%token	NUM

%left	'-' '+'
%left	'*' '/'
%left	NEG     /* negation--unary minus */
%right	'^'     /* exponentiation */

%type	<value>	NUM, exp

%% /* The grammar follows.  */

input:    /* empty */
        | input line
;

line:     '\n'
        | exp '\n'  { fmt.Printf("\t%.10g\n", $1) }
;

exp:      NUM                { $$ = $1          }
        | exp '+' exp        { $$ = $1 + $3     }
        | exp '-' exp        { $$ = $1 - $3     }
        | exp '*' exp        { $$ = $1 * $3     }
        | exp '/' exp        { $$ = $1 / $3     }
        | '-' exp  %prec NEG { $$ = -$2         }
        | exp '^' exp        { $$ = math.Pow($1, $3) }
        | '(' exp ')'        { $$ = $2;         }
;
%%

func main() {
    os.Exit(yyParse(newLexer(bufio.NewReader(os.Stdin))))
}
