%{
package main
%}

%right '='
%precedence CAST
%left '('

%token AUTO BOOL BYTE DOUBLE FLOAT INT LONG SHORT SIGNED STRING UNSIGNED VOID

%token IDENTIFIER

%start file

%%

file
  : /* empty */
  | statement file
  ;

statement
  : expression ';'
  ;

expression
  : expression '=' expression
  | '(' type ')' expression %prec CAST
  | '(' expression ')'
  | IDENTIFIER
  ;

type
  : VOID
  | AUTO
  | BOOL
  | BYTE
  | SHORT
  | INT
  | LONG
  | FLOAT
  | DOUBLE
  | SIGNED
  | UNSIGNED
  | STRING
  | IDENTIFIER
  ;
