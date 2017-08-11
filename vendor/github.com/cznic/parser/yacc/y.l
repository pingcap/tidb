%{
// Copyright 2015 The parser Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// CAUTION: Generated file (unless this is y.l) - DO NOT EDIT!

package parser

import (
	"github.com/cznic/golex/lex"
)

func (l *lexer) scan() lex.Char {
	c := l.Enter()
%}

%yyc c
%yyn c = l.Next()
%yym l.Mark()

eof			\x80

identifier		{name}|{literal}
literal			'(\\[^\n\r\x80]|[^\\'\n\r\x80])*'
name			{name-first}{name-next}*
name-first		[_a-zA-Z]
name-next		{name-first}|[0-9.]
str-literal		\x22(\\[^\n\r\x80]|[^\\\x22\x80\n\r])*\x22

%%

	c = l.Rule0()

[ \n\r\t\f]

"//"[^\x80\n\r]*	|
"/*"([^*\x80]|\*+[^*/\x80])*\*+\/
		return l.char(COMMENT)

%"{"		return l.char(LCURL)
%"}"		return l.char(RCURL)
%%		return l.char(MARK)
%left		return l.char(LEFT)
%nonassoc	return l.char(NONASSOC)
%prec		return l.char(PREC)
%precedence	return l.char(PRECEDENCE)
%right		return l.char(RIGHT)
%start		return l.char(START)
%token		return l.char(TOKEN)
%type		return l.char(TYPE)
%union		return l.char(UNION)
%error-verbose	return l.char(ERROR_VERBOSE)

[0-9]+		return l.char(NUMBER)
{identifier}	return l.char(IDENTIFIER)
{str-literal}	return l.char(STRING_LITERAL)

%%
	if c, ok := l.Abort(); ok {
		return l.char(c)
	}

	goto yyAction
}
