%{
// Copyright 2015 The parser Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// CAUTION: Generated file (unless this is go.l) - DO NOT EDIT!

package parser

import (
	"fmt"

	"github.com/cznic/golex/lex"
)

func (l *lexer) scanGo() lex.Char {
	c := l.Enter()
%}

%yyc c
%yyn c = l.Next()
%yym l.Mark()
%yyt l.state

eof		\x80
other		\x81

intlit		[0-9]+
punct		[-!#%&()*+.:;<=>?@[-_|~]|"/"[^/\*]
runelit		'(\\[^\n\r\x80]|[^\\'\x80\n\r])*'
strlit		\x22(\\[^\n\r\x80]|[^\\\x22\x80\n\r])*\x22
white		[ \t\n\r]+
comment		"//"[^\x80\n\r]*|"/*"([^*\x80]|\*+[^*/\x80])*\*+\/

n		-?{intlit}
tag		[_a-zA-Z][_a-zA-Z0-9]*

%x DLR

%%

<*>"{"			return l.char('{')
<*>"}"			return l.char('}')

"$"			|
<*>{intlit}		|
<*>{punct}		|
<*>{runelit}		|
<*>{strlit}		|
<*>{white}		|
<*>{comment}		|
<*>{other}+		return l.char(' ')

<DLR>"$$"		return l.char(' ')
<DLR>"$"<{tag}>"$"	return l.char(' ')
<DLR>"$"{n}		return l.char(' ')
<DLR>"$"<{tag}>{n}	return l.char(' ')

%%
	if c, ok := l.Abort(); ok {
		return l.char(c)
	}

	goto yyAction
}
