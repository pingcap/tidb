// CAUTION: Generated file - DO NOT EDIT.

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

yystate0:
	yyrule := -1
	_ = yyrule

	switch yyt := l.state; yyt {
	default:
		panic(fmt.Errorf(`invalid start condition %d`, yyt))
	case 0: // start condition: INITIAL
		goto yystart1
	case 1: // start condition: DLR
		goto yystart20
	}

	goto yystate0 // silence unused label error
	goto yyAction // silence unused label error
yyAction:
	switch yyrule {
	case 1:
		goto yyrule1
	case 2:
		goto yyrule2
	case 3:
		goto yyrule3
	case 4:
		goto yyrule4
	case 5:
		goto yyrule5
	case 6:
		goto yyrule6
	case 7:
		goto yyrule7
	case 8:
		goto yyrule8
	case 9:
		goto yyrule9
	case 10:
		goto yyrule10
	case 11:
		goto yyrule11
	case 12:
		goto yyrule12
	case 13:
		goto yyrule13
	case 14:
		goto yyrule14
	}
	goto yystate1 // silence unused label error
yystate1:
	c = l.Next()
yystart1:
	switch {
	default:
		goto yyabort
	case c == '!' || c == '#' || c == '%' || c == '&' || c >= '(' && c <= '+' || c == '-' || c == '.' || c >= ':' && c <= '@' || c == '[' || c == '_' || c == '|' || c == '~':
		goto yystate3
	case c == '"':
		goto yystate4
	case c == '$':
		goto yystate7
	case c == '/':
		goto yystate11
	case c == '\'':
		goto yystate8
	case c == '\t' || c == '\n' || c == '\r' || c == ' ':
		goto yystate2
	case c == '\u0081':
		goto yystate19
	case c == '{':
		goto yystate17
	case c == '}':
		goto yystate18
	case c >= '0' && c <= '9':
		goto yystate16
	}

yystate2:
	c = l.Next()
	yyrule = 8
	l.Mark()
	switch {
	default:
		goto yyrule8
	case c == '\t' || c == '\n' || c == '\r' || c == ' ':
		goto yystate2
	}

yystate3:
	c = l.Next()
	yyrule = 5
	l.Mark()
	goto yyrule5

yystate4:
	c = l.Next()
	switch {
	default:
		goto yyabort
	case c == '"':
		goto yystate5
	case c == '\\':
		goto yystate6
	case c >= '\x01' && c <= '\t' || c == '\v' || c == '\f' || c >= '\x0e' && c <= '!' || c >= '#' && c <= '[' || c >= ']' && c <= '\u007f' || c >= '\u0081' && c <= 'ÿ':
		goto yystate4
	}

yystate5:
	c = l.Next()
	yyrule = 7
	l.Mark()
	goto yyrule7

yystate6:
	c = l.Next()
	switch {
	default:
		goto yyabort
	case c >= '\x01' && c <= '\t' || c == '\v' || c == '\f' || c >= '\x0e' && c <= '\u007f' || c >= '\u0081' && c <= 'ÿ':
		goto yystate4
	}

yystate7:
	c = l.Next()
	yyrule = 3
	l.Mark()
	goto yyrule3

yystate8:
	c = l.Next()
	switch {
	default:
		goto yyabort
	case c == '\'':
		goto yystate9
	case c == '\\':
		goto yystate10
	case c >= '\x01' && c <= '\t' || c == '\v' || c == '\f' || c >= '\x0e' && c <= '&' || c >= '(' && c <= '[' || c >= ']' && c <= '\u007f' || c >= '\u0081' && c <= 'ÿ':
		goto yystate8
	}

yystate9:
	c = l.Next()
	yyrule = 6
	l.Mark()
	goto yyrule6

yystate10:
	c = l.Next()
	switch {
	default:
		goto yyabort
	case c >= '\x01' && c <= '\t' || c == '\v' || c == '\f' || c >= '\x0e' && c <= '\u007f' || c >= '\u0081' && c <= 'ÿ':
		goto yystate8
	}

yystate11:
	c = l.Next()
	switch {
	default:
		goto yyabort
	case c == '*':
		goto yystate12
	case c == '/':
		goto yystate15
	case c >= '\x01' && c <= ')' || c >= '+' && c <= '.' || c >= '0' && c <= 'ÿ':
		goto yystate3
	}

yystate12:
	c = l.Next()
	switch {
	default:
		goto yyabort
	case c == '*':
		goto yystate13
	case c >= '\x01' && c <= ')' || c >= '+' && c <= '\u007f' || c >= '\u0081' && c <= 'ÿ':
		goto yystate12
	}

yystate13:
	c = l.Next()
	switch {
	default:
		goto yyabort
	case c == '*':
		goto yystate13
	case c == '/':
		goto yystate14
	case c >= '\x01' && c <= ')' || c >= '+' && c <= '.' || c >= '0' && c <= '\u007f' || c >= '\u0081' && c <= 'ÿ':
		goto yystate12
	}

yystate14:
	c = l.Next()
	yyrule = 9
	l.Mark()
	goto yyrule9

yystate15:
	c = l.Next()
	yyrule = 9
	l.Mark()
	switch {
	default:
		goto yyrule9
	case c >= '\x01' && c <= '\t' || c == '\v' || c == '\f' || c >= '\x0e' && c <= '\u007f' || c >= '\u0081' && c <= 'ÿ':
		goto yystate15
	}

yystate16:
	c = l.Next()
	yyrule = 4
	l.Mark()
	switch {
	default:
		goto yyrule4
	case c >= '0' && c <= '9':
		goto yystate16
	}

yystate17:
	c = l.Next()
	yyrule = 1
	l.Mark()
	goto yyrule1

yystate18:
	c = l.Next()
	yyrule = 2
	l.Mark()
	goto yyrule2

yystate19:
	c = l.Next()
	yyrule = 10
	l.Mark()
	switch {
	default:
		goto yyrule10
	case c == '\u0081':
		goto yystate19
	}

	goto yystate20 // silence unused label error
yystate20:
	c = l.Next()
yystart20:
	switch {
	default:
		goto yyabort
	case c == '!' || c == '#' || c == '%' || c == '&' || c >= '(' && c <= '+' || c == '-' || c == '.' || c >= ':' && c <= '@' || c == '[' || c == '_' || c == '|' || c == '~':
		goto yystate3
	case c == '"':
		goto yystate4
	case c == '$':
		goto yystate21
	case c == '/':
		goto yystate11
	case c == '\'':
		goto yystate8
	case c == '\t' || c == '\n' || c == '\r' || c == ' ':
		goto yystate2
	case c == '\u0081':
		goto yystate19
	case c == '{':
		goto yystate17
	case c == '}':
		goto yystate18
	case c >= '0' && c <= '9':
		goto yystate16
	}

yystate21:
	c = l.Next()
	switch {
	default:
		goto yyabort
	case c == '$':
		goto yystate22
	case c == '-':
		goto yystate23
	case c == '<':
		goto yystate25
	case c >= '0' && c <= '9':
		goto yystate24
	}

yystate22:
	c = l.Next()
	yyrule = 11
	l.Mark()
	goto yyrule11

yystate23:
	c = l.Next()
	switch {
	default:
		goto yyabort
	case c >= '0' && c <= '9':
		goto yystate24
	}

yystate24:
	c = l.Next()
	yyrule = 13
	l.Mark()
	switch {
	default:
		goto yyrule13
	case c >= '0' && c <= '9':
		goto yystate24
	}

yystate25:
	c = l.Next()
	switch {
	default:
		goto yyabort
	case c >= 'A' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'z':
		goto yystate26
	}

yystate26:
	c = l.Next()
	switch {
	default:
		goto yyabort
	case c == '>':
		goto yystate27
	case c >= '0' && c <= '9' || c >= 'A' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'z':
		goto yystate26
	}

yystate27:
	c = l.Next()
	switch {
	default:
		goto yyabort
	case c == '$':
		goto yystate28
	case c == '-':
		goto yystate29
	case c >= '0' && c <= '9':
		goto yystate30
	}

yystate28:
	c = l.Next()
	yyrule = 12
	l.Mark()
	goto yyrule12

yystate29:
	c = l.Next()
	switch {
	default:
		goto yyabort
	case c >= '0' && c <= '9':
		goto yystate30
	}

yystate30:
	c = l.Next()
	yyrule = 14
	l.Mark()
	switch {
	default:
		goto yyrule14
	case c >= '0' && c <= '9':
		goto yystate30
	}

yyrule1: // "{"
	{
		return l.char('{')
	}
yyrule2: // "}"
	{
		return l.char('}')
	}
yyrule3: // "$"
yyrule4: // {intlit}
yyrule5: // {punct}
yyrule6: // {runelit}
yyrule7: // {strlit}
yyrule8: // {white}
yyrule9: // {comment}
yyrule10: // {other}+
	{
		return l.char(' ')
	}
yyrule11: // "$$"
	{
		return l.char(' ')
	}
yyrule12: // "$"<{tag}>"$"
	{
		return l.char(' ')
	}
yyrule13: // "$"{n}
	{
		return l.char(' ')
	}
yyrule14: // "$"<{tag}>{n}
	{
		return l.char(' ')
	}
	panic("unreachable")

	goto yyabort // silence unused label error

yyabort: // no lexem recognized
	if c, ok := l.Abort(); ok {
		return l.char(c)
	}

	goto yyAction
}
