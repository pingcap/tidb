// CAUTION: Generated file - DO NOT EDIT.

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

yystate0:
	yyrule := -1
	_ = yyrule
	c = l.Rule0()

	goto yystart1

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
	case 15:
		goto yyrule15
	case 16:
		goto yyrule16
	case 17:
		goto yyrule17
	case 18:
		goto yyrule18
	case 19:
		goto yyrule19
	}
	goto yystate1 // silence unused label error
yystate1:
	c = l.Next()
yystart1:
	switch {
	default:
		goto yyabort
	case c == '"':
		goto yystate3
	case c == '%':
		goto yystate6
	case c == '/':
		goto yystate71
	case c == '\'':
		goto yystate68
	case c == '\t' || c == '\n' || c == '\f' || c == '\r' || c == ' ':
		goto yystate2
	case c >= '0' && c <= '9':
		goto yystate76
	case c >= 'A' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'z':
		goto yystate77
	}

yystate2:
	c = l.Next()
	yyrule = 1
	l.Mark()
	goto yyrule1

yystate3:
	c = l.Next()
	switch {
	default:
		goto yyabort
	case c == '"':
		goto yystate4
	case c == '\\':
		goto yystate5
	case c >= '\x01' && c <= '\t' || c == '\v' || c == '\f' || c >= '\x0e' && c <= '!' || c >= '#' && c <= '[' || c >= ']' && c <= '\u007f' || c >= '\u0081' && c <= 'ÿ':
		goto yystate3
	}

yystate4:
	c = l.Next()
	yyrule = 19
	l.Mark()
	goto yyrule19

yystate5:
	c = l.Next()
	switch {
	default:
		goto yyabort
	case c >= '\x01' && c <= '\t' || c == '\v' || c == '\f' || c >= '\x0e' && c <= '\u007f' || c >= '\u0081' && c <= 'ÿ':
		goto yystate3
	}

yystate6:
	c = l.Next()
	switch {
	default:
		goto yyabort
	case c == '%':
		goto yystate7
	case c == 'e':
		goto yystate8
	case c == 'l':
		goto yystate21
	case c == 'n':
		goto yystate25
	case c == 'p':
		goto yystate33
	case c == 'r':
		goto yystate43
	case c == 's':
		goto yystate48
	case c == 't':
		goto yystate53
	case c == 'u':
		goto yystate61
	case c == '{':
		goto yystate66
	case c == '}':
		goto yystate67
	}

yystate7:
	c = l.Next()
	yyrule = 6
	l.Mark()
	goto yyrule6

yystate8:
	c = l.Next()
	switch {
	default:
		goto yyabort
	case c == 'r':
		goto yystate9
	}

yystate9:
	c = l.Next()
	switch {
	default:
		goto yyabort
	case c == 'r':
		goto yystate10
	}

yystate10:
	c = l.Next()
	switch {
	default:
		goto yyabort
	case c == 'o':
		goto yystate11
	}

yystate11:
	c = l.Next()
	switch {
	default:
		goto yyabort
	case c == 'r':
		goto yystate12
	}

yystate12:
	c = l.Next()
	switch {
	default:
		goto yyabort
	case c == '-':
		goto yystate13
	}

yystate13:
	c = l.Next()
	switch {
	default:
		goto yyabort
	case c == 'v':
		goto yystate14
	}

yystate14:
	c = l.Next()
	switch {
	default:
		goto yyabort
	case c == 'e':
		goto yystate15
	}

yystate15:
	c = l.Next()
	switch {
	default:
		goto yyabort
	case c == 'r':
		goto yystate16
	}

yystate16:
	c = l.Next()
	switch {
	default:
		goto yyabort
	case c == 'b':
		goto yystate17
	}

yystate17:
	c = l.Next()
	switch {
	default:
		goto yyabort
	case c == 'o':
		goto yystate18
	}

yystate18:
	c = l.Next()
	switch {
	default:
		goto yyabort
	case c == 's':
		goto yystate19
	}

yystate19:
	c = l.Next()
	switch {
	default:
		goto yyabort
	case c == 'e':
		goto yystate20
	}

yystate20:
	c = l.Next()
	yyrule = 16
	l.Mark()
	goto yyrule16

yystate21:
	c = l.Next()
	switch {
	default:
		goto yyabort
	case c == 'e':
		goto yystate22
	}

yystate22:
	c = l.Next()
	switch {
	default:
		goto yyabort
	case c == 'f':
		goto yystate23
	}

yystate23:
	c = l.Next()
	switch {
	default:
		goto yyabort
	case c == 't':
		goto yystate24
	}

yystate24:
	c = l.Next()
	yyrule = 7
	l.Mark()
	goto yyrule7

yystate25:
	c = l.Next()
	switch {
	default:
		goto yyabort
	case c == 'o':
		goto yystate26
	}

yystate26:
	c = l.Next()
	switch {
	default:
		goto yyabort
	case c == 'n':
		goto yystate27
	}

yystate27:
	c = l.Next()
	switch {
	default:
		goto yyabort
	case c == 'a':
		goto yystate28
	}

yystate28:
	c = l.Next()
	switch {
	default:
		goto yyabort
	case c == 's':
		goto yystate29
	}

yystate29:
	c = l.Next()
	switch {
	default:
		goto yyabort
	case c == 's':
		goto yystate30
	}

yystate30:
	c = l.Next()
	switch {
	default:
		goto yyabort
	case c == 'o':
		goto yystate31
	}

yystate31:
	c = l.Next()
	switch {
	default:
		goto yyabort
	case c == 'c':
		goto yystate32
	}

yystate32:
	c = l.Next()
	yyrule = 8
	l.Mark()
	goto yyrule8

yystate33:
	c = l.Next()
	switch {
	default:
		goto yyabort
	case c == 'r':
		goto yystate34
	}

yystate34:
	c = l.Next()
	switch {
	default:
		goto yyabort
	case c == 'e':
		goto yystate35
	}

yystate35:
	c = l.Next()
	switch {
	default:
		goto yyabort
	case c == 'c':
		goto yystate36
	}

yystate36:
	c = l.Next()
	yyrule = 9
	l.Mark()
	switch {
	default:
		goto yyrule9
	case c == 'e':
		goto yystate37
	}

yystate37:
	c = l.Next()
	switch {
	default:
		goto yyabort
	case c == 'd':
		goto yystate38
	}

yystate38:
	c = l.Next()
	switch {
	default:
		goto yyabort
	case c == 'e':
		goto yystate39
	}

yystate39:
	c = l.Next()
	switch {
	default:
		goto yyabort
	case c == 'n':
		goto yystate40
	}

yystate40:
	c = l.Next()
	switch {
	default:
		goto yyabort
	case c == 'c':
		goto yystate41
	}

yystate41:
	c = l.Next()
	switch {
	default:
		goto yyabort
	case c == 'e':
		goto yystate42
	}

yystate42:
	c = l.Next()
	yyrule = 10
	l.Mark()
	goto yyrule10

yystate43:
	c = l.Next()
	switch {
	default:
		goto yyabort
	case c == 'i':
		goto yystate44
	}

yystate44:
	c = l.Next()
	switch {
	default:
		goto yyabort
	case c == 'g':
		goto yystate45
	}

yystate45:
	c = l.Next()
	switch {
	default:
		goto yyabort
	case c == 'h':
		goto yystate46
	}

yystate46:
	c = l.Next()
	switch {
	default:
		goto yyabort
	case c == 't':
		goto yystate47
	}

yystate47:
	c = l.Next()
	yyrule = 11
	l.Mark()
	goto yyrule11

yystate48:
	c = l.Next()
	switch {
	default:
		goto yyabort
	case c == 't':
		goto yystate49
	}

yystate49:
	c = l.Next()
	switch {
	default:
		goto yyabort
	case c == 'a':
		goto yystate50
	}

yystate50:
	c = l.Next()
	switch {
	default:
		goto yyabort
	case c == 'r':
		goto yystate51
	}

yystate51:
	c = l.Next()
	switch {
	default:
		goto yyabort
	case c == 't':
		goto yystate52
	}

yystate52:
	c = l.Next()
	yyrule = 12
	l.Mark()
	goto yyrule12

yystate53:
	c = l.Next()
	switch {
	default:
		goto yyabort
	case c == 'o':
		goto yystate54
	case c == 'y':
		goto yystate58
	}

yystate54:
	c = l.Next()
	switch {
	default:
		goto yyabort
	case c == 'k':
		goto yystate55
	}

yystate55:
	c = l.Next()
	switch {
	default:
		goto yyabort
	case c == 'e':
		goto yystate56
	}

yystate56:
	c = l.Next()
	switch {
	default:
		goto yyabort
	case c == 'n':
		goto yystate57
	}

yystate57:
	c = l.Next()
	yyrule = 13
	l.Mark()
	goto yyrule13

yystate58:
	c = l.Next()
	switch {
	default:
		goto yyabort
	case c == 'p':
		goto yystate59
	}

yystate59:
	c = l.Next()
	switch {
	default:
		goto yyabort
	case c == 'e':
		goto yystate60
	}

yystate60:
	c = l.Next()
	yyrule = 14
	l.Mark()
	goto yyrule14

yystate61:
	c = l.Next()
	switch {
	default:
		goto yyabort
	case c == 'n':
		goto yystate62
	}

yystate62:
	c = l.Next()
	switch {
	default:
		goto yyabort
	case c == 'i':
		goto yystate63
	}

yystate63:
	c = l.Next()
	switch {
	default:
		goto yyabort
	case c == 'o':
		goto yystate64
	}

yystate64:
	c = l.Next()
	switch {
	default:
		goto yyabort
	case c == 'n':
		goto yystate65
	}

yystate65:
	c = l.Next()
	yyrule = 15
	l.Mark()
	goto yyrule15

yystate66:
	c = l.Next()
	yyrule = 4
	l.Mark()
	goto yyrule4

yystate67:
	c = l.Next()
	yyrule = 5
	l.Mark()
	goto yyrule5

yystate68:
	c = l.Next()
	switch {
	default:
		goto yyabort
	case c == '\'':
		goto yystate69
	case c == '\\':
		goto yystate70
	case c >= '\x01' && c <= '\t' || c == '\v' || c == '\f' || c >= '\x0e' && c <= '&' || c >= '(' && c <= '[' || c >= ']' && c <= '\u007f' || c >= '\u0081' && c <= 'ÿ':
		goto yystate68
	}

yystate69:
	c = l.Next()
	yyrule = 18
	l.Mark()
	goto yyrule18

yystate70:
	c = l.Next()
	switch {
	default:
		goto yyabort
	case c >= '\x01' && c <= '\t' || c == '\v' || c == '\f' || c >= '\x0e' && c <= '\u007f' || c >= '\u0081' && c <= 'ÿ':
		goto yystate68
	}

yystate71:
	c = l.Next()
	switch {
	default:
		goto yyabort
	case c == '*':
		goto yystate72
	case c == '/':
		goto yystate75
	}

yystate72:
	c = l.Next()
	switch {
	default:
		goto yyabort
	case c == '*':
		goto yystate73
	case c >= '\x01' && c <= ')' || c >= '+' && c <= '\u007f' || c >= '\u0081' && c <= 'ÿ':
		goto yystate72
	}

yystate73:
	c = l.Next()
	switch {
	default:
		goto yyabort
	case c == '*':
		goto yystate73
	case c == '/':
		goto yystate74
	case c >= '\x01' && c <= ')' || c >= '+' && c <= '.' || c >= '0' && c <= '\u007f' || c >= '\u0081' && c <= 'ÿ':
		goto yystate72
	}

yystate74:
	c = l.Next()
	yyrule = 3
	l.Mark()
	goto yyrule3

yystate75:
	c = l.Next()
	yyrule = 2
	l.Mark()
	switch {
	default:
		goto yyrule2
	case c >= '\x01' && c <= '\t' || c == '\v' || c == '\f' || c >= '\x0e' && c <= '\u007f' || c >= '\u0081' && c <= 'ÿ':
		goto yystate75
	}

yystate76:
	c = l.Next()
	yyrule = 17
	l.Mark()
	switch {
	default:
		goto yyrule17
	case c >= '0' && c <= '9':
		goto yystate76
	}

yystate77:
	c = l.Next()
	yyrule = 18
	l.Mark()
	switch {
	default:
		goto yyrule18
	case c == '.' || c >= '0' && c <= '9' || c >= 'A' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'z':
		goto yystate77
	}

yyrule1: // [ \n\r\t\f]

	goto yystate0
yyrule2: // "//"[^\x80\n\r]*
yyrule3: // "/*"([^*\x80]|\*+[^*/\x80])*\*+\/
	{

		return l.char(COMMENT)
	}
yyrule4: // %"{"
	{
		return l.char(LCURL)
	}
yyrule5: // %"}"
	{
		return l.char(RCURL)
	}
yyrule6: // %%
	{
		return l.char(MARK)
	}
yyrule7: // %left
	{
		return l.char(LEFT)
	}
yyrule8: // %nonassoc
	{
		return l.char(NONASSOC)
	}
yyrule9: // %prec
	{
		return l.char(PREC)
	}
yyrule10: // %precedence
	{
		return l.char(PRECEDENCE)
	}
yyrule11: // %right
	{
		return l.char(RIGHT)
	}
yyrule12: // %start
	{
		return l.char(START)
	}
yyrule13: // %token
	{
		return l.char(TOKEN)
	}
yyrule14: // %type
	{
		return l.char(TYPE)
	}
yyrule15: // %union
	{
		return l.char(UNION)
	}
yyrule16: // %error-verbose
	{
		return l.char(ERROR_VERBOSE)
	}
yyrule17: // [0-9]+
	{
		return l.char(NUMBER)
	}
yyrule18: // {identifier}
	{
		return l.char(IDENTIFIER)
	}
yyrule19: // {str-literal}
	{
		return l.char(STRING_LITERAL)
	}
	panic("unreachable")

	goto yyabort // silence unused label error

yyabort: // no lexem recognized
	if c, ok := l.Abort(); ok {
		return l.char(c)
	}

	goto yyAction
}
