// CAUTION: Generated file - DO NOT EDIT.

// Copyright 2013 The ql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSES/QL-LICENSE file.

// Copyright 2015 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package parser

import (
	"fmt"
	"math"
	"strconv"
	"strings"
	"unicode"

	"github.com/pingcap/tidb/ast"
	mysql "github.com/pingcap/tidb/mysqldef"
)

type lexer struct {
	c            int
	col          int
	errs         []error
	expr         ast.ExprNode
	i            int
	inj          int
	lcol         int
	line         int
	list         []ast.StmtNode
	ncol         int
	nline        int
	sc           int
	src          string
	val          []byte
	ungetBuf     []byte
	root         bool
	stmtStartPos int
	stringLit    []byte

	// record token's offset of the input
	tokenEndOffset   int
	tokenStartOffset int
}

// NewLexer builds a new lexer.
func NewLexer(src string) (l *lexer) {
	l = &lexer{
		src:   src,
		nline: 1,
		ncol:  0,
	}
	l.next()
	return
}

func (l *lexer) Errors() []error {
	return l.errs
}

func (l *lexer) Stmts() []ast.StmtNode {
	return l.list
}

func (l *lexer) Expr() ast.ExprNode {
	return l.expr
}

func (l *lexer) Inj() int {
	return l.inj
}

func (l *lexer) SetInj(inj int) {
	l.inj = inj
}

func (l *lexer) Root() bool {
	return l.root
}

func (l *lexer) SetRoot(root bool) {
	l.root = root
}

func (l *lexer) unget(b byte) {
	l.ungetBuf = append(l.ungetBuf, b)
	l.i--
	l.ncol--
	l.tokenEndOffset--
}

func (l *lexer) next() int {
	if un := len(l.ungetBuf); un > 0 {
		nc := l.ungetBuf[0]
		l.ungetBuf = l.ungetBuf[1:]
		l.c = int(nc)
		return l.c
	}

	if l.c != 0 {
		l.val = append(l.val, byte(l.c))
	}
	l.c = 0
	if l.i < len(l.src) {
		l.c = int(l.src[l.i])
		l.i++
	}
	switch l.c {
	case '\n':
		l.lcol = l.ncol
		l.nline++
		l.ncol = 0
	default:
		l.ncol++
	}
	l.tokenEndOffset++
	return l.c
}

func (l *lexer) err0(ln, c int, arg interface{}) {
	var argStr string
	if arg != nil {
		argStr = fmt.Sprintf(" %v", arg)
	}

	err := fmt.Errorf("line %d column %d near \"%s\"%s", ln, c, l.val, argStr)
	l.errs = append(l.errs, err)
}

func (l *lexer) err(arg interface{}) {
	l.err0(l.line, l.col, arg)
}

func (l *lexer) errf(format string, args ...interface{}) {
	s := fmt.Sprintf(format, args...)
	l.err0(l.line, l.col, s)
}

func (l *lexer) Error(s string) {
	// Notice: ignore origin error info.
	l.err(nil)
}

func (l *lexer) stmtText() string {
	endPos := l.i
	if l.src[l.i-1] == '\n' {
		endPos = l.i - 1 // trim new line
	}
	if l.src[l.stmtStartPos] == '\n' {
		l.stmtStartPos++
	}

	text := l.src[l.stmtStartPos:endPos]

	l.stmtStartPos = l.i
	return text
}

func (l *lexer) Lex(lval *yySymType) (r int) {
	defer func() {
		lval.line, lval.col, lval.offset = l.line, l.col, l.tokenStartOffset
		l.tokenStartOffset = l.tokenEndOffset
	}()
	const (
		INITIAL = iota
		S1
		S2
		S3
		S4
	)

	if n := l.inj; n != 0 {
		l.inj = 0
		return n
	}

	c0, c := 0, l.c

yystate0:

	l.val = l.val[:0]
	c0, l.line, l.col = l.c, l.nline, l.ncol

	switch yyt := l.sc; yyt {
	default:
		panic(fmt.Errorf(`invalid start condition %d`, yyt))
	case 0: // start condition: INITIAL
		goto yystart1
	case 1: // start condition: S1
		goto yystart1079
	case 2: // start condition: S2
		goto yystart1085
	case 3: // start condition: S3
		goto yystart1091
	case 4: // start condition: S4
		goto yystart1094
	}

	goto yystate0 // silence unused label error
	goto yystate1 // silence unused label error
yystate1:
	c = l.next()
yystart1:
	switch {
	default:
		goto yystate3 // c >= '\x01' && c <= '\b' || c == '\v' || c == '\f' || c >= '\x0e' && c <= '\x1f' || c == '$' || c == '%%' || c >= '(' && c <= ',' || c == ':' || c == ';' || c >= '[' && c <= '^' || c == '{' || c >= '}' && c <= 'ÿ'
	case c == '!':
		goto yystate6
	case c == '"':
		goto yystate8
	case c == '#':
		goto yystate9
	case c == '&':
		goto yystate11
	case c == '-':
		goto yystate15
	case c == '.':
		goto yystate17
	case c == '/':
		goto yystate22
	case c == '0':
		goto yystate27
	case c == '<':
		goto yystate36
	case c == '=':
		goto yystate41
	case c == '>':
		goto yystate42
	case c == '?':
		goto yystate45
	case c == '@':
		goto yystate46
	case c == 'A' || c == 'a':
		goto yystate65
	case c == 'B' || c == 'b':
		goto yystate111
	case c == 'C' || c == 'c':
		goto yystate148
	case c == 'D' || c == 'd':
		goto yystate268
	case c == 'E' || c == 'e':
		goto yystate383
	case c == 'F' || c == 'f':
		goto yystate421
	case c == 'G' || c == 'g':
		goto yystate458
	case c == 'H' || c == 'h':
		goto yystate475
	case c == 'I' || c == 'i':
		goto yystate518
	case c == 'J' || c == 'j':
		goto yystate556
	case c == 'K' || c == 'k':
		goto yystate560
	case c == 'L' || c == 'l':
		goto yystate574
	case c == 'M' || c == 'm':
		goto yystate631
	case c == 'N' || c == 'n':
		goto yystate698
	case c == 'O' || c == 'o':
		goto yystate722
	case c == 'P' || c == 'p':
		goto yystate737
	case c == 'Q' || c == 'q':
		goto yystate762
	case c == 'R' || c == 'r':
		goto yystate772
	case c == 'S' || c == 's':
		goto yystate811
	case c == 'T' || c == 't':
		goto yystate909
	case c == 'U' || c == 'u':
		goto yystate966
	case c == 'V' || c == 'v':
		goto yystate999
	case c == 'W' || c == 'w':
		goto yystate1022
	case c == 'X' || c == 'x':
		goto yystate1047
	case c == 'Y' || c == 'y':
		goto yystate1053
	case c == 'Z' || c == 'z':
		goto yystate1067
	case c == '\'':
		goto yystate14
	case c == '\n':
		goto yystate5
	case c == '\t' || c == '\r' || c == ' ':
		goto yystate4
	case c == '\x00':
		goto yystate2
	case c == '_':
		goto yystate1075
	case c == '`':
		goto yystate1076
	case c == '|':
		goto yystate1077
	case c >= '1' && c <= '9':
		goto yystate34
	}

yystate2:
	c = l.next()
	goto yyrule1

yystate3:
	c = l.next()
	goto yyrule275

yystate4:
	c = l.next()
	switch {
	default:
		goto yyrule2
	case c == '\t' || c == '\n' || c == '\r' || c == ' ':
		goto yystate5
	}

yystate5:
	c = l.next()
	switch {
	default:
		goto yyrule2
	case c == '\t' || c == '\n' || c == '\r' || c == ' ':
		goto yystate5
	}

yystate6:
	c = l.next()
	switch {
	default:
		goto yyrule275
	case c == '=':
		goto yystate7
	}

yystate7:
	c = l.next()
	goto yyrule33

yystate8:
	c = l.next()
	goto yyrule13

yystate9:
	c = l.next()
	switch {
	default:
		goto yyrule3
	case c >= '\x01' && c <= '\t' || c >= '\v' && c <= 'ÿ':
		goto yystate10
	}

yystate10:
	c = l.next()
	switch {
	default:
		goto yyrule3
	case c >= '\x01' && c <= '\t' || c >= '\v' && c <= 'ÿ':
		goto yystate10
	}

yystate11:
	c = l.next()
	switch {
	default:
		goto yyrule275
	case c == '&':
		goto yystate12
	case c == '^':
		goto yystate13
	}

yystate12:
	c = l.next()
	goto yyrule27

yystate13:
	c = l.next()
	goto yyrule28

yystate14:
	c = l.next()
	goto yyrule14

yystate15:
	c = l.next()
	switch {
	default:
		goto yyrule275
	case c == '-':
		goto yystate16
	}

yystate16:
	c = l.next()
	goto yyrule6

yystate17:
	c = l.next()
	switch {
	default:
		goto yyrule275
	case c >= '0' && c <= '9':
		goto yystate18
	}

yystate18:
	c = l.next()
	switch {
	default:
		goto yyrule10
	case c == 'E' || c == 'e':
		goto yystate19
	case c >= '0' && c <= '9':
		goto yystate18
	}

yystate19:
	c = l.next()
	switch {
	default:
		goto yyabort
	case c == '+' || c == '-':
		goto yystate20
	case c >= '0' && c <= '9':
		goto yystate21
	}

yystate20:
	c = l.next()
	switch {
	default:
		goto yyabort
	case c >= '0' && c <= '9':
		goto yystate21
	}

yystate21:
	c = l.next()
	switch {
	default:
		goto yyrule10
	case c >= '0' && c <= '9':
		goto yystate21
	}

yystate22:
	c = l.next()
	switch {
	default:
		goto yyrule275
	case c == '*':
		goto yystate23
	case c == '/':
		goto yystate26
	}

yystate23:
	c = l.next()
	switch {
	default:
		goto yyabort
	case c == '*':
		goto yystate24
	case c >= '\x01' && c <= ')' || c >= '+' && c <= 'ÿ':
		goto yystate23
	}

yystate24:
	c = l.next()
	switch {
	default:
		goto yyabort
	case c == '*':
		goto yystate24
	case c == '/':
		goto yystate25
	case c >= '\x01' && c <= ')' || c >= '+' && c <= '.' || c >= '0' && c <= 'ÿ':
		goto yystate23
	}

yystate25:
	c = l.next()
	goto yyrule5

yystate26:
	c = l.next()
	switch {
	default:
		goto yyrule4
	case c >= '\x01' && c <= '\t' || c >= '\v' && c <= 'ÿ':
		goto yystate26
	}

yystate27:
	c = l.next()
	switch {
	default:
		goto yyrule9
	case c == '.':
		goto yystate18
	case c == '8' || c == '9':
		goto yystate29
	case c == 'B' || c == 'b':
		goto yystate30
	case c == 'E' || c == 'e':
		goto yystate19
	case c == 'X' || c == 'x':
		goto yystate32
	case c >= '0' && c <= '7':
		goto yystate28
	}

yystate28:
	c = l.next()
	switch {
	default:
		goto yyrule9
	case c == '.':
		goto yystate18
	case c == '8' || c == '9':
		goto yystate29
	case c == 'E' || c == 'e':
		goto yystate19
	case c >= '0' && c <= '7':
		goto yystate28
	}

yystate29:
	c = l.next()
	switch {
	default:
		goto yyabort
	case c == '.':
		goto yystate18
	case c == 'E' || c == 'e':
		goto yystate19
	case c >= '0' && c <= '9':
		goto yystate29
	}

yystate30:
	c = l.next()
	switch {
	default:
		goto yyabort
	case c == '0' || c == '1':
		goto yystate31
	}

yystate31:
	c = l.next()
	switch {
	default:
		goto yyrule12
	case c == '0' || c == '1':
		goto yystate31
	}

yystate32:
	c = l.next()
	switch {
	default:
		goto yyabort
	case c >= '0' && c <= '9' || c >= 'A' && c <= 'F' || c >= 'a' && c <= 'f':
		goto yystate33
	}

yystate33:
	c = l.next()
	switch {
	default:
		goto yyrule11
	case c >= '0' && c <= '9' || c >= 'A' && c <= 'F' || c >= 'a' && c <= 'f':
		goto yystate33
	}

yystate34:
	c = l.next()
	switch {
	default:
		goto yyrule9
	case c == '.':
		goto yystate18
	case c == 'E' || c == 'e':
		goto yystate19
	case c >= '0' && c <= '9':
		goto yystate35
	}

yystate35:
	c = l.next()
	switch {
	default:
		goto yyrule9
	case c == '.':
		goto yystate18
	case c == 'E' || c == 'e':
		goto yystate19
	case c >= '0' && c <= '9':
		goto yystate35
	}

yystate36:
	c = l.next()
	switch {
	default:
		goto yyrule275
	case c == '<':
		goto yystate37
	case c == '=':
		goto yystate38
	case c == '>':
		goto yystate40
	}

yystate37:
	c = l.next()
	goto yyrule29

yystate38:
	c = l.next()
	switch {
	default:
		goto yyrule30
	case c == '>':
		goto yystate39
	}

yystate39:
	c = l.next()
	goto yyrule37

yystate40:
	c = l.next()
	goto yyrule34

yystate41:
	c = l.next()
	goto yyrule31

yystate42:
	c = l.next()
	switch {
	default:
		goto yyrule275
	case c == '=':
		goto yystate43
	case c == '>':
		goto yystate44
	}

yystate43:
	c = l.next()
	goto yyrule32

yystate44:
	c = l.next()
	goto yyrule36

yystate45:
	c = l.next()
	goto yyrule39

yystate46:
	c = l.next()
	switch {
	default:
		goto yyrule38
	case c == '@':
		goto yystate47
	case c >= 'A' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'z':
		goto yystate64
	}

yystate47:
	c = l.next()
	switch {
	default:
		goto yyabort
	case c == 'G' || c == 'g':
		goto yystate49
	case c == 'L' || c == 'l':
		goto yystate56
	case c == 'S' || c == 's':
		goto yystate58
	case c >= 'A' && c <= 'F' || c >= 'H' && c <= 'K' || c >= 'M' && c <= 'R' || c >= 'T' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'f' || c >= 'h' && c <= 'k' || c >= 'm' && c <= 'r' || c >= 't' && c <= 'z':
		goto yystate48
	}

yystate48:
	c = l.next()
	switch {
	default:
		goto yyrule189
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'z':
		goto yystate48
	}

yystate49:
	c = l.next()
	switch {
	default:
		goto yyrule189
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'K' || c >= 'M' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'k' || c >= 'm' && c <= 'z':
		goto yystate48
	case c == 'L' || c == 'l':
		goto yystate50
	}

yystate50:
	c = l.next()
	switch {
	default:
		goto yyrule189
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'N' || c >= 'P' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'n' || c >= 'p' && c <= 'z':
		goto yystate48
	case c == 'O' || c == 'o':
		goto yystate51
	}

yystate51:
	c = l.next()
	switch {
	default:
		goto yyrule189
	case c == '$' || c >= '0' && c <= '9' || c == 'A' || c >= 'C' && c <= 'Z' || c == '_' || c == 'a' || c >= 'c' && c <= 'z':
		goto yystate48
	case c == 'B' || c == 'b':
		goto yystate52
	}

yystate52:
	c = l.next()
	switch {
	default:
		goto yyrule189
	case c == '$' || c >= '0' && c <= '9' || c >= 'B' && c <= 'Z' || c == '_' || c >= 'b' && c <= 'z':
		goto yystate48
	case c == 'A' || c == 'a':
		goto yystate53
	}

yystate53:
	c = l.next()
	switch {
	default:
		goto yyrule189
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'K' || c >= 'M' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'k' || c >= 'm' && c <= 'z':
		goto yystate48
	case c == 'L' || c == 'l':
		goto yystate54
	}

yystate54:
	c = l.next()
	switch {
	default:
		goto yyrule189
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'z':
		goto yystate48
	case c == '.':
		goto yystate55
	}

yystate55:
	c = l.next()
	switch {
	default:
		goto yyabort
	case c >= 'A' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'z':
		goto yystate48
	}

yystate56:
	c = l.next()
	switch {
	default:
		goto yyrule189
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'N' || c >= 'P' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'n' || c >= 'p' && c <= 'z':
		goto yystate48
	case c == 'O' || c == 'o':
		goto yystate57
	}

yystate57:
	c = l.next()
	switch {
	default:
		goto yyrule189
	case c == '$' || c >= '0' && c <= '9' || c == 'A' || c == 'B' || c >= 'D' && c <= 'Z' || c == '_' || c == 'a' || c == 'b' || c >= 'd' && c <= 'z':
		goto yystate48
	case c == 'C' || c == 'c':
		goto yystate52
	}

yystate58:
	c = l.next()
	switch {
	default:
		goto yyrule189
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'D' || c >= 'F' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'd' || c >= 'f' && c <= 'z':
		goto yystate48
	case c == 'E' || c == 'e':
		goto yystate59
	}

yystate59:
	c = l.next()
	switch {
	default:
		goto yyrule189
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'R' || c >= 'T' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'r' || c >= 't' && c <= 'z':
		goto yystate48
	case c == 'S' || c == 's':
		goto yystate60
	}

yystate60:
	c = l.next()
	switch {
	default:
		goto yyrule189
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'R' || c >= 'T' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'r' || c >= 't' && c <= 'z':
		goto yystate48
	case c == 'S' || c == 's':
		goto yystate61
	}

yystate61:
	c = l.next()
	switch {
	default:
		goto yyrule189
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'H' || c >= 'J' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'h' || c >= 'j' && c <= 'z':
		goto yystate48
	case c == 'I' || c == 'i':
		goto yystate62
	}

yystate62:
	c = l.next()
	switch {
	default:
		goto yyrule189
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'N' || c >= 'P' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'n' || c >= 'p' && c <= 'z':
		goto yystate48
	case c == 'O' || c == 'o':
		goto yystate63
	}

yystate63:
	c = l.next()
	switch {
	default:
		goto yyrule189
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'M' || c >= 'O' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'm' || c >= 'o' && c <= 'z':
		goto yystate48
	case c == 'N' || c == 'n':
		goto yystate54
	}

yystate64:
	c = l.next()
	switch {
	default:
		goto yyrule190
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'z':
		goto yystate64
	}

yystate65:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c == 'A' || c == 'C' || c == 'E' || c >= 'G' && c <= 'K' || c == 'M' || c >= 'O' && c <= 'R' || c == 'T' || c >= 'W' && c <= 'Z' || c == '_' || c == 'a' || c == 'c' || c == 'e' || c >= 'g' && c <= 'k' || c == 'm' || c >= 'o' && c <= 'r' || c == 't' || c >= 'w' && c <= 'z':
		goto yystate66
	case c == 'B' || c == 'b':
		goto yystate67
	case c == 'D' || c == 'd':
		goto yystate69
	case c == 'F' || c == 'f':
		goto yystate71
	case c == 'L' || c == 'l':
		goto yystate75
	case c == 'N' || c == 'n':
		goto yystate80
	case c == 'S' || c == 's':
		goto yystate83
	case c == 'U' || c == 'u':
		goto yystate85
	case c == 'V' || c == 'v':
		goto yystate98
	}

yystate66:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'z':
		goto yystate66
	}

yystate67:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'R' || c >= 'T' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'r' || c >= 't' && c <= 'z':
		goto yystate66
	case c == 'S' || c == 's':
		goto yystate68
	}

yystate68:
	c = l.next()
	switch {
	default:
		goto yyrule40
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'z':
		goto yystate66
	}

yystate69:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'C' || c >= 'E' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'c' || c >= 'e' && c <= 'z':
		goto yystate66
	case c == 'D' || c == 'd':
		goto yystate70
	}

yystate70:
	c = l.next()
	switch {
	default:
		goto yyrule41
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'z':
		goto yystate66
	}

yystate71:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'S' || c >= 'U' && c <= 'Z' || c == '_' || c >= 'a' && c <= 's' || c >= 'u' && c <= 'z':
		goto yystate66
	case c == 'T' || c == 't':
		goto yystate72
	}

yystate72:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'D' || c >= 'F' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'd' || c >= 'f' && c <= 'z':
		goto yystate66
	case c == 'E' || c == 'e':
		goto yystate73
	}

yystate73:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'Q' || c >= 'S' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'q' || c >= 's' && c <= 'z':
		goto yystate66
	case c == 'R' || c == 'r':
		goto yystate74
	}

yystate74:
	c = l.next()
	switch {
	default:
		goto yyrule42
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'z':
		goto yystate66
	}

yystate75:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'K' || c >= 'M' && c <= 'S' || c >= 'U' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'k' || c >= 'm' && c <= 's' || c >= 'u' && c <= 'z':
		goto yystate66
	case c == 'L' || c == 'l':
		goto yystate76
	case c == 'T' || c == 't':
		goto yystate77
	}

yystate76:
	c = l.next()
	switch {
	default:
		goto yyrule43
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'z':
		goto yystate66
	}

yystate77:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'D' || c >= 'F' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'd' || c >= 'f' && c <= 'z':
		goto yystate66
	case c == 'E' || c == 'e':
		goto yystate78
	}

yystate78:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'Q' || c >= 'S' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'q' || c >= 's' && c <= 'z':
		goto yystate66
	case c == 'R' || c == 'r':
		goto yystate79
	}

yystate79:
	c = l.next()
	switch {
	default:
		goto yyrule44
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'z':
		goto yystate66
	}

yystate80:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'C' || c >= 'E' && c <= 'X' || c == 'Z' || c == '_' || c >= 'a' && c <= 'c' || c >= 'e' && c <= 'x' || c == 'z':
		goto yystate66
	case c == 'D' || c == 'd':
		goto yystate81
	case c == 'Y' || c == 'y':
		goto yystate82
	}

yystate81:
	c = l.next()
	switch {
	default:
		goto yyrule45
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'z':
		goto yystate66
	}

yystate82:
	c = l.next()
	switch {
	default:
		goto yyrule46
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'z':
		goto yystate66
	}

yystate83:
	c = l.next()
	switch {
	default:
		goto yyrule48
	case c == '$' || c >= '0' && c <= '9' || c == 'A' || c == 'B' || c >= 'D' && c <= 'Z' || c == '_' || c == 'a' || c == 'b' || c >= 'd' && c <= 'z':
		goto yystate66
	case c == 'C' || c == 'c':
		goto yystate84
	}

yystate84:
	c = l.next()
	switch {
	default:
		goto yyrule47
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'z':
		goto yystate66
	}

yystate85:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'S' || c >= 'U' && c <= 'Z' || c == '_' || c >= 'a' && c <= 's' || c >= 'u' && c <= 'z':
		goto yystate66
	case c == 'T' || c == 't':
		goto yystate86
	}

yystate86:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'N' || c >= 'P' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'n' || c >= 'p' && c <= 'z':
		goto yystate66
	case c == 'O' || c == 'o':
		goto yystate87
	}

yystate87:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'Z' || c >= 'a' && c <= 'z':
		goto yystate66
	case c == '_':
		goto yystate88
	}

yystate88:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'H' || c >= 'J' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'h' || c >= 'j' && c <= 'z':
		goto yystate66
	case c == 'I' || c == 'i':
		goto yystate89
	}

yystate89:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'M' || c >= 'O' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'm' || c >= 'o' && c <= 'z':
		goto yystate66
	case c == 'N' || c == 'n':
		goto yystate90
	}

yystate90:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c == 'A' || c == 'B' || c >= 'D' && c <= 'Z' || c == '_' || c == 'a' || c == 'b' || c >= 'd' && c <= 'z':
		goto yystate66
	case c == 'C' || c == 'c':
		goto yystate91
	}

yystate91:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'Q' || c >= 'S' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'q' || c >= 's' && c <= 'z':
		goto yystate66
	case c == 'R' || c == 'r':
		goto yystate92
	}

yystate92:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'D' || c >= 'F' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'd' || c >= 'f' && c <= 'z':
		goto yystate66
	case c == 'E' || c == 'e':
		goto yystate93
	}

yystate93:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'L' || c >= 'N' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'l' || c >= 'n' && c <= 'z':
		goto yystate66
	case c == 'M' || c == 'm':
		goto yystate94
	}

yystate94:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'D' || c >= 'F' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'd' || c >= 'f' && c <= 'z':
		goto yystate66
	case c == 'E' || c == 'e':
		goto yystate95
	}

yystate95:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'M' || c >= 'O' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'm' || c >= 'o' && c <= 'z':
		goto yystate66
	case c == 'N' || c == 'n':
		goto yystate96
	}

yystate96:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'S' || c >= 'U' && c <= 'Z' || c == '_' || c >= 'a' && c <= 's' || c >= 'u' && c <= 'z':
		goto yystate66
	case c == 'T' || c == 't':
		goto yystate97
	}

yystate97:
	c = l.next()
	switch {
	default:
		goto yyrule49
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'z':
		goto yystate66
	}

yystate98:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'F' || c >= 'H' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'f' || c >= 'h' && c <= 'z':
		goto yystate66
	case c == 'G' || c == 'g':
		goto yystate99
	}

yystate99:
	c = l.next()
	switch {
	default:
		goto yyrule50
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'Z' || c >= 'a' && c <= 'z':
		goto yystate66
	case c == '_':
		goto yystate100
	}

yystate100:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'Q' || c >= 'S' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'q' || c >= 's' && c <= 'z':
		goto yystate66
	case c == 'R' || c == 'r':
		goto yystate101
	}

yystate101:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'N' || c >= 'P' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'n' || c >= 'p' && c <= 'z':
		goto yystate66
	case c == 'O' || c == 'o':
		goto yystate102
	}

yystate102:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'V' || c >= 'X' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'v' || c >= 'x' && c <= 'z':
		goto yystate66
	case c == 'W' || c == 'w':
		goto yystate103
	}

yystate103:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'Z' || c >= 'a' && c <= 'z':
		goto yystate66
	case c == '_':
		goto yystate104
	}

yystate104:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'K' || c >= 'M' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'k' || c >= 'm' && c <= 'z':
		goto yystate66
	case c == 'L' || c == 'l':
		goto yystate105
	}

yystate105:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'D' || c >= 'F' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'd' || c >= 'f' && c <= 'z':
		goto yystate66
	case c == 'E' || c == 'e':
		goto yystate106
	}

yystate106:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'M' || c >= 'O' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'm' || c >= 'o' && c <= 'z':
		goto yystate66
	case c == 'N' || c == 'n':
		goto yystate107
	}

yystate107:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'F' || c >= 'H' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'f' || c >= 'h' && c <= 'z':
		goto yystate66
	case c == 'G' || c == 'g':
		goto yystate108
	}

yystate108:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'S' || c >= 'U' && c <= 'Z' || c == '_' || c >= 'a' && c <= 's' || c >= 'u' && c <= 'z':
		goto yystate66
	case c == 'T' || c == 't':
		goto yystate109
	}

yystate109:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'G' || c >= 'I' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'g' || c >= 'i' && c <= 'z':
		goto yystate66
	case c == 'H' || c == 'h':
		goto yystate110
	}

yystate110:
	c = l.next()
	switch {
	default:
		goto yyrule51
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'z':
		goto yystate66
	}

yystate111:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'D' || c >= 'F' && c <= 'H' || c == 'J' || c == 'K' || c == 'M' || c == 'N' || c >= 'P' && c <= 'X' || c == 'Z' || c == '_' || c >= 'a' && c <= 'd' || c >= 'f' && c <= 'h' || c == 'j' || c == 'k' || c == 'm' || c == 'n' || c >= 'p' && c <= 'x' || c == 'z':
		goto yystate66
	case c == 'E' || c == 'e':
		goto yystate115
	case c == 'I' || c == 'i':
		goto yystate124
	case c == 'L' || c == 'l':
		goto yystate134
	case c == 'O' || c == 'o':
		goto yystate137
	case c == 'Y' || c == 'y':
		goto yystate145
	case c == '\'':
		goto yystate112
	}

yystate112:
	c = l.next()
	switch {
	default:
		goto yyabort
	case c == '0' || c == '1':
		goto yystate113
	}

yystate113:
	c = l.next()
	switch {
	default:
		goto yyabort
	case c == '0' || c == '1':
		goto yystate113
	case c == '\'':
		goto yystate114
	}

yystate114:
	c = l.next()
	goto yyrule12

yystate115:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'F' || c >= 'H' && c <= 'S' || c >= 'U' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'f' || c >= 'h' && c <= 's' || c >= 'u' && c <= 'z':
		goto yystate66
	case c == 'G' || c == 'g':
		goto yystate116
	case c == 'T' || c == 't':
		goto yystate119
	}

yystate116:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'H' || c >= 'J' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'h' || c >= 'j' && c <= 'z':
		goto yystate66
	case c == 'I' || c == 'i':
		goto yystate117
	}

yystate117:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'M' || c >= 'O' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'm' || c >= 'o' && c <= 'z':
		goto yystate66
	case c == 'N' || c == 'n':
		goto yystate118
	}

yystate118:
	c = l.next()
	switch {
	default:
		goto yyrule52
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'z':
		goto yystate66
	}

yystate119:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'V' || c >= 'X' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'v' || c >= 'x' && c <= 'z':
		goto yystate66
	case c == 'W' || c == 'w':
		goto yystate120
	}

yystate120:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'D' || c >= 'F' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'd' || c >= 'f' && c <= 'z':
		goto yystate66
	case c == 'E' || c == 'e':
		goto yystate121
	}

yystate121:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'D' || c >= 'F' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'd' || c >= 'f' && c <= 'z':
		goto yystate66
	case c == 'E' || c == 'e':
		goto yystate122
	}

yystate122:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'M' || c >= 'O' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'm' || c >= 'o' && c <= 'z':
		goto yystate66
	case c == 'N' || c == 'n':
		goto yystate123
	}

yystate123:
	c = l.next()
	switch {
	default:
		goto yyrule53
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'z':
		goto yystate66
	}

yystate124:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'F' || c >= 'H' && c <= 'M' || c >= 'O' && c <= 'S' || c >= 'U' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'f' || c >= 'h' && c <= 'm' || c >= 'o' && c <= 's' || c >= 'u' && c <= 'z':
		goto yystate66
	case c == 'G' || c == 'g':
		goto yystate125
	case c == 'N' || c == 'n':
		goto yystate129
	case c == 'T' || c == 't':
		goto yystate133
	}

yystate125:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'H' || c >= 'J' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'h' || c >= 'j' && c <= 'z':
		goto yystate66
	case c == 'I' || c == 'i':
		goto yystate126
	}

yystate126:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'M' || c >= 'O' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'm' || c >= 'o' && c <= 'z':
		goto yystate66
	case c == 'N' || c == 'n':
		goto yystate127
	}

yystate127:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'S' || c >= 'U' && c <= 'Z' || c == '_' || c >= 'a' && c <= 's' || c >= 'u' && c <= 'z':
		goto yystate66
	case c == 'T' || c == 't':
		goto yystate128
	}

yystate128:
	c = l.next()
	switch {
	default:
		goto yyrule245
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'z':
		goto yystate66
	}

yystate129:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'B' && c <= 'Z' || c == '_' || c >= 'b' && c <= 'z':
		goto yystate66
	case c == 'A' || c == 'a':
		goto yystate130
	}

yystate130:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'Q' || c >= 'S' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'q' || c >= 's' && c <= 'z':
		goto yystate66
	case c == 'R' || c == 'r':
		goto yystate131
	}

yystate131:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'X' || c == 'Z' || c == '_' || c >= 'a' && c <= 'x' || c == 'z':
		goto yystate66
	case c == 'Y' || c == 'y':
		goto yystate132
	}

yystate132:
	c = l.next()
	switch {
	default:
		goto yyrule259
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'z':
		goto yystate66
	}

yystate133:
	c = l.next()
	switch {
	default:
		goto yyrule240
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'z':
		goto yystate66
	}

yystate134:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'N' || c >= 'P' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'n' || c >= 'p' && c <= 'z':
		goto yystate66
	case c == 'O' || c == 'o':
		goto yystate135
	}

yystate135:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c == 'A' || c >= 'C' && c <= 'Z' || c == '_' || c == 'a' || c >= 'c' && c <= 'z':
		goto yystate66
	case c == 'B' || c == 'b':
		goto yystate136
	}

yystate136:
	c = l.next()
	switch {
	default:
		goto yyrule262
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'z':
		goto yystate66
	}

yystate137:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'N' || c >= 'P' && c <= 'S' || c >= 'U' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'n' || c >= 'p' && c <= 's' || c >= 'u' && c <= 'z':
		goto yystate66
	case c == 'O' || c == 'o':
		goto yystate138
	case c == 'T' || c == 't':
		goto yystate143
	}

yystate138:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'K' || c >= 'M' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'k' || c >= 'm' && c <= 'z':
		goto yystate66
	case c == 'L' || c == 'l':
		goto yystate139
	}

yystate139:
	c = l.next()
	switch {
	default:
		goto yyrule269
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'D' || c >= 'F' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'd' || c >= 'f' && c <= 'z':
		goto yystate66
	case c == 'E' || c == 'e':
		goto yystate140
	}

yystate140:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'B' && c <= 'Z' || c == '_' || c >= 'b' && c <= 'z':
		goto yystate66
	case c == 'A' || c == 'a':
		goto yystate141
	}

yystate141:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'M' || c >= 'O' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'm' || c >= 'o' && c <= 'z':
		goto yystate66
	case c == 'N' || c == 'n':
		goto yystate142
	}

yystate142:
	c = l.next()
	switch {
	default:
		goto yyrule270
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'z':
		goto yystate66
	}

yystate143:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'G' || c >= 'I' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'g' || c >= 'i' && c <= 'z':
		goto yystate66
	case c == 'H' || c == 'h':
		goto yystate144
	}

yystate144:
	c = l.next()
	switch {
	default:
		goto yyrule54
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'z':
		goto yystate66
	}

yystate145:
	c = l.next()
	switch {
	default:
		goto yyrule55
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'S' || c >= 'U' && c <= 'Z' || c == '_' || c >= 'a' && c <= 's' || c >= 'u' && c <= 'z':
		goto yystate66
	case c == 'T' || c == 't':
		goto yystate146
	}

yystate146:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'D' || c >= 'F' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'd' || c >= 'f' && c <= 'z':
		goto yystate66
	case c == 'E' || c == 'e':
		goto yystate147
	}

yystate147:
	c = l.next()
	switch {
	default:
		goto yyrule271
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'z':
		goto yystate66
	}

yystate148:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'B' && c <= 'G' || c >= 'I' && c <= 'N' || c == 'P' || c == 'Q' || c == 'S' || c == 'T' || c >= 'V' && c <= 'Z' || c == '_' || c >= 'b' && c <= 'g' || c >= 'i' && c <= 'n' || c == 'p' || c == 'q' || c == 's' || c == 't' || c >= 'v' && c <= 'z':
		goto yystate66
	case c == 'A' || c == 'a':
		goto yystate149
	case c == 'H' || c == 'h':
		goto yystate153
	case c == 'O' || c == 'o':
		goto yystate170
	case c == 'R' || c == 'r':
		goto yystate232
	case c == 'U' || c == 'u':
		goto yystate240
	}

yystate149:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'R' || c >= 'T' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'r' || c >= 't' && c <= 'z':
		goto yystate66
	case c == 'S' || c == 's':
		goto yystate150
	}

yystate150:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'D' || c >= 'F' && c <= 'S' || c >= 'U' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'd' || c >= 'f' && c <= 's' || c >= 'u' && c <= 'z':
		goto yystate66
	case c == 'E' || c == 'e':
		goto yystate151
	case c == 'T' || c == 't':
		goto yystate152
	}

yystate151:
	c = l.next()
	switch {
	default:
		goto yyrule56
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'z':
		goto yystate66
	}

yystate152:
	c = l.next()
	switch {
	default:
		goto yyrule57
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'z':
		goto yystate66
	}

yystate153:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'B' && c <= 'D' || c >= 'F' && c <= 'Z' || c == '_' || c >= 'b' && c <= 'd' || c >= 'f' && c <= 'z':
		goto yystate66
	case c == 'A' || c == 'a':
		goto yystate154
	case c == 'E' || c == 'e':
		goto yystate164
	}

yystate154:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'Q' || c >= 'S' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'q' || c >= 's' && c <= 'z':
		goto yystate66
	case c == 'R' || c == 'r':
		goto yystate155
	}

yystate155:
	c = l.next()
	switch {
	default:
		goto yyrule257
	case c == '$' || c >= '0' && c <= '9' || c >= 'B' && c <= 'R' || c >= 'T' && c <= 'Z' || c == '_' || c >= 'b' && c <= 'r' || c >= 't' && c <= 'z':
		goto yystate66
	case c == 'A' || c == 'a':
		goto yystate156
	case c == 'S' || c == 's':
		goto yystate161
	}

yystate156:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c == 'A' || c == 'B' || c >= 'D' && c <= 'Z' || c == '_' || c == 'a' || c == 'b' || c >= 'd' && c <= 'z':
		goto yystate66
	case c == 'C' || c == 'c':
		goto yystate157
	}

yystate157:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'S' || c >= 'U' && c <= 'Z' || c == '_' || c >= 'a' && c <= 's' || c >= 'u' && c <= 'z':
		goto yystate66
	case c == 'T' || c == 't':
		goto yystate158
	}

yystate158:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'D' || c >= 'F' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'd' || c >= 'f' && c <= 'z':
		goto yystate66
	case c == 'E' || c == 'e':
		goto yystate159
	}

yystate159:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'Q' || c >= 'S' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'q' || c >= 's' && c <= 'z':
		goto yystate66
	case c == 'R' || c == 'r':
		goto yystate160
	}

yystate160:
	c = l.next()
	switch {
	default:
		goto yyrule58
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'z':
		goto yystate66
	}

yystate161:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'D' || c >= 'F' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'd' || c >= 'f' && c <= 'z':
		goto yystate66
	case c == 'E' || c == 'e':
		goto yystate162
	}

yystate162:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'S' || c >= 'U' && c <= 'Z' || c == '_' || c >= 'a' && c <= 's' || c >= 'u' && c <= 'z':
		goto yystate66
	case c == 'T' || c == 't':
		goto yystate163
	}

yystate163:
	c = l.next()
	switch {
	default:
		goto yyrule59
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'z':
		goto yystate66
	}

yystate164:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c == 'A' || c == 'B' || c >= 'D' && c <= 'Z' || c == '_' || c == 'a' || c == 'b' || c >= 'd' && c <= 'z':
		goto yystate66
	case c == 'C' || c == 'c':
		goto yystate165
	}

yystate165:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'J' || c >= 'L' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'j' || c >= 'l' && c <= 'z':
		goto yystate66
	case c == 'K' || c == 'k':
		goto yystate166
	}

yystate166:
	c = l.next()
	switch {
	default:
		goto yyrule60
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'R' || c >= 'T' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'r' || c >= 't' && c <= 'z':
		goto yystate66
	case c == 'S' || c == 's':
		goto yystate167
	}

yystate167:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'T' || c >= 'V' && c <= 'Z' || c == '_' || c >= 'a' && c <= 't' || c >= 'v' && c <= 'z':
		goto yystate66
	case c == 'U' || c == 'u':
		goto yystate168
	}

yystate168:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'L' || c >= 'N' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'l' || c >= 'n' && c <= 'z':
		goto yystate66
	case c == 'M' || c == 'm':
		goto yystate169
	}

yystate169:
	c = l.next()
	switch {
	default:
		goto yyrule61
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'z':
		goto yystate66
	}

yystate170:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'B' && c <= 'K' || c >= 'O' && c <= 'T' || c >= 'V' && c <= 'Z' || c == '_' || c >= 'b' && c <= 'k' || c >= 'o' && c <= 't' || c >= 'v' && c <= 'z':
		goto yystate66
	case c == 'A' || c == 'a':
		goto yystate171
	case c == 'L' || c == 'l':
		goto yystate177
	case c == 'M' || c == 'm':
		goto yystate189
	case c == 'N' || c == 'n':
		goto yystate204
	case c == 'U' || c == 'u':
		goto yystate229
	}

yystate171:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'K' || c >= 'M' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'k' || c >= 'm' && c <= 'z':
		goto yystate66
	case c == 'L' || c == 'l':
		goto yystate172
	}

yystate172:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'D' || c >= 'F' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'd' || c >= 'f' && c <= 'z':
		goto yystate66
	case c == 'E' || c == 'e':
		goto yystate173
	}

yystate173:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'R' || c >= 'T' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'r' || c >= 't' && c <= 'z':
		goto yystate66
	case c == 'S' || c == 's':
		goto yystate174
	}

yystate174:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c == 'A' || c == 'B' || c >= 'D' && c <= 'Z' || c == '_' || c == 'a' || c == 'b' || c >= 'd' && c <= 'z':
		goto yystate66
	case c == 'C' || c == 'c':
		goto yystate175
	}

yystate175:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'D' || c >= 'F' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'd' || c >= 'f' && c <= 'z':
		goto yystate66
	case c == 'E' || c == 'e':
		goto yystate176
	}

yystate176:
	c = l.next()
	switch {
	default:
		goto yyrule62
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'z':
		goto yystate66
	}

yystate177:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'K' || c >= 'M' && c <= 'T' || c >= 'V' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'k' || c >= 'm' && c <= 't' || c >= 'v' && c <= 'z':
		goto yystate66
	case c == 'L' || c == 'l':
		goto yystate178
	case c == 'U' || c == 'u':
		goto yystate185
	}

yystate178:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'B' && c <= 'Z' || c == '_' || c >= 'b' && c <= 'z':
		goto yystate66
	case c == 'A' || c == 'a':
		goto yystate179
	}

yystate179:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'S' || c >= 'U' && c <= 'Z' || c == '_' || c >= 'a' && c <= 's' || c >= 'u' && c <= 'z':
		goto yystate66
	case c == 'T' || c == 't':
		goto yystate180
	}

yystate180:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'D' || c >= 'F' && c <= 'H' || c >= 'J' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'd' || c >= 'f' && c <= 'h' || c >= 'j' && c <= 'z':
		goto yystate66
	case c == 'E' || c == 'e':
		goto yystate181
	case c == 'I' || c == 'i':
		goto yystate182
	}

yystate181:
	c = l.next()
	switch {
	default:
		goto yyrule63
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'z':
		goto yystate66
	}

yystate182:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'N' || c >= 'P' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'n' || c >= 'p' && c <= 'z':
		goto yystate66
	case c == 'O' || c == 'o':
		goto yystate183
	}

yystate183:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'M' || c >= 'O' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'm' || c >= 'o' && c <= 'z':
		goto yystate66
	case c == 'N' || c == 'n':
		goto yystate184
	}

yystate184:
	c = l.next()
	switch {
	default:
		goto yyrule64
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'z':
		goto yystate66
	}

yystate185:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'L' || c >= 'N' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'l' || c >= 'n' && c <= 'z':
		goto yystate66
	case c == 'M' || c == 'm':
		goto yystate186
	}

yystate186:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'M' || c >= 'O' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'm' || c >= 'o' && c <= 'z':
		goto yystate66
	case c == 'N' || c == 'n':
		goto yystate187
	}

yystate187:
	c = l.next()
	switch {
	default:
		goto yyrule65
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'R' || c >= 'T' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'r' || c >= 't' && c <= 'z':
		goto yystate66
	case c == 'S' || c == 's':
		goto yystate188
	}

yystate188:
	c = l.next()
	switch {
	default:
		goto yyrule66
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'z':
		goto yystate66
	}

yystate189:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'L' || c == 'N' || c == 'O' || c >= 'Q' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'l' || c == 'n' || c == 'o' || c >= 'q' && c <= 'z':
		goto yystate66
	case c == 'M' || c == 'm':
		goto yystate190
	case c == 'P' || c == 'p':
		goto yystate196
	}

yystate190:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'D' || c >= 'F' && c <= 'H' || c >= 'J' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'd' || c >= 'f' && c <= 'h' || c >= 'j' && c <= 'z':
		goto yystate66
	case c == 'E' || c == 'e':
		goto yystate191
	case c == 'I' || c == 'i':
		goto yystate194
	}

yystate191:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'M' || c >= 'O' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'm' || c >= 'o' && c <= 'z':
		goto yystate66
	case c == 'N' || c == 'n':
		goto yystate192
	}

yystate192:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'S' || c >= 'U' && c <= 'Z' || c == '_' || c >= 'a' && c <= 's' || c >= 'u' && c <= 'z':
		goto yystate66
	case c == 'T' || c == 't':
		goto yystate193
	}

yystate193:
	c = l.next()
	switch {
	default:
		goto yyrule67
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'z':
		goto yystate66
	}

yystate194:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'S' || c >= 'U' && c <= 'Z' || c == '_' || c >= 'a' && c <= 's' || c >= 'u' && c <= 'z':
		goto yystate66
	case c == 'T' || c == 't':
		goto yystate195
	}

yystate195:
	c = l.next()
	switch {
	default:
		goto yyrule68
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'z':
		goto yystate66
	}

yystate196:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'Q' || c >= 'S' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'q' || c >= 's' && c <= 'z':
		goto yystate66
	case c == 'R' || c == 'r':
		goto yystate197
	}

yystate197:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'D' || c >= 'F' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'd' || c >= 'f' && c <= 'z':
		goto yystate66
	case c == 'E' || c == 'e':
		goto yystate198
	}

yystate198:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'R' || c >= 'T' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'r' || c >= 't' && c <= 'z':
		goto yystate66
	case c == 'S' || c == 's':
		goto yystate199
	}

yystate199:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'R' || c >= 'T' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'r' || c >= 't' && c <= 'z':
		goto yystate66
	case c == 'S' || c == 's':
		goto yystate200
	}

yystate200:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'H' || c >= 'J' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'h' || c >= 'j' && c <= 'z':
		goto yystate66
	case c == 'I' || c == 'i':
		goto yystate201
	}

yystate201:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'N' || c >= 'P' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'n' || c >= 'p' && c <= 'z':
		goto yystate66
	case c == 'O' || c == 'o':
		goto yystate202
	}

yystate202:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'M' || c >= 'O' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'm' || c >= 'o' && c <= 'z':
		goto yystate66
	case c == 'N' || c == 'n':
		goto yystate203
	}

yystate203:
	c = l.next()
	switch {
	default:
		goto yyrule69
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'z':
		goto yystate66
	}

yystate204:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c == 'A' || c == 'B' || c >= 'D' && c <= 'M' || c >= 'O' && c <= 'R' || c == 'T' || c == 'U' || c >= 'W' && c <= 'Z' || c == '_' || c == 'a' || c == 'b' || c >= 'd' && c <= 'm' || c >= 'o' && c <= 'r' || c == 't' || c == 'u' || c >= 'w' && c <= 'z':
		goto yystate66
	case c == 'C' || c == 'c':
		goto yystate205
	case c == 'N' || c == 'n':
		goto yystate211
	case c == 'S' || c == 's':
		goto yystate218
	case c == 'V' || c == 'v':
		goto yystate225
	}

yystate205:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'B' && c <= 'Z' || c == '_' || c >= 'b' && c <= 'z':
		goto yystate66
	case c == 'A' || c == 'a':
		goto yystate206
	}

yystate206:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'S' || c >= 'U' && c <= 'Z' || c == '_' || c >= 'a' && c <= 's' || c >= 'u' && c <= 'z':
		goto yystate66
	case c == 'T' || c == 't':
		goto yystate207
	}

yystate207:
	c = l.next()
	switch {
	default:
		goto yyrule70
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'Z' || c >= 'a' && c <= 'z':
		goto yystate66
	case c == '_':
		goto yystate208
	}

yystate208:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'V' || c >= 'X' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'v' || c >= 'x' && c <= 'z':
		goto yystate66
	case c == 'W' || c == 'w':
		goto yystate209
	}

yystate209:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'R' || c >= 'T' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'r' || c >= 't' && c <= 'z':
		goto yystate66
	case c == 'S' || c == 's':
		goto yystate210
	}

yystate210:
	c = l.next()
	switch {
	default:
		goto yyrule71
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'z':
		goto yystate66
	}

yystate211:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'D' || c >= 'F' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'd' || c >= 'f' && c <= 'z':
		goto yystate66
	case c == 'E' || c == 'e':
		goto yystate212
	}

yystate212:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c == 'A' || c == 'B' || c >= 'D' && c <= 'Z' || c == '_' || c == 'a' || c == 'b' || c >= 'd' && c <= 'z':
		goto yystate66
	case c == 'C' || c == 'c':
		goto yystate213
	}

yystate213:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'S' || c >= 'U' && c <= 'Z' || c == '_' || c >= 'a' && c <= 's' || c >= 'u' && c <= 'z':
		goto yystate66
	case c == 'T' || c == 't':
		goto yystate214
	}

yystate214:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'H' || c >= 'J' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'h' || c >= 'j' && c <= 'z':
		goto yystate66
	case c == 'I' || c == 'i':
		goto yystate215
	}

yystate215:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'N' || c >= 'P' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'n' || c >= 'p' && c <= 'z':
		goto yystate66
	case c == 'O' || c == 'o':
		goto yystate216
	}

yystate216:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'M' || c >= 'O' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'm' || c >= 'o' && c <= 'z':
		goto yystate66
	case c == 'N' || c == 'n':
		goto yystate217
	}

yystate217:
	c = l.next()
	switch {
	default:
		goto yyrule72
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'z':
		goto yystate66
	}

yystate218:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'S' || c >= 'U' && c <= 'Z' || c == '_' || c >= 'a' && c <= 's' || c >= 'u' && c <= 'z':
		goto yystate66
	case c == 'T' || c == 't':
		goto yystate219
	}

yystate219:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'Q' || c >= 'S' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'q' || c >= 's' && c <= 'z':
		goto yystate66
	case c == 'R' || c == 'r':
		goto yystate220
	}

yystate220:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'B' && c <= 'Z' || c == '_' || c >= 'b' && c <= 'z':
		goto yystate66
	case c == 'A' || c == 'a':
		goto yystate221
	}

yystate221:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'H' || c >= 'J' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'h' || c >= 'j' && c <= 'z':
		goto yystate66
	case c == 'I' || c == 'i':
		goto yystate222
	}

yystate222:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'M' || c >= 'O' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'm' || c >= 'o' && c <= 'z':
		goto yystate66
	case c == 'N' || c == 'n':
		goto yystate223
	}

yystate223:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'S' || c >= 'U' && c <= 'Z' || c == '_' || c >= 'a' && c <= 's' || c >= 'u' && c <= 'z':
		goto yystate66
	case c == 'T' || c == 't':
		goto yystate224
	}

yystate224:
	c = l.next()
	switch {
	default:
		goto yyrule73
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'z':
		goto yystate66
	}

yystate225:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'D' || c >= 'F' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'd' || c >= 'f' && c <= 'z':
		goto yystate66
	case c == 'E' || c == 'e':
		goto yystate226
	}

yystate226:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'Q' || c >= 'S' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'q' || c >= 's' && c <= 'z':
		goto yystate66
	case c == 'R' || c == 'r':
		goto yystate227
	}

yystate227:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'S' || c >= 'U' && c <= 'Z' || c == '_' || c >= 'a' && c <= 's' || c >= 'u' && c <= 'z':
		goto yystate66
	case c == 'T' || c == 't':
		goto yystate228
	}

yystate228:
	c = l.next()
	switch {
	default:
		goto yyrule74
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'z':
		goto yystate66
	}

yystate229:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'M' || c >= 'O' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'm' || c >= 'o' && c <= 'z':
		goto yystate66
	case c == 'N' || c == 'n':
		goto yystate230
	}

yystate230:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'S' || c >= 'U' && c <= 'Z' || c == '_' || c >= 'a' && c <= 's' || c >= 'u' && c <= 'z':
		goto yystate66
	case c == 'T' || c == 't':
		goto yystate231
	}

yystate231:
	c = l.next()
	switch {
	default:
		goto yyrule75
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'z':
		goto yystate66
	}

yystate232:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'D' || c >= 'F' && c <= 'N' || c >= 'P' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'd' || c >= 'f' && c <= 'n' || c >= 'p' && c <= 'z':
		goto yystate66
	case c == 'E' || c == 'e':
		goto yystate233
	case c == 'O' || c == 'o':
		goto yystate237
	}

yystate233:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'B' && c <= 'Z' || c == '_' || c >= 'b' && c <= 'z':
		goto yystate66
	case c == 'A' || c == 'a':
		goto yystate234
	}

yystate234:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'S' || c >= 'U' && c <= 'Z' || c == '_' || c >= 'a' && c <= 's' || c >= 'u' && c <= 'z':
		goto yystate66
	case c == 'T' || c == 't':
		goto yystate235
	}

yystate235:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'D' || c >= 'F' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'd' || c >= 'f' && c <= 'z':
		goto yystate66
	case c == 'E' || c == 'e':
		goto yystate236
	}

yystate236:
	c = l.next()
	switch {
	default:
		goto yyrule76
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'z':
		goto yystate66
	}

yystate237:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'R' || c >= 'T' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'r' || c >= 't' && c <= 'z':
		goto yystate66
	case c == 'S' || c == 's':
		goto yystate238
	}

yystate238:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'R' || c >= 'T' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'r' || c >= 't' && c <= 'z':
		goto yystate66
	case c == 'S' || c == 's':
		goto yystate239
	}

yystate239:
	c = l.next()
	switch {
	default:
		goto yyrule77
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'z':
		goto yystate66
	}

yystate240:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'Q' || c >= 'S' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'q' || c >= 's' && c <= 'z':
		goto yystate66
	case c == 'R' || c == 'r':
		goto yystate241
	}

yystate241:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'C' || c >= 'E' && c <= 'Q' || c >= 'S' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'c' || c >= 'e' && c <= 'q' || c >= 's' && c <= 'z':
		goto yystate66
	case c == 'D' || c == 'd':
		goto yystate242
	case c == 'R' || c == 'r':
		goto yystate246
	}

yystate242:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'B' && c <= 'Z' || c == '_' || c >= 'b' && c <= 'z':
		goto yystate66
	case c == 'A' || c == 'a':
		goto yystate243
	}

yystate243:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'S' || c >= 'U' && c <= 'Z' || c == '_' || c >= 'a' && c <= 's' || c >= 'u' && c <= 'z':
		goto yystate66
	case c == 'T' || c == 't':
		goto yystate244
	}

yystate244:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'D' || c >= 'F' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'd' || c >= 'f' && c <= 'z':
		goto yystate66
	case c == 'E' || c == 'e':
		goto yystate245
	}

yystate245:
	c = l.next()
	switch {
	default:
		goto yyrule78
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'z':
		goto yystate66
	}

yystate246:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'D' || c >= 'F' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'd' || c >= 'f' && c <= 'z':
		goto yystate66
	case c == 'E' || c == 'e':
		goto yystate247
	}

yystate247:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'M' || c >= 'O' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'm' || c >= 'o' && c <= 'z':
		goto yystate66
	case c == 'N' || c == 'n':
		goto yystate248
	}

yystate248:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'S' || c >= 'U' && c <= 'Z' || c == '_' || c >= 'a' && c <= 's' || c >= 'u' && c <= 'z':
		goto yystate66
	case c == 'T' || c == 't':
		goto yystate249
	}

yystate249:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'Z' || c >= 'a' && c <= 'z':
		goto yystate66
	case c == '_':
		goto yystate250
	}

yystate250:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'C' || c >= 'E' && c <= 'S' || c >= 'V' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'c' || c >= 'e' && c <= 's' || c >= 'v' && c <= 'z':
		goto yystate66
	case c == 'D' || c == 'd':
		goto yystate251
	case c == 'T' || c == 't':
		goto yystate255
	case c == 'U' || c == 'u':
		goto yystate264
	}

yystate251:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'B' && c <= 'Z' || c == '_' || c >= 'b' && c <= 'z':
		goto yystate66
	case c == 'A' || c == 'a':
		goto yystate252
	}

yystate252:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'S' || c >= 'U' && c <= 'Z' || c == '_' || c >= 'a' && c <= 's' || c >= 'u' && c <= 'z':
		goto yystate66
	case c == 'T' || c == 't':
		goto yystate253
	}

yystate253:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'D' || c >= 'F' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'd' || c >= 'f' && c <= 'z':
		goto yystate66
	case c == 'E' || c == 'e':
		goto yystate254
	}

yystate254:
	c = l.next()
	switch {
	default:
		goto yyrule79
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'z':
		goto yystate66
	}

yystate255:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'H' || c >= 'J' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'h' || c >= 'j' && c <= 'z':
		goto yystate66
	case c == 'I' || c == 'i':
		goto yystate256
	}

yystate256:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'L' || c >= 'N' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'l' || c >= 'n' && c <= 'z':
		goto yystate66
	case c == 'M' || c == 'm':
		goto yystate257
	}

yystate257:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'D' || c >= 'F' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'd' || c >= 'f' && c <= 'z':
		goto yystate66
	case c == 'E' || c == 'e':
		goto yystate258
	}

yystate258:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'R' || c >= 'T' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'r' || c >= 't' && c <= 'z':
		goto yystate66
	case c == 'S' || c == 's':
		goto yystate259
	}

yystate259:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'S' || c >= 'U' && c <= 'Z' || c == '_' || c >= 'a' && c <= 's' || c >= 'u' && c <= 'z':
		goto yystate66
	case c == 'T' || c == 't':
		goto yystate260
	}

yystate260:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'B' && c <= 'Z' || c == '_' || c >= 'b' && c <= 'z':
		goto yystate66
	case c == 'A' || c == 'a':
		goto yystate261
	}

yystate261:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'L' || c >= 'N' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'l' || c >= 'n' && c <= 'z':
		goto yystate66
	case c == 'M' || c == 'm':
		goto yystate262
	}

yystate262:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'O' || c >= 'Q' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'o' || c >= 'q' && c <= 'z':
		goto yystate66
	case c == 'P' || c == 'p':
		goto yystate263
	}

yystate263:
	c = l.next()
	switch {
	default:
		goto yyrule236
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'z':
		goto yystate66
	}

yystate264:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'R' || c >= 'T' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'r' || c >= 't' && c <= 'z':
		goto yystate66
	case c == 'S' || c == 's':
		goto yystate265
	}

yystate265:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'D' || c >= 'F' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'd' || c >= 'f' && c <= 'z':
		goto yystate66
	case c == 'E' || c == 'e':
		goto yystate266
	}

yystate266:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'Q' || c >= 'S' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'q' || c >= 's' && c <= 'z':
		goto yystate66
	case c == 'R' || c == 'r':
		goto yystate267
	}

yystate267:
	c = l.next()
	switch {
	default:
		goto yyrule80
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'z':
		goto yystate66
	}

yystate268:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'B' && c <= 'D' || c >= 'F' && c <= 'H' || c >= 'J' && c <= 'N' || c == 'P' || c == 'Q' || c == 'S' || c == 'T' || c >= 'V' && c <= 'Z' || c == '_' || c >= 'b' && c <= 'd' || c >= 'f' && c <= 'h' || c >= 'j' && c <= 'n' || c == 'p' || c == 'q' || c == 's' || c == 't' || c >= 'v' && c <= 'z':
		goto yystate66
	case c == 'A' || c == 'a':
		goto yystate269
	case c == 'E' || c == 'e':
		goto yystate324
	case c == 'I' || c == 'i':
		goto yystate357
	case c == 'O' || c == 'o':
		goto yystate365
	case c == 'R' || c == 'r':
		goto yystate370
	case c == 'U' || c == 'u':
		goto yystate373
	}

yystate269:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'S' || c >= 'U' && c <= 'X' || c == 'Z' || c == '_' || c >= 'a' && c <= 's' || c >= 'u' && c <= 'x' || c == 'z':
		goto yystate66
	case c == 'T' || c == 't':
		goto yystate270
	case c == 'Y' || c == 'y':
		goto yystate282
	}

yystate270:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'B' && c <= 'D' || c >= 'F' && c <= 'Z' || c == '_' || c >= 'b' && c <= 'd' || c >= 'f' && c <= 'z':
		goto yystate66
	case c == 'A' || c == 'a':
		goto yystate271
	case c == 'E' || c == 'e':
		goto yystate277
	}

yystate271:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c == 'A' || c >= 'C' && c <= 'Z' || c == '_' || c == 'a' || c >= 'c' && c <= 'z':
		goto yystate66
	case c == 'B' || c == 'b':
		goto yystate272
	}

yystate272:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'B' && c <= 'Z' || c == '_' || c >= 'b' && c <= 'z':
		goto yystate66
	case c == 'A' || c == 'a':
		goto yystate273
	}

yystate273:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'R' || c >= 'T' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'r' || c >= 't' && c <= 'z':
		goto yystate66
	case c == 'S' || c == 's':
		goto yystate274
	}

yystate274:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'D' || c >= 'F' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'd' || c >= 'f' && c <= 'z':
		goto yystate66
	case c == 'E' || c == 'e':
		goto yystate275
	}

yystate275:
	c = l.next()
	switch {
	default:
		goto yyrule81
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'R' || c >= 'T' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'r' || c >= 't' && c <= 'z':
		goto yystate66
	case c == 'S' || c == 's':
		goto yystate276
	}

yystate276:
	c = l.next()
	switch {
	default:
		goto yyrule82
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'z':
		goto yystate66
	}

yystate277:
	c = l.next()
	switch {
	default:
		goto yyrule252
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'S' || c >= 'U' && c <= 'Z' || c == '_' || c >= 'a' && c <= 's' || c >= 'u' && c <= 'z':
		goto yystate66
	case c == 'T' || c == 't':
		goto yystate278
	}

yystate278:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'H' || c >= 'J' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'h' || c >= 'j' && c <= 'z':
		goto yystate66
	case c == 'I' || c == 'i':
		goto yystate279
	}

yystate279:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'L' || c >= 'N' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'l' || c >= 'n' && c <= 'z':
		goto yystate66
	case c == 'M' || c == 'm':
		goto yystate280
	}

yystate280:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'D' || c >= 'F' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'd' || c >= 'f' && c <= 'z':
		goto yystate66
	case c == 'E' || c == 'e':
		goto yystate281
	}

yystate281:
	c = l.next()
	switch {
	default:
		goto yyrule255
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'z':
		goto yystate66
	}

yystate282:
	c = l.next()
	switch {
	default:
		goto yyrule83
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'N' || c >= 'P' && c <= 'Z' || c >= 'a' && c <= 'n' || c >= 'p' && c <= 'z':
		goto yystate66
	case c == 'O' || c == 'o':
		goto yystate283
	case c == '_':
		goto yystate298
	}

yystate283:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'E' || c >= 'G' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'e' || c >= 'g' && c <= 'z':
		goto yystate66
	case c == 'F' || c == 'f':
		goto yystate284
	}

yystate284:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'L' || c >= 'N' && c <= 'V' || c == 'X' || c == 'Z' || c == '_' || c >= 'a' && c <= 'l' || c >= 'n' && c <= 'v' || c == 'x' || c == 'z':
		goto yystate66
	case c == 'M' || c == 'm':
		goto yystate285
	case c == 'W' || c == 'w':
		goto yystate290
	case c == 'Y' || c == 'y':
		goto yystate294
	}

yystate285:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'N' || c >= 'P' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'n' || c >= 'p' && c <= 'z':
		goto yystate66
	case c == 'O' || c == 'o':
		goto yystate286
	}

yystate286:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'M' || c >= 'O' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'm' || c >= 'o' && c <= 'z':
		goto yystate66
	case c == 'N' || c == 'n':
		goto yystate287
	}

yystate287:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'S' || c >= 'U' && c <= 'Z' || c == '_' || c >= 'a' && c <= 's' || c >= 'u' && c <= 'z':
		goto yystate66
	case c == 'T' || c == 't':
		goto yystate288
	}

yystate288:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'G' || c >= 'I' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'g' || c >= 'i' && c <= 'z':
		goto yystate66
	case c == 'H' || c == 'h':
		goto yystate289
	}

yystate289:
	c = l.next()
	switch {
	default:
		goto yyrule85
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'z':
		goto yystate66
	}

yystate290:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'D' || c >= 'F' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'd' || c >= 'f' && c <= 'z':
		goto yystate66
	case c == 'E' || c == 'e':
		goto yystate291
	}

yystate291:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'D' || c >= 'F' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'd' || c >= 'f' && c <= 'z':
		goto yystate66
	case c == 'E' || c == 'e':
		goto yystate292
	}

yystate292:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'J' || c >= 'L' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'j' || c >= 'l' && c <= 'z':
		goto yystate66
	case c == 'K' || c == 'k':
		goto yystate293
	}

yystate293:
	c = l.next()
	switch {
	default:
		goto yyrule84
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'z':
		goto yystate66
	}

yystate294:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'D' || c >= 'F' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'd' || c >= 'f' && c <= 'z':
		goto yystate66
	case c == 'E' || c == 'e':
		goto yystate295
	}

yystate295:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'B' && c <= 'Z' || c == '_' || c >= 'b' && c <= 'z':
		goto yystate66
	case c == 'A' || c == 'a':
		goto yystate296
	}

yystate296:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'Q' || c >= 'S' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'q' || c >= 's' && c <= 'z':
		goto yystate66
	case c == 'R' || c == 'r':
		goto yystate297
	}

yystate297:
	c = l.next()
	switch {
	default:
		goto yyrule86
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'z':
		goto yystate66
	}

yystate298:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'G' || c >= 'I' && c <= 'L' || c >= 'N' && c <= 'R' || c >= 'T' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'g' || c >= 'i' && c <= 'l' || c >= 'n' && c <= 'r' || c >= 't' && c <= 'z':
		goto yystate66
	case c == 'H' || c == 'h':
		goto yystate299
	case c == 'M' || c == 'm':
		goto yystate303
	case c == 'S' || c == 's':
		goto yystate318
	}

yystate299:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'N' || c >= 'P' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'n' || c >= 'p' && c <= 'z':
		goto yystate66
	case c == 'O' || c == 'o':
		goto yystate300
	}

yystate300:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'T' || c >= 'V' && c <= 'Z' || c == '_' || c >= 'a' && c <= 't' || c >= 'v' && c <= 'z':
		goto yystate66
	case c == 'U' || c == 'u':
		goto yystate301
	}

yystate301:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'Q' || c >= 'S' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'q' || c >= 's' && c <= 'z':
		goto yystate66
	case c == 'R' || c == 'r':
		goto yystate302
	}

yystate302:
	c = l.next()
	switch {
	default:
		goto yyrule87
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'z':
		goto yystate66
	}

yystate303:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'H' || c >= 'J' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'h' || c >= 'j' && c <= 'z':
		goto yystate66
	case c == 'I' || c == 'i':
		goto yystate304
	}

yystate304:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c == 'A' || c == 'B' || c >= 'D' && c <= 'M' || c >= 'O' && c <= 'Z' || c == '_' || c == 'a' || c == 'b' || c >= 'd' && c <= 'm' || c >= 'o' && c <= 'z':
		goto yystate66
	case c == 'C' || c == 'c':
		goto yystate305
	case c == 'N' || c == 'n':
		goto yystate314
	}

yystate305:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'Q' || c >= 'S' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'q' || c >= 's' && c <= 'z':
		goto yystate66
	case c == 'R' || c == 'r':
		goto yystate306
	}

yystate306:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'N' || c >= 'P' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'n' || c >= 'p' && c <= 'z':
		goto yystate66
	case c == 'O' || c == 'o':
		goto yystate307
	}

yystate307:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'R' || c >= 'T' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'r' || c >= 't' && c <= 'z':
		goto yystate66
	case c == 'S' || c == 's':
		goto yystate308
	}

yystate308:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'D' || c >= 'F' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'd' || c >= 'f' && c <= 'z':
		goto yystate66
	case c == 'E' || c == 'e':
		goto yystate309
	}

yystate309:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c == 'A' || c == 'B' || c >= 'D' && c <= 'Z' || c == '_' || c == 'a' || c == 'b' || c >= 'd' && c <= 'z':
		goto yystate66
	case c == 'C' || c == 'c':
		goto yystate310
	}

yystate310:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'N' || c >= 'P' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'n' || c >= 'p' && c <= 'z':
		goto yystate66
	case c == 'O' || c == 'o':
		goto yystate311
	}

yystate311:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'M' || c >= 'O' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'm' || c >= 'o' && c <= 'z':
		goto yystate66
	case c == 'N' || c == 'n':
		goto yystate312
	}

yystate312:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'C' || c >= 'E' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'c' || c >= 'e' && c <= 'z':
		goto yystate66
	case c == 'D' || c == 'd':
		goto yystate313
	}

yystate313:
	c = l.next()
	switch {
	default:
		goto yyrule88
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'z':
		goto yystate66
	}

yystate314:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'T' || c >= 'V' && c <= 'Z' || c == '_' || c >= 'a' && c <= 't' || c >= 'v' && c <= 'z':
		goto yystate66
	case c == 'U' || c == 'u':
		goto yystate315
	}

yystate315:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'S' || c >= 'U' && c <= 'Z' || c == '_' || c >= 'a' && c <= 's' || c >= 'u' && c <= 'z':
		goto yystate66
	case c == 'T' || c == 't':
		goto yystate316
	}

yystate316:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'D' || c >= 'F' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'd' || c >= 'f' && c <= 'z':
		goto yystate66
	case c == 'E' || c == 'e':
		goto yystate317
	}

yystate317:
	c = l.next()
	switch {
	default:
		goto yyrule89
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'z':
		goto yystate66
	}

yystate318:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'D' || c >= 'F' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'd' || c >= 'f' && c <= 'z':
		goto yystate66
	case c == 'E' || c == 'e':
		goto yystate319
	}

yystate319:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c == 'A' || c == 'B' || c >= 'D' && c <= 'Z' || c == '_' || c == 'a' || c == 'b' || c >= 'd' && c <= 'z':
		goto yystate66
	case c == 'C' || c == 'c':
		goto yystate320
	}

yystate320:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'N' || c >= 'P' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'n' || c >= 'p' && c <= 'z':
		goto yystate66
	case c == 'O' || c == 'o':
		goto yystate321
	}

yystate321:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'M' || c >= 'O' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'm' || c >= 'o' && c <= 'z':
		goto yystate66
	case c == 'N' || c == 'n':
		goto yystate322
	}

yystate322:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'C' || c >= 'E' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'c' || c >= 'e' && c <= 'z':
		goto yystate66
	case c == 'D' || c == 'd':
		goto yystate323
	}

yystate323:
	c = l.next()
	switch {
	default:
		goto yyrule90
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'z':
		goto yystate66
	}

yystate324:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c == 'B' || c == 'D' || c == 'E' || c >= 'G' && c <= 'K' || c >= 'M' && c <= 'R' || c >= 'T' && c <= 'Z' || c == '_' || c == 'b' || c == 'd' || c == 'e' || c >= 'g' && c <= 'k' || c >= 'm' && c <= 'r' || c >= 't' && c <= 'z':
		goto yystate66
	case c == 'A' || c == 'a':
		goto yystate325
	case c == 'C' || c == 'c':
		goto yystate333
	case c == 'F' || c == 'f':
		goto yystate338
	case c == 'L' || c == 'l':
		goto yystate343
	case c == 'S' || c == 's':
		goto yystate351
	}

yystate325:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'K' || c >= 'M' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'k' || c >= 'm' && c <= 'z':
		goto yystate66
	case c == 'L' || c == 'l':
		goto yystate326
	}

yystate326:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'K' || c >= 'M' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'k' || c >= 'm' && c <= 'z':
		goto yystate66
	case c == 'L' || c == 'l':
		goto yystate327
	}

yystate327:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'N' || c >= 'P' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'n' || c >= 'p' && c <= 'z':
		goto yystate66
	case c == 'O' || c == 'o':
		goto yystate328
	}

yystate328:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c == 'A' || c == 'B' || c >= 'D' && c <= 'Z' || c == '_' || c == 'a' || c == 'b' || c >= 'd' && c <= 'z':
		goto yystate66
	case c == 'C' || c == 'c':
		goto yystate329
	}

yystate329:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'B' && c <= 'Z' || c == '_' || c >= 'b' && c <= 'z':
		goto yystate66
	case c == 'A' || c == 'a':
		goto yystate330
	}

yystate330:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'S' || c >= 'U' && c <= 'Z' || c == '_' || c >= 'a' && c <= 's' || c >= 'u' && c <= 'z':
		goto yystate66
	case c == 'T' || c == 't':
		goto yystate331
	}

yystate331:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'D' || c >= 'F' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'd' || c >= 'f' && c <= 'z':
		goto yystate66
	case c == 'E' || c == 'e':
		goto yystate332
	}

yystate332:
	c = l.next()
	switch {
	default:
		goto yyrule91
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'z':
		goto yystate66
	}

yystate333:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'H' || c >= 'J' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'h' || c >= 'j' && c <= 'z':
		goto yystate66
	case c == 'I' || c == 'i':
		goto yystate334
	}

yystate334:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'L' || c >= 'N' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'l' || c >= 'n' && c <= 'z':
		goto yystate66
	case c == 'M' || c == 'm':
		goto yystate335
	}

yystate335:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'B' && c <= 'Z' || c == '_' || c >= 'b' && c <= 'z':
		goto yystate66
	case c == 'A' || c == 'a':
		goto yystate336
	}

yystate336:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'K' || c >= 'M' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'k' || c >= 'm' && c <= 'z':
		goto yystate66
	case c == 'L' || c == 'l':
		goto yystate337
	}

yystate337:
	c = l.next()
	switch {
	default:
		goto yyrule246
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'z':
		goto yystate66
	}

yystate338:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'B' && c <= 'Z' || c == '_' || c >= 'b' && c <= 'z':
		goto yystate66
	case c == 'A' || c == 'a':
		goto yystate339
	}

yystate339:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'T' || c >= 'V' && c <= 'Z' || c == '_' || c >= 'a' && c <= 't' || c >= 'v' && c <= 'z':
		goto yystate66
	case c == 'U' || c == 'u':
		goto yystate340
	}

yystate340:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'K' || c >= 'M' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'k' || c >= 'm' && c <= 'z':
		goto yystate66
	case c == 'L' || c == 'l':
		goto yystate341
	}

yystate341:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'S' || c >= 'U' && c <= 'Z' || c == '_' || c >= 'a' && c <= 's' || c >= 'u' && c <= 'z':
		goto yystate66
	case c == 'T' || c == 't':
		goto yystate342
	}

yystate342:
	c = l.next()
	switch {
	default:
		goto yyrule92
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'z':
		goto yystate66
	}

yystate343:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'B' && c <= 'D' || c >= 'F' && c <= 'Z' || c == '_' || c >= 'b' && c <= 'd' || c >= 'f' && c <= 'z':
		goto yystate66
	case c == 'A' || c == 'a':
		goto yystate344
	case c == 'E' || c == 'e':
		goto yystate348
	}

yystate344:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'X' || c == 'Z' || c == '_' || c >= 'a' && c <= 'x' || c == 'z':
		goto yystate66
	case c == 'Y' || c == 'y':
		goto yystate345
	}

yystate345:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'D' || c >= 'F' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'd' || c >= 'f' && c <= 'z':
		goto yystate66
	case c == 'E' || c == 'e':
		goto yystate346
	}

yystate346:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'C' || c >= 'E' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'c' || c >= 'e' && c <= 'z':
		goto yystate66
	case c == 'D' || c == 'd':
		goto yystate347
	}

yystate347:
	c = l.next()
	switch {
	default:
		goto yyrule93
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'z':
		goto yystate66
	}

yystate348:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'S' || c >= 'U' && c <= 'Z' || c == '_' || c >= 'a' && c <= 's' || c >= 'u' && c <= 'z':
		goto yystate66
	case c == 'T' || c == 't':
		goto yystate349
	}

yystate349:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'D' || c >= 'F' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'd' || c >= 'f' && c <= 'z':
		goto yystate66
	case c == 'E' || c == 'e':
		goto yystate350
	}

yystate350:
	c = l.next()
	switch {
	default:
		goto yyrule94
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'z':
		goto yystate66
	}

yystate351:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c == 'A' || c == 'B' || c >= 'D' && c <= 'Z' || c == '_' || c == 'a' || c == 'b' || c >= 'd' && c <= 'z':
		goto yystate66
	case c == 'C' || c == 'c':
		goto yystate352
	}

yystate352:
	c = l.next()
	switch {
	default:
		goto yyrule95
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'Q' || c >= 'S' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'q' || c >= 's' && c <= 'z':
		goto yystate66
	case c == 'R' || c == 'r':
		goto yystate353
	}

yystate353:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'H' || c >= 'J' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'h' || c >= 'j' && c <= 'z':
		goto yystate66
	case c == 'I' || c == 'i':
		goto yystate354
	}

yystate354:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c == 'A' || c >= 'C' && c <= 'Z' || c == '_' || c == 'a' || c >= 'c' && c <= 'z':
		goto yystate66
	case c == 'B' || c == 'b':
		goto yystate355
	}

yystate355:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'D' || c >= 'F' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'd' || c >= 'f' && c <= 'z':
		goto yystate66
	case c == 'E' || c == 'e':
		goto yystate356
	}

yystate356:
	c = l.next()
	switch {
	default:
		goto yyrule96
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'z':
		goto yystate66
	}

yystate357:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'R' || c == 'T' || c == 'U' || c >= 'W' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'r' || c == 't' || c == 'u' || c >= 'w' && c <= 'z':
		goto yystate66
	case c == 'S' || c == 's':
		goto yystate358
	case c == 'V' || c == 'v':
		goto yystate364
	}

yystate358:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'S' || c >= 'U' && c <= 'Z' || c == '_' || c >= 'a' && c <= 's' || c >= 'u' && c <= 'z':
		goto yystate66
	case c == 'T' || c == 't':
		goto yystate359
	}

yystate359:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'H' || c >= 'J' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'h' || c >= 'j' && c <= 'z':
		goto yystate66
	case c == 'I' || c == 'i':
		goto yystate360
	}

yystate360:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'M' || c >= 'O' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'm' || c >= 'o' && c <= 'z':
		goto yystate66
	case c == 'N' || c == 'n':
		goto yystate361
	}

yystate361:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c == 'A' || c == 'B' || c >= 'D' && c <= 'Z' || c == '_' || c == 'a' || c == 'b' || c >= 'd' && c <= 'z':
		goto yystate66
	case c == 'C' || c == 'c':
		goto yystate362
	}

yystate362:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'S' || c >= 'U' && c <= 'Z' || c == '_' || c >= 'a' && c <= 's' || c >= 'u' && c <= 'z':
		goto yystate66
	case c == 'T' || c == 't':
		goto yystate363
	}

yystate363:
	c = l.next()
	switch {
	default:
		goto yyrule98
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'z':
		goto yystate66
	}

yystate364:
	c = l.next()
	switch {
	default:
		goto yyrule99
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'z':
		goto yystate66
	}

yystate365:
	c = l.next()
	switch {
	default:
		goto yyrule100
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'T' || c >= 'V' && c <= 'Z' || c == '_' || c >= 'a' && c <= 't' || c >= 'v' && c <= 'z':
		goto yystate66
	case c == 'U' || c == 'u':
		goto yystate366
	}

yystate366:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c == 'A' || c >= 'C' && c <= 'Z' || c == '_' || c == 'a' || c >= 'c' && c <= 'z':
		goto yystate66
	case c == 'B' || c == 'b':
		goto yystate367
	}

yystate367:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'K' || c >= 'M' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'k' || c >= 'm' && c <= 'z':
		goto yystate66
	case c == 'L' || c == 'l':
		goto yystate368
	}

yystate368:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'D' || c >= 'F' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'd' || c >= 'f' && c <= 'z':
		goto yystate66
	case c == 'E' || c == 'e':
		goto yystate369
	}

yystate369:
	c = l.next()
	switch {
	default:
		goto yyrule249
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'z':
		goto yystate66
	}

yystate370:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'N' || c >= 'P' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'n' || c >= 'p' && c <= 'z':
		goto yystate66
	case c == 'O' || c == 'o':
		goto yystate371
	}

yystate371:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'O' || c >= 'Q' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'o' || c >= 'q' && c <= 'z':
		goto yystate66
	case c == 'P' || c == 'p':
		goto yystate372
	}

yystate372:
	c = l.next()
	switch {
	default:
		goto yyrule97
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'z':
		goto yystate66
	}

yystate373:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'B' && c <= 'O' || c >= 'Q' && c <= 'Z' || c == '_' || c >= 'b' && c <= 'o' || c >= 'q' && c <= 'z':
		goto yystate66
	case c == 'A' || c == 'a':
		goto yystate374
	case c == 'P' || c == 'p':
		goto yystate376
	}

yystate374:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'K' || c >= 'M' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'k' || c >= 'm' && c <= 'z':
		goto yystate66
	case c == 'L' || c == 'l':
		goto yystate375
	}

yystate375:
	c = l.next()
	switch {
	default:
		goto yyrule101
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'z':
		goto yystate66
	}

yystate376:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'K' || c >= 'M' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'k' || c >= 'm' && c <= 'z':
		goto yystate66
	case c == 'L' || c == 'l':
		goto yystate377
	}

yystate377:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'H' || c >= 'J' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'h' || c >= 'j' && c <= 'z':
		goto yystate66
	case c == 'I' || c == 'i':
		goto yystate378
	}

yystate378:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c == 'A' || c == 'B' || c >= 'D' && c <= 'Z' || c == '_' || c == 'a' || c == 'b' || c >= 'd' && c <= 'z':
		goto yystate66
	case c == 'C' || c == 'c':
		goto yystate379
	}

yystate379:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'B' && c <= 'Z' || c == '_' || c >= 'b' && c <= 'z':
		goto yystate66
	case c == 'A' || c == 'a':
		goto yystate380
	}

yystate380:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'S' || c >= 'U' && c <= 'Z' || c == '_' || c >= 'a' && c <= 's' || c >= 'u' && c <= 'z':
		goto yystate66
	case c == 'T' || c == 't':
		goto yystate381
	}

yystate381:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'D' || c >= 'F' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'd' || c >= 'f' && c <= 'z':
		goto yystate66
	case c == 'E' || c == 'e':
		goto yystate382
	}

yystate382:
	c = l.next()
	switch {
	default:
		goto yyrule102
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'z':
		goto yystate66
	}

yystate383:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'K' || c == 'M' || c >= 'O' && c <= 'R' || c >= 'T' && c <= 'W' || c == 'Y' || c == 'Z' || c == '_' || c >= 'a' && c <= 'k' || c == 'm' || c >= 'o' && c <= 'r' || c >= 't' && c <= 'w' || c == 'y' || c == 'z':
		goto yystate66
	case c == 'L' || c == 'l':
		goto yystate384
	case c == 'N' || c == 'n':
		goto yystate387
	case c == 'S' || c == 's':
		goto yystate396
	case c == 'X' || c == 'x':
		goto yystate401
	}

yystate384:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'R' || c >= 'T' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'r' || c >= 't' && c <= 'z':
		goto yystate66
	case c == 'S' || c == 's':
		goto yystate385
	}

yystate385:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'D' || c >= 'F' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'd' || c >= 'f' && c <= 'z':
		goto yystate66
	case c == 'E' || c == 'e':
		goto yystate386
	}

yystate386:
	c = l.next()
	switch {
	default:
		goto yyrule103
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'z':
		goto yystate66
	}

yystate387:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'C' || c == 'E' || c == 'F' || c >= 'H' && c <= 'T' || c >= 'V' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'c' || c == 'e' || c == 'f' || c >= 'h' && c <= 't' || c >= 'v' && c <= 'z':
		goto yystate66
	case c == 'D' || c == 'd':
		goto yystate388
	case c == 'G' || c == 'g':
		goto yystate389
	case c == 'U' || c == 'u':
		goto yystate394
	}

yystate388:
	c = l.next()
	switch {
	default:
		goto yyrule104
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'z':
		goto yystate66
	}

yystate389:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'H' || c >= 'J' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'h' || c >= 'j' && c <= 'z':
		goto yystate66
	case c == 'I' || c == 'i':
		goto yystate390
	}

yystate390:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'M' || c >= 'O' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'm' || c >= 'o' && c <= 'z':
		goto yystate66
	case c == 'N' || c == 'n':
		goto yystate391
	}

yystate391:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'D' || c >= 'F' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'd' || c >= 'f' && c <= 'z':
		goto yystate66
	case c == 'E' || c == 'e':
		goto yystate392
	}

yystate392:
	c = l.next()
	switch {
	default:
		goto yyrule105
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'R' || c >= 'T' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'r' || c >= 't' && c <= 'z':
		goto yystate66
	case c == 'S' || c == 's':
		goto yystate393
	}

yystate393:
	c = l.next()
	switch {
	default:
		goto yyrule106
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'z':
		goto yystate66
	}

yystate394:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'L' || c >= 'N' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'l' || c >= 'n' && c <= 'z':
		goto yystate66
	case c == 'M' || c == 'm':
		goto yystate395
	}

yystate395:
	c = l.next()
	switch {
	default:
		goto yyrule108
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'z':
		goto yystate66
	}

yystate396:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c == 'A' || c == 'B' || c >= 'D' && c <= 'Z' || c == '_' || c == 'a' || c == 'b' || c >= 'd' && c <= 'z':
		goto yystate66
	case c == 'C' || c == 'c':
		goto yystate397
	}

yystate397:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'B' && c <= 'Z' || c == '_' || c >= 'b' && c <= 'z':
		goto yystate66
	case c == 'A' || c == 'a':
		goto yystate398
	}

yystate398:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'O' || c >= 'Q' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'o' || c >= 'q' && c <= 'z':
		goto yystate66
	case c == 'P' || c == 'p':
		goto yystate399
	}

yystate399:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'D' || c >= 'F' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'd' || c >= 'f' && c <= 'z':
		goto yystate66
	case c == 'E' || c == 'e':
		goto yystate400
	}

yystate400:
	c = l.next()
	switch {
	default:
		goto yyrule109
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'z':
		goto yystate66
	}

yystate401:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'D' || c >= 'F' && c <= 'H' || c >= 'J' && c <= 'O' || c >= 'Q' && c <= 'S' || c >= 'U' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'd' || c >= 'f' && c <= 'h' || c >= 'j' && c <= 'o' || c >= 'q' && c <= 's' || c >= 'u' && c <= 'z':
		goto yystate66
	case c == 'E' || c == 'e':
		goto yystate402
	case c == 'I' || c == 'i':
		goto yystate407
	case c == 'P' || c == 'p':
		goto yystate411
	case c == 'T' || c == 't':
		goto yystate416
	}

yystate402:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c == 'A' || c == 'B' || c >= 'D' && c <= 'Z' || c == '_' || c == 'a' || c == 'b' || c >= 'd' && c <= 'z':
		goto yystate66
	case c == 'C' || c == 'c':
		goto yystate403
	}

yystate403:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'T' || c >= 'V' && c <= 'Z' || c == '_' || c >= 'a' && c <= 't' || c >= 'v' && c <= 'z':
		goto yystate66
	case c == 'U' || c == 'u':
		goto yystate404
	}

yystate404:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'S' || c >= 'U' && c <= 'Z' || c == '_' || c >= 'a' && c <= 's' || c >= 'u' && c <= 'z':
		goto yystate66
	case c == 'T' || c == 't':
		goto yystate405
	}

yystate405:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'D' || c >= 'F' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'd' || c >= 'f' && c <= 'z':
		goto yystate66
	case c == 'E' || c == 'e':
		goto yystate406
	}

yystate406:
	c = l.next()
	switch {
	default:
		goto yyrule107
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'z':
		goto yystate66
	}

yystate407:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'R' || c >= 'T' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'r' || c >= 't' && c <= 'z':
		goto yystate66
	case c == 'S' || c == 's':
		goto yystate408
	}

yystate408:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'S' || c >= 'U' && c <= 'Z' || c == '_' || c >= 'a' && c <= 's' || c >= 'u' && c <= 'z':
		goto yystate66
	case c == 'T' || c == 't':
		goto yystate409
	}

yystate409:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'R' || c >= 'T' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'r' || c >= 't' && c <= 'z':
		goto yystate66
	case c == 'S' || c == 's':
		goto yystate410
	}

yystate410:
	c = l.next()
	switch {
	default:
		goto yyrule110
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'z':
		goto yystate66
	}

yystate411:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'K' || c >= 'M' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'k' || c >= 'm' && c <= 'z':
		goto yystate66
	case c == 'L' || c == 'l':
		goto yystate412
	}

yystate412:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'B' && c <= 'Z' || c == '_' || c >= 'b' && c <= 'z':
		goto yystate66
	case c == 'A' || c == 'a':
		goto yystate413
	}

yystate413:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'H' || c >= 'J' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'h' || c >= 'j' && c <= 'z':
		goto yystate66
	case c == 'I' || c == 'i':
		goto yystate414
	}

yystate414:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'M' || c >= 'O' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'm' || c >= 'o' && c <= 'z':
		goto yystate66
	case c == 'N' || c == 'n':
		goto yystate415
	}

yystate415:
	c = l.next()
	switch {
	default:
		goto yyrule111
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'z':
		goto yystate66
	}

yystate416:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'Q' || c >= 'S' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'q' || c >= 's' && c <= 'z':
		goto yystate66
	case c == 'R' || c == 'r':
		goto yystate417
	}

yystate417:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'B' && c <= 'Z' || c == '_' || c >= 'b' && c <= 'z':
		goto yystate66
	case c == 'A' || c == 'a':
		goto yystate418
	}

yystate418:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c == 'A' || c == 'B' || c >= 'D' && c <= 'Z' || c == '_' || c == 'a' || c == 'b' || c >= 'd' && c <= 'z':
		goto yystate66
	case c == 'C' || c == 'c':
		goto yystate419
	}

yystate419:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'S' || c >= 'U' && c <= 'Z' || c == '_' || c >= 'a' && c <= 's' || c >= 'u' && c <= 'z':
		goto yystate66
	case c == 'T' || c == 't':
		goto yystate420
	}

yystate420:
	c = l.next()
	switch {
	default:
		goto yyrule112
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'z':
		goto yystate66
	}

yystate421:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'B' && c <= 'H' || c == 'J' || c == 'K' || c == 'M' || c == 'N' || c == 'P' || c == 'Q' || c == 'S' || c == 'T' || c >= 'V' && c <= 'Z' || c == '_' || c >= 'b' && c <= 'h' || c == 'j' || c == 'k' || c == 'm' || c == 'n' || c == 'p' || c == 'q' || c == 's' || c == 't' || c >= 'v' && c <= 'z':
		goto yystate66
	case c == 'A' || c == 'a':
		goto yystate422
	case c == 'I' || c == 'i':
		goto yystate426
	case c == 'L' || c == 'l':
		goto yystate430
	case c == 'O' || c == 'o':
		goto yystate434
	case c == 'R' || c == 'r':
		goto yystate448
	case c == 'U' || c == 'u':
		goto yystate451
	}

yystate422:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'K' || c >= 'M' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'k' || c >= 'm' && c <= 'z':
		goto yystate66
	case c == 'L' || c == 'l':
		goto yystate423
	}

yystate423:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'R' || c >= 'T' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'r' || c >= 't' && c <= 'z':
		goto yystate66
	case c == 'S' || c == 's':
		goto yystate424
	}

yystate424:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'D' || c >= 'F' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'd' || c >= 'f' && c <= 'z':
		goto yystate66
	case c == 'E' || c == 'e':
		goto yystate425
	}

yystate425:
	c = l.next()
	switch {
	default:
		goto yyrule233
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'z':
		goto yystate66
	}

yystate426:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'Q' || c >= 'S' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'q' || c >= 's' && c <= 'z':
		goto yystate66
	case c == 'R' || c == 'r':
		goto yystate427
	}

yystate427:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'R' || c >= 'T' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'r' || c >= 't' && c <= 'z':
		goto yystate66
	case c == 'S' || c == 's':
		goto yystate428
	}

yystate428:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'S' || c >= 'U' && c <= 'Z' || c == '_' || c >= 'a' && c <= 's' || c >= 'u' && c <= 'z':
		goto yystate66
	case c == 'T' || c == 't':
		goto yystate429
	}

yystate429:
	c = l.next()
	switch {
	default:
		goto yyrule113
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'z':
		goto yystate66
	}

yystate430:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'N' || c >= 'P' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'n' || c >= 'p' && c <= 'z':
		goto yystate66
	case c == 'O' || c == 'o':
		goto yystate431
	}

yystate431:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'B' && c <= 'Z' || c == '_' || c >= 'b' && c <= 'z':
		goto yystate66
	case c == 'A' || c == 'a':
		goto yystate432
	}

yystate432:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'S' || c >= 'U' && c <= 'Z' || c == '_' || c >= 'a' && c <= 's' || c >= 'u' && c <= 'z':
		goto yystate66
	case c == 'T' || c == 't':
		goto yystate433
	}

yystate433:
	c = l.next()
	switch {
	default:
		goto yyrule248
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'z':
		goto yystate66
	}

yystate434:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'Q' || c == 'S' || c == 'T' || c >= 'V' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'q' || c == 's' || c == 't' || c >= 'v' && c <= 'z':
		goto yystate66
	case c == 'R' || c == 'r':
		goto yystate435
	case c == 'U' || c == 'u':
		goto yystate440
	}

yystate435:
	c = l.next()
	switch {
	default:
		goto yyrule114
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'D' || c >= 'F' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'd' || c >= 'f' && c <= 'z':
		goto yystate66
	case c == 'E' || c == 'e':
		goto yystate436
	}

yystate436:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'H' || c >= 'J' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'h' || c >= 'j' && c <= 'z':
		goto yystate66
	case c == 'I' || c == 'i':
		goto yystate437
	}

yystate437:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'F' || c >= 'H' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'f' || c >= 'h' && c <= 'z':
		goto yystate66
	case c == 'G' || c == 'g':
		goto yystate438
	}

yystate438:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'M' || c >= 'O' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'm' || c >= 'o' && c <= 'z':
		goto yystate66
	case c == 'N' || c == 'n':
		goto yystate439
	}

yystate439:
	c = l.next()
	switch {
	default:
		goto yyrule115
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'z':
		goto yystate66
	}

yystate440:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'M' || c >= 'O' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'm' || c >= 'o' && c <= 'z':
		goto yystate66
	case c == 'N' || c == 'n':
		goto yystate441
	}

yystate441:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'C' || c >= 'E' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'c' || c >= 'e' && c <= 'z':
		goto yystate66
	case c == 'D' || c == 'd':
		goto yystate442
	}

yystate442:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'Z' || c >= 'a' && c <= 'z':
		goto yystate66
	case c == '_':
		goto yystate443
	}

yystate443:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'Q' || c >= 'S' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'q' || c >= 's' && c <= 'z':
		goto yystate66
	case c == 'R' || c == 'r':
		goto yystate444
	}

yystate444:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'N' || c >= 'P' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'n' || c >= 'p' && c <= 'z':
		goto yystate66
	case c == 'O' || c == 'o':
		goto yystate445
	}

yystate445:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'V' || c >= 'X' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'v' || c >= 'x' && c <= 'z':
		goto yystate66
	case c == 'W' || c == 'w':
		goto yystate446
	}

yystate446:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'R' || c >= 'T' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'r' || c >= 't' && c <= 'z':
		goto yystate66
	case c == 'S' || c == 's':
		goto yystate447
	}

yystate447:
	c = l.next()
	switch {
	default:
		goto yyrule116
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'z':
		goto yystate66
	}

yystate448:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'N' || c >= 'P' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'n' || c >= 'p' && c <= 'z':
		goto yystate66
	case c == 'O' || c == 'o':
		goto yystate449
	}

yystate449:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'L' || c >= 'N' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'l' || c >= 'n' && c <= 'z':
		goto yystate66
	case c == 'M' || c == 'm':
		goto yystate450
	}

yystate450:
	c = l.next()
	switch {
	default:
		goto yyrule117
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'z':
		goto yystate66
	}

yystate451:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'K' || c >= 'M' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'k' || c >= 'm' && c <= 'z':
		goto yystate66
	case c == 'L' || c == 'l':
		goto yystate452
	}

yystate452:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'K' || c >= 'M' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'k' || c >= 'm' && c <= 'z':
		goto yystate66
	case c == 'L' || c == 'l':
		goto yystate453
	}

yystate453:
	c = l.next()
	switch {
	default:
		goto yyrule118
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'S' || c >= 'U' && c <= 'Z' || c == '_' || c >= 'a' && c <= 's' || c >= 'u' && c <= 'z':
		goto yystate66
	case c == 'T' || c == 't':
		goto yystate454
	}

yystate454:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'D' || c >= 'F' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'd' || c >= 'f' && c <= 'z':
		goto yystate66
	case c == 'E' || c == 'e':
		goto yystate455
	}

yystate455:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'W' || c == 'Y' || c == 'Z' || c == '_' || c >= 'a' && c <= 'w' || c == 'y' || c == 'z':
		goto yystate66
	case c == 'X' || c == 'x':
		goto yystate456
	}

yystate456:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'S' || c >= 'U' && c <= 'Z' || c == '_' || c >= 'a' && c <= 's' || c >= 'u' && c <= 'z':
		goto yystate66
	case c == 'T' || c == 't':
		goto yystate457
	}

yystate457:
	c = l.next()
	switch {
	default:
		goto yyrule119
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'z':
		goto yystate66
	}

yystate458:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'K' || c >= 'M' && c <= 'Q' || c >= 'S' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'k' || c >= 'm' && c <= 'q' || c >= 's' && c <= 'z':
		goto yystate66
	case c == 'L' || c == 'l':
		goto yystate459
	case c == 'R' || c == 'r':
		goto yystate464
	}

yystate459:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'N' || c >= 'P' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'n' || c >= 'p' && c <= 'z':
		goto yystate66
	case c == 'O' || c == 'o':
		goto yystate460
	}

yystate460:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c == 'A' || c >= 'C' && c <= 'Z' || c == '_' || c == 'a' || c >= 'c' && c <= 'z':
		goto yystate66
	case c == 'B' || c == 'b':
		goto yystate461
	}

yystate461:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'B' && c <= 'Z' || c == '_' || c >= 'b' && c <= 'z':
		goto yystate66
	case c == 'A' || c == 'a':
		goto yystate462
	}

yystate462:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'K' || c >= 'M' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'k' || c >= 'm' && c <= 'z':
		goto yystate66
	case c == 'L' || c == 'l':
		goto yystate463
	}

yystate463:
	c = l.next()
	switch {
	default:
		goto yyrule183
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'z':
		goto yystate66
	}

yystate464:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'N' || c >= 'P' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'n' || c >= 'p' && c <= 'z':
		goto yystate66
	case c == 'O' || c == 'o':
		goto yystate465
	}

yystate465:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'T' || c >= 'V' && c <= 'Z' || c == '_' || c >= 'a' && c <= 't' || c >= 'v' && c <= 'z':
		goto yystate66
	case c == 'U' || c == 'u':
		goto yystate466
	}

yystate466:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'O' || c >= 'Q' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'o' || c >= 'q' && c <= 'z':
		goto yystate66
	case c == 'P' || c == 'p':
		goto yystate467
	}

yystate467:
	c = l.next()
	switch {
	default:
		goto yyrule120
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'Z' || c >= 'a' && c <= 'z':
		goto yystate66
	case c == '_':
		goto yystate468
	}

yystate468:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c == 'A' || c == 'B' || c >= 'D' && c <= 'Z' || c == '_' || c == 'a' || c == 'b' || c >= 'd' && c <= 'z':
		goto yystate66
	case c == 'C' || c == 'c':
		goto yystate469
	}

yystate469:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'N' || c >= 'P' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'n' || c >= 'p' && c <= 'z':
		goto yystate66
	case c == 'O' || c == 'o':
		goto yystate470
	}

yystate470:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'M' || c >= 'O' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'm' || c >= 'o' && c <= 'z':
		goto yystate66
	case c == 'N' || c == 'n':
		goto yystate471
	}

yystate471:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c == 'A' || c == 'B' || c >= 'D' && c <= 'Z' || c == '_' || c == 'a' || c == 'b' || c >= 'd' && c <= 'z':
		goto yystate66
	case c == 'C' || c == 'c':
		goto yystate472
	}

yystate472:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'B' && c <= 'Z' || c == '_' || c >= 'b' && c <= 'z':
		goto yystate66
	case c == 'A' || c == 'a':
		goto yystate473
	}

yystate473:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'S' || c >= 'U' && c <= 'Z' || c == '_' || c >= 'a' && c <= 's' || c >= 'u' && c <= 'z':
		goto yystate66
	case c == 'T' || c == 't':
		goto yystate474
	}

yystate474:
	c = l.next()
	switch {
	default:
		goto yyrule121
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'z':
		goto yystate66
	}

yystate475:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'B' && c <= 'H' || c >= 'J' && c <= 'N' || c >= 'P' && c <= 'Z' || c == '_' || c >= 'b' && c <= 'h' || c >= 'j' && c <= 'n' || c >= 'p' && c <= 'z':
		goto yystate66
	case c == 'A' || c == 'a':
		goto yystate476
	case c == 'I' || c == 'i':
		goto yystate481
	case c == 'O' || c == 'o':
		goto yystate493
	}

yystate476:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'U' || c >= 'W' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'u' || c >= 'w' && c <= 'z':
		goto yystate66
	case c == 'V' || c == 'v':
		goto yystate477
	}

yystate477:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'H' || c >= 'J' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'h' || c >= 'j' && c <= 'z':
		goto yystate66
	case c == 'I' || c == 'i':
		goto yystate478
	}

yystate478:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'M' || c >= 'O' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'm' || c >= 'o' && c <= 'z':
		goto yystate66
	case c == 'N' || c == 'n':
		goto yystate479
	}

yystate479:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'F' || c >= 'H' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'f' || c >= 'h' && c <= 'z':
		goto yystate66
	case c == 'G' || c == 'g':
		goto yystate480
	}

yystate480:
	c = l.next()
	switch {
	default:
		goto yyrule122
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'z':
		goto yystate66
	}

yystate481:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'F' || c >= 'H' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'f' || c >= 'h' && c <= 'z':
		goto yystate66
	case c == 'G' || c == 'g':
		goto yystate482
	}

yystate482:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'G' || c >= 'I' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'g' || c >= 'i' && c <= 'z':
		goto yystate66
	case c == 'H' || c == 'h':
		goto yystate483
	}

yystate483:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'Z' || c >= 'a' && c <= 'z':
		goto yystate66
	case c == '_':
		goto yystate484
	}

yystate484:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'O' || c >= 'Q' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'o' || c >= 'q' && c <= 'z':
		goto yystate66
	case c == 'P' || c == 'p':
		goto yystate485
	}

yystate485:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'Q' || c >= 'S' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'q' || c >= 's' && c <= 'z':
		goto yystate66
	case c == 'R' || c == 'r':
		goto yystate486
	}

yystate486:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'H' || c >= 'J' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'h' || c >= 'j' && c <= 'z':
		goto yystate66
	case c == 'I' || c == 'i':
		goto yystate487
	}

yystate487:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'N' || c >= 'P' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'n' || c >= 'p' && c <= 'z':
		goto yystate66
	case c == 'O' || c == 'o':
		goto yystate488
	}

yystate488:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'Q' || c >= 'S' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'q' || c >= 's' && c <= 'z':
		goto yystate66
	case c == 'R' || c == 'r':
		goto yystate489
	}

yystate489:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'H' || c >= 'J' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'h' || c >= 'j' && c <= 'z':
		goto yystate66
	case c == 'I' || c == 'i':
		goto yystate490
	}

yystate490:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'S' || c >= 'U' && c <= 'Z' || c == '_' || c >= 'a' && c <= 's' || c >= 'u' && c <= 'z':
		goto yystate66
	case c == 'T' || c == 't':
		goto yystate491
	}

yystate491:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'X' || c == 'Z' || c == '_' || c >= 'a' && c <= 'x' || c == 'z':
		goto yystate66
	case c == 'Y' || c == 'y':
		goto yystate492
	}

yystate492:
	c = l.next()
	switch {
	default:
		goto yyrule123
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'z':
		goto yystate66
	}

yystate493:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'T' || c >= 'V' && c <= 'Z' || c == '_' || c >= 'a' && c <= 't' || c >= 'v' && c <= 'z':
		goto yystate66
	case c == 'U' || c == 'u':
		goto yystate494
	}

yystate494:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'Q' || c >= 'S' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'q' || c >= 's' && c <= 'z':
		goto yystate66
	case c == 'R' || c == 'r':
		goto yystate495
	}

yystate495:
	c = l.next()
	switch {
	default:
		goto yyrule124
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'Z' || c >= 'a' && c <= 'z':
		goto yystate66
	case c == '_':
		goto yystate496
	}

yystate496:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'L' || c >= 'N' && c <= 'R' || c >= 'T' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'l' || c >= 'n' && c <= 'r' || c >= 't' && c <= 'z':
		goto yystate66
	case c == 'M' || c == 'm':
		goto yystate497
	case c == 'S' || c == 's':
		goto yystate512
	}

yystate497:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'H' || c >= 'J' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'h' || c >= 'j' && c <= 'z':
		goto yystate66
	case c == 'I' || c == 'i':
		goto yystate498
	}

yystate498:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c == 'A' || c == 'B' || c >= 'D' && c <= 'M' || c >= 'O' && c <= 'Z' || c == '_' || c == 'a' || c == 'b' || c >= 'd' && c <= 'm' || c >= 'o' && c <= 'z':
		goto yystate66
	case c == 'C' || c == 'c':
		goto yystate499
	case c == 'N' || c == 'n':
		goto yystate508
	}

yystate499:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'Q' || c >= 'S' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'q' || c >= 's' && c <= 'z':
		goto yystate66
	case c == 'R' || c == 'r':
		goto yystate500
	}

yystate500:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'N' || c >= 'P' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'n' || c >= 'p' && c <= 'z':
		goto yystate66
	case c == 'O' || c == 'o':
		goto yystate501
	}

yystate501:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'R' || c >= 'T' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'r' || c >= 't' && c <= 'z':
		goto yystate66
	case c == 'S' || c == 's':
		goto yystate502
	}

yystate502:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'D' || c >= 'F' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'd' || c >= 'f' && c <= 'z':
		goto yystate66
	case c == 'E' || c == 'e':
		goto yystate503
	}

yystate503:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c == 'A' || c == 'B' || c >= 'D' && c <= 'Z' || c == '_' || c == 'a' || c == 'b' || c >= 'd' && c <= 'z':
		goto yystate66
	case c == 'C' || c == 'c':
		goto yystate504
	}

yystate504:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'N' || c >= 'P' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'n' || c >= 'p' && c <= 'z':
		goto yystate66
	case c == 'O' || c == 'o':
		goto yystate505
	}

yystate505:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'M' || c >= 'O' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'm' || c >= 'o' && c <= 'z':
		goto yystate66
	case c == 'N' || c == 'n':
		goto yystate506
	}

yystate506:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'C' || c >= 'E' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'c' || c >= 'e' && c <= 'z':
		goto yystate66
	case c == 'D' || c == 'd':
		goto yystate507
	}

yystate507:
	c = l.next()
	switch {
	default:
		goto yyrule125
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'z':
		goto yystate66
	}

yystate508:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'T' || c >= 'V' && c <= 'Z' || c == '_' || c >= 'a' && c <= 't' || c >= 'v' && c <= 'z':
		goto yystate66
	case c == 'U' || c == 'u':
		goto yystate509
	}

yystate509:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'S' || c >= 'U' && c <= 'Z' || c == '_' || c >= 'a' && c <= 's' || c >= 'u' && c <= 'z':
		goto yystate66
	case c == 'T' || c == 't':
		goto yystate510
	}

yystate510:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'D' || c >= 'F' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'd' || c >= 'f' && c <= 'z':
		goto yystate66
	case c == 'E' || c == 'e':
		goto yystate511
	}

yystate511:
	c = l.next()
	switch {
	default:
		goto yyrule126
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'z':
		goto yystate66
	}

yystate512:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'D' || c >= 'F' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'd' || c >= 'f' && c <= 'z':
		goto yystate66
	case c == 'E' || c == 'e':
		goto yystate513
	}

yystate513:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c == 'A' || c == 'B' || c >= 'D' && c <= 'Z' || c == '_' || c == 'a' || c == 'b' || c >= 'd' && c <= 'z':
		goto yystate66
	case c == 'C' || c == 'c':
		goto yystate514
	}

yystate514:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'N' || c >= 'P' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'n' || c >= 'p' && c <= 'z':
		goto yystate66
	case c == 'O' || c == 'o':
		goto yystate515
	}

yystate515:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'M' || c >= 'O' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'm' || c >= 'o' && c <= 'z':
		goto yystate66
	case c == 'N' || c == 'n':
		goto yystate516
	}

yystate516:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'C' || c >= 'E' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'c' || c >= 'e' && c <= 'z':
		goto yystate66
	case c == 'D' || c == 'd':
		goto yystate517
	}

yystate517:
	c = l.next()
	switch {
	default:
		goto yyrule127
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'z':
		goto yystate66
	}

yystate518:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'C' || c == 'E' || c >= 'H' && c <= 'M' || c >= 'O' && c <= 'R' || c >= 'T' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'c' || c == 'e' || c >= 'h' && c <= 'm' || c >= 'o' && c <= 'r' || c >= 't' && c <= 'z':
		goto yystate66
	case c == 'D' || c == 'd':
		goto yystate519
	case c == 'F' || c == 'f':
		goto yystate528
	case c == 'G' || c == 'g':
		goto yystate533
	case c == 'N' || c == 'n':
		goto yystate538
	case c == 'S' || c == 's':
		goto yystate555
	}

yystate519:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'D' || c >= 'F' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'd' || c >= 'f' && c <= 'z':
		goto yystate66
	case c == 'E' || c == 'e':
		goto yystate520
	}

yystate520:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'M' || c >= 'O' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'm' || c >= 'o' && c <= 'z':
		goto yystate66
	case c == 'N' || c == 'n':
		goto yystate521
	}

yystate521:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'S' || c >= 'U' && c <= 'Z' || c == '_' || c >= 'a' && c <= 's' || c >= 'u' && c <= 'z':
		goto yystate66
	case c == 'T' || c == 't':
		goto yystate522
	}

yystate522:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'H' || c >= 'J' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'h' || c >= 'j' && c <= 'z':
		goto yystate66
	case c == 'I' || c == 'i':
		goto yystate523
	}

yystate523:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'E' || c >= 'G' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'e' || c >= 'g' && c <= 'z':
		goto yystate66
	case c == 'F' || c == 'f':
		goto yystate524
	}

yystate524:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'H' || c >= 'J' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'h' || c >= 'j' && c <= 'z':
		goto yystate66
	case c == 'I' || c == 'i':
		goto yystate525
	}

yystate525:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'D' || c >= 'F' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'd' || c >= 'f' && c <= 'z':
		goto yystate66
	case c == 'E' || c == 'e':
		goto yystate526
	}

yystate526:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'C' || c >= 'E' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'c' || c >= 'e' && c <= 'z':
		goto yystate66
	case c == 'D' || c == 'd':
		goto yystate527
	}

yystate527:
	c = l.next()
	switch {
	default:
		goto yyrule128
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'z':
		goto yystate66
	}

yystate528:
	c = l.next()
	switch {
	default:
		goto yyrule129
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'M' || c >= 'O' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'm' || c >= 'o' && c <= 'z':
		goto yystate66
	case c == 'N' || c == 'n':
		goto yystate529
	}

yystate529:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'T' || c >= 'V' && c <= 'Z' || c == '_' || c >= 'a' && c <= 't' || c >= 'v' && c <= 'z':
		goto yystate66
	case c == 'U' || c == 'u':
		goto yystate530
	}

yystate530:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'K' || c >= 'M' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'k' || c >= 'm' && c <= 'z':
		goto yystate66
	case c == 'L' || c == 'l':
		goto yystate531
	}

yystate531:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'K' || c >= 'M' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'k' || c >= 'm' && c <= 'z':
		goto yystate66
	case c == 'L' || c == 'l':
		goto yystate532
	}

yystate532:
	c = l.next()
	switch {
	default:
		goto yyrule130
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'z':
		goto yystate66
	}

yystate533:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'M' || c >= 'O' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'm' || c >= 'o' && c <= 'z':
		goto yystate66
	case c == 'N' || c == 'n':
		goto yystate534
	}

yystate534:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'N' || c >= 'P' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'n' || c >= 'p' && c <= 'z':
		goto yystate66
	case c == 'O' || c == 'o':
		goto yystate535
	}

yystate535:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'Q' || c >= 'S' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'q' || c >= 's' && c <= 'z':
		goto yystate66
	case c == 'R' || c == 'r':
		goto yystate536
	}

yystate536:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'D' || c >= 'F' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'd' || c >= 'f' && c <= 'z':
		goto yystate66
	case c == 'E' || c == 'e':
		goto yystate537
	}

yystate537:
	c = l.next()
	switch {
	default:
		goto yyrule131
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'z':
		goto yystate66
	}

yystate538:
	c = l.next()
	switch {
	default:
		goto yyrule136
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'C' || c >= 'E' && c <= 'M' || c >= 'O' && c <= 'R' || c >= 'U' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'c' || c >= 'e' && c <= 'm' || c >= 'o' && c <= 'r' || c >= 'u' && c <= 'z':
		goto yystate66
	case c == 'D' || c == 'd':
		goto yystate539
	case c == 'N' || c == 'n':
		goto yystate542
	case c == 'S' || c == 's':
		goto yystate545
	case c == 'T' || c == 't':
		goto yystate549
	}

yystate539:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'D' || c >= 'F' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'd' || c >= 'f' && c <= 'z':
		goto yystate66
	case c == 'E' || c == 'e':
		goto yystate540
	}

yystate540:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'W' || c == 'Y' || c == 'Z' || c == '_' || c >= 'a' && c <= 'w' || c == 'y' || c == 'z':
		goto yystate66
	case c == 'X' || c == 'x':
		goto yystate541
	}

yystate541:
	c = l.next()
	switch {
	default:
		goto yyrule132
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'z':
		goto yystate66
	}

yystate542:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'D' || c >= 'F' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'd' || c >= 'f' && c <= 'z':
		goto yystate66
	case c == 'E' || c == 'e':
		goto yystate543
	}

yystate543:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'Q' || c >= 'S' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'q' || c >= 's' && c <= 'z':
		goto yystate66
	case c == 'R' || c == 'r':
		goto yystate544
	}

yystate544:
	c = l.next()
	switch {
	default:
		goto yyrule133
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'z':
		goto yystate66
	}

yystate545:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'D' || c >= 'F' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'd' || c >= 'f' && c <= 'z':
		goto yystate66
	case c == 'E' || c == 'e':
		goto yystate546
	}

yystate546:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'Q' || c >= 'S' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'q' || c >= 's' && c <= 'z':
		goto yystate66
	case c == 'R' || c == 'r':
		goto yystate547
	}

yystate547:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'S' || c >= 'U' && c <= 'Z' || c == '_' || c >= 'a' && c <= 's' || c >= 'u' && c <= 'z':
		goto yystate66
	case c == 'T' || c == 't':
		goto yystate548
	}

yystate548:
	c = l.next()
	switch {
	default:
		goto yyrule134
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'z':
		goto yystate66
	}

yystate549:
	c = l.next()
	switch {
	default:
		goto yyrule272
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'D' || c >= 'F' && c <= 'N' || c >= 'P' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'd' || c >= 'f' && c <= 'n' || c >= 'p' && c <= 'z':
		goto yystate66
	case c == 'E' || c == 'e':
		goto yystate550
	case c == 'O' || c == 'o':
		goto yystate554
	}

yystate550:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'F' || c >= 'H' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'f' || c >= 'h' && c <= 'z':
		goto yystate66
	case c == 'G' || c == 'g':
		goto yystate551
	}

yystate551:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'D' || c >= 'F' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'd' || c >= 'f' && c <= 'z':
		goto yystate66
	case c == 'E' || c == 'e':
		goto yystate552
	}

yystate552:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'Q' || c >= 'S' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'q' || c >= 's' && c <= 'z':
		goto yystate66
	case c == 'R' || c == 'r':
		goto yystate553
	}

yystate553:
	c = l.next()
	switch {
	default:
		goto yyrule273
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'z':
		goto yystate66
	}

yystate554:
	c = l.next()
	switch {
	default:
		goto yyrule135
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'z':
		goto yystate66
	}

yystate555:
	c = l.next()
	switch {
	default:
		goto yyrule137
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'z':
		goto yystate66
	}

yystate556:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'N' || c >= 'P' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'n' || c >= 'p' && c <= 'z':
		goto yystate66
	case c == 'O' || c == 'o':
		goto yystate557
	}

yystate557:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'H' || c >= 'J' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'h' || c >= 'j' && c <= 'z':
		goto yystate66
	case c == 'I' || c == 'i':
		goto yystate558
	}

yystate558:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'M' || c >= 'O' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'm' || c >= 'o' && c <= 'z':
		goto yystate66
	case c == 'N' || c == 'n':
		goto yystate559
	}

yystate559:
	c = l.next()
	switch {
	default:
		goto yyrule138
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'z':
		goto yystate66
	}

yystate560:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'D' || c >= 'F' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'd' || c >= 'f' && c <= 'z':
		goto yystate66
	case c == 'E' || c == 'e':
		goto yystate561
	}

yystate561:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'X' || c == 'Z' || c == '_' || c >= 'a' && c <= 'x' || c == 'z':
		goto yystate66
	case c == 'Y' || c == 'y':
		goto yystate562
	}

yystate562:
	c = l.next()
	switch {
	default:
		goto yyrule139
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'Z' || c >= 'a' && c <= 'z':
		goto yystate66
	case c == '_':
		goto yystate563
	}

yystate563:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c == 'A' || c >= 'C' && c <= 'Z' || c == '_' || c == 'a' || c >= 'c' && c <= 'z':
		goto yystate66
	case c == 'B' || c == 'b':
		goto yystate564
	}

yystate564:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'K' || c >= 'M' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'k' || c >= 'm' && c <= 'z':
		goto yystate66
	case c == 'L' || c == 'l':
		goto yystate565
	}

yystate565:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'N' || c >= 'P' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'n' || c >= 'p' && c <= 'z':
		goto yystate66
	case c == 'O' || c == 'o':
		goto yystate566
	}

yystate566:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c == 'A' || c == 'B' || c >= 'D' && c <= 'Z' || c == '_' || c == 'a' || c == 'b' || c >= 'd' && c <= 'z':
		goto yystate66
	case c == 'C' || c == 'c':
		goto yystate567
	}

yystate567:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'J' || c >= 'L' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'j' || c >= 'l' && c <= 'z':
		goto yystate66
	case c == 'K' || c == 'k':
		goto yystate568
	}

yystate568:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'Z' || c >= 'a' && c <= 'z':
		goto yystate66
	case c == '_':
		goto yystate569
	}

yystate569:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'R' || c >= 'T' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'r' || c >= 't' && c <= 'z':
		goto yystate66
	case c == 'S' || c == 's':
		goto yystate570
	}

yystate570:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'H' || c >= 'J' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'h' || c >= 'j' && c <= 'z':
		goto yystate66
	case c == 'I' || c == 'i':
		goto yystate571
	}

yystate571:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'Y' || c == '_' || c >= 'a' && c <= 'y':
		goto yystate66
	case c == 'Z' || c == 'z':
		goto yystate572
	}

yystate572:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'D' || c >= 'F' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'd' || c >= 'f' && c <= 'z':
		goto yystate66
	case c == 'E' || c == 'e':
		goto yystate573
	}

yystate573:
	c = l.next()
	switch {
	default:
		goto yyrule140
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'z':
		goto yystate66
	}

yystate574:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'D' || c >= 'F' && c <= 'H' || c >= 'J' && c <= 'N' || c >= 'P' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'd' || c >= 'f' && c <= 'h' || c >= 'j' && c <= 'n' || c >= 'p' && c <= 'z':
		goto yystate66
	case c == 'E' || c == 'e':
		goto yystate575
	case c == 'I' || c == 'i':
		goto yystate587
	case c == 'O' || c == 'o':
		goto yystate593
	}

yystate575:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'B' && c <= 'E' || c >= 'G' && c <= 'M' || c >= 'O' && c <= 'Z' || c == '_' || c >= 'b' && c <= 'e' || c >= 'g' && c <= 'm' || c >= 'o' && c <= 'z':
		goto yystate66
	case c == 'A' || c == 'a':
		goto yystate576
	case c == 'F' || c == 'f':
		goto yystate581
	case c == 'N' || c == 'n':
		goto yystate583
	}

yystate576:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'C' || c >= 'E' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'c' || c >= 'e' && c <= 'z':
		goto yystate66
	case c == 'D' || c == 'd':
		goto yystate577
	}

yystate577:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'H' || c >= 'J' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'h' || c >= 'j' && c <= 'z':
		goto yystate66
	case c == 'I' || c == 'i':
		goto yystate578
	}

yystate578:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'M' || c >= 'O' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'm' || c >= 'o' && c <= 'z':
		goto yystate66
	case c == 'N' || c == 'n':
		goto yystate579
	}

yystate579:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'F' || c >= 'H' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'f' || c >= 'h' && c <= 'z':
		goto yystate66
	case c == 'G' || c == 'g':
		goto yystate580
	}

yystate580:
	c = l.next()
	switch {
	default:
		goto yyrule141
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'z':
		goto yystate66
	}

yystate581:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'S' || c >= 'U' && c <= 'Z' || c == '_' || c >= 'a' && c <= 's' || c >= 'u' && c <= 'z':
		goto yystate66
	case c == 'T' || c == 't':
		goto yystate582
	}

yystate582:
	c = l.next()
	switch {
	default:
		goto yyrule142
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'z':
		goto yystate66
	}

yystate583:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'F' || c >= 'H' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'f' || c >= 'h' && c <= 'z':
		goto yystate66
	case c == 'G' || c == 'g':
		goto yystate584
	}

yystate584:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'S' || c >= 'U' && c <= 'Z' || c == '_' || c >= 'a' && c <= 's' || c >= 'u' && c <= 'z':
		goto yystate66
	case c == 'T' || c == 't':
		goto yystate585
	}

yystate585:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'G' || c >= 'I' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'g' || c >= 'i' && c <= 'z':
		goto yystate66
	case c == 'H' || c == 'h':
		goto yystate586
	}

yystate586:
	c = l.next()
	switch {
	default:
		goto yyrule143
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'z':
		goto yystate66
	}

yystate587:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'J' || c == 'L' || c >= 'N' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'j' || c == 'l' || c >= 'n' && c <= 'z':
		goto yystate66
	case c == 'K' || c == 'k':
		goto yystate588
	case c == 'M' || c == 'm':
		goto yystate590
	}

yystate588:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'D' || c >= 'F' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'd' || c >= 'f' && c <= 'z':
		goto yystate66
	case c == 'E' || c == 'e':
		goto yystate589
	}

yystate589:
	c = l.next()
	switch {
	default:
		goto yyrule144
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'z':
		goto yystate66
	}

yystate590:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'H' || c >= 'J' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'h' || c >= 'j' && c <= 'z':
		goto yystate66
	case c == 'I' || c == 'i':
		goto yystate591
	}

yystate591:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'S' || c >= 'U' && c <= 'Z' || c == '_' || c >= 'a' && c <= 's' || c >= 'u' && c <= 'z':
		goto yystate66
	case c == 'T' || c == 't':
		goto yystate592
	}

yystate592:
	c = l.next()
	switch {
	default:
		goto yyrule145
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'z':
		goto yystate66
	}

yystate593:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c == 'A' || c == 'B' || c >= 'D' && c <= 'M' || c >= 'O' && c <= 'V' || c >= 'X' && c <= 'Z' || c == '_' || c == 'a' || c == 'b' || c >= 'd' && c <= 'm' || c >= 'o' && c <= 'v' || c >= 'x' && c <= 'z':
		goto yystate66
	case c == 'C' || c == 'c':
		goto yystate594
	case c == 'N' || c == 'n':
		goto yystate609
	case c == 'W' || c == 'w':
		goto yystate619
	}

yystate594:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'B' && c <= 'J' || c >= 'L' && c <= 'Z' || c == '_' || c >= 'b' && c <= 'j' || c >= 'l' && c <= 'z':
		goto yystate66
	case c == 'A' || c == 'a':
		goto yystate595
	case c == 'K' || c == 'k':
		goto yystate608
	}

yystate595:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'K' || c >= 'M' && c <= 'S' || c >= 'U' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'k' || c >= 'm' && c <= 's' || c >= 'u' && c <= 'z':
		goto yystate66
	case c == 'L' || c == 'l':
		goto yystate596
	case c == 'T' || c == 't':
		goto yystate606
	}

yystate596:
	c = l.next()
	switch {
	default:
		goto yyrule146
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'S' || c >= 'U' && c <= 'Z' || c == '_' || c >= 'a' && c <= 's' || c >= 'u' && c <= 'z':
		goto yystate66
	case c == 'T' || c == 't':
		goto yystate597
	}

yystate597:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'H' || c >= 'J' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'h' || c >= 'j' && c <= 'z':
		goto yystate66
	case c == 'I' || c == 'i':
		goto yystate598
	}

yystate598:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'L' || c >= 'N' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'l' || c >= 'n' && c <= 'z':
		goto yystate66
	case c == 'M' || c == 'm':
		goto yystate599
	}

yystate599:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'D' || c >= 'F' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'd' || c >= 'f' && c <= 'z':
		goto yystate66
	case c == 'E' || c == 'e':
		goto yystate600
	}

yystate600:
	c = l.next()
	switch {
	default:
		goto yyrule237
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'R' || c >= 'T' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'r' || c >= 't' && c <= 'z':
		goto yystate66
	case c == 'S' || c == 's':
		goto yystate601
	}

yystate601:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'S' || c >= 'U' && c <= 'Z' || c == '_' || c >= 'a' && c <= 's' || c >= 'u' && c <= 'z':
		goto yystate66
	case c == 'T' || c == 't':
		goto yystate602
	}

yystate602:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'B' && c <= 'Z' || c == '_' || c >= 'b' && c <= 'z':
		goto yystate66
	case c == 'A' || c == 'a':
		goto yystate603
	}

yystate603:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'L' || c >= 'N' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'l' || c >= 'n' && c <= 'z':
		goto yystate66
	case c == 'M' || c == 'm':
		goto yystate604
	}

yystate604:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'O' || c >= 'Q' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'o' || c >= 'q' && c <= 'z':
		goto yystate66
	case c == 'P' || c == 'p':
		goto yystate605
	}

yystate605:
	c = l.next()
	switch {
	default:
		goto yyrule238
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'z':
		goto yystate66
	}

yystate606:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'D' || c >= 'F' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'd' || c >= 'f' && c <= 'z':
		goto yystate66
	case c == 'E' || c == 'e':
		goto yystate607
	}

yystate607:
	c = l.next()
	switch {
	default:
		goto yyrule147
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'z':
		goto yystate66
	}

yystate608:
	c = l.next()
	switch {
	default:
		goto yyrule148
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'z':
		goto yystate66
	}

yystate609:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'F' || c >= 'H' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'f' || c >= 'h' && c <= 'z':
		goto yystate66
	case c == 'G' || c == 'g':
		goto yystate610
	}

yystate610:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c == 'A' || c >= 'C' && c <= 'S' || c >= 'U' && c <= 'Z' || c == '_' || c == 'a' || c >= 'c' && c <= 's' || c >= 'u' && c <= 'z':
		goto yystate66
	case c == 'B' || c == 'b':
		goto yystate611
	case c == 'T' || c == 't':
		goto yystate615
	}

yystate611:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'K' || c >= 'M' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'k' || c >= 'm' && c <= 'z':
		goto yystate66
	case c == 'L' || c == 'l':
		goto yystate612
	}

yystate612:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'N' || c >= 'P' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'n' || c >= 'p' && c <= 'z':
		goto yystate66
	case c == 'O' || c == 'o':
		goto yystate613
	}

yystate613:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c == 'A' || c >= 'C' && c <= 'Z' || c == '_' || c == 'a' || c >= 'c' && c <= 'z':
		goto yystate66
	case c == 'B' || c == 'b':
		goto yystate614
	}

yystate614:
	c = l.next()
	switch {
	default:
		goto yyrule264
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'z':
		goto yystate66
	}

yystate615:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'D' || c >= 'F' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'd' || c >= 'f' && c <= 'z':
		goto yystate66
	case c == 'E' || c == 'e':
		goto yystate616
	}

yystate616:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'W' || c == 'Y' || c == 'Z' || c == '_' || c >= 'a' && c <= 'w' || c == 'y' || c == 'z':
		goto yystate66
	case c == 'X' || c == 'x':
		goto yystate617
	}

yystate617:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'S' || c >= 'U' && c <= 'Z' || c == '_' || c >= 'a' && c <= 's' || c >= 'u' && c <= 'z':
		goto yystate66
	case c == 'T' || c == 't':
		goto yystate618
	}

yystate618:
	c = l.next()
	switch {
	default:
		goto yyrule268
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'z':
		goto yystate66
	}

yystate619:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'D' || c >= 'F' && c <= 'Z' || c >= 'a' && c <= 'd' || c >= 'f' && c <= 'z':
		goto yystate66
	case c == 'E' || c == 'e':
		goto yystate620
	case c == '_':
		goto yystate622
	}

yystate620:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'Q' || c >= 'S' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'q' || c >= 's' && c <= 'z':
		goto yystate66
	case c == 'R' || c == 'r':
		goto yystate621
	}

yystate621:
	c = l.next()
	switch {
	default:
		goto yyrule149
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'z':
		goto yystate66
	}

yystate622:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'O' || c >= 'Q' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'o' || c >= 'q' && c <= 'z':
		goto yystate66
	case c == 'P' || c == 'p':
		goto yystate623
	}

yystate623:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'Q' || c >= 'S' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'q' || c >= 's' && c <= 'z':
		goto yystate66
	case c == 'R' || c == 'r':
		goto yystate624
	}

yystate624:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'H' || c >= 'J' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'h' || c >= 'j' && c <= 'z':
		goto yystate66
	case c == 'I' || c == 'i':
		goto yystate625
	}

yystate625:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'N' || c >= 'P' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'n' || c >= 'p' && c <= 'z':
		goto yystate66
	case c == 'O' || c == 'o':
		goto yystate626
	}

yystate626:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'Q' || c >= 'S' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'q' || c >= 's' && c <= 'z':
		goto yystate66
	case c == 'R' || c == 'r':
		goto yystate627
	}

yystate627:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'H' || c >= 'J' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'h' || c >= 'j' && c <= 'z':
		goto yystate66
	case c == 'I' || c == 'i':
		goto yystate628
	}

yystate628:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'S' || c >= 'U' && c <= 'Z' || c == '_' || c >= 'a' && c <= 's' || c >= 'u' && c <= 'z':
		goto yystate66
	case c == 'T' || c == 't':
		goto yystate629
	}

yystate629:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'X' || c == 'Z' || c == '_' || c >= 'a' && c <= 'x' || c == 'z':
		goto yystate66
	case c == 'Y' || c == 'y':
		goto yystate630
	}

yystate630:
	c = l.next()
	switch {
	default:
		goto yyrule150
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'z':
		goto yystate66
	}

yystate631:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'B' && c <= 'D' || c >= 'F' && c <= 'H' || c >= 'J' && c <= 'N' || c >= 'P' && c <= 'Z' || c == '_' || c >= 'b' && c <= 'd' || c >= 'f' && c <= 'h' || c >= 'j' && c <= 'n' || c >= 'p' && c <= 'z':
		goto yystate66
	case c == 'A' || c == 'a':
		goto yystate632
	case c == 'E' || c == 'e':
		goto yystate639
	case c == 'I' || c == 'i':
		goto yystate655
	case c == 'O' || c == 'o':
		goto yystate692
	}

yystate632:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'W' || c == 'Y' || c == 'Z' || c == '_' || c >= 'a' && c <= 'w' || c == 'y' || c == 'z':
		goto yystate66
	case c == 'X' || c == 'x':
		goto yystate633
	}

yystate633:
	c = l.next()
	switch {
	default:
		goto yyrule151
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'Z' || c >= 'a' && c <= 'z':
		goto yystate66
	case c == '_':
		goto yystate634
	}

yystate634:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'Q' || c >= 'S' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'q' || c >= 's' && c <= 'z':
		goto yystate66
	case c == 'R' || c == 'r':
		goto yystate635
	}

yystate635:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'N' || c >= 'P' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'n' || c >= 'p' && c <= 'z':
		goto yystate66
	case c == 'O' || c == 'o':
		goto yystate636
	}

yystate636:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'V' || c >= 'X' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'v' || c >= 'x' && c <= 'z':
		goto yystate66
	case c == 'W' || c == 'w':
		goto yystate637
	}

yystate637:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'R' || c >= 'T' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'r' || c >= 't' && c <= 'z':
		goto yystate66
	case c == 'S' || c == 's':
		goto yystate638
	}

yystate638:
	c = l.next()
	switch {
	default:
		goto yyrule152
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'z':
		goto yystate66
	}

yystate639:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'C' || c >= 'E' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'c' || c >= 'e' && c <= 'z':
		goto yystate66
	case c == 'D' || c == 'd':
		goto yystate640
	}

yystate640:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'H' || c >= 'J' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'h' || c >= 'j' && c <= 'z':
		goto yystate66
	case c == 'I' || c == 'i':
		goto yystate641
	}

yystate641:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'T' || c >= 'V' && c <= 'Z' || c == '_' || c >= 'a' && c <= 't' || c >= 'v' && c <= 'z':
		goto yystate66
	case c == 'U' || c == 'u':
		goto yystate642
	}

yystate642:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'L' || c >= 'N' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'l' || c >= 'n' && c <= 'z':
		goto yystate66
	case c == 'M' || c == 'm':
		goto yystate643
	}

yystate643:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c == 'A' || c >= 'C' && c <= 'H' || c >= 'J' && c <= 'S' || c >= 'U' && c <= 'Z' || c == '_' || c == 'a' || c >= 'c' && c <= 'h' || c >= 'j' && c <= 's' || c >= 'u' && c <= 'z':
		goto yystate66
	case c == 'B' || c == 'b':
		goto yystate644
	case c == 'I' || c == 'i':
		goto yystate648
	case c == 'T' || c == 't':
		goto yystate651
	}

yystate644:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'K' || c >= 'M' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'k' || c >= 'm' && c <= 'z':
		goto yystate66
	case c == 'L' || c == 'l':
		goto yystate645
	}

yystate645:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'N' || c >= 'P' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'n' || c >= 'p' && c <= 'z':
		goto yystate66
	case c == 'O' || c == 'o':
		goto yystate646
	}

yystate646:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c == 'A' || c >= 'C' && c <= 'Z' || c == '_' || c == 'a' || c >= 'c' && c <= 'z':
		goto yystate66
	case c == 'B' || c == 'b':
		goto yystate647
	}

yystate647:
	c = l.next()
	switch {
	default:
		goto yyrule263
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'z':
		goto yystate66
	}

yystate648:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'M' || c >= 'O' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'm' || c >= 'o' && c <= 'z':
		goto yystate66
	case c == 'N' || c == 'n':
		goto yystate649
	}

yystate649:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'S' || c >= 'U' && c <= 'Z' || c == '_' || c >= 'a' && c <= 's' || c >= 'u' && c <= 'z':
		goto yystate66
	case c == 'T' || c == 't':
		goto yystate650
	}

yystate650:
	c = l.next()
	switch {
	default:
		goto yyrule244
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'z':
		goto yystate66
	}

yystate651:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'D' || c >= 'F' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'd' || c >= 'f' && c <= 'z':
		goto yystate66
	case c == 'E' || c == 'e':
		goto yystate652
	}

yystate652:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'W' || c == 'Y' || c == 'Z' || c == '_' || c >= 'a' && c <= 'w' || c == 'y' || c == 'z':
		goto yystate66
	case c == 'X' || c == 'x':
		goto yystate653
	}

yystate653:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'S' || c >= 'U' && c <= 'Z' || c == '_' || c >= 'a' && c <= 's' || c >= 'u' && c <= 'z':
		goto yystate66
	case c == 'T' || c == 't':
		goto yystate654
	}

yystate654:
	c = l.next()
	switch {
	default:
		goto yyrule266
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'z':
		goto yystate66
	}

yystate655:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c == 'A' || c == 'B' || c >= 'D' && c <= 'M' || c >= 'O' && c <= 'Z' || c == '_' || c == 'a' || c == 'b' || c >= 'd' && c <= 'm' || c >= 'o' && c <= 'z':
		goto yystate66
	case c == 'C' || c == 'c':
		goto yystate656
	case c == 'N' || c == 'n':
		goto yystate665
	}

yystate656:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'Q' || c >= 'S' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'q' || c >= 's' && c <= 'z':
		goto yystate66
	case c == 'R' || c == 'r':
		goto yystate657
	}

yystate657:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'N' || c >= 'P' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'n' || c >= 'p' && c <= 'z':
		goto yystate66
	case c == 'O' || c == 'o':
		goto yystate658
	}

yystate658:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'R' || c >= 'T' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'r' || c >= 't' && c <= 'z':
		goto yystate66
	case c == 'S' || c == 's':
		goto yystate659
	}

yystate659:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'D' || c >= 'F' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'd' || c >= 'f' && c <= 'z':
		goto yystate66
	case c == 'E' || c == 'e':
		goto yystate660
	}

yystate660:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c == 'A' || c == 'B' || c >= 'D' && c <= 'Z' || c == '_' || c == 'a' || c == 'b' || c >= 'd' && c <= 'z':
		goto yystate66
	case c == 'C' || c == 'c':
		goto yystate661
	}

yystate661:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'N' || c >= 'P' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'n' || c >= 'p' && c <= 'z':
		goto yystate66
	case c == 'O' || c == 'o':
		goto yystate662
	}

yystate662:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'M' || c >= 'O' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'm' || c >= 'o' && c <= 'z':
		goto yystate66
	case c == 'N' || c == 'n':
		goto yystate663
	}

yystate663:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'C' || c >= 'E' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'c' || c >= 'e' && c <= 'z':
		goto yystate66
	case c == 'D' || c == 'd':
		goto yystate664
	}

yystate664:
	c = l.next()
	switch {
	default:
		goto yyrule153
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'z':
		goto yystate66
	}

yystate665:
	c = l.next()
	switch {
	default:
		goto yyrule154
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'T' || c >= 'V' && c <= 'Z' || c >= 'a' && c <= 't' || c >= 'v' && c <= 'z':
		goto yystate66
	case c == 'U' || c == 'u':
		goto yystate666
	case c == '_':
		goto yystate687
	}

yystate666:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'S' || c >= 'U' && c <= 'Z' || c == '_' || c >= 'a' && c <= 's' || c >= 'u' && c <= 'z':
		goto yystate66
	case c == 'T' || c == 't':
		goto yystate667
	}

yystate667:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'D' || c >= 'F' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'd' || c >= 'f' && c <= 'z':
		goto yystate66
	case c == 'E' || c == 'e':
		goto yystate668
	}

yystate668:
	c = l.next()
	switch {
	default:
		goto yyrule155
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'Z' || c >= 'a' && c <= 'z':
		goto yystate66
	case c == '_':
		goto yystate669
	}

yystate669:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'L' || c >= 'N' && c <= 'R' || c >= 'T' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'l' || c >= 'n' && c <= 'r' || c >= 't' && c <= 'z':
		goto yystate66
	case c == 'M' || c == 'm':
		goto yystate670
	case c == 'S' || c == 's':
		goto yystate681
	}

yystate670:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'H' || c >= 'J' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'h' || c >= 'j' && c <= 'z':
		goto yystate66
	case c == 'I' || c == 'i':
		goto yystate671
	}

yystate671:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c == 'A' || c == 'B' || c >= 'D' && c <= 'Z' || c == '_' || c == 'a' || c == 'b' || c >= 'd' && c <= 'z':
		goto yystate66
	case c == 'C' || c == 'c':
		goto yystate672
	}

yystate672:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'Q' || c >= 'S' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'q' || c >= 's' && c <= 'z':
		goto yystate66
	case c == 'R' || c == 'r':
		goto yystate673
	}

yystate673:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'N' || c >= 'P' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'n' || c >= 'p' && c <= 'z':
		goto yystate66
	case c == 'O' || c == 'o':
		goto yystate674
	}

yystate674:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'R' || c >= 'T' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'r' || c >= 't' && c <= 'z':
		goto yystate66
	case c == 'S' || c == 's':
		goto yystate675
	}

yystate675:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'D' || c >= 'F' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'd' || c >= 'f' && c <= 'z':
		goto yystate66
	case c == 'E' || c == 'e':
		goto yystate676
	}

yystate676:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c == 'A' || c == 'B' || c >= 'D' && c <= 'Z' || c == '_' || c == 'a' || c == 'b' || c >= 'd' && c <= 'z':
		goto yystate66
	case c == 'C' || c == 'c':
		goto yystate677
	}

yystate677:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'N' || c >= 'P' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'n' || c >= 'p' && c <= 'z':
		goto yystate66
	case c == 'O' || c == 'o':
		goto yystate678
	}

yystate678:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'M' || c >= 'O' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'm' || c >= 'o' && c <= 'z':
		goto yystate66
	case c == 'N' || c == 'n':
		goto yystate679
	}

yystate679:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'C' || c >= 'E' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'c' || c >= 'e' && c <= 'z':
		goto yystate66
	case c == 'D' || c == 'd':
		goto yystate680
	}

yystate680:
	c = l.next()
	switch {
	default:
		goto yyrule156
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'z':
		goto yystate66
	}

yystate681:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'D' || c >= 'F' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'd' || c >= 'f' && c <= 'z':
		goto yystate66
	case c == 'E' || c == 'e':
		goto yystate682
	}

yystate682:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c == 'A' || c == 'B' || c >= 'D' && c <= 'Z' || c == '_' || c == 'a' || c == 'b' || c >= 'd' && c <= 'z':
		goto yystate66
	case c == 'C' || c == 'c':
		goto yystate683
	}

yystate683:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'N' || c >= 'P' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'n' || c >= 'p' && c <= 'z':
		goto yystate66
	case c == 'O' || c == 'o':
		goto yystate684
	}

yystate684:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'M' || c >= 'O' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'm' || c >= 'o' && c <= 'z':
		goto yystate66
	case c == 'N' || c == 'n':
		goto yystate685
	}

yystate685:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'C' || c >= 'E' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'c' || c >= 'e' && c <= 'z':
		goto yystate66
	case c == 'D' || c == 'd':
		goto yystate686
	}

yystate686:
	c = l.next()
	switch {
	default:
		goto yyrule157
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'z':
		goto yystate66
	}

yystate687:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'Q' || c >= 'S' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'q' || c >= 's' && c <= 'z':
		goto yystate66
	case c == 'R' || c == 'r':
		goto yystate688
	}

yystate688:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'N' || c >= 'P' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'n' || c >= 'p' && c <= 'z':
		goto yystate66
	case c == 'O' || c == 'o':
		goto yystate689
	}

yystate689:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'V' || c >= 'X' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'v' || c >= 'x' && c <= 'z':
		goto yystate66
	case c == 'W' || c == 'w':
		goto yystate690
	}

yystate690:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'R' || c >= 'T' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'r' || c >= 't' && c <= 'z':
		goto yystate66
	case c == 'S' || c == 's':
		goto yystate691
	}

yystate691:
	c = l.next()
	switch {
	default:
		goto yyrule158
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'z':
		goto yystate66
	}

yystate692:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'C' || c >= 'E' && c <= 'M' || c >= 'O' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'c' || c >= 'e' && c <= 'm' || c >= 'o' && c <= 'z':
		goto yystate66
	case c == 'D' || c == 'd':
		goto yystate693
	case c == 'N' || c == 'n':
		goto yystate695
	}

yystate693:
	c = l.next()
	switch {
	default:
		goto yyrule159
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'D' || c >= 'F' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'd' || c >= 'f' && c <= 'z':
		goto yystate66
	case c == 'E' || c == 'e':
		goto yystate694
	}

yystate694:
	c = l.next()
	switch {
	default:
		goto yyrule160
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'z':
		goto yystate66
	}

yystate695:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'S' || c >= 'U' && c <= 'Z' || c == '_' || c >= 'a' && c <= 's' || c >= 'u' && c <= 'z':
		goto yystate66
	case c == 'T' || c == 't':
		goto yystate696
	}

yystate696:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'G' || c >= 'I' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'g' || c >= 'i' && c <= 'z':
		goto yystate66
	case c == 'H' || c == 'h':
		goto yystate697
	}

yystate697:
	c = l.next()
	switch {
	default:
		goto yyrule161
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'z':
		goto yystate66
	}

yystate698:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'B' && c <= 'N' || c >= 'P' && c <= 'T' || c >= 'V' && c <= 'Z' || c == '_' || c >= 'b' && c <= 'n' || c >= 'p' && c <= 't' || c >= 'v' && c <= 'z':
		goto yystate66
	case c == 'A' || c == 'a':
		goto yystate699
	case c == 'O' || c == 'o':
		goto yystate709
	case c == 'U' || c == 'u':
		goto yystate712
	}

yystate699:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'L' || c >= 'N' && c <= 'S' || c >= 'U' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'l' || c >= 'n' && c <= 's' || c >= 'u' && c <= 'z':
		goto yystate66
	case c == 'M' || c == 'm':
		goto yystate700
	case c == 'T' || c == 't':
		goto yystate703
	}

yystate700:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'D' || c >= 'F' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'd' || c >= 'f' && c <= 'z':
		goto yystate66
	case c == 'E' || c == 'e':
		goto yystate701
	}

yystate701:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'R' || c >= 'T' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'r' || c >= 't' && c <= 'z':
		goto yystate66
	case c == 'S' || c == 's':
		goto yystate702
	}

yystate702:
	c = l.next()
	switch {
	default:
		goto yyrule162
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'z':
		goto yystate66
	}

yystate703:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'H' || c >= 'J' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'h' || c >= 'j' && c <= 'z':
		goto yystate66
	case c == 'I' || c == 'i':
		goto yystate704
	}

yystate704:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'N' || c >= 'P' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'n' || c >= 'p' && c <= 'z':
		goto yystate66
	case c == 'O' || c == 'o':
		goto yystate705
	}

yystate705:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'M' || c >= 'O' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'm' || c >= 'o' && c <= 'z':
		goto yystate66
	case c == 'N' || c == 'n':
		goto yystate706
	}

yystate706:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'B' && c <= 'Z' || c == '_' || c >= 'b' && c <= 'z':
		goto yystate66
	case c == 'A' || c == 'a':
		goto yystate707
	}

yystate707:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'K' || c >= 'M' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'k' || c >= 'm' && c <= 'z':
		goto yystate66
	case c == 'L' || c == 'l':
		goto yystate708
	}

yystate708:
	c = l.next()
	switch {
	default:
		goto yyrule163
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'z':
		goto yystate66
	}

yystate709:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'S' || c == 'U' || c == 'V' || c >= 'X' && c <= 'Z' || c == '_' || c >= 'a' && c <= 's' || c == 'u' || c == 'v' || c >= 'x' && c <= 'z':
		goto yystate66
	case c == 'T' || c == 't':
		goto yystate710
	case c == 'W' || c == 'w':
		goto yystate711
	}

yystate710:
	c = l.next()
	switch {
	default:
		goto yyrule164
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'z':
		goto yystate66
	}

yystate711:
	c = l.next()
	switch {
	default:
		goto yyrule239
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'z':
		goto yystate66
	}

yystate712:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'K' || c >= 'N' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'k' || c >= 'n' && c <= 'z':
		goto yystate66
	case c == 'L' || c == 'l':
		goto yystate713
	case c == 'M' || c == 'm':
		goto yystate717
	}

yystate713:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'K' || c >= 'M' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'k' || c >= 'm' && c <= 'z':
		goto yystate66
	case c == 'L' || c == 'l':
		goto yystate714
	}

yystate714:
	c = l.next()
	switch {
	default:
		goto yyrule232
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'H' || c >= 'J' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'h' || c >= 'j' && c <= 'z':
		goto yystate66
	case c == 'I' || c == 'i':
		goto yystate715
	}

yystate715:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'E' || c >= 'G' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'e' || c >= 'g' && c <= 'z':
		goto yystate66
	case c == 'F' || c == 'f':
		goto yystate716
	}

yystate716:
	c = l.next()
	switch {
	default:
		goto yyrule211
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'z':
		goto yystate66
	}

yystate717:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'D' || c >= 'F' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'd' || c >= 'f' && c <= 'z':
		goto yystate66
	case c == 'E' || c == 'e':
		goto yystate718
	}

yystate718:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'Q' || c >= 'S' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'q' || c >= 's' && c <= 'z':
		goto yystate66
	case c == 'R' || c == 'r':
		goto yystate719
	}

yystate719:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'H' || c >= 'J' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'h' || c >= 'j' && c <= 'z':
		goto yystate66
	case c == 'I' || c == 'i':
		goto yystate720
	}

yystate720:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c == 'A' || c == 'B' || c >= 'D' && c <= 'Z' || c == '_' || c == 'a' || c == 'b' || c >= 'd' && c <= 'z':
		goto yystate66
	case c == 'C' || c == 'c':
		goto yystate721
	}

yystate721:
	c = l.next()
	switch {
	default:
		goto yyrule247
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'z':
		goto yystate66
	}

yystate722:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'E' || c >= 'G' && c <= 'M' || c >= 'O' && c <= 'Q' || c == 'S' || c == 'T' || c >= 'V' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'e' || c >= 'g' && c <= 'm' || c >= 'o' && c <= 'q' || c == 's' || c == 't' || c >= 'v' && c <= 'z':
		goto yystate66
	case c == 'F' || c == 'f':
		goto yystate723
	case c == 'N' || c == 'n':
		goto yystate728
	case c == 'R' || c == 'r':
		goto yystate729
	case c == 'U' || c == 'u':
		goto yystate733
	}

yystate723:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'E' || c >= 'G' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'e' || c >= 'g' && c <= 'z':
		goto yystate66
	case c == 'F' || c == 'f':
		goto yystate724
	}

yystate724:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'R' || c >= 'T' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'r' || c >= 't' && c <= 'z':
		goto yystate66
	case c == 'S' || c == 's':
		goto yystate725
	}

yystate725:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'D' || c >= 'F' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'd' || c >= 'f' && c <= 'z':
		goto yystate66
	case c == 'E' || c == 'e':
		goto yystate726
	}

yystate726:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'S' || c >= 'U' && c <= 'Z' || c == '_' || c >= 'a' && c <= 's' || c >= 'u' && c <= 'z':
		goto yystate66
	case c == 'T' || c == 't':
		goto yystate727
	}

yystate727:
	c = l.next()
	switch {
	default:
		goto yyrule165
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'z':
		goto yystate66
	}

yystate728:
	c = l.next()
	switch {
	default:
		goto yyrule166
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'z':
		goto yystate66
	}

yystate729:
	c = l.next()
	switch {
	default:
		goto yyrule168
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'C' || c >= 'E' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'c' || c >= 'e' && c <= 'z':
		goto yystate66
	case c == 'D' || c == 'd':
		goto yystate730
	}

yystate730:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'D' || c >= 'F' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'd' || c >= 'f' && c <= 'z':
		goto yystate66
	case c == 'E' || c == 'e':
		goto yystate731
	}

yystate731:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'Q' || c >= 'S' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'q' || c >= 's' && c <= 'z':
		goto yystate66
	case c == 'R' || c == 'r':
		goto yystate732
	}

yystate732:
	c = l.next()
	switch {
	default:
		goto yyrule167
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'z':
		goto yystate66
	}

yystate733:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'S' || c >= 'U' && c <= 'Z' || c == '_' || c >= 'a' && c <= 's' || c >= 'u' && c <= 'z':
		goto yystate66
	case c == 'T' || c == 't':
		goto yystate734
	}

yystate734:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'D' || c >= 'F' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'd' || c >= 'f' && c <= 'z':
		goto yystate66
	case c == 'E' || c == 'e':
		goto yystate735
	}

yystate735:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'Q' || c >= 'S' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'q' || c >= 's' && c <= 'z':
		goto yystate66
	case c == 'R' || c == 'r':
		goto yystate736
	}

yystate736:
	c = l.next()
	switch {
	default:
		goto yyrule169
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'z':
		goto yystate66
	}

yystate737:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'B' && c <= 'Q' || c >= 'S' && c <= 'Z' || c == '_' || c >= 'b' && c <= 'q' || c >= 's' && c <= 'z':
		goto yystate66
	case c == 'A' || c == 'a':
		goto yystate738
	case c == 'R' || c == 'r':
		goto yystate745
	}

yystate738:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'R' || c >= 'T' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'r' || c >= 't' && c <= 'z':
		goto yystate66
	case c == 'S' || c == 's':
		goto yystate739
	}

yystate739:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'R' || c >= 'T' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'r' || c >= 't' && c <= 'z':
		goto yystate66
	case c == 'S' || c == 's':
		goto yystate740
	}

yystate740:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'V' || c >= 'X' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'v' || c >= 'x' && c <= 'z':
		goto yystate66
	case c == 'W' || c == 'w':
		goto yystate741
	}

yystate741:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'N' || c >= 'P' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'n' || c >= 'p' && c <= 'z':
		goto yystate66
	case c == 'O' || c == 'o':
		goto yystate742
	}

yystate742:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'Q' || c >= 'S' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'q' || c >= 's' && c <= 'z':
		goto yystate66
	case c == 'R' || c == 'r':
		goto yystate743
	}

yystate743:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'C' || c >= 'E' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'c' || c >= 'e' && c <= 'z':
		goto yystate66
	case c == 'D' || c == 'd':
		goto yystate744
	}

yystate744:
	c = l.next()
	switch {
	default:
		goto yyrule170
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'z':
		goto yystate66
	}

yystate745:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'D' || c >= 'F' && c <= 'H' || c >= 'J' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'd' || c >= 'f' && c <= 'h' || c >= 'j' && c <= 'z':
		goto yystate66
	case c == 'E' || c == 'e':
		goto yystate746
	case c == 'I' || c == 'i':
		goto yystate757
	}

yystate746:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c == 'A' || c == 'B' || c >= 'D' && c <= 'O' || c >= 'Q' && c <= 'Z' || c == '_' || c == 'a' || c == 'b' || c >= 'd' && c <= 'o' || c >= 'q' && c <= 'z':
		goto yystate66
	case c == 'C' || c == 'c':
		goto yystate747
	case c == 'P' || c == 'p':
		goto yystate753
	}

yystate747:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'H' || c >= 'J' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'h' || c >= 'j' && c <= 'z':
		goto yystate66
	case c == 'I' || c == 'i':
		goto yystate748
	}

yystate748:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'R' || c >= 'T' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'r' || c >= 't' && c <= 'z':
		goto yystate66
	case c == 'S' || c == 's':
		goto yystate749
	}

yystate749:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'H' || c >= 'J' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'h' || c >= 'j' && c <= 'z':
		goto yystate66
	case c == 'I' || c == 'i':
		goto yystate750
	}

yystate750:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'N' || c >= 'P' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'n' || c >= 'p' && c <= 'z':
		goto yystate66
	case c == 'O' || c == 'o':
		goto yystate751
	}

yystate751:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'M' || c >= 'O' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'm' || c >= 'o' && c <= 'z':
		goto yystate66
	case c == 'N' || c == 'n':
		goto yystate752
	}

yystate752:
	c = l.next()
	switch {
	default:
		goto yyrule250
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'z':
		goto yystate66
	}

yystate753:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'B' && c <= 'Z' || c == '_' || c >= 'b' && c <= 'z':
		goto yystate66
	case c == 'A' || c == 'a':
		goto yystate754
	}

yystate754:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'Q' || c >= 'S' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'q' || c >= 's' && c <= 'z':
		goto yystate66
	case c == 'R' || c == 'r':
		goto yystate755
	}

yystate755:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'D' || c >= 'F' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'd' || c >= 'f' && c <= 'z':
		goto yystate66
	case c == 'E' || c == 'e':
		goto yystate756
	}

yystate756:
	c = l.next()
	switch {
	default:
		goto yyrule171
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'z':
		goto yystate66
	}

yystate757:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'L' || c >= 'N' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'l' || c >= 'n' && c <= 'z':
		goto yystate66
	case c == 'M' || c == 'm':
		goto yystate758
	}

yystate758:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'B' && c <= 'Z' || c == '_' || c >= 'b' && c <= 'z':
		goto yystate66
	case c == 'A' || c == 'a':
		goto yystate759
	}

yystate759:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'Q' || c >= 'S' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'q' || c >= 's' && c <= 'z':
		goto yystate66
	case c == 'R' || c == 'r':
		goto yystate760
	}

yystate760:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'X' || c == 'Z' || c == '_' || c >= 'a' && c <= 'x' || c == 'z':
		goto yystate66
	case c == 'Y' || c == 'y':
		goto yystate761
	}

yystate761:
	c = l.next()
	switch {
	default:
		goto yyrule172
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'z':
		goto yystate66
	}

yystate762:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'T' || c >= 'V' && c <= 'Z' || c == '_' || c >= 'a' && c <= 't' || c >= 'v' && c <= 'z':
		goto yystate66
	case c == 'U' || c == 'u':
		goto yystate763
	}

yystate763:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'B' && c <= 'H' || c >= 'J' && c <= 'Z' || c == '_' || c >= 'b' && c <= 'h' || c >= 'j' && c <= 'z':
		goto yystate66
	case c == 'A' || c == 'a':
		goto yystate764
	case c == 'I' || c == 'i':
		goto yystate769
	}

yystate764:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'Q' || c >= 'S' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'q' || c >= 's' && c <= 'z':
		goto yystate66
	case c == 'R' || c == 'r':
		goto yystate765
	}

yystate765:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'S' || c >= 'U' && c <= 'Z' || c == '_' || c >= 'a' && c <= 's' || c >= 'u' && c <= 'z':
		goto yystate66
	case c == 'T' || c == 't':
		goto yystate766
	}

yystate766:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'D' || c >= 'F' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'd' || c >= 'f' && c <= 'z':
		goto yystate66
	case c == 'E' || c == 'e':
		goto yystate767
	}

yystate767:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'Q' || c >= 'S' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'q' || c >= 's' && c <= 'z':
		goto yystate66
	case c == 'R' || c == 'r':
		goto yystate768
	}

yystate768:
	c = l.next()
	switch {
	default:
		goto yyrule173
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'z':
		goto yystate66
	}

yystate769:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c == 'A' || c == 'B' || c >= 'D' && c <= 'Z' || c == '_' || c == 'a' || c == 'b' || c >= 'd' && c <= 'z':
		goto yystate66
	case c == 'C' || c == 'c':
		goto yystate770
	}

yystate770:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'J' || c >= 'L' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'j' || c >= 'l' && c <= 'z':
		goto yystate66
	case c == 'K' || c == 'k':
		goto yystate771
	}

yystate771:
	c = l.next()
	switch {
	default:
		goto yyrule174
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'z':
		goto yystate66
	}

yystate772:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'B' && c <= 'D' || c >= 'F' && c <= 'H' || c == 'J' || c == 'K' || c == 'M' || c == 'N' || c >= 'P' && c <= 'Z' || c == '_' || c >= 'b' && c <= 'd' || c >= 'f' && c <= 'h' || c == 'j' || c == 'k' || c == 'm' || c == 'n' || c >= 'p' && c <= 'z':
		goto yystate66
	case c == 'A' || c == 'a':
		goto yystate773
	case c == 'E' || c == 'e':
		goto yystate776
	case c == 'I' || c == 'i':
		goto yystate795
	case c == 'L' || c == 'l':
		goto yystate799
	case c == 'O' || c == 'o':
		goto yystate803
	}

yystate773:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'M' || c >= 'O' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'm' || c >= 'o' && c <= 'z':
		goto yystate66
	case c == 'N' || c == 'n':
		goto yystate774
	}

yystate774:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'C' || c >= 'E' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'c' || c >= 'e' && c <= 'z':
		goto yystate66
	case c == 'D' || c == 'd':
		goto yystate775
	}

yystate775:
	c = l.next()
	switch {
	default:
		goto yyrule184
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'z':
		goto yystate66
	}

yystate776:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'B' && c <= 'E' || c >= 'H' && c <= 'O' || c >= 'Q' && c <= 'Z' || c == '_' || c >= 'b' && c <= 'e' || c >= 'h' && c <= 'o' || c >= 'q' && c <= 'z':
		goto yystate66
	case c == 'A' || c == 'a':
		goto yystate777
	case c == 'F' || c == 'f':
		goto yystate779
	case c == 'G' || c == 'g':
		goto yystate787
	case c == 'P' || c == 'p':
		goto yystate791
	}

yystate777:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'K' || c >= 'M' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'k' || c >= 'm' && c <= 'z':
		goto yystate66
	case c == 'L' || c == 'l':
		goto yystate778
	}

yystate778:
	c = l.next()
	switch {
	default:
		goto yyrule251
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'z':
		goto yystate66
	}

yystate779:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'D' || c >= 'F' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'd' || c >= 'f' && c <= 'z':
		goto yystate66
	case c == 'E' || c == 'e':
		goto yystate780
	}

yystate780:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'Q' || c >= 'S' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'q' || c >= 's' && c <= 'z':
		goto yystate66
	case c == 'R' || c == 'r':
		goto yystate781
	}

yystate781:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'D' || c >= 'F' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'd' || c >= 'f' && c <= 'z':
		goto yystate66
	case c == 'E' || c == 'e':
		goto yystate782
	}

yystate782:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'M' || c >= 'O' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'm' || c >= 'o' && c <= 'z':
		goto yystate66
	case c == 'N' || c == 'n':
		goto yystate783
	}

yystate783:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c == 'A' || c == 'B' || c >= 'D' && c <= 'Z' || c == '_' || c == 'a' || c == 'b' || c >= 'd' && c <= 'z':
		goto yystate66
	case c == 'C' || c == 'c':
		goto yystate784
	}

yystate784:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'D' || c >= 'F' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'd' || c >= 'f' && c <= 'z':
		goto yystate66
	case c == 'E' || c == 'e':
		goto yystate785
	}

yystate785:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'R' || c >= 'T' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'r' || c >= 't' && c <= 'z':
		goto yystate66
	case c == 'S' || c == 's':
		goto yystate786
	}

yystate786:
	c = l.next()
	switch {
	default:
		goto yyrule187
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'z':
		goto yystate66
	}

yystate787:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'D' || c >= 'F' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'd' || c >= 'f' && c <= 'z':
		goto yystate66
	case c == 'E' || c == 'e':
		goto yystate788
	}

yystate788:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'W' || c == 'Y' || c == 'Z' || c == '_' || c >= 'a' && c <= 'w' || c == 'y' || c == 'z':
		goto yystate66
	case c == 'X' || c == 'x':
		goto yystate789
	}

yystate789:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'O' || c >= 'Q' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'o' || c >= 'q' && c <= 'z':
		goto yystate66
	case c == 'P' || c == 'p':
		goto yystate790
	}

yystate790:
	c = l.next()
	switch {
	default:
		goto yyrule186
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'z':
		goto yystate66
	}

yystate791:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'D' || c >= 'F' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'd' || c >= 'f' && c <= 'z':
		goto yystate66
	case c == 'E' || c == 'e':
		goto yystate792
	}

yystate792:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'B' && c <= 'Z' || c == '_' || c >= 'b' && c <= 'z':
		goto yystate66
	case c == 'A' || c == 'a':
		goto yystate793
	}

yystate793:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'S' || c >= 'U' && c <= 'Z' || c == '_' || c >= 'a' && c <= 's' || c >= 'u' && c <= 'z':
		goto yystate66
	case c == 'T' || c == 't':
		goto yystate794
	}

yystate794:
	c = l.next()
	switch {
	default:
		goto yyrule185
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'z':
		goto yystate66
	}

yystate795:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'F' || c >= 'H' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'f' || c >= 'h' && c <= 'z':
		goto yystate66
	case c == 'G' || c == 'g':
		goto yystate796
	}

yystate796:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'G' || c >= 'I' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'g' || c >= 'i' && c <= 'z':
		goto yystate66
	case c == 'H' || c == 'h':
		goto yystate797
	}

yystate797:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'S' || c >= 'U' && c <= 'Z' || c == '_' || c >= 'a' && c <= 's' || c >= 'u' && c <= 'z':
		goto yystate66
	case c == 'T' || c == 't':
		goto yystate798
	}

yystate798:
	c = l.next()
	switch {
	default:
		goto yyrule175
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'z':
		goto yystate66
	}

yystate799:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'H' || c >= 'J' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'h' || c >= 'j' && c <= 'z':
		goto yystate66
	case c == 'I' || c == 'i':
		goto yystate800
	}

yystate800:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'J' || c >= 'L' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'j' || c >= 'l' && c <= 'z':
		goto yystate66
	case c == 'K' || c == 'k':
		goto yystate801
	}

yystate801:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'D' || c >= 'F' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'd' || c >= 'f' && c <= 'z':
		goto yystate66
	case c == 'E' || c == 'e':
		goto yystate802
	}

yystate802:
	c = l.next()
	switch {
	default:
		goto yyrule188
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'z':
		goto yystate66
	}

yystate803:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'K' || c >= 'M' && c <= 'V' || c >= 'X' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'k' || c >= 'm' && c <= 'v' || c >= 'x' && c <= 'z':
		goto yystate66
	case c == 'L' || c == 'l':
		goto yystate804
	case c == 'W' || c == 'w':
		goto yystate810
	}

yystate804:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'K' || c >= 'M' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'k' || c >= 'm' && c <= 'z':
		goto yystate66
	case c == 'L' || c == 'l':
		goto yystate805
	}

yystate805:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c == 'A' || c >= 'C' && c <= 'Z' || c == '_' || c == 'a' || c >= 'c' && c <= 'z':
		goto yystate66
	case c == 'B' || c == 'b':
		goto yystate806
	}

yystate806:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'B' && c <= 'Z' || c == '_' || c >= 'b' && c <= 'z':
		goto yystate66
	case c == 'A' || c == 'a':
		goto yystate807
	}

yystate807:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c == 'A' || c == 'B' || c >= 'D' && c <= 'Z' || c == '_' || c == 'a' || c == 'b' || c >= 'd' && c <= 'z':
		goto yystate66
	case c == 'C' || c == 'c':
		goto yystate808
	}

yystate808:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'J' || c >= 'L' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'j' || c >= 'l' && c <= 'z':
		goto yystate66
	case c == 'K' || c == 'k':
		goto yystate809
	}

yystate809:
	c = l.next()
	switch {
	default:
		goto yyrule176
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'z':
		goto yystate66
	}

yystate810:
	c = l.next()
	switch {
	default:
		goto yyrule177
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'z':
		goto yystate66
	}

yystate811:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c == 'A' || c == 'B' || c == 'D' || c == 'F' || c == 'G' || c >= 'J' && c <= 'L' || c == 'N' || c == 'P' || c == 'R' || c == 'S' || c >= 'V' && c <= 'X' || c == 'Z' || c == '_' || c == 'a' || c == 'b' || c == 'd' || c == 'f' || c == 'g' || c >= 'j' && c <= 'l' || c == 'n' || c == 'p' || c == 'r' || c == 's' || c >= 'v' && c <= 'x' || c == 'z':
		goto yystate66
	case c == 'C' || c == 'c':
		goto yystate812
	case c == 'E' || c == 'e':
		goto yystate818
	case c == 'H' || c == 'h':
		goto yystate845
	case c == 'I' || c == 'i':
		goto yystate851
	case c == 'M' || c == 'm':
		goto yystate856
	case c == 'O' || c == 'o':
		goto yystate863
	case c == 'Q' || c == 'q':
		goto yystate866
	case c == 'T' || c == 't':
		goto yystate884
	case c == 'U' || c == 'u':
		goto yystate888
	case c == 'Y' || c == 'y':
		goto yystate903
	}

yystate812:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'G' || c >= 'I' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'g' || c >= 'i' && c <= 'z':
		goto yystate66
	case c == 'H' || c == 'h':
		goto yystate813
	}

yystate813:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'D' || c >= 'F' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'd' || c >= 'f' && c <= 'z':
		goto yystate66
	case c == 'E' || c == 'e':
		goto yystate814
	}

yystate814:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'L' || c >= 'N' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'l' || c >= 'n' && c <= 'z':
		goto yystate66
	case c == 'M' || c == 'm':
		goto yystate815
	}

yystate815:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'B' && c <= 'Z' || c == '_' || c >= 'b' && c <= 'z':
		goto yystate66
	case c == 'A' || c == 'a':
		goto yystate816
	}

yystate816:
	c = l.next()
	switch {
	default:
		goto yyrule178
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'R' || c >= 'T' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'r' || c >= 't' && c <= 'z':
		goto yystate66
	case c == 'S' || c == 's':
		goto yystate817
	}

yystate817:
	c = l.next()
	switch {
	default:
		goto yyrule179
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'z':
		goto yystate66
	}

yystate818:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c == 'A' || c == 'B' || c >= 'D' && c <= 'K' || c >= 'M' && c <= 'R' || c >= 'U' && c <= 'Z' || c == '_' || c == 'a' || c == 'b' || c >= 'd' && c <= 'k' || c >= 'm' && c <= 'r' || c >= 'u' && c <= 'z':
		goto yystate66
	case c == 'C' || c == 'c':
		goto yystate819
	case c == 'L' || c == 'l':
		goto yystate835
	case c == 'S' || c == 's':
		goto yystate839
	case c == 'T' || c == 't':
		goto yystate844
	}

yystate819:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'N' || c >= 'P' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'n' || c >= 'p' && c <= 'z':
		goto yystate66
	case c == 'O' || c == 'o':
		goto yystate820
	}

yystate820:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'M' || c >= 'O' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'm' || c >= 'o' && c <= 'z':
		goto yystate66
	case c == 'N' || c == 'n':
		goto yystate821
	}

yystate821:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'C' || c >= 'E' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'c' || c >= 'e' && c <= 'z':
		goto yystate66
	case c == 'D' || c == 'd':
		goto yystate822
	}

yystate822:
	c = l.next()
	switch {
	default:
		goto yyrule191
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'Z' || c >= 'a' && c <= 'z':
		goto yystate66
	case c == '_':
		goto yystate823
	}

yystate823:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'L' || c >= 'N' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'l' || c >= 'n' && c <= 'z':
		goto yystate66
	case c == 'M' || c == 'm':
		goto yystate824
	}

yystate824:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'H' || c >= 'J' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'h' || c >= 'j' && c <= 'z':
		goto yystate66
	case c == 'I' || c == 'i':
		goto yystate825
	}

yystate825:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c == 'A' || c == 'B' || c >= 'D' && c <= 'Z' || c == '_' || c == 'a' || c == 'b' || c >= 'd' && c <= 'z':
		goto yystate66
	case c == 'C' || c == 'c':
		goto yystate826
	}

yystate826:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'Q' || c >= 'S' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'q' || c >= 's' && c <= 'z':
		goto yystate66
	case c == 'R' || c == 'r':
		goto yystate827
	}

yystate827:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'N' || c >= 'P' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'n' || c >= 'p' && c <= 'z':
		goto yystate66
	case c == 'O' || c == 'o':
		goto yystate828
	}

yystate828:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'R' || c >= 'T' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'r' || c >= 't' && c <= 'z':
		goto yystate66
	case c == 'S' || c == 's':
		goto yystate829
	}

yystate829:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'D' || c >= 'F' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'd' || c >= 'f' && c <= 'z':
		goto yystate66
	case c == 'E' || c == 'e':
		goto yystate830
	}

yystate830:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c == 'A' || c == 'B' || c >= 'D' && c <= 'Z' || c == '_' || c == 'a' || c == 'b' || c >= 'd' && c <= 'z':
		goto yystate66
	case c == 'C' || c == 'c':
		goto yystate831
	}

yystate831:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'N' || c >= 'P' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'n' || c >= 'p' && c <= 'z':
		goto yystate66
	case c == 'O' || c == 'o':
		goto yystate832
	}

yystate832:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'M' || c >= 'O' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'm' || c >= 'o' && c <= 'z':
		goto yystate66
	case c == 'N' || c == 'n':
		goto yystate833
	}

yystate833:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'C' || c >= 'E' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'c' || c >= 'e' && c <= 'z':
		goto yystate66
	case c == 'D' || c == 'd':
		goto yystate834
	}

yystate834:
	c = l.next()
	switch {
	default:
		goto yyrule192
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'z':
		goto yystate66
	}

yystate835:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'D' || c >= 'F' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'd' || c >= 'f' && c <= 'z':
		goto yystate66
	case c == 'E' || c == 'e':
		goto yystate836
	}

yystate836:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c == 'A' || c == 'B' || c >= 'D' && c <= 'Z' || c == '_' || c == 'a' || c == 'b' || c >= 'd' && c <= 'z':
		goto yystate66
	case c == 'C' || c == 'c':
		goto yystate837
	}

yystate837:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'S' || c >= 'U' && c <= 'Z' || c == '_' || c >= 'a' && c <= 's' || c >= 'u' && c <= 'z':
		goto yystate66
	case c == 'T' || c == 't':
		goto yystate838
	}

yystate838:
	c = l.next()
	switch {
	default:
		goto yyrule193
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'z':
		goto yystate66
	}

yystate839:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'R' || c >= 'T' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'r' || c >= 't' && c <= 'z':
		goto yystate66
	case c == 'S' || c == 's':
		goto yystate840
	}

yystate840:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'H' || c >= 'J' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'h' || c >= 'j' && c <= 'z':
		goto yystate66
	case c == 'I' || c == 'i':
		goto yystate841
	}

yystate841:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'N' || c >= 'P' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'n' || c >= 'p' && c <= 'z':
		goto yystate66
	case c == 'O' || c == 'o':
		goto yystate842
	}

yystate842:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'M' || c >= 'O' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'm' || c >= 'o' && c <= 'z':
		goto yystate66
	case c == 'N' || c == 'n':
		goto yystate843
	}

yystate843:
	c = l.next()
	switch {
	default:
		goto yyrule180
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'z':
		goto yystate66
	}

yystate844:
	c = l.next()
	switch {
	default:
		goto yyrule194
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'z':
		goto yystate66
	}

yystate845:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'B' && c <= 'N' || c >= 'P' && c <= 'Z' || c == '_' || c >= 'b' && c <= 'n' || c >= 'p' && c <= 'z':
		goto yystate66
	case c == 'A' || c == 'a':
		goto yystate846
	case c == 'O' || c == 'o':
		goto yystate849
	}

yystate846:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'Q' || c >= 'S' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'q' || c >= 's' && c <= 'z':
		goto yystate66
	case c == 'R' || c == 'r':
		goto yystate847
	}

yystate847:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'D' || c >= 'F' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'd' || c >= 'f' && c <= 'z':
		goto yystate66
	case c == 'E' || c == 'e':
		goto yystate848
	}

yystate848:
	c = l.next()
	switch {
	default:
		goto yyrule195
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'z':
		goto yystate66
	}

yystate849:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'V' || c >= 'X' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'v' || c >= 'x' && c <= 'z':
		goto yystate66
	case c == 'W' || c == 'w':
		goto yystate850
	}

yystate850:
	c = l.next()
	switch {
	default:
		goto yyrule196
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'z':
		goto yystate66
	}

yystate851:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'F' || c >= 'H' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'f' || c >= 'h' && c <= 'z':
		goto yystate66
	case c == 'G' || c == 'g':
		goto yystate852
	}

yystate852:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'M' || c >= 'O' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'm' || c >= 'o' && c <= 'z':
		goto yystate66
	case c == 'N' || c == 'n':
		goto yystate853
	}

yystate853:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'D' || c >= 'F' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'd' || c >= 'f' && c <= 'z':
		goto yystate66
	case c == 'E' || c == 'e':
		goto yystate854
	}

yystate854:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'C' || c >= 'E' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'c' || c >= 'e' && c <= 'z':
		goto yystate66
	case c == 'D' || c == 'd':
		goto yystate855
	}

yystate855:
	c = l.next()
	switch {
	default:
		goto yyrule229
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'z':
		goto yystate66
	}

yystate856:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'B' && c <= 'Z' || c == '_' || c >= 'b' && c <= 'z':
		goto yystate66
	case c == 'A' || c == 'a':
		goto yystate857
	}

yystate857:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'K' || c >= 'M' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'k' || c >= 'm' && c <= 'z':
		goto yystate66
	case c == 'L' || c == 'l':
		goto yystate858
	}

yystate858:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'K' || c >= 'M' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'k' || c >= 'm' && c <= 'z':
		goto yystate66
	case c == 'L' || c == 'l':
		goto yystate859
	}

yystate859:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'H' || c >= 'J' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'h' || c >= 'j' && c <= 'z':
		goto yystate66
	case c == 'I' || c == 'i':
		goto yystate860
	}

yystate860:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'M' || c >= 'O' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'm' || c >= 'o' && c <= 'z':
		goto yystate66
	case c == 'N' || c == 'n':
		goto yystate861
	}

yystate861:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'S' || c >= 'U' && c <= 'Z' || c == '_' || c >= 'a' && c <= 's' || c >= 'u' && c <= 'z':
		goto yystate66
	case c == 'T' || c == 't':
		goto yystate862
	}

yystate862:
	c = l.next()
	switch {
	default:
		goto yyrule243
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'z':
		goto yystate66
	}

yystate863:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'L' || c >= 'N' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'l' || c >= 'n' && c <= 'z':
		goto yystate66
	case c == 'M' || c == 'm':
		goto yystate864
	}

yystate864:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'D' || c >= 'F' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'd' || c >= 'f' && c <= 'z':
		goto yystate66
	case c == 'E' || c == 'e':
		goto yystate865
	}

yystate865:
	c = l.next()
	switch {
	default:
		goto yyrule181
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'z':
		goto yystate66
	}

yystate866:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'K' || c >= 'M' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'k' || c >= 'm' && c <= 'z':
		goto yystate66
	case c == 'L' || c == 'l':
		goto yystate867
	}

yystate867:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'Z' || c >= 'a' && c <= 'z':
		goto yystate66
	case c == '_':
		goto yystate868
	}

yystate868:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c == 'A' || c == 'B' || c >= 'D' && c <= 'Z' || c == '_' || c == 'a' || c == 'b' || c >= 'd' && c <= 'z':
		goto yystate66
	case c == 'C' || c == 'c':
		goto yystate869
	}

yystate869:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'B' && c <= 'Z' || c == '_' || c >= 'b' && c <= 'z':
		goto yystate66
	case c == 'A' || c == 'a':
		goto yystate870
	}

yystate870:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'K' || c >= 'M' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'k' || c >= 'm' && c <= 'z':
		goto yystate66
	case c == 'L' || c == 'l':
		goto yystate871
	}

yystate871:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c == 'A' || c == 'B' || c >= 'D' && c <= 'Z' || c == '_' || c == 'a' || c == 'b' || c >= 'd' && c <= 'z':
		goto yystate66
	case c == 'C' || c == 'c':
		goto yystate872
	}

yystate872:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'Z' || c >= 'a' && c <= 'z':
		goto yystate66
	case c == '_':
		goto yystate873
	}

yystate873:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'E' || c >= 'G' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'e' || c >= 'g' && c <= 'z':
		goto yystate66
	case c == 'F' || c == 'f':
		goto yystate874
	}

yystate874:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'N' || c >= 'P' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'n' || c >= 'p' && c <= 'z':
		goto yystate66
	case c == 'O' || c == 'o':
		goto yystate875
	}

yystate875:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'T' || c >= 'V' && c <= 'Z' || c == '_' || c >= 'a' && c <= 't' || c >= 'v' && c <= 'z':
		goto yystate66
	case c == 'U' || c == 'u':
		goto yystate876
	}

yystate876:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'M' || c >= 'O' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'm' || c >= 'o' && c <= 'z':
		goto yystate66
	case c == 'N' || c == 'n':
		goto yystate877
	}

yystate877:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'C' || c >= 'E' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'c' || c >= 'e' && c <= 'z':
		goto yystate66
	case c == 'D' || c == 'd':
		goto yystate878
	}

yystate878:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'Z' || c >= 'a' && c <= 'z':
		goto yystate66
	case c == '_':
		goto yystate879
	}

yystate879:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'Q' || c >= 'S' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'q' || c >= 's' && c <= 'z':
		goto yystate66
	case c == 'R' || c == 'r':
		goto yystate880
	}

yystate880:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'N' || c >= 'P' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'n' || c >= 'p' && c <= 'z':
		goto yystate66
	case c == 'O' || c == 'o':
		goto yystate881
	}

yystate881:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'V' || c >= 'X' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'v' || c >= 'x' && c <= 'z':
		goto yystate66
	case c == 'W' || c == 'w':
		goto yystate882
	}

yystate882:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'R' || c >= 'T' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'r' || c >= 't' && c <= 'z':
		goto yystate66
	case c == 'S' || c == 's':
		goto yystate883
	}

yystate883:
	c = l.next()
	switch {
	default:
		goto yyrule235
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'z':
		goto yystate66
	}

yystate884:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'B' && c <= 'Z' || c == '_' || c >= 'b' && c <= 'z':
		goto yystate66
	case c == 'A' || c == 'a':
		goto yystate885
	}

yystate885:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'Q' || c >= 'S' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'q' || c >= 's' && c <= 'z':
		goto yystate66
	case c == 'R' || c == 'r':
		goto yystate886
	}

yystate886:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'S' || c >= 'U' && c <= 'Z' || c == '_' || c >= 'a' && c <= 's' || c >= 'u' && c <= 'z':
		goto yystate66
	case c == 'T' || c == 't':
		goto yystate887
	}

yystate887:
	c = l.next()
	switch {
	default:
		goto yyrule182
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'z':
		goto yystate66
	}

yystate888:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c == 'A' || c >= 'C' && c <= 'L' || c >= 'N' && c <= 'Z' || c == '_' || c == 'a' || c >= 'c' && c <= 'l' || c >= 'n' && c <= 'z':
		goto yystate66
	case c == 'B' || c == 'b':
		goto yystate889
	case c == 'M' || c == 'm':
		goto yystate902
	}

yystate889:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'R' || c >= 'T' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'r' || c >= 't' && c <= 'z':
		goto yystate66
	case c == 'S' || c == 's':
		goto yystate890
	}

yystate890:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'S' || c >= 'U' && c <= 'Z' || c == '_' || c >= 'a' && c <= 's' || c >= 'u' && c <= 'z':
		goto yystate66
	case c == 'T' || c == 't':
		goto yystate891
	}

yystate891:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'Q' || c >= 'S' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'q' || c >= 's' && c <= 'z':
		goto yystate66
	case c == 'R' || c == 'r':
		goto yystate892
	}

yystate892:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'H' || c >= 'J' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'h' || c >= 'j' && c <= 'z':
		goto yystate66
	case c == 'I' || c == 'i':
		goto yystate893
	}

yystate893:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'M' || c >= 'O' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'm' || c >= 'o' && c <= 'z':
		goto yystate66
	case c == 'N' || c == 'n':
		goto yystate894
	}

yystate894:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'F' || c >= 'H' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'f' || c >= 'h' && c <= 'z':
		goto yystate66
	case c == 'G' || c == 'g':
		goto yystate895
	}

yystate895:
	c = l.next()
	switch {
	default:
		goto yyrule197
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'Z' || c >= 'a' && c <= 'z':
		goto yystate66
	case c == '_':
		goto yystate896
	}

yystate896:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'H' || c >= 'J' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'h' || c >= 'j' && c <= 'z':
		goto yystate66
	case c == 'I' || c == 'i':
		goto yystate897
	}

yystate897:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'M' || c >= 'O' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'm' || c >= 'o' && c <= 'z':
		goto yystate66
	case c == 'N' || c == 'n':
		goto yystate898
	}

yystate898:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'C' || c >= 'E' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'c' || c >= 'e' && c <= 'z':
		goto yystate66
	case c == 'D' || c == 'd':
		goto yystate899
	}

yystate899:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'D' || c >= 'F' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'd' || c >= 'f' && c <= 'z':
		goto yystate66
	case c == 'E' || c == 'e':
		goto yystate900
	}

yystate900:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'W' || c == 'Y' || c == 'Z' || c == '_' || c >= 'a' && c <= 'w' || c == 'y' || c == 'z':
		goto yystate66
	case c == 'X' || c == 'x':
		goto yystate901
	}

yystate901:
	c = l.next()
	switch {
	default:
		goto yyrule198
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'z':
		goto yystate66
	}

yystate902:
	c = l.next()
	switch {
	default:
		goto yyrule199
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'z':
		goto yystate66
	}

yystate903:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'R' || c >= 'T' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'r' || c >= 't' && c <= 'z':
		goto yystate66
	case c == 'S' || c == 's':
		goto yystate904
	}

yystate904:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'C' || c >= 'E' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'c' || c >= 'e' && c <= 'z':
		goto yystate66
	case c == 'D' || c == 'd':
		goto yystate905
	}

yystate905:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'B' && c <= 'Z' || c == '_' || c >= 'b' && c <= 'z':
		goto yystate66
	case c == 'A' || c == 'a':
		goto yystate906
	}

yystate906:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'S' || c >= 'U' && c <= 'Z' || c == '_' || c >= 'a' && c <= 's' || c >= 'u' && c <= 'z':
		goto yystate66
	case c == 'T' || c == 't':
		goto yystate907
	}

yystate907:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'D' || c >= 'F' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'd' || c >= 'f' && c <= 'z':
		goto yystate66
	case c == 'E' || c == 'e':
		goto yystate908
	}

yystate908:
	c = l.next()
	switch {
	default:
		goto yyrule200
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'z':
		goto yystate66
	}

yystate909:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'B' && c <= 'D' || c == 'F' || c == 'G' || c >= 'J' && c <= 'Q' || c >= 'S' && c <= 'Z' || c == '_' || c >= 'b' && c <= 'd' || c == 'f' || c == 'g' || c >= 'j' && c <= 'q' || c >= 's' && c <= 'z':
		goto yystate66
	case c == 'A' || c == 'a':
		goto yystate910
	case c == 'E' || c == 'e':
		goto yystate915
	case c == 'H' || c == 'h':
		goto yystate918
	case c == 'I' || c == 'i':
		goto yystate921
	case c == 'R' || c == 'r':
		goto yystate942
	}

yystate910:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c == 'A' || c >= 'C' && c <= 'Z' || c == '_' || c == 'a' || c >= 'c' && c <= 'z':
		goto yystate66
	case c == 'B' || c == 'b':
		goto yystate911
	}

yystate911:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'K' || c >= 'M' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'k' || c >= 'm' && c <= 'z':
		goto yystate66
	case c == 'L' || c == 'l':
		goto yystate912
	}

yystate912:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'D' || c >= 'F' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'd' || c >= 'f' && c <= 'z':
		goto yystate66
	case c == 'E' || c == 'e':
		goto yystate913
	}

yystate913:
	c = l.next()
	switch {
	default:
		goto yyrule201
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'R' || c >= 'T' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'r' || c >= 't' && c <= 'z':
		goto yystate66
	case c == 'S' || c == 's':
		goto yystate914
	}

yystate914:
	c = l.next()
	switch {
	default:
		goto yyrule202
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'z':
		goto yystate66
	}

yystate915:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'W' || c == 'Y' || c == 'Z' || c == '_' || c >= 'a' && c <= 'w' || c == 'y' || c == 'z':
		goto yystate66
	case c == 'X' || c == 'x':
		goto yystate916
	}

yystate916:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'S' || c >= 'U' && c <= 'Z' || c == '_' || c >= 'a' && c <= 's' || c >= 'u' && c <= 'z':
		goto yystate66
	case c == 'T' || c == 't':
		goto yystate917
	}

yystate917:
	c = l.next()
	switch {
	default:
		goto yyrule267
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'z':
		goto yystate66
	}

yystate918:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'D' || c >= 'F' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'd' || c >= 'f' && c <= 'z':
		goto yystate66
	case c == 'E' || c == 'e':
		goto yystate919
	}

yystate919:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'M' || c >= 'O' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'm' || c >= 'o' && c <= 'z':
		goto yystate66
	case c == 'N' || c == 'n':
		goto yystate920
	}

yystate920:
	c = l.next()
	switch {
	default:
		goto yyrule203
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'z':
		goto yystate66
	}

yystate921:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'L' || c >= 'O' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'l' || c >= 'o' && c <= 'z':
		goto yystate66
	case c == 'M' || c == 'm':
		goto yystate922
	case c == 'N' || c == 'n':
		goto yystate929
	}

yystate922:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'D' || c >= 'F' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'd' || c >= 'f' && c <= 'z':
		goto yystate66
	case c == 'E' || c == 'e':
		goto yystate923
	}

yystate923:
	c = l.next()
	switch {
	default:
		goto yyrule253
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'R' || c >= 'T' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'r' || c >= 't' && c <= 'z':
		goto yystate66
	case c == 'S' || c == 's':
		goto yystate924
	}

yystate924:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'S' || c >= 'U' && c <= 'Z' || c == '_' || c >= 'a' && c <= 's' || c >= 'u' && c <= 'z':
		goto yystate66
	case c == 'T' || c == 't':
		goto yystate925
	}

yystate925:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'B' && c <= 'Z' || c == '_' || c >= 'b' && c <= 'z':
		goto yystate66
	case c == 'A' || c == 'a':
		goto yystate926
	}

yystate926:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'L' || c >= 'N' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'l' || c >= 'n' && c <= 'z':
		goto yystate66
	case c == 'M' || c == 'm':
		goto yystate927
	}

yystate927:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'O' || c >= 'Q' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'o' || c >= 'q' && c <= 'z':
		goto yystate66
	case c == 'P' || c == 'p':
		goto yystate928
	}

yystate928:
	c = l.next()
	switch {
	default:
		goto yyrule254
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'z':
		goto yystate66
	}

yystate929:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'X' || c == 'Z' || c == '_' || c >= 'a' && c <= 'x' || c == 'z':
		goto yystate66
	case c == 'Y' || c == 'y':
		goto yystate930
	}

yystate930:
	c = l.next()
	switch {
	default:
		goto yyrule241
	case c == '$' || c >= '0' && c <= '9' || c == 'A' || c >= 'C' && c <= 'H' || c >= 'J' && c <= 'S' || c >= 'U' && c <= 'Z' || c == '_' || c == 'a' || c >= 'c' && c <= 'h' || c >= 'j' && c <= 's' || c >= 'u' && c <= 'z':
		goto yystate66
	case c == 'B' || c == 'b':
		goto yystate931
	case c == 'I' || c == 'i':
		goto yystate935
	case c == 'T' || c == 't':
		goto yystate938
	}

yystate931:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'K' || c >= 'M' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'k' || c >= 'm' && c <= 'z':
		goto yystate66
	case c == 'L' || c == 'l':
		goto yystate932
	}

yystate932:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'N' || c >= 'P' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'n' || c >= 'p' && c <= 'z':
		goto yystate66
	case c == 'O' || c == 'o':
		goto yystate933
	}

yystate933:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c == 'A' || c >= 'C' && c <= 'Z' || c == '_' || c == 'a' || c >= 'c' && c <= 'z':
		goto yystate66
	case c == 'B' || c == 'b':
		goto yystate934
	}

yystate934:
	c = l.next()
	switch {
	default:
		goto yyrule261
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'z':
		goto yystate66
	}

yystate935:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'M' || c >= 'O' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'm' || c >= 'o' && c <= 'z':
		goto yystate66
	case c == 'N' || c == 'n':
		goto yystate936
	}

yystate936:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'S' || c >= 'U' && c <= 'Z' || c == '_' || c >= 'a' && c <= 's' || c >= 'u' && c <= 'z':
		goto yystate66
	case c == 'T' || c == 't':
		goto yystate937
	}

yystate937:
	c = l.next()
	switch {
	default:
		goto yyrule242
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'z':
		goto yystate66
	}

yystate938:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'D' || c >= 'F' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'd' || c >= 'f' && c <= 'z':
		goto yystate66
	case c == 'E' || c == 'e':
		goto yystate939
	}

yystate939:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'W' || c == 'Y' || c == 'Z' || c == '_' || c >= 'a' && c <= 'w' || c == 'y' || c == 'z':
		goto yystate66
	case c == 'X' || c == 'x':
		goto yystate940
	}

yystate940:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'S' || c >= 'U' && c <= 'Z' || c == '_' || c >= 'a' && c <= 's' || c >= 'u' && c <= 'z':
		goto yystate66
	case c == 'T' || c == 't':
		goto yystate941
	}

yystate941:
	c = l.next()
	switch {
	default:
		goto yyrule265
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'z':
		goto yystate66
	}

yystate942:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'B' && c <= 'H' || c >= 'J' && c <= 'T' || c >= 'V' && c <= 'Z' || c == '_' || c >= 'b' && c <= 'h' || c >= 'j' && c <= 't' || c >= 'v' && c <= 'z':
		goto yystate66
	case c == 'A' || c == 'a':
		goto yystate943
	case c == 'I' || c == 'i':
		goto yystate957
	case c == 'U' || c == 'u':
		goto yystate959
	}

yystate943:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'H' || c >= 'J' && c <= 'M' || c >= 'O' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'h' || c >= 'j' && c <= 'm' || c >= 'o' && c <= 'z':
		goto yystate66
	case c == 'I' || c == 'i':
		goto yystate944
	case c == 'N' || c == 'n':
		goto yystate949
	}

yystate944:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'K' || c >= 'M' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'k' || c >= 'm' && c <= 'z':
		goto yystate66
	case c == 'L' || c == 'l':
		goto yystate945
	}

yystate945:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'H' || c >= 'J' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'h' || c >= 'j' && c <= 'z':
		goto yystate66
	case c == 'I' || c == 'i':
		goto yystate946
	}

yystate946:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'M' || c >= 'O' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'm' || c >= 'o' && c <= 'z':
		goto yystate66
	case c == 'N' || c == 'n':
		goto yystate947
	}

yystate947:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'F' || c >= 'H' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'f' || c >= 'h' && c <= 'z':
		goto yystate66
	case c == 'G' || c == 'g':
		goto yystate948
	}

yystate948:
	c = l.next()
	switch {
	default:
		goto yyrule204
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'z':
		goto yystate66
	}

yystate949:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'R' || c >= 'T' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'r' || c >= 't' && c <= 'z':
		goto yystate66
	case c == 'S' || c == 's':
		goto yystate950
	}

yystate950:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'B' && c <= 'Z' || c == '_' || c >= 'b' && c <= 'z':
		goto yystate66
	case c == 'A' || c == 'a':
		goto yystate951
	}

yystate951:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c == 'A' || c == 'B' || c >= 'D' && c <= 'Z' || c == '_' || c == 'a' || c == 'b' || c >= 'd' && c <= 'z':
		goto yystate66
	case c == 'C' || c == 'c':
		goto yystate952
	}

yystate952:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'S' || c >= 'U' && c <= 'Z' || c == '_' || c >= 'a' && c <= 's' || c >= 'u' && c <= 'z':
		goto yystate66
	case c == 'T' || c == 't':
		goto yystate953
	}

yystate953:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'H' || c >= 'J' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'h' || c >= 'j' && c <= 'z':
		goto yystate66
	case c == 'I' || c == 'i':
		goto yystate954
	}

yystate954:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'N' || c >= 'P' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'n' || c >= 'p' && c <= 'z':
		goto yystate66
	case c == 'O' || c == 'o':
		goto yystate955
	}

yystate955:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'M' || c >= 'O' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'm' || c >= 'o' && c <= 'z':
		goto yystate66
	case c == 'N' || c == 'n':
		goto yystate956
	}

yystate956:
	c = l.next()
	switch {
	default:
		goto yyrule205
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'z':
		goto yystate66
	}

yystate957:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'L' || c >= 'N' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'l' || c >= 'n' && c <= 'z':
		goto yystate66
	case c == 'M' || c == 'm':
		goto yystate958
	}

yystate958:
	c = l.next()
	switch {
	default:
		goto yyrule206
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'z':
		goto yystate66
	}

yystate959:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'D' || c >= 'F' && c <= 'M' || c >= 'O' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'd' || c >= 'f' && c <= 'm' || c >= 'o' && c <= 'z':
		goto yystate66
	case c == 'E' || c == 'e':
		goto yystate960
	case c == 'N' || c == 'n':
		goto yystate961
	}

yystate960:
	c = l.next()
	switch {
	default:
		goto yyrule234
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'z':
		goto yystate66
	}

yystate961:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c == 'A' || c == 'B' || c >= 'D' && c <= 'Z' || c == '_' || c == 'a' || c == 'b' || c >= 'd' && c <= 'z':
		goto yystate66
	case c == 'C' || c == 'c':
		goto yystate962
	}

yystate962:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'B' && c <= 'Z' || c == '_' || c >= 'b' && c <= 'z':
		goto yystate66
	case c == 'A' || c == 'a':
		goto yystate963
	}

yystate963:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'S' || c >= 'U' && c <= 'Z' || c == '_' || c >= 'a' && c <= 's' || c >= 'u' && c <= 'z':
		goto yystate66
	case c == 'T' || c == 't':
		goto yystate964
	}

yystate964:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'D' || c >= 'F' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'd' || c >= 'f' && c <= 'z':
		goto yystate66
	case c == 'E' || c == 'e':
		goto yystate965
	}

yystate965:
	c = l.next()
	switch {
	default:
		goto yyrule207
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'z':
		goto yystate66
	}

yystate966:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'M' || c == 'O' || c == 'Q' || c == 'R' || c >= 'T' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'm' || c == 'o' || c == 'q' || c == 'r' || c >= 't' && c <= 'z':
		goto yystate66
	case c == 'N' || c == 'n':
		goto yystate967
	case c == 'P' || c == 'p':
		goto yystate985
	case c == 'S' || c == 's':
		goto yystate993
	}

yystate967:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'H' || c == 'J' || c >= 'L' && c <= 'R' || c >= 'T' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'h' || c == 'j' || c >= 'l' && c <= 'r' || c >= 't' && c <= 'z':
		goto yystate66
	case c == 'I' || c == 'i':
		goto yystate968
	case c == 'K' || c == 'k':
		goto yystate974
	case c == 'S' || c == 's':
		goto yystate979
	}

yystate968:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'N' || c == 'P' || c >= 'R' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'n' || c == 'p' || c >= 'r' && c <= 'z':
		goto yystate66
	case c == 'O' || c == 'o':
		goto yystate969
	case c == 'Q' || c == 'q':
		goto yystate971
	}

yystate969:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'M' || c >= 'O' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'm' || c >= 'o' && c <= 'z':
		goto yystate66
	case c == 'N' || c == 'n':
		goto yystate970
	}

yystate970:
	c = l.next()
	switch {
	default:
		goto yyrule208
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'z':
		goto yystate66
	}

yystate971:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'T' || c >= 'V' && c <= 'Z' || c == '_' || c >= 'a' && c <= 't' || c >= 'v' && c <= 'z':
		goto yystate66
	case c == 'U' || c == 'u':
		goto yystate972
	}

yystate972:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'D' || c >= 'F' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'd' || c >= 'f' && c <= 'z':
		goto yystate66
	case c == 'E' || c == 'e':
		goto yystate973
	}

yystate973:
	c = l.next()
	switch {
	default:
		goto yyrule209
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'z':
		goto yystate66
	}

yystate974:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'M' || c >= 'O' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'm' || c >= 'o' && c <= 'z':
		goto yystate66
	case c == 'N' || c == 'n':
		goto yystate975
	}

yystate975:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'N' || c >= 'P' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'n' || c >= 'p' && c <= 'z':
		goto yystate66
	case c == 'O' || c == 'o':
		goto yystate976
	}

yystate976:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'V' || c >= 'X' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'v' || c >= 'x' && c <= 'z':
		goto yystate66
	case c == 'W' || c == 'w':
		goto yystate977
	}

yystate977:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'M' || c >= 'O' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'm' || c >= 'o' && c <= 'z':
		goto yystate66
	case c == 'N' || c == 'n':
		goto yystate978
	}

yystate978:
	c = l.next()
	switch {
	default:
		goto yyrule210
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'z':
		goto yystate66
	}

yystate979:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'H' || c >= 'J' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'h' || c >= 'j' && c <= 'z':
		goto yystate66
	case c == 'I' || c == 'i':
		goto yystate980
	}

yystate980:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'F' || c >= 'H' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'f' || c >= 'h' && c <= 'z':
		goto yystate66
	case c == 'G' || c == 'g':
		goto yystate981
	}

yystate981:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'M' || c >= 'O' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'm' || c >= 'o' && c <= 'z':
		goto yystate66
	case c == 'N' || c == 'n':
		goto yystate982
	}

yystate982:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'D' || c >= 'F' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'd' || c >= 'f' && c <= 'z':
		goto yystate66
	case c == 'E' || c == 'e':
		goto yystate983
	}

yystate983:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'C' || c >= 'E' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'c' || c >= 'e' && c <= 'z':
		goto yystate66
	case c == 'D' || c == 'd':
		goto yystate984
	}

yystate984:
	c = l.next()
	switch {
	default:
		goto yyrule230
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'z':
		goto yystate66
	}

yystate985:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'C' || c >= 'E' && c <= 'O' || c >= 'Q' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'c' || c >= 'e' && c <= 'o' || c >= 'q' && c <= 'z':
		goto yystate66
	case c == 'D' || c == 'd':
		goto yystate986
	case c == 'P' || c == 'p':
		goto yystate990
	}

yystate986:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'B' && c <= 'Z' || c == '_' || c >= 'b' && c <= 'z':
		goto yystate66
	case c == 'A' || c == 'a':
		goto yystate987
	}

yystate987:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'S' || c >= 'U' && c <= 'Z' || c == '_' || c >= 'a' && c <= 's' || c >= 'u' && c <= 'z':
		goto yystate66
	case c == 'T' || c == 't':
		goto yystate988
	}

yystate988:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'D' || c >= 'F' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'd' || c >= 'f' && c <= 'z':
		goto yystate66
	case c == 'E' || c == 'e':
		goto yystate989
	}

yystate989:
	c = l.next()
	switch {
	default:
		goto yyrule212
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'z':
		goto yystate66
	}

yystate990:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'D' || c >= 'F' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'd' || c >= 'f' && c <= 'z':
		goto yystate66
	case c == 'E' || c == 'e':
		goto yystate991
	}

yystate991:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'Q' || c >= 'S' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'q' || c >= 's' && c <= 'z':
		goto yystate66
	case c == 'R' || c == 'r':
		goto yystate992
	}

yystate992:
	c = l.next()
	switch {
	default:
		goto yyrule213
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'z':
		goto yystate66
	}

yystate993:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'D' || c >= 'F' && c <= 'H' || c >= 'J' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'd' || c >= 'f' && c <= 'h' || c >= 'j' && c <= 'z':
		goto yystate66
	case c == 'E' || c == 'e':
		goto yystate994
	case c == 'I' || c == 'i':
		goto yystate996
	}

yystate994:
	c = l.next()
	switch {
	default:
		goto yyrule214
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'Q' || c >= 'S' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'q' || c >= 's' && c <= 'z':
		goto yystate66
	case c == 'R' || c == 'r':
		goto yystate995
	}

yystate995:
	c = l.next()
	switch {
	default:
		goto yyrule215
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'z':
		goto yystate66
	}

yystate996:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'M' || c >= 'O' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'm' || c >= 'o' && c <= 'z':
		goto yystate66
	case c == 'N' || c == 'n':
		goto yystate997
	}

yystate997:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'F' || c >= 'H' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'f' || c >= 'h' && c <= 'z':
		goto yystate66
	case c == 'G' || c == 'g':
		goto yystate998
	}

yystate998:
	c = l.next()
	switch {
	default:
		goto yyrule216
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'z':
		goto yystate66
	}

yystate999:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'B' && c <= 'Z' || c == '_' || c >= 'b' && c <= 'z':
		goto yystate66
	case c == 'A' || c == 'a':
		goto yystate1000
	}

yystate1000:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'K' || c >= 'M' && c <= 'Q' || c >= 'S' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'k' || c >= 'm' && c <= 'q' || c >= 's' && c <= 'z':
		goto yystate66
	case c == 'L' || c == 'l':
		goto yystate1001
	case c == 'R' || c == 'r':
		goto yystate1005
	}

yystate1001:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'T' || c >= 'V' && c <= 'Z' || c == '_' || c >= 'a' && c <= 't' || c >= 'v' && c <= 'z':
		goto yystate66
	case c == 'U' || c == 'u':
		goto yystate1002
	}

yystate1002:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'D' || c >= 'F' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'd' || c >= 'f' && c <= 'z':
		goto yystate66
	case c == 'E' || c == 'e':
		goto yystate1003
	}

yystate1003:
	c = l.next()
	switch {
	default:
		goto yyrule217
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'R' || c >= 'T' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'r' || c >= 't' && c <= 'z':
		goto yystate66
	case c == 'S' || c == 's':
		goto yystate1004
	}

yystate1004:
	c = l.next()
	switch {
	default:
		goto yyrule218
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'z':
		goto yystate66
	}

yystate1005:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c == 'A' || c >= 'D' && c <= 'H' || c >= 'J' && c <= 'Z' || c == '_' || c == 'a' || c >= 'd' && c <= 'h' || c >= 'j' && c <= 'z':
		goto yystate66
	case c == 'B' || c == 'b':
		goto yystate1006
	case c == 'C' || c == 'c':
		goto yystate1012
	case c == 'I' || c == 'i':
		goto yystate1016
	}

yystate1006:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'H' || c >= 'J' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'h' || c >= 'j' && c <= 'z':
		goto yystate66
	case c == 'I' || c == 'i':
		goto yystate1007
	}

yystate1007:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'M' || c >= 'O' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'm' || c >= 'o' && c <= 'z':
		goto yystate66
	case c == 'N' || c == 'n':
		goto yystate1008
	}

yystate1008:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'B' && c <= 'Z' || c == '_' || c >= 'b' && c <= 'z':
		goto yystate66
	case c == 'A' || c == 'a':
		goto yystate1009
	}

yystate1009:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'Q' || c >= 'S' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'q' || c >= 's' && c <= 'z':
		goto yystate66
	case c == 'R' || c == 'r':
		goto yystate1010
	}

yystate1010:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'X' || c == 'Z' || c == '_' || c >= 'a' && c <= 'x' || c == 'z':
		goto yystate66
	case c == 'Y' || c == 'y':
		goto yystate1011
	}

yystate1011:
	c = l.next()
	switch {
	default:
		goto yyrule260
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'z':
		goto yystate66
	}

yystate1012:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'G' || c >= 'I' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'g' || c >= 'i' && c <= 'z':
		goto yystate66
	case c == 'H' || c == 'h':
		goto yystate1013
	}

yystate1013:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'B' && c <= 'Z' || c == '_' || c >= 'b' && c <= 'z':
		goto yystate66
	case c == 'A' || c == 'a':
		goto yystate1014
	}

yystate1014:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'Q' || c >= 'S' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'q' || c >= 's' && c <= 'z':
		goto yystate66
	case c == 'R' || c == 'r':
		goto yystate1015
	}

yystate1015:
	c = l.next()
	switch {
	default:
		goto yyrule258
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'z':
		goto yystate66
	}

yystate1016:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'B' && c <= 'Z' || c == '_' || c >= 'b' && c <= 'z':
		goto yystate66
	case c == 'A' || c == 'a':
		goto yystate1017
	}

yystate1017:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c == 'A' || c >= 'C' && c <= 'Z' || c == '_' || c == 'a' || c >= 'c' && c <= 'z':
		goto yystate66
	case c == 'B' || c == 'b':
		goto yystate1018
	}

yystate1018:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'K' || c >= 'M' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'k' || c >= 'm' && c <= 'z':
		goto yystate66
	case c == 'L' || c == 'l':
		goto yystate1019
	}

yystate1019:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'D' || c >= 'F' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'd' || c >= 'f' && c <= 'z':
		goto yystate66
	case c == 'E' || c == 'e':
		goto yystate1020
	}

yystate1020:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'R' || c >= 'T' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'r' || c >= 't' && c <= 'z':
		goto yystate66
	case c == 'S' || c == 's':
		goto yystate1021
	}

yystate1021:
	c = l.next()
	switch {
	default:
		goto yyrule219
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'z':
		goto yystate66
	}

yystate1022:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'B' && c <= 'D' || c == 'F' || c == 'G' || c >= 'I' && c <= 'Z' || c == '_' || c >= 'b' && c <= 'd' || c == 'f' || c == 'g' || c >= 'i' && c <= 'z':
		goto yystate66
	case c == 'A' || c == 'a':
		goto yystate1023
	case c == 'E' || c == 'e':
		goto yystate1030
	case c == 'H' || c == 'h':
		goto yystate1042
	}

yystate1023:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'Q' || c >= 'S' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'q' || c >= 's' && c <= 'z':
		goto yystate66
	case c == 'R' || c == 'r':
		goto yystate1024
	}

yystate1024:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'M' || c >= 'O' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'm' || c >= 'o' && c <= 'z':
		goto yystate66
	case c == 'N' || c == 'n':
		goto yystate1025
	}

yystate1025:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'H' || c >= 'J' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'h' || c >= 'j' && c <= 'z':
		goto yystate66
	case c == 'I' || c == 'i':
		goto yystate1026
	}

yystate1026:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'M' || c >= 'O' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'm' || c >= 'o' && c <= 'z':
		goto yystate66
	case c == 'N' || c == 'n':
		goto yystate1027
	}

yystate1027:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'F' || c >= 'H' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'f' || c >= 'h' && c <= 'z':
		goto yystate66
	case c == 'G' || c == 'g':
		goto yystate1028
	}

yystate1028:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'R' || c >= 'T' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'r' || c >= 't' && c <= 'z':
		goto yystate66
	case c == 'S' || c == 's':
		goto yystate1029
	}

yystate1029:
	c = l.next()
	switch {
	default:
		goto yyrule220
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'z':
		goto yystate66
	}

yystate1030:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'D' || c >= 'F' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'd' || c >= 'f' && c <= 'z':
		goto yystate66
	case c == 'E' || c == 'e':
		goto yystate1031
	}

yystate1031:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'J' || c >= 'L' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'j' || c >= 'l' && c <= 'z':
		goto yystate66
	case c == 'K' || c == 'k':
		goto yystate1032
	}

yystate1032:
	c = l.next()
	switch {
	default:
		goto yyrule221
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'C' || c >= 'E' && c <= 'N' || c >= 'P' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'c' || c >= 'e' && c <= 'n' || c >= 'p' && c <= 'z':
		goto yystate66
	case c == 'D' || c == 'd':
		goto yystate1033
	case c == 'O' || c == 'o':
		goto yystate1036
	}

yystate1033:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'B' && c <= 'Z' || c == '_' || c >= 'b' && c <= 'z':
		goto yystate66
	case c == 'A' || c == 'a':
		goto yystate1034
	}

yystate1034:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'X' || c == 'Z' || c == '_' || c >= 'a' && c <= 'x' || c == 'z':
		goto yystate66
	case c == 'Y' || c == 'y':
		goto yystate1035
	}

yystate1035:
	c = l.next()
	switch {
	default:
		goto yyrule222
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'z':
		goto yystate66
	}

yystate1036:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'E' || c >= 'G' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'e' || c >= 'g' && c <= 'z':
		goto yystate66
	case c == 'F' || c == 'f':
		goto yystate1037
	}

yystate1037:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'X' || c == 'Z' || c == '_' || c >= 'a' && c <= 'x' || c == 'z':
		goto yystate66
	case c == 'Y' || c == 'y':
		goto yystate1038
	}

yystate1038:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'D' || c >= 'F' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'd' || c >= 'f' && c <= 'z':
		goto yystate66
	case c == 'E' || c == 'e':
		goto yystate1039
	}

yystate1039:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'B' && c <= 'Z' || c == '_' || c >= 'b' && c <= 'z':
		goto yystate66
	case c == 'A' || c == 'a':
		goto yystate1040
	}

yystate1040:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'Q' || c >= 'S' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'q' || c >= 's' && c <= 'z':
		goto yystate66
	case c == 'R' || c == 'r':
		goto yystate1041
	}

yystate1041:
	c = l.next()
	switch {
	default:
		goto yyrule223
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'z':
		goto yystate66
	}

yystate1042:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'D' || c >= 'F' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'd' || c >= 'f' && c <= 'z':
		goto yystate66
	case c == 'E' || c == 'e':
		goto yystate1043
	}

yystate1043:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'M' || c >= 'O' && c <= 'Q' || c >= 'S' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'm' || c >= 'o' && c <= 'q' || c >= 's' && c <= 'z':
		goto yystate66
	case c == 'N' || c == 'n':
		goto yystate1044
	case c == 'R' || c == 'r':
		goto yystate1045
	}

yystate1044:
	c = l.next()
	switch {
	default:
		goto yyrule224
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'z':
		goto yystate66
	}

yystate1045:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'D' || c >= 'F' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'd' || c >= 'f' && c <= 'z':
		goto yystate66
	case c == 'E' || c == 'e':
		goto yystate1046
	}

yystate1046:
	c = l.next()
	switch {
	default:
		goto yyrule225
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'z':
		goto yystate66
	}

yystate1047:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'N' || c >= 'P' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'n' || c >= 'p' && c <= 'z':
		goto yystate66
	case c == 'O' || c == 'o':
		goto yystate1051
	case c == '\'':
		goto yystate1048
	}

yystate1048:
	c = l.next()
	switch {
	default:
		goto yyabort
	case c >= '0' && c <= '9' || c >= 'A' && c <= 'F' || c >= 'a' && c <= 'f':
		goto yystate1049
	}

yystate1049:
	c = l.next()
	switch {
	default:
		goto yyabort
	case c == '\'':
		goto yystate1050
	case c >= '0' && c <= '9' || c >= 'A' && c <= 'F' || c >= 'a' && c <= 'f':
		goto yystate1049
	}

yystate1050:
	c = l.next()
	goto yyrule11

yystate1051:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'Q' || c >= 'S' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'q' || c >= 's' && c <= 'z':
		goto yystate66
	case c == 'R' || c == 'r':
		goto yystate1052
	}

yystate1052:
	c = l.next()
	switch {
	default:
		goto yyrule226
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'z':
		goto yystate66
	}

yystate1053:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'D' || c >= 'F' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'd' || c >= 'f' && c <= 'z':
		goto yystate66
	case c == 'E' || c == 'e':
		goto yystate1054
	}

yystate1054:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'B' && c <= 'Z' || c == '_' || c >= 'b' && c <= 'z':
		goto yystate66
	case c == 'A' || c == 'a':
		goto yystate1055
	}

yystate1055:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'Q' || c >= 'S' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'q' || c >= 's' && c <= 'z':
		goto yystate66
	case c == 'R' || c == 'r':
		goto yystate1056
	}

yystate1056:
	c = l.next()
	switch {
	default:
		goto yyrule256
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'V' || c >= 'X' && c <= 'Z' || c >= 'a' && c <= 'v' || c >= 'x' && c <= 'z':
		goto yystate66
	case c == 'W' || c == 'w':
		goto yystate1057
	case c == '_':
		goto yystate1061
	}

yystate1057:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'D' || c >= 'F' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'd' || c >= 'f' && c <= 'z':
		goto yystate66
	case c == 'E' || c == 'e':
		goto yystate1058
	}

yystate1058:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'D' || c >= 'F' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'd' || c >= 'f' && c <= 'z':
		goto yystate66
	case c == 'E' || c == 'e':
		goto yystate1059
	}

yystate1059:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'J' || c >= 'L' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'j' || c >= 'l' && c <= 'z':
		goto yystate66
	case c == 'K' || c == 'k':
		goto yystate1060
	}

yystate1060:
	c = l.next()
	switch {
	default:
		goto yyrule227
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'z':
		goto yystate66
	}

yystate1061:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'L' || c >= 'N' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'l' || c >= 'n' && c <= 'z':
		goto yystate66
	case c == 'M' || c == 'm':
		goto yystate1062
	}

yystate1062:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'N' || c >= 'P' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'n' || c >= 'p' && c <= 'z':
		goto yystate66
	case c == 'O' || c == 'o':
		goto yystate1063
	}

yystate1063:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'M' || c >= 'O' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'm' || c >= 'o' && c <= 'z':
		goto yystate66
	case c == 'N' || c == 'n':
		goto yystate1064
	}

yystate1064:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'S' || c >= 'U' && c <= 'Z' || c == '_' || c >= 'a' && c <= 's' || c >= 'u' && c <= 'z':
		goto yystate66
	case c == 'T' || c == 't':
		goto yystate1065
	}

yystate1065:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'G' || c >= 'I' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'g' || c >= 'i' && c <= 'z':
		goto yystate66
	case c == 'H' || c == 'h':
		goto yystate1066
	}

yystate1066:
	c = l.next()
	switch {
	default:
		goto yyrule228
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'z':
		goto yystate66
	}

yystate1067:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'D' || c >= 'F' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'd' || c >= 'f' && c <= 'z':
		goto yystate66
	case c == 'E' || c == 'e':
		goto yystate1068
	}

yystate1068:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'Q' || c >= 'S' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'q' || c >= 's' && c <= 'z':
		goto yystate66
	case c == 'R' || c == 'r':
		goto yystate1069
	}

yystate1069:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'N' || c >= 'P' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'n' || c >= 'p' && c <= 'z':
		goto yystate66
	case c == 'O' || c == 'o':
		goto yystate1070
	}

yystate1070:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'E' || c >= 'G' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'e' || c >= 'g' && c <= 'z':
		goto yystate66
	case c == 'F' || c == 'f':
		goto yystate1071
	}

yystate1071:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'H' || c >= 'J' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'h' || c >= 'j' && c <= 'z':
		goto yystate66
	case c == 'I' || c == 'i':
		goto yystate1072
	}

yystate1072:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'K' || c >= 'M' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'k' || c >= 'm' && c <= 'z':
		goto yystate66
	case c == 'L' || c == 'l':
		goto yystate1073
	}

yystate1073:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'K' || c >= 'M' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'k' || c >= 'm' && c <= 'z':
		goto yystate66
	case c == 'L' || c == 'l':
		goto yystate1074
	}

yystate1074:
	c = l.next()
	switch {
	default:
		goto yyrule231
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'z':
		goto yystate66
	}

yystate1075:
	c = l.next()
	switch {
	default:
		goto yyrule274
	case c == '$' || c >= '0' && c <= '9' || c >= 'A' && c <= 'Z' || c == '_' || c >= 'a' && c <= 'z':
		goto yystate66
	}

yystate1076:
	c = l.next()
	goto yyrule15

yystate1077:
	c = l.next()
	switch {
	default:
		goto yyrule275
	case c == '|':
		goto yystate1078
	}

yystate1078:
	c = l.next()
	goto yyrule35

	goto yystate1079 // silence unused label error
yystate1079:
	c = l.next()
yystart1079:
	switch {
	default:
		goto yyrule16
	case c == '"':
		goto yystate1081
	case c == '\\':
		goto yystate1083
	case c == '\x00':
		goto yystate2
	case c >= '\x01' && c <= '!' || c >= '#' && c <= '[' || c >= ']' && c <= 'ÿ':
		goto yystate1080
	}

yystate1080:
	c = l.next()
	switch {
	default:
		goto yyrule16
	case c >= '\x01' && c <= '!' || c >= '#' && c <= '[' || c >= ']' && c <= 'ÿ':
		goto yystate1080
	}

yystate1081:
	c = l.next()
	switch {
	default:
		goto yyrule19
	case c == '"':
		goto yystate1082
	}

yystate1082:
	c = l.next()
	goto yyrule18

yystate1083:
	c = l.next()
	switch {
	default:
		goto yyabort
	case c >= '\x01' && c <= '\t' || c >= '\v' && c <= 'ÿ':
		goto yystate1084
	}

yystate1084:
	c = l.next()
	goto yyrule17

	goto yystate1085 // silence unused label error
yystate1085:
	c = l.next()
yystart1085:
	switch {
	default:
		goto yyrule20
	case c == '\'':
		goto yystate1087
	case c == '\\':
		goto yystate1089
	case c == '\x00':
		goto yystate2
	case c >= '\x01' && c <= '&' || c >= '(' && c <= '[' || c >= ']' && c <= 'ÿ':
		goto yystate1086
	}

yystate1086:
	c = l.next()
	switch {
	default:
		goto yyrule20
	case c >= '\x01' && c <= '&' || c >= '(' && c <= '[' || c >= ']' && c <= 'ÿ':
		goto yystate1086
	}

yystate1087:
	c = l.next()
	switch {
	default:
		goto yyrule23
	case c == '\'':
		goto yystate1088
	}

yystate1088:
	c = l.next()
	goto yyrule22

yystate1089:
	c = l.next()
	switch {
	default:
		goto yyabort
	case c >= '\x01' && c <= '\t' || c >= '\v' && c <= 'ÿ':
		goto yystate1090
	}

yystate1090:
	c = l.next()
	goto yyrule21

	goto yystate1091 // silence unused label error
yystate1091:
	c = l.next()
yystart1091:
	switch {
	default:
		goto yystate1092 // c >= '\x01' && c <= '\b' || c >= '\n' && c <= '\x1f' || c >= '!' && c <= 'ÿ'
	case c == '\t' || c == ' ':
		goto yystate1093
	case c == '\x00':
		goto yystate2
	}

yystate1092:
	c = l.next()
	goto yyrule8

yystate1093:
	c = l.next()
	switch {
	default:
		goto yyrule7
	case c >= '\x01' && c <= '\t' || c >= '\v' && c <= 'ÿ':
		goto yystate1093
	}

	goto yystate1094 // silence unused label error
yystate1094:
	c = l.next()
yystart1094:
	switch {
	default:
		goto yyrule24
	case c == '\x00':
		goto yystate2
	case c == '`':
		goto yystate1096
	case c >= '\x01' && c <= '_' || c >= 'a' && c <= 'ÿ':
		goto yystate1095
	}

yystate1095:
	c = l.next()
	switch {
	default:
		goto yyrule24
	case c >= '\x01' && c <= '_' || c >= 'a' && c <= 'ÿ':
		goto yystate1095
	}

yystate1096:
	c = l.next()
	switch {
	default:
		goto yyrule26
	case c == '`':
		goto yystate1097
	}

yystate1097:
	c = l.next()
	goto yyrule25

yyrule1: // \0
	{
		return 0
	}
yyrule2: // [ \t\n\r]+

	goto yystate0
yyrule3: // #.*

	goto yystate0
yyrule4: // \/\/.*

	goto yystate0
yyrule5: // \/\*([^*]|\*+[^*/])*\*+\/

	goto yystate0
yyrule6: // --
	{
		l.sc = S3
		goto yystate0
	}
yyrule7: // [ \t]+.*
	{
		{
			l.sc = 0
		}
		goto yystate0
	}
yyrule8: // [^ \t]
	{
		{
			l.sc = 0
			l.c = '-'
			n := len(l.val)
			l.unget(l.val[n-1])
			return '-'
		}
		goto yystate0
	}
yyrule9: // {int_lit}
	{
		return l.int(lval)
	}
yyrule10: // {float_lit}
	{
		return l.float(lval)
	}
yyrule11: // {hex_lit}
	{
		return l.hex(lval)
	}
yyrule12: // {bit_lit}
	{
		return l.bit(lval)
	}
yyrule13: // \"
	{
		l.sc = S1
		goto yystate0
	}
yyrule14: // '
	{
		l.sc = S2
		goto yystate0
	}
yyrule15: // `
	{
		l.sc = S4
		goto yystate0
	}
yyrule16: // [^\"\\]*
	{
		l.stringLit = append(l.stringLit, l.val...)
		goto yystate0
	}
yyrule17: // \\.
	{
		l.stringLit = append(l.stringLit, l.val...)
		goto yystate0
	}
yyrule18: // \"\"
	{
		l.stringLit = append(l.stringLit, '"')
		goto yystate0
	}
yyrule19: // \"
	{
		l.stringLit = append(l.stringLit, '"')
		l.sc = 0
		return l.str(lval, "\"")
	}
yyrule20: // [^'\\]*
	{
		l.stringLit = append(l.stringLit, l.val...)
		goto yystate0
	}
yyrule21: // \\.
	{
		l.stringLit = append(l.stringLit, l.val...)
		goto yystate0
	}
yyrule22: // ''
	{
		l.stringLit = append(l.stringLit, '\'')
		goto yystate0
	}
yyrule23: // '
	{
		l.stringLit = append(l.stringLit, '\'')
		l.sc = 0
		return l.str(lval, "'")
	}
yyrule24: // [^`]*
	{
		l.stringLit = append(l.stringLit, l.val...)
		goto yystate0
	}
yyrule25: // ``
	{
		l.stringLit = append(l.stringLit, '`')
		goto yystate0
	}
yyrule26: // `
	{
		l.sc = 0
		lval.item = string(l.stringLit)
		l.stringLit = l.stringLit[0:0]
		return identifier
	}
yyrule27: // "&&"
	{
		return andand
	}
yyrule28: // "&^"
	{
		return andnot
	}
yyrule29: // "<<"
	{
		return lsh
	}
yyrule30: // "<="
	{
		return le
	}
yyrule31: // "="
	{
		return eq
	}
yyrule32: // ">="
	{
		return ge
	}
yyrule33: // "!="
	{
		return neq
	}
yyrule34: // "<>"
	{
		return neq
	}
yyrule35: // "||"
	{
		return oror
	}
yyrule36: // ">>"
	{
		return rsh
	}
yyrule37: // "<=>"
	{
		return nulleq
	}
yyrule38: // "@"
	{
		return at
	}
yyrule39: // "?"
	{
		return placeholder
	}
yyrule40: // {abs}
	{
		lval.item = string(l.val)
		return abs
	}
yyrule41: // {add}
	{
		return add
	}
yyrule42: // {after}
	{
		lval.item = string(l.val)
		return after
	}
yyrule43: // {all}
	{
		return all
	}
yyrule44: // {alter}
	{
		return alter
	}
yyrule45: // {and}
	{
		return and
	}
yyrule46: // {any}
	{
		lval.item = string(l.val)
		return any
	}
yyrule47: // {asc}
	{
		return asc
	}
yyrule48: // {as}
	{
		return as
	}
yyrule49: // {auto_increment}
	{
		lval.item = string(l.val)
		return autoIncrement
	}
yyrule50: // {avg}
	{
		lval.item = string(l.val)
		return avg
	}
yyrule51: // {avg_row_length}
	{
		lval.item = string(l.val)
		return avgRowLength
	}
yyrule52: // {begin}
	{
		lval.item = string(l.val)
		return begin
	}
yyrule53: // {between}
	{
		return between
	}
yyrule54: // {both}
	{
		return both
	}
yyrule55: // {by}
	{
		return by
	}
yyrule56: // {case}
	{
		return caseKwd
	}
yyrule57: // {cast}
	{
		return cast
	}
yyrule58: // {character}
	{
		return character
	}
yyrule59: // {charset}
	{
		lval.item = string(l.val)
		return charsetKwd
	}
yyrule60: // {check}
	{
		return check
	}
yyrule61: // {checksum}
	{
		lval.item = string(l.val)
		return checksum
	}
yyrule62: // {coalesce}
	{
		lval.item = string(l.val)
		return coalesce
	}
yyrule63: // {collate}
	{
		return collate
	}
yyrule64: // {collation}
	{
		lval.item = string(l.val)
		return collation
	}
yyrule65: // {column}
	{
		return column
	}
yyrule66: // {columns}
	{
		lval.item = string(l.val)
		return columns
	}
yyrule67: // {comment}
	{
		lval.item = string(l.val)
		return comment
	}
yyrule68: // {commit}
	{
		lval.item = string(l.val)
		return commit
	}
yyrule69: // {compression}
	{
		lval.item = string(l.val)
		return compression
	}
yyrule70: // {concat}
	{
		lval.item = string(l.val)
		return concat
	}
yyrule71: // {concat_ws}
	{
		lval.item = string(l.val)
		return concatWs
	}
yyrule72: // {connection}
	{
		lval.item = string(l.val)
		return connection
	}
yyrule73: // {constraint}
	{
		return constraint
	}
yyrule74: // {convert}
	{
		return convert
	}
yyrule75: // {count}
	{
		lval.item = string(l.val)
		return count
	}
yyrule76: // {create}
	{
		return create
	}
yyrule77: // {cross}
	{
		return cross
	}
yyrule78: // {curdate}
	{
		lval.item = string(l.val)
		return curDate
	}
yyrule79: // {current_date}
	{
		lval.item = string(l.val)
		return currentDate
	}
yyrule80: // {current_user}
	{
		lval.item = string(l.val)
		return currentUser
	}
yyrule81: // {database}
	{
		lval.item = string(l.val)
		return database
	}
yyrule82: // {databases}
	{
		return databases
	}
yyrule83: // {day}
	{
		lval.item = string(l.val)
		return day
	}
yyrule84: // {dayofweek}
	{
		lval.item = string(l.val)
		return dayofweek
	}
yyrule85: // {dayofmonth}
	{
		lval.item = string(l.val)
		return dayofmonth
	}
yyrule86: // {dayofyear}
	{
		lval.item = string(l.val)
		return dayofyear
	}
yyrule87: // {day_hour}
	{
		lval.item = string(l.val)
		return dayHour
	}
yyrule88: // {day_microsecond}
	{
		lval.item = string(l.val)
		return dayMicrosecond
	}
yyrule89: // {day_minute}
	{
		lval.item = string(l.val)
		return dayMinute
	}
yyrule90: // {day_second}
	{
		lval.item = string(l.val)
		return daySecond
	}
yyrule91: // {deallocate}
	{
		lval.item = string(l.val)
		return deallocate
	}
yyrule92: // {default}
	{
		return defaultKwd
	}
yyrule93: // {delayed}
	{
		return delayed
	}
yyrule94: // {delete}
	{
		return deleteKwd
	}
yyrule95: // {desc}
	{
		return desc
	}
yyrule96: // {describe}
	{
		return describe
	}
yyrule97: // {drop}
	{
		return drop
	}
yyrule98: // {distinct}
	{
		return distinct
	}
yyrule99: // {div}
	{
		return div
	}
yyrule100: // {do}
	{
		lval.item = string(l.val)
		return do
	}
yyrule101: // {dual}
	{
		return dual
	}
yyrule102: // {duplicate}
	{
		lval.item = string(l.val)
		return duplicate
	}
yyrule103: // {else}
	{
		return elseKwd
	}
yyrule104: // {end}
	{
		lval.item = string(l.val)
		return end
	}
yyrule105: // {engine}
	{
		lval.item = string(l.val)
		return engine
	}
yyrule106: // {engines}
	{
		lval.item = string(l.val)
		return engines
	}
yyrule107: // {execute}
	{
		lval.item = string(l.val)
		return execute
	}
yyrule108: // {enum}
	{
		return enum
	}
yyrule109: // {escape}
	{
		lval.item = string(l.val)
		return escape
	}
yyrule110: // {exists}
	{
		return exists
	}
yyrule111: // {explain}
	{
		return explain
	}
yyrule112: // {extract}
	{
		lval.item = string(l.val)
		return extract
	}
yyrule113: // {first}
	{
		lval.item = string(l.val)
		return first
	}
yyrule114: // {for}
	{
		return forKwd
	}
yyrule115: // {foreign}
	{
		return foreign
	}
yyrule116: // {found_rows}
	{
		lval.item = string(l.val)
		return foundRows
	}
yyrule117: // {from}
	{
		return from
	}
yyrule118: // {full}
	{
		lval.item = string(l.val)
		return full
	}
yyrule119: // {fulltext}
	{
		return fulltext
	}
yyrule120: // {group}
	{
		return group
	}
yyrule121: // {group_concat}
	{
		lval.item = string(l.val)
		return groupConcat
	}
yyrule122: // {having}
	{
		return having
	}
yyrule123: // {high_priority}
	{
		return highPriority
	}
yyrule124: // {hour}
	{
		lval.item = string(l.val)
		return hour
	}
yyrule125: // {hour_microsecond}
	{
		lval.item = string(l.val)
		return hourMicrosecond
	}
yyrule126: // {hour_minute}
	{
		lval.item = string(l.val)
		return hourMinute
	}
yyrule127: // {hour_second}
	{
		lval.item = string(l.val)
		return hourSecond
	}
yyrule128: // {identified}
	{
		lval.item = string(l.val)
		return identified
	}
yyrule129: // {if}
	{
		lval.item = string(l.val)
		return ifKwd
	}
yyrule130: // {ifnull}
	{
		lval.item = string(l.val)
		return ifNull
	}
yyrule131: // {ignore}
	{
		return ignore
	}
yyrule132: // {index}
	{
		return index
	}
yyrule133: // {inner}
	{
		return inner
	}
yyrule134: // {insert}
	{
		return insert
	}
yyrule135: // {into}
	{
		return into
	}
yyrule136: // {in}
	{
		return in
	}
yyrule137: // {is}
	{
		return is
	}
yyrule138: // {join}
	{
		return join
	}
yyrule139: // {key}
	{
		return key
	}
yyrule140: // {key_block_size}
	{
		lval.item = string(l.val)
		return keyBlockSize
	}
yyrule141: // {leading}
	{
		return leading
	}
yyrule142: // {left}
	{
		lval.item = string(l.val)
		return left
	}
yyrule143: // {length}
	{
		lval.item = string(l.val)
		return length
	}
yyrule144: // {like}
	{
		return like
	}
yyrule145: // {limit}
	{
		return limit
	}
yyrule146: // {local}
	{
		lval.item = string(l.val)
		return local
	}
yyrule147: // {locate}
	{
		lval.item = string(l.val)
		return locate
	}
yyrule148: // {lock}
	{
		return lock
	}
yyrule149: // {lower}
	{
		lval.item = string(l.val)
		return lower
	}
yyrule150: // {low_priority}
	{
		return lowPriority
	}
yyrule151: // {max}
	{
		lval.item = string(l.val)
		return max
	}
yyrule152: // {max_rows}
	{
		lval.item = string(l.val)
		return maxRows
	}
yyrule153: // {microsecond}
	{
		lval.item = string(l.val)
		return microsecond
	}
yyrule154: // {min}
	{
		lval.item = string(l.val)
		return min
	}
yyrule155: // {minute}
	{
		lval.item = string(l.val)
		return minute
	}
yyrule156: // {minute_microsecond}
	{
		lval.item = string(l.val)
		return minuteMicrosecond
	}
yyrule157: // {minute_second}
	{
		lval.item = string(l.val)
		return minuteSecond
	}
yyrule158: // {min_rows}
	{
		lval.item = string(l.val)
		return minRows
	}
yyrule159: // {mod}
	{
		return mod
	}
yyrule160: // {mode}
	{
		lval.item = string(l.val)
		return mode
	}
yyrule161: // {month}
	{
		lval.item = string(l.val)
		return month
	}
yyrule162: // {names}
	{
		lval.item = string(l.val)
		return names
	}
yyrule163: // {national}
	{
		lval.item = string(l.val)
		return national
	}
yyrule164: // {not}
	{
		return not
	}
yyrule165: // {offset}
	{
		lval.item = string(l.val)
		return offset
	}
yyrule166: // {on}
	{
		return on
	}
yyrule167: // {order}
	{
		return order
	}
yyrule168: // {or}
	{
		return or
	}
yyrule169: // {outer}
	{
		return outer
	}
yyrule170: // {password}
	{
		lval.item = string(l.val)
		return password
	}
yyrule171: // {prepare}
	{
		lval.item = string(l.val)
		return prepare
	}
yyrule172: // {primary}
	{
		return primary
	}
yyrule173: // {quarter}
	{
		lval.item = string(l.val)
		return quarter
	}
yyrule174: // {quick}
	{
		lval.item = string(l.val)
		return quick
	}
yyrule175: // {right}
	{
		return right
	}
yyrule176: // {rollback}
	{
		lval.item = string(l.val)
		return rollback
	}
yyrule177: // {row}
	{
		lval.item = string(l.val)
		return row
	}
yyrule178: // {schema}
	{
		lval.item = string(l.val)
		return schema
	}
yyrule179: // {schemas}
	{
		return schemas
	}
yyrule180: // {session}
	{
		lval.item = string(l.val)
		return session
	}
yyrule181: // {some}
	{
		lval.item = string(l.val)
		return some
	}
yyrule182: // {start}
	{
		lval.item = string(l.val)
		return start
	}
yyrule183: // {global}
	{
		lval.item = string(l.val)
		return global
	}
yyrule184: // {rand}
	{
		lval.item = string(l.val)
		return rand
	}
yyrule185: // {repeat}
	{
		lval.item = string(l.val)
		return repeat
	}
yyrule186: // {regexp}
	{
		return regexp
	}
yyrule187: // {references}
	{
		return references
	}
yyrule188: // {rlike}
	{
		return rlike
	}
yyrule189: // {sys_var}
	{
		lval.item = string(l.val)
		return sysVar
	}
yyrule190: // {user_var}
	{
		lval.item = string(l.val)
		return userVar
	}
yyrule191: // {second}
	{
		lval.item = string(l.val)
		return second
	}
yyrule192: // {second_microsecond}
	{
		lval.item = string(l.val)
		return secondMicrosecond
	}
yyrule193: // {select}
	{
		return selectKwd
	}
yyrule194: // {set}
	{
		return set
	}
yyrule195: // {share}
	{
		return share
	}
yyrule196: // {show}
	{
		return show
	}
yyrule197: // {substring}
	{
		lval.item = string(l.val)
		return substring
	}
yyrule198: // {substring_index}
	{
		lval.item = string(l.val)
		return substringIndex
	}
yyrule199: // {sum}
	{
		lval.item = string(l.val)
		return sum
	}
yyrule200: // {sysdate}
	{
		lval.item = string(l.val)
		return sysDate
	}
yyrule201: // {table}
	{
		return tableKwd
	}
yyrule202: // {tables}
	{
		lval.item = string(l.val)
		return tables
	}
yyrule203: // {then}
	{
		return then
	}
yyrule204: // {trailing}
	{
		return trailing
	}
yyrule205: // {transaction}
	{
		lval.item = string(l.val)
		return transaction
	}
yyrule206: // {trim}
	{
		lval.item = string(l.val)
		return trim
	}
yyrule207: // {truncate}
	{
		lval.item = string(l.val)
		return truncate
	}
yyrule208: // {union}
	{
		return union
	}
yyrule209: // {unique}
	{
		return unique
	}
yyrule210: // {unknown}
	{
		lval.item = string(l.val)
		return unknown
	}
yyrule211: // {nullif}
	{
		lval.item = string(l.val)
		return nullIf
	}
yyrule212: // {update}
	{
		return update
	}
yyrule213: // {upper}
	{
		lval.item = string(l.val)
		return upper
	}
yyrule214: // {use}
	{
		return use
	}
yyrule215: // {user}
	{
		lval.item = string(l.val)
		return user
	}
yyrule216: // {using}
	{
		return using
	}
yyrule217: // {value}
	{
		lval.item = string(l.val)
		return value
	}
yyrule218: // {values}
	{
		return values
	}
yyrule219: // {variables}
	{
		lval.item = string(l.val)
		return variables
	}
yyrule220: // {warnings}
	{
		lval.item = string(l.val)
		return warnings
	}
yyrule221: // {week}
	{
		lval.item = string(l.val)
		return week
	}
yyrule222: // {weekday}
	{
		lval.item = string(l.val)
		return weekday
	}
yyrule223: // {weekofyear}
	{
		lval.item = string(l.val)
		return weekofyear
	}
yyrule224: // {when}
	{
		return when
	}
yyrule225: // {where}
	{
		return where
	}
yyrule226: // {xor}
	{
		return xor
	}
yyrule227: // {yearweek}
	{
		lval.item = string(l.val)
		return yearweek
	}
yyrule228: // {year_month}
	{
		lval.item = string(l.val)
		return yearMonth

	}
yyrule229: // {signed}
	{
		lval.item = string(l.val)
		return signed
	}
yyrule230: // {unsigned}
	{
		return unsigned
	}
yyrule231: // {zerofill}
	{
		return zerofill
	}
yyrule232: // {null}
	{
		lval.item = nil
		return null
	}
yyrule233: // {false}
	{
		return falseKwd
	}
yyrule234: // {true}
	{
		return trueKwd
	}
yyrule235: // {calc_found_rows}
	{
		lval.item = string(l.val)
		return calcFoundRows
	}
yyrule236: // {current_ts}
	{
		lval.item = string(l.val)
		return currentTs
	}
yyrule237: // {localtime}
	{
		return localTime
	}
yyrule238: // {localts}
	{
		return localTs
	}
yyrule239: // {now}
	{
		lval.item = string(l.val)
		return now
	}
yyrule240: // {bit}
	{
		lval.item = string(l.val)
		return bitType
	}
yyrule241: // {tiny}
	{
		lval.item = string(l.val)
		return tinyIntType
	}
yyrule242: // {tinyint}
	{
		lval.item = string(l.val)
		return tinyIntType
	}
yyrule243: // {smallint}
	{
		lval.item = string(l.val)
		return smallIntType
	}
yyrule244: // {mediumint}
	{
		lval.item = string(l.val)
		return mediumIntType
	}
yyrule245: // {bigint}
	{
		lval.item = string(l.val)
		return bigIntType
	}
yyrule246: // {decimal}
	{
		lval.item = string(l.val)
		return decimalType
	}
yyrule247: // {numeric}
	{
		lval.item = string(l.val)
		return numericType
	}
yyrule248: // {float}
	{
		lval.item = string(l.val)
		return floatType
	}
yyrule249: // {double}
	{
		lval.item = string(l.val)
		return doubleType
	}
yyrule250: // {precision}
	{
		lval.item = string(l.val)
		return precisionType
	}
yyrule251: // {real}
	{
		lval.item = string(l.val)
		return realType
	}
yyrule252: // {date}
	{
		lval.item = string(l.val)
		return dateType
	}
yyrule253: // {time}
	{
		lval.item = string(l.val)
		return timeType
	}
yyrule254: // {timestamp}
	{
		lval.item = string(l.val)
		return timestampType
	}
yyrule255: // {datetime}
	{
		lval.item = string(l.val)
		return datetimeType
	}
yyrule256: // {year}
	{
		lval.item = string(l.val)
		return yearType
	}
yyrule257: // {char}
	{
		lval.item = string(l.val)
		return charType
	}
yyrule258: // {varchar}
	{
		lval.item = string(l.val)
		return varcharType
	}
yyrule259: // {binary}
	{
		lval.item = string(l.val)
		return binaryType
	}
yyrule260: // {varbinary}
	{
		lval.item = string(l.val)
		return varbinaryType
	}
yyrule261: // {tinyblob}
	{
		lval.item = string(l.val)
		return tinyblobType
	}
yyrule262: // {blob}
	{
		lval.item = string(l.val)
		return blobType
	}
yyrule263: // {mediumblob}
	{
		lval.item = string(l.val)
		return mediumblobType
	}
yyrule264: // {longblob}
	{
		lval.item = string(l.val)
		return longblobType
	}
yyrule265: // {tinytext}
	{
		lval.item = string(l.val)
		return tinytextType
	}
yyrule266: // {mediumtext}
	{
		lval.item = string(l.val)
		return mediumtextType
	}
yyrule267: // {text}
	{
		lval.item = string(l.val)
		return textType
	}
yyrule268: // {longtext}
	{
		lval.item = string(l.val)
		return longtextType
	}
yyrule269: // {bool}
	{
		lval.item = string(l.val)
		return boolType
	}
yyrule270: // {boolean}
	{
		lval.item = string(l.val)
		return booleanType
	}
yyrule271: // {byte}
	{
		lval.item = string(l.val)
		return byteType
	}
yyrule272: // {int}
	{
		lval.item = string(l.val)
		return intType
	}
yyrule273: // {integer}
	{
		lval.item = string(l.val)
		return integerType
	}
yyrule274: // {ident}
	{
		lval.item = string(l.val)
		return identifier
	}
yyrule275: // .
	{
		return c0
	}
	panic("unreachable")

	goto yyabort // silence unused label error

yyabort: // no lexem recognized
	return int(unicode.ReplacementChar)
}

func (l *lexer) npos() (line, col int) {
	if line, col = l.nline, l.ncol; col == 0 {
		line--
		col = l.lcol + 1
	}
	return
}

func (l *lexer) str(lval *yySymType, pref string) int {
	l.sc = 0
	// TODO: performance issue.
	s := string(l.stringLit)
	l.stringLit = l.stringLit[0:0]
	if pref == "'" {
		s = strings.Replace(s, "\\'", "'", -1)
		s = strings.TrimSuffix(s, "'") + "\""
		pref = "\""
	}
	v, err := strconv.Unquote(pref + s)
	if err != nil {
		v = strings.TrimSuffix(s, pref)
	}
	lval.item = v
	return stringLit
}

func (l *lexer) trimIdent(idt string) string {
	idt = strings.TrimPrefix(idt, "`")
	idt = strings.TrimSuffix(idt, "`")
	return idt
}

func (l *lexer) int(lval *yySymType) int {
	n, err := strconv.ParseUint(string(l.val), 0, 64)
	if err != nil {
		l.errf("integer literal: %v", err)
		return int(unicode.ReplacementChar)
	}

	switch {
	case n < math.MaxInt64:
		lval.item = int64(n)
	default:
		lval.item = uint64(n)
	}
	return intLit
}

func (l *lexer) float(lval *yySymType) int {
	n, err := strconv.ParseFloat(string(l.val), 64)
	if err != nil {
		l.errf("float literal: %v", err)
		return int(unicode.ReplacementChar)
	}

	lval.item = float64(n)
	return floatLit
}

// https://dev.mysql.com/doc/refman/5.7/en/hexadecimal-literals.html
func (l *lexer) hex(lval *yySymType) int {
	s := string(l.val)
	h, err := mysql.ParseHex(s)
	if err != nil {
		l.errf("hexadecimal literal: %v", err)
		return int(unicode.ReplacementChar)
	}
	lval.item = h
	return hexLit
}

// https://dev.mysql.com/doc/refman/5.7/en/bit-type.html
func (l *lexer) bit(lval *yySymType) int {
	s := string(l.val)
	b, err := mysql.ParseBit(s, -1)
	if err != nil {
		l.errf("bit literal: %v", err)
		return int(unicode.ReplacementChar)
	}
	lval.item = b
	return bitLit
}
