// Copyright (c) 2014 The lex Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package lex

import (
	"fmt"
	"github.com/qiuyesuifeng/golex/Godeps/_workspace/src/github.com/cznic/lexer"
)

func (t *tokenizer) Lex(lval *yySymType) (c int) {
	//defer func() { dbg("Lex %q %d", c, c) }()
	rc, ok := t.scanner.Scan()
	c = int(rc)
	if ok {
		lval.str = string(t.scanner.Token())
	} else {
		lval.str = string(c)
	}
	return
}

func (t *tokenizer) Error(e string) {
	println("Error:", fmt.Sprintf("%s:%q\n", t.scanner.Position(), e))
	logErr(fmt.Sprintf("%s:%q\n", t.scanner.Position(), e))
}

type tokenizer struct {
	scanner *lexer.Scanner
	val     string
}

func newTokenizer(sc *lexer.Scanner) *tokenizer {
	return &tokenizer{scanner: sc}
}

func sc(y yyLexer) *lexer.Scanner {
	return y.(*tokenizer).scanner
}

const (
	_DEF lexer.StartSetID = iota
	_DEF_NAME
	_DEF_STARTS
	_VERBATIM
	_RULES
	_STARTS
	_USER
)

var lxr *lexer.Lexer

func init() {
	lxr = lexer.MustCompileLexer(
		[][]int{
			// _DEF
			{tDEF_NAME, tUNINDENTED_COMMENT, tINDENTED_TEXT, tVERBATIM_OPEN, tSECTION_DIV,
				tSSTART, tXSTART, tYYT, tYYB, tYYC, tYYN, tYYM},
			// _DEF_NAME
			{tBLANKS, tDEFINITION},
			// _DEF_STARTS
			{tBLANKS, tNAME},
			// _VERBATIM
			{tVERBATIM_LINE, tVERBATIM_CLOSE},
			// _RULES
			{tINDENTED_TEXT, tVERBATIM_OPEN, tSTARTS, tPATTERN_LINE, tSECTION_DIV, tSTARTS_PATTERN_LINE},
			// _STARTS
			{tNAME},
			// _USER
			{tUSER_CODE_LINE},
		},
		map[string]int{
			`/^[a-zA-Z_][-a-zA-Z0-9_]*/`:       tDEF_NAME,
			`/^%%$|^%%\n/`:                     tSECTION_DIV,
			`/[ \t]+/`:                         tBLANKS,
			`/[^ \t\n].*/`:                     tDEFINITION,
			`/^/\*([^*]|\*+[^*/])*\*+/[ \t]*/`: tUNINDENTED_COMMENT,
			`/^[ \t].*/`:                       tINDENTED_TEXT,
			`/^%{\n/`:                          tVERBATIM_OPEN,
			`/^%}\n/`:                          tVERBATIM_CLOSE,
			`/^.*\n/`:                          tVERBATIM_LINE,
			`/^</`:                             tSTARTS,
			`/[a-zA-Z_][-a-zA-Z0-9_]*/`: tNAME,
			`/^[^< \t\n].*/`:            tPATTERN_LINE,
			`/[^< \t\n].*/`:             tSTARTS_PATTERN_LINE,
			`/\n|.+\n?/`:                tUSER_CODE_LINE,
			`/^%s/`:                     tSSTART,
			`/^%x/`:                     tXSTART,
			`/^%yyt/`:                   tYYT,
			`/^%yyb/`:                   tYYB,
			`/^%yyc/`:                   tYYC,
			`/^%yyn/`:                   tYYN,
			`/^%yym/`:                   tYYM,
		},
		"",
		"",
	)
}
