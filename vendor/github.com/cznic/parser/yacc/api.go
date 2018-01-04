// Copyright 2015 The parser Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

//go:generate go run generate.go
//go:generate golex -o goscanner.go go.l
//go:generate golex -o scanner.go y.l
//go:generate go run generate.go -2

// Package parser implements a parser for yacc source files.
//
// Note: Rule.Body element's type
//
//	int		Eg. 65 represents literal 'A'
//
//	string		Eg. "Start" represents rule component Start
//
//	*Action		Mid rule action or rule semantic action
package parser

import (
	"bytes"
	"fmt"
	"go/token"

	"github.com/cznic/golex/lex"
)

const (
	// ActionValueGo is used for a Go code fragment
	ActionValueGo = iota
	// ActionValueDlrDlr is used for $$.
	ActionValueDlrDlr
	// ActionValueDlrTagDlr is used for $<tag>$.
	ActionValueDlrTagDlr
	// ActionValueDlrNum is used for $num.
	ActionValueDlrNum
	// ActionValueDlrTagNum is used for $<tag>num.
	ActionValueDlrTagNum
)

// ActionValue is an item of Action.Value
type ActionValue struct {
	Num  int       // The number in $num.
	Pos  token.Pos // Position of the start of the ActionValue.
	Src  string    // Source for this value.
	Tag  string    // The tag in $<tag>$ or $<tag>num.
	Type int       // One of ActionValue{Go,DlrDlr,DlrTagDlr,DlrNum,DlrTagNum} constants.
}

// Token captures a lexem with position, value and comments, if any.
type Token struct {
	Comments []string
	Val      string
	File     *token.File
	lex.Char
}

// Pos retruns the token.Pos for t.
func (t *Token) Pos() token.Pos { return t.Char.Pos() }

// Position returns the token.Position for t
func (t *Token) Position() token.Position { return t.File.Position(t.Pos()) }

// Strings implements fmt.Stringer.
func (t *Token) String() string {
	return fmt.Sprintf("%v: %v %q, Comments: %q", t.File.Position(t.Char.Pos()), yySymName(int(t.Char.Rune)), t.Val, t.Comments)
}

// Parse parses src as a single yacc source file fname and returns the
// corresponding Specification. If the source couldn't be read, the returned
// Specification is nil and the error indicates all of the specific failures.
func Parse(fset *token.FileSet, fname string, src []byte) (s *Specification, err error) {
	r := bytes.NewBuffer(src)
	file := fset.AddFile(fname, -1, len(src))
	lx, err := newLexer(file, r)
	if err != nil {
		return nil, err
	}

	y := yyParse(lx)
	n := len(lx.errors)
	if y != 0 || n != 0 {
		if n == 0 {
			panic("internal error")
		}

		return nil, lx.errors
	}

	return lx.spec, nil
}
