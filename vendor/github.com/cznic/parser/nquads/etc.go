// Copyright (c) 2014 The parser Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package parser implements a parser for N-Quads[0] source text.
//
// Links
//
// Referenced from elsewhere.
//
//  [0]: http://www.w3.org/TR/n-quads/
//  [1]: http://www.w3.org/TR/n-quads/#grammar-production-statement
//  [2]: http://www.w3.org/TR/n-quads/#grammar-production-IRIREF
//  [3]: http://www.w3.org/TR/n-quads/#grammar-production-BLANK_NODE_LABEL
//  [4]: http://www.w3.org/TR/n-quads/#grammar-production-literal
//  [5]: http://www.w3.org/TR/n-quads/#grammar-production-LANGTAG
//  [6]: http://www.w3.org/TR/n-quads/#grammar-production-subject
//  [7]: http://www.w3.org/TR/n-quads/#grammar-production-predicate
//  [8]: http://www.w3.org/TR/n-quads/#grammar-production-object
//  [9]: http://www.w3.org/TR/n-quads/#grammar-production-graphLabel
package parser

import (
	"fmt"
	"strings"

	"github.com/cznic/scanner/nquads"
)

type lexer struct {
	*scanner.Scanner
	ast  []*Statement
	prev scanner.Token
}

func (l *lexer) error(line, col int, msg string) {
	l.Error(fmt.Sprintf("%s:%d:%d %s", l.Fname, line, col, msg))
}

func (l *lexer) Lex(lval *yySymType) int {
again:
	tok, val := l.Scan()
	if tok == scanner.EOL && l.prev == scanner.EOL {
		goto again
	}
	l.prev = tok

	col := l.Col
	if col < 1 {
		col = 1
	}
	lval.pos, lval.val = Pos{l.Line, col}, val
	//dbg("%s:%d:%d %v %q", l.Fname, l.Line, l.Col, tok, val)
	switch tok {
	case scanner.EOF:
		return 0
	case scanner.DOT:
		return dot
	case scanner.DACCENT:
		return daccent
	case scanner.LABEL:
		return label
	case scanner.EOL:
		return eol
	case scanner.IRIREF:
		return iriref
	case scanner.LANGTAG:
		return langtag
	case scanner.STRING:
		return str
	case scanner.ILLEGAL:
		c := l.NCol
		if c > 1 {
			c--
		}
		l.error(l.NLine, c, "syntax error")
		panic(nil)
	default:
		panic("internal error")
	}
}

// Pos describes a position within the parsed source.
type Pos struct {
	Line int
	Col  int
}

func (p Pos) String() string { return fmt.Sprintf("%d:%d", p.Line, p.Col) }

// Tag is a kind of the Value field of Subject, Predicate, Object or GraphLabel.
type Tag int

// Values of type Tag.
const (
	_              Tag = iota
	IRIRef             // [2]
	BlankNodeLabel     // [3]
	Literal            // [4]
	LangTag            // [5]
)

// String implements fmt.Stringer()
func (t Tag) String() string {
	switch t {
	case 0:
		return ""
	case IRIRef:
		return "IRIRef"
	case BlankNodeLabel:
		return "BlankNodeLabel"
	case Literal:
		return "Literal"
	case LangTag:
		return "LangTag"
	default:
		return fmt.Sprintf("%T(%d)", t, int(t))
	}
}

// Statement describes a parsed statement[1].
type Statement struct {
	Pos
	*Subject
	*Predicate
	*Object
	*GraphLabel // GraphLabel might be nil.
}

// String implements fmt.Stringer()
func (s *Statement) String() string {
	switch {
	case s.GraphLabel == nil:
		return fmt.Sprintf("stmt@%v{%v, %v, %v}", s.Pos, s.Subject, s.Predicate, s.Object)
	default:
		return fmt.Sprintf("stmt@%v{%v, %v, %v, %v}", s.Pos, s.Subject, s.Predicate, s.Object, s.GraphLabel)
	}
}

// Subject describes a parsed subject[6].
type Subject struct {
	Pos
	Tag
	Value string
}

// String implements fmt.Stringer()
func (s *Subject) String() string { return fmt.Sprintf("subj@%v{%v=%q}", s.Pos, s.Tag, s.Value) }

// Predicate describes a parser predicate[7].
type Predicate struct {
	Pos
	Value string
}

// String implements fmt.Stringer()
func (p *Predicate) String() string { return fmt.Sprintf("pred@%v{%q}", p.Pos, p.Value) }

// Object describes a parsed object[8].
type Object struct {
	Pos
	Tag
	Value  string
	Tag2   Tag // Tag2 is nonzero iff Tag == Literal and the literal has the optional IRIREF or LANGTAG value present.
	Value2 string
}

// String implements fmt.Stringer()
func (o *Object) String() string {
	switch {
	case o.Tag2 == 0:
		return fmt.Sprintf("obj@%v{%v=%q}", o.Pos, o.Tag, o.Value)
	default:
		return fmt.Sprintf("obj@%v{%v=%q, %v=%q}", o.Pos, o.Tag, o.Value, o.Tag2, o.Value2)
	}
}

// GraphLabel describes a parsed graphLabel[9].
type GraphLabel struct {
	Pos
	Tag
	Value string
}

// String implements fmt.Stringer()
func (g *GraphLabel) String() string { return fmt.Sprintf("graph@%v{%v=%q}", g.Pos, g.Tag, g.Value) }

// Parse parses src as a single N-Quads source file fname and returns the
// corresponding AST. If the source couldn't be parsed, the returned AST is
// nil and the error indicates all of the specific failures.
func Parse(fname string, src []byte) (ast []*Statement, err error) {
	l := lexer{
		Scanner: scanner.New(fname, src),
	}
	defer func() {
		if e := recover(); e != nil {
			l.Error(fmt.Sprintf("%v", e))
			ast, err = nil, errList(l.Errors)
			return
		}

		if len(l.Errors) != 0 {
			ast, err = nil, errList(l.Errors)
		}
	}()
	if yyParse(&l) != 0 || len(l.Errors) != 0 {
		return nil, errList(l.Errors)
	}

	return l.ast, nil
}

type errList []error

func (e errList) Error() string {
	a := []string{}
	for _, v := range e {
		a = append(a, v.Error())
	}
	return strings.Join(a, "\n")
}
