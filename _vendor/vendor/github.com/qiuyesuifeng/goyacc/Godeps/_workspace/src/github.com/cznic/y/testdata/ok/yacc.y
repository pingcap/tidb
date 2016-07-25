%{

/*

Copyright © 2001-2004 The IEEE and The Open Group, All Rights reserved.

Original source text: http://pubs.opengroup.org/onlinepubs/009695399/utilities/yacc.html

Modifications: Copyright 2014 The parser Authors. All rights reserved.
Use of this source code is governed by a BSD-style
license that can be found in the LICENSE file.

Grammar for the input to yacc.

*/

// Package parser implements a parser for yacc source files.
package parser

import (
	"bytes"
	"fmt"
	"go/token"
	"strings"

	"github.com/cznic/scanner/yacc"
	"github.com/cznic/strutil"
)

%}

%union {
	act    []*Act
	def    *Def
	defs   []*Def
	item   interface{}
	list   []interface{}
	nlist  []*Nmno
	nmno   *Nmno
	number int
	pos    token.Pos
	prec   *Prec
	rule   *Rule
	rules  []*Rule
	rword  Rword
	s      string
}

%token illegal

/* Basic entries. The following are recognized by the lexical analyzer. */

%token	<item>   tkIdent  /* Includes identifiers and literals */
%token	<s>      tkCIdent /* identifier (but not literal) followed by a :. */
%token	<number> tkNumber /* [0-9][0-9]* */

/* Reserved words : %type=>tkType %left=>tkLeft, and so on */

%token	tkLeft tkRight tkNonAssoc tkToken tkPrec tkType tkStart tkUnion tkErrorVerbose

%token	tkMark            /* The %% mark. */
%token	tkLCurl           /* The %{ mark. */
%token	tkRCurl           /* The %} mark. */

%type	<act>	act
%type	<def>	def
%type	<defs>	defs
%type	<list>	rbody
%type	<nlist>	nlist
%type	<nmno>	nmno
%type	<prec>	prec
%type	<rule>	rule
%type	<rules>	rules
%type	<rword>	rword
%type	<s>	tag tail

/* 8-bit character literals stand for themselves; */
/* tokens have to be defined for multi-byte characters. */

%start	spec

%%

spec:
	defs tkMark rules tail
	{
		lx(yylex).ast = &AST{Defs: $1, Rules: $3, Tail: $4}
	}

tail:
	/* Empty; the second _MARK is optional. */
	{
		$$ = ""
	}
|	tkMark
	{
        	/* In this action, set up the rest of the file. */
		lx := lx(yylex)
		$$ = string(lx.src[lx.Pos()+1:])
		lx.closed = true
	}

defs:
	/* Empty. */
	{
		$$ = []*Def(nil)
	}
|	defs def
	{
		$$ = append($1, $2)
	}

def:
   	tkStart tkIdent
	{
		s, ok := $2.(string)
		if !ok {
			lx := lx(yylex)
			lx.Error(fmt.Sprintf("%v: expected name", $<pos>2))
		}
		$$ = &Def{Pos: $<pos>1, Rword: Start, Tag: s}
	}
|	tkUnion
	{
        	/* Copy union definition to output. */
		lx := lx(yylex)
		lx.Mode(false)
		off0 := lx.Pos()+5
		n := 0
	union_loop:
		for {
			tok, _, _ := lx.Scan()
			switch tok {
			case scanner.LBRACE:
				n++
			case scanner.RBRACE:
				n--
				if n == 0 {
					lx.Mode(true)
					break union_loop
				}
			}
		}
		s := string(lx.src[off0:lx.Pos()])
		$$ = &Def{Pos: $<pos>1, Rword: Union, Tag: s}
	}
|	tkLCurl
	{
		/* Copy Go code to output file. */
		lx := lx(yylex)
		off0, lpos := lx.Pos(), lx.Pos()
		lx.Mode(false)
		var last scanner.Token
	lcurl_loop:
		for {
			tok, _, _ := lx.ScanRaw()
			if tok == scanner.RBRACE && last == scanner.REM && lx.Pos() == lpos+1 {
				lx.Mode(true)
				s := string(lx.src[off0+1:lpos-1])
				//dbg("----\n%q\n----\n", s)
				$$ = &Def{Pos: $<pos>1, Rword: Copy, Tag: s}
				break lcurl_loop
			}

			last, lpos = tok, lx.Pos()
		}
	}
|	tkErrorVerbose
	{
		$$ = &Def{Pos: $<pos>1, Rword: ErrVerbose}
	}
|	rword tag nlist
	{
		if $1 == Type {
			for _, v := range $3 {
				switch v.Identifier.(type) {
				case int:
					yylex.Error("literal invalid with %type.") // % is ok
					goto ret1
				}

				if v.Number > 0 {
					yylex.Error("number invalid with %type.") // % is ok
					goto ret1
				}
			}
		}

		$$ = &Def{Pos: $<pos>1, Rword: $1, Tag: $2, Nlist: $3}
	}

rword:
	tkToken
	{
		$<pos>$ = $<pos>1
		$$ = Token
	}
|	tkLeft
	{
		$<pos>$ = $<pos>1
		$$ = Left
	}
|	tkRight
	{
		$<pos>$ = $<pos>1
		$$ = Right
	}
|	tkNonAssoc
	{
		$<pos>$ = $<pos>1
		$$ = Nonassoc
	}
|	tkType
	{
		$<pos>$ = $<pos>1
		$$ = Type
	}

tag:
	/* Empty: union tag ID optional. */
	{
		$$ = ""
	}
|	'<' tkIdent '>'
	{
		lx := lx(yylex)
		s, ok := $2.(string)
		if ! ok {
			lx.Error(fmt.Sprintf("%v: expected name", $<pos>2))
		}
		$<pos>$ = $<pos>2
		$$ = s
	}

nlist:
	nmno
	{
		$$ = []*Nmno{$1}
	}
|	nlist nmno
	{
		$$ = append($1, $2)
	}

nmno:
	tkIdent
	{
		$$ = &Nmno{$<pos>1, $1, -1}
	}
|	tkIdent tkNumber
	{
		$$ = &Nmno{$<pos>1, $1, $2}
	}

/* Rule section */

rules:
	tkCIdent  rbody prec
	{
		lx(yylex).rname = $1
		$$ = []*Rule{&Rule{Pos: $<pos>1, Name: $1, Body: $2, Prec: $3}}
	}
|	rules rule
	{
		$$ = append($1, $2)
	}

rule:
	tkCIdent  rbody prec
	{
		lx(yylex).rname = $1
		$$ = &Rule{Pos: $<pos>1, Name: $1, Body: $2, Prec: $3}
	}
|	'|' rbody prec
	{
		$$ = &Rule{Pos: $<pos>1, Name: lx(yylex).rname, Body: $2, Prec: $3}
	}

rbody:
	/* empty */
	{
		$$ = []interface{}(nil)
	}
|	rbody tkIdent
	{
		$$ = append($1, $2)
	}
|	rbody act
	{
		$$ = append($1, $2)
	}

act:
	'{'
	{
		/* Copy action, translate $$, and so on. */
		lx := lx(yylex)
		lx.Mode(false)
		a := []*Act{}
		start := lx.Pos()
		n := 0
		pos := token.Pos(-1)
	act_loop:
		for {
			tok, tag, num := lx.Scan()
			if pos < 0 {
				pos = token.Pos(lx.Pos())
			}
			tokStart := lx.Pos()-1
			switch tok {
			case scanner.DLR_DLR, scanner.DLR_NUM, scanner.DLR_TAG_DLR, scanner.DLR_TAG_NUM:
				s, ok := tag.(string)
				if !ok {
					s = ""
				}

				src := ""
				if start > 0 {
					src = string(lx.src[start:tokStart])
				}
				
				a = append(a, &Act{Pos: token.Pos(lx.Pos()), Src: src, Tok: tok, Tag: s, Num: num})
				start = -1
			case scanner.LBRACE:
				n++
			case scanner.RBRACE:
				if n == 0 {
					if start < 0 {
						start = tokStart
						pos = token.Pos(lx.Pos())
					}
					src := lx.src[start:tokStart]
					if len(src) != 0 {
						a = append(a, &Act{Pos: pos, Src: string(src)})
					}
					lx.Mode(true)
					break act_loop
				}

				n--
			case scanner.EOF:
				lx.Error("unexpected EOF")
				goto ret1
			default:
				if start < 0 {
					start = tokStart
					pos = token.Pos(lx.Pos())
				}
			}
		}
		$$ = a
	}

prec:
	/* Empty */
	{
		$$ = nil
	}
|	tkPrec tkIdent
	{
		$$ = &Prec{Pos: $<pos>1, Identifier: $2}
	}
|	tkPrec tkIdent act
	{
		$$ = &Prec{Pos: $<pos>1, Identifier: $2, Act: $3}
	}
|	prec ';'
	{
		$$ = &Prec{}
	}

%%

// AST holds the parsed .y source.
type AST struct {
	Defs  []*Def  // Definitions
	Rules []*Rule // Rules
	Tail  string  // Optional rest of the file
	fset  *token.FileSet
}

// String implements fmt.Stringer.
func (s *AST) String() string {
	return str(s.fset, s)
}

// Def is the definition section definition entity
type Def struct {
	token.Pos
	Rword Rword
	Tag   string
	Nlist []*Nmno
}

// Rule is the rules section rule.
type Rule struct{
	token.Pos
	Name string
	Body []interface{}
	Prec *Prec
}

// Nmno (Name-or-number) is a definition section name list item. It's either a
// production name (type string), or a rune literal. Optional number associated
// with the name is in number, if non-negative.
type Nmno struct {
	token.Pos
	Identifier interface{}
	Number int
}

// Prec defines the optional precedence of a rule.
type Prec struct {
	token.Pos
	Identifier interface{}
	Act []*Act
}

// Act captures the action optionally associated with a rule.
type Act struct{
	token.Pos
	Src string
	Tok scanner.Token       // github.com/cznic/scanner/yacc.DLR_* or zero
	Tag string              // DLR_TAG_*
	Num int                 // DLR_NUM, DLR_TAG_NUM
}

// Rword is a definition tag (Def.Rword).
type Rword int

// Values of Def.Rword
const (
	_ Rword = iota

	Copy       // %{ ... %}
	ErrVerbose // %error-verbose
	Left       // %left
	Nonassoc   // %nonassoc
	Right      // %right
	Start      // %start
	Token      // %token
	Type       // %type
	Union      // %union
)

var rwords = map[Rword]string{
	Copy:       "Copy",
	ErrVerbose: "ErrorVerbose",
	Left:       "Left",
	Nonassoc:   "Nonassoc",
	Right:      "Right",
	Start:      "Start",
	Token:      "Token",
	Type:       "Type",
	Union:      "Union",
}

// String implements fmt.Stringer.
func (r Rword) String() string {
	if s := rwords[r]; s != "" {
		return s
	}

	return fmt.Sprintf("Rword(%d)", r)
}

type lexer struct {
	*scanner.Scanner
	ast    *AST
	closed bool
	fset   *token.FileSet
	rname  string // last rule name for '|' rules
	src    []byte
}

var xlat = map[scanner.Token]int{
	scanner.LCURL:        tkLCurl,
	scanner.LEFT:         tkLeft,
	scanner.MARK:         tkMark,
	scanner.NONASSOC:     tkNonAssoc,
	scanner.PREC:         tkPrec,
	scanner.RCURL:        tkRCurl,
	scanner.RIGHT:        tkRight,
	scanner.START:        tkStart,
	scanner.TOKEN:        tkToken,
	scanner.TYPE:         tkType,
	scanner.UNION:        tkUnion,
	scanner.ERR_VERBOSE:  tkErrorVerbose,

	scanner.EOF:          0,
	scanner.OR:           '|',
}

var todo = strings.ToUpper("todo")

func (l *lexer) Lex(lval *yySymType) (y int) {
	if l.closed {
		return 0
	}

	for {
		tok, val, _ := l.Scan()
		lval.pos = token.Pos(l.Pos())
		switch tok {
		case scanner.COMMENT:
			continue
		case scanner.C_IDENTIFIER:
			if s, ok := val.(string); ok {
				lval.s = s
			}
			return tkCIdent 
		case scanner.IDENTIFIER:
			if s, ok := val.(string); ok {
				lval.item = s
			}
			return tkIdent
		case scanner.INT:
			if n, ok := val.(uint64); ok {
				lval.number = int(n)
			}
			return tkNumber
		case scanner.CHAR:
			if n, ok := val.(int32); ok {
				lval.item = int(n)
			}
			return tkIdent
		case scanner.ILLEGAL:
			if s, ok := val.(string); ok && s != "" {
				return int([]rune(s)[0])
			}
			return illegal
		default:
			return xlat[tok]
		}
	}
}

type errList []error

func (e errList) Error() string {
	a := []string{}
	for _, v := range e {
		a = append(a, v.Error())
	}
	return strings.Join(a, "\n")
}

func lx(yylex yyLexer) *lexer {
	return yylex.(*lexer)
}


// Parse parses src as a single yacc source file fname and returns the
// corresponding AST. If the source couldn't be read, the returned AST is nil
// and the error indicates all of the specific failures.
func Parse(fset *token.FileSet, fname string, src []byte) (s *AST, err error) {
	l := lexer{
		fset:    fset,
		Scanner: scanner.New(fset, fname, src),
		src:     src,
	}
	defer func() {
		if e := recover(); e != nil {
			l.Error(fmt.Sprintf("%v", e))
			err = errList(l.Errors)
			return
		}
	}()
	if yyParse(&l) != 0 {
		return nil, errList(l.Errors)
	}

	l.ast.fset = fset
	return l.ast, nil
}

func str(fset *token.FileSet, v interface{}) string {
	var buf bytes.Buffer
	f := strutil.IndentFormatter(&buf, "· ")
	g := func(interface{}){}
	g = func(v interface{}){
		switch x := v.(type) {
		case nil:
			f.Format("<nil>")
		case int:
			f.Format("'%c'\n", x)
		case string:
			f.Format("%q\n", x)
		case []*Act:
			f.Format("%T{%i\n", x)
			for _, v := range x {
				g(v)
			}
			f.Format("%u}\n")
		case *Act:
			f.Format("%T@%v{%i\n", x, fset.Position(x.Pos))
			f.Format("Src: %q\n", x.Src)
			if x.Tok != 0 {
				f.Format("Tok: %s, Tag: %q, Num: %d\n", x.Tok, x.Tag, x.Num)
			}
			f.Format("%u}\n")
		case *Def:
			f.Format("%T@%v{%i\n", x, fset.Position(x.Pos))
			f.Format("Rword: %s, ", x.Rword)
			f.Format("Tag: %q, ", x.Tag)
			f.Format("Nlist: %T{%i\n", x.Nlist)
			for _, v := range x.Nlist {
				g(v)
			}
			f.Format("%u}\n")
			f.Format("%u}\n")
		case *Nmno:
			var s string
			switch v := x.Identifier.(type) {
			case string:
				s = fmt.Sprintf("%q", v)
			case int:
				s = fmt.Sprintf("'%c'", v)
			}
			f.Format("%T@%v{Identifier: %s, Number: %d}\n", x, fset.Position(x.Pos), s, x.Number)
		case *Prec:
			var s string
			switch v := x.Identifier.(type) {
			case string:
				s = fmt.Sprintf("%q", v)
			case int:
				s = fmt.Sprintf("'%c'", v)
			}
			f.Format("%T@%v{%i\n", x, fset.Position(x.Pos))
			f.Format("Identifier: %s\n", s)
			g(x.Act)
			f.Format("%u}\n")
		case *Rule:
			f.Format("%T@%v{%i\n", x, fset.Position(x.Pos))
			f.Format("Name: %q, ", x.Name)
			f.Format("Body: %T{%i\n", x.Body)
			for _, v := range x.Body {
				g(v)
			}
			f.Format("%u}\n")
			if x.Prec != nil {
				f.Format("Prec: ")
				g(x.Prec)
			}
			f.Format("%u}\n")
		case *AST:
			f.Format("%T{%i\n", x)
			f.Format("Defs: %T{%i\n", x.Defs)
			for _, v := range x.Defs {
				g(v)
			}
			f.Format("%u}\n")
			f.Format("Rules: %T{%i\n", x.Rules)
			for _, v := range x.Rules {
				g(v)
			}
			f.Format("%u}\n")
			f.Format("Tail: %q\n", x.Tail)
			f.Format("%u}\n")
		default:
			f.Format("%s(str): %T(%#v)\n", todo, x, x)
		}
	}
	g(v)
	return buf.String()
}
