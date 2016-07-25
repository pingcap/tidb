// CAUTION: Generated file, DO NOT EDIT!

// Copyright 2015 The parser Authors. All rights reserved.  Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.
//
// This is a derived work base on the original at
//
// http://pubs.opengroup.org/onlinepubs/009695399/utilities/yacc.html
//
// The original work is
//
// Copyright © 2001-2004 The IEEE and The Open Group, All Rights reserved.
//
// Grammar for the input to yacc.
//
// CAUTION: Generated file (unless this is parser.y) - DO NOT EDIT!

package parser

import (
	"go/token"
)

// Action represents data reduced by production(s):
//
//	Action:
//		'{' '}'
type Action struct {
	Token  *Token
	Token2 *Token
	Pos    token.Pos
	Values []*ActionValue // For backward compatibility.
}

func (a *Action) fragment() interface{} { return a }

// String implements fmt.Stringer.
func (a *Action) String() string {
	return prettyString(a)
}

// Definition represents data reduced by production(s):
//
//	Definition:
//		START IDENTIFIER
//	|	UNION                      // Case 1
//	|	LCURL RCURL                // Case 2
//	|	ReservedWord Tag NameList  // Case 3
//	|	ReservedWord Tag           // Case 4
//	|	ERROR_VERBOSE              // Case 5
type Definition struct {
	Case         int // 0-5
	NameList     *NameList
	ReservedWord *ReservedWord
	Tag          *Tag
	Token        *Token
	Token2       *Token
	Pos          token.Pos
	Value        string
	Nlist        []*Name // For backward compatibility.
}

func (d *Definition) fragment() interface{} { return d }

// String implements fmt.Stringer.
func (d *Definition) String() string {
	return prettyString(d)
}

// DefinitionList represents data reduced by production(s):
//
//	DefinitionList:
//		/* empty */
//	|	DefinitionList Definition  // Case 1
type DefinitionList struct {
	Case           int // 0-1
	Definition     *Definition
	DefinitionList *DefinitionList
}

func (d *DefinitionList) reverse() *DefinitionList {
	if d == nil {
		return nil
	}

	na := d
	nb := na.DefinitionList
	for nb != nil {
		nc := nb.DefinitionList
		nb.DefinitionList = na
		na = nb
		nb = nc
	}
	d.DefinitionList = nil
	return na
}

func (d *DefinitionList) fragment() interface{} { return d.reverse() }

// String implements fmt.Stringer.
func (d *DefinitionList) String() string {
	return prettyString(d)
}

// LiteralStringOpt represents data reduced by production(s):
//
//	LiteralStringOpt:
//		/* empty */
//	|	STRING_LITERAL  // Case 1
type LiteralStringOpt struct {
	Case  int // 0-1
	Token *Token
}

func (l *LiteralStringOpt) fragment() interface{} { return l }

// String implements fmt.Stringer.
func (l *LiteralStringOpt) String() string {
	return prettyString(l)
}

// Name represents data reduced by production(s):
//
//	Name:
//		IDENTIFIER LiteralStringOpt
//	|	IDENTIFIER NUMBER LiteralStringOpt  // Case 1
type Name struct {
	Case             int // 0-1
	LiteralStringOpt *LiteralStringOpt
	Token            *Token
	Token2           *Token
	Identifier       interface{} // For backward compatibility.
	Number           int         // For backward compatibility.
}

func (n *Name) fragment() interface{} { return n }

// String implements fmt.Stringer.
func (n *Name) String() string {
	return prettyString(n)
}

// NameList represents data reduced by production(s):
//
//	NameList:
//		Name
//	|	NameList Name      // Case 1
//	|	NameList ',' Name  // Case 2
type NameList struct {
	Case     int // 0-2
	Name     *Name
	NameList *NameList
	Token    *Token
}

func (n *NameList) reverse() *NameList {
	if n == nil {
		return nil
	}

	na := n
	nb := na.NameList
	for nb != nil {
		nc := nb.NameList
		nb.NameList = na
		na = nb
		nb = nc
	}
	n.NameList = nil
	return na
}

func (n *NameList) fragment() interface{} { return n.reverse() }

// String implements fmt.Stringer.
func (n *NameList) String() string {
	return prettyString(n)
}

// Precedence represents data reduced by production(s):
//
//	Precedence:
//		/* empty */
//	|	PREC IDENTIFIER         // Case 1
//	|	PREC IDENTIFIER Action  // Case 2
//	|	Precedence ';'          // Case 3
type Precedence struct {
	Action     *Action
	Case       int // 0-3
	Precedence *Precedence
	Token      *Token
	Token2     *Token
	Identifier interface{} // Name string or literal int.
}

func (p *Precedence) fragment() interface{} { return p }

// String implements fmt.Stringer.
func (p *Precedence) String() string {
	return prettyString(p)
}

// ReservedWord represents data reduced by production(s):
//
//	ReservedWord:
//		TOKEN
//	|	LEFT        // Case 1
//	|	RIGHT       // Case 2
//	|	NONASSOC    // Case 3
//	|	TYPE        // Case 4
//	|	PRECEDENCE  // Case 5
type ReservedWord struct {
	Case  int // 0-5
	Token *Token
}

func (r *ReservedWord) fragment() interface{} { return r }

// String implements fmt.Stringer.
func (r *ReservedWord) String() string {
	return prettyString(r)
}

// Rule represents data reduced by production(s):
//
//	Rule:
//		C_IDENTIFIER RuleItemList Precedence
//	|	'|' RuleItemList Precedence  // Case 1
type Rule struct {
	Case         int // 0-1
	Precedence   *Precedence
	RuleItemList *RuleItemList
	Token        *Token
	Name         *Token
	Body         []interface{} // For backward compatibility.
}

func (r *Rule) fragment() interface{} { return r }

// String implements fmt.Stringer.
func (r *Rule) String() string {
	return prettyString(r)
}

// RuleItemList represents data reduced by production(s):
//
//	RuleItemList:
//		/* empty */
//	|	RuleItemList IDENTIFIER      // Case 1
//	|	RuleItemList Action          // Case 2
//	|	RuleItemList STRING_LITERAL  // Case 3
type RuleItemList struct {
	Action       *Action
	Case         int // 0-3
	RuleItemList *RuleItemList
	Token        *Token
}

func (r *RuleItemList) reverse() *RuleItemList {
	if r == nil {
		return nil
	}

	na := r
	nb := na.RuleItemList
	for nb != nil {
		nc := nb.RuleItemList
		nb.RuleItemList = na
		na = nb
		nb = nc
	}
	r.RuleItemList = nil
	return na
}

func (r *RuleItemList) fragment() interface{} { return r.reverse() }

// String implements fmt.Stringer.
func (r *RuleItemList) String() string {
	return prettyString(r)
}

// RuleList represents data reduced by production(s):
//
//	RuleList:
//		C_IDENTIFIER RuleItemList Precedence
//	|	RuleList Rule  // Case 1
type RuleList struct {
	Case         int // 0-1
	Precedence   *Precedence
	Rule         *Rule
	RuleItemList *RuleItemList
	RuleList     *RuleList
	Token        *Token
}

func (r *RuleList) reverse() *RuleList {
	if r == nil {
		return nil
	}

	na := r
	nb := na.RuleList
	for nb != nil {
		nc := nb.RuleList
		nb.RuleList = na
		na = nb
		nb = nc
	}
	r.RuleList = nil
	return na
}

func (r *RuleList) fragment() interface{} { return r.reverse() }

// String implements fmt.Stringer.
func (r *RuleList) String() string {
	return prettyString(r)
}

// Specification represents data reduced by production(s):
//
//	Specification:
//		DefinitionList MARK RuleList Tail
type Specification struct {
	DefinitionList *DefinitionList
	RuleList       *RuleList
	Tail           *Tail
	Token          *Token
	Defs           []*Definition // For backward compatibility.
	Rules          []*Rule       // For backward compatibility.
}

func (s *Specification) fragment() interface{} { return s }

// String implements fmt.Stringer.
func (s *Specification) String() string {
	return prettyString(s)
}

// Tag represents data reduced by production(s):
//
//	Tag:
//		/* empty */
//	|	'<' IDENTIFIER '>'  // Case 1
type Tag struct {
	Case   int // 0-1
	Token  *Token
	Token2 *Token
	Token3 *Token
}

func (t *Tag) fragment() interface{} { return t }

// String implements fmt.Stringer.
func (t *Tag) String() string {
	return prettyString(t)
}

// Tail represents data reduced by production(s):
//
//	Tail:
//		MARK
//	|	/* empty */  // Case 1
type Tail struct {
	Case  int // 0-1
	Token *Token
	Value string
}

func (t *Tail) fragment() interface{} { return t }

// String implements fmt.Stringer.
func (t *Tail) String() string {
	return prettyString(t)
}
