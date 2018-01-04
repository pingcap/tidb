%{
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

package parser

import (
	"go/token"
)
%}

%union {
	node	Node
	Token	*Token
}

%token	<Token>
	/*yy:token "%c:"    */	C_IDENTIFIER	"rule name"
	/*yy:token "%c"     */	IDENTIFIER	"identifier"
	/*yy:token "\"%c\"" */	STRING_LITERAL	"string literal"
	/*yy:token "%d"     */	NUMBER		"number"

	COMMENT
	ERROR_VERBOSE	"%error-verbose"
	LCURL		"%{"
	LEFT		"%left"
	MARK		"%%"
	NONASSOC	"%nonassoc"
	PREC		"%prec"
	PRECEDENCE	"%precedence"
	RCURL		"%}"
	RIGHT		"%right"
	START		"%start"
	TOKEN		"%token"
	TYPE		"%type"
	UNION		"%union"

%start Specification

%%

//yy:field	Values	[]*ActionValue	// For backward compatibility.
Action:
	'{'
	{
		lx.values2 = append([]string(nil), lx.values...)
		lx.positions2 = append([]token.Pos(nil), lx.positions...)
	}
	'}'
	{
		for i, v := range lx.values2 {
			a := lx.parseActionValue(lx.positions2[i], v)
			if a != nil {
				lhs.Values = append(lhs.Values, a)
			}
		}
	}

//yy:field	Nlist	[]*Name // For backward compatibility.
//yy:field	Value	string
Definition:
	START IDENTIFIER
//yy:example	"%union{int i} %%"
|	UNION
	{
		lhs.Value = lx.value
	}
|	LCURL
	{
		lx.pos2 = lx.pos
		lx.value2 = lx.value
	}
	RCURL
	{
		lhs.Value = lx.value2
	}
|	ReservedWord Tag NameList
	{
		for n := lhs.NameList; n != nil; n = n.NameList {
			lhs.Nlist = append(lhs.Nlist, n.Name)
		}
	}
|	ReservedWord Tag
|	ERROR_VERBOSE

DefinitionList:
|	DefinitionList Definition
	{
		lx.defs = append(lx.defs, lhs.Definition)
	}

LiteralStringOpt:
|	STRING_LITERAL

//yy:field	Identifier	interface{}	// For backward compatibility.
//yy:field	Number		int		// For backward compatibility.
Name:
	IDENTIFIER LiteralStringOpt
	{
		lhs.Identifier = lx.ident(lhs.Token)
		lhs.Number = -1
	}
|	IDENTIFIER NUMBER LiteralStringOpt
	{
		lhs.Identifier = lx.ident(lhs.Token)
		lhs.Number = lx.number(lhs.Token2)
	}

NameList:
	Name
|	NameList Name
|	NameList ',' Name

//yy:field	Identifier	interface{}	// Name string or literal int.
Precedence:
	/* empty */ {}
|	PREC IDENTIFIER
	{
		lhs.Identifier = lx.ident(lhs.Token2)
	}
|	PREC IDENTIFIER Action
	{
		lhs.Identifier = lx.ident(lhs.Token2)
	}
|	Precedence ';'

ReservedWord:
	TOKEN
|	LEFT
|	RIGHT
|	NONASSOC
|	TYPE
|	PRECEDENCE

//yy:field	Body	[]interface{}	// For backward compatibility.
//yy:field	Name	*Token
Rule:
	C_IDENTIFIER RuleItemList Precedence
	{
		lx.ruleName = lhs.Token
		lhs.Name = lhs.Token
	}
|	'|' RuleItemList Precedence
	{
		lhs.Name = lx.ruleName
	}

RuleItemList:
|	RuleItemList IDENTIFIER
|	RuleItemList Action
|	RuleItemList STRING_LITERAL

RuleList:
	C_IDENTIFIER RuleItemList Precedence
	{
		lx.ruleName = lhs.Token
		rule := &Rule{
			Token: lhs.Token,
			Name: lhs.Token,
			RuleItemList: lhs.RuleItemList,
			Precedence: lhs.Precedence,
		}
		rule.collect()
		lx.rules = append(lx.rules, rule)
	}
|	RuleList Rule
	{
		rule := lhs.Rule
		rule.collect()
		lx.rules = append(lx.rules, rule)
	}

//yy:field	Defs	[]*Definition	// For backward compatibility.
//yy:field	Rules	[]*Rule		// For backward compatibility.
Specification:
	DefinitionList "%%" RuleList Tail
	{
		lhs.Defs = lx.defs
		lhs.Rules = lx.rules
		lx.spec = lhs
	}

Tag:
|	'<' IDENTIFIER '>'

//yy:field	Value	string
Tail:
	"%%"
	{
		lhs.Value = lx.value
	}
|	/* empty */
