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
// 
// CAUTION: Generated file (unless this is parser.y) - DO NOT EDIT!

package parser

import (
	"go/token"
)
%}

%union {
	token	*Token
}

%token	<token>
	','
	';'
	'<'
	'>'
	'{'
	'|'
	'}'
	COMMENT
	C_IDENTIFIER
	ERROR_VERBOSE
	IDENTIFIER
	LCURL
	LEFT
	MARK
	NONASSOC
	NUMBER
	PREC
	PRECEDENCE
	RCURL
	RIGHT
	START
	STRING_LITERAL
	TOKEN
	TYPE
	UNION

%start Specification

%%

Action:
	'{'
	{
		lx.values2 = append([]string(nil), lx.values...)
		lx.positions2 = append([]token.Pos(nil), lx.positions...)
	}
	'}'
	{
		//yy:field Pos token.Pos
		//yy:field Values []*ActionValue // For backward compatibility.
		lhs.Pos = lx.pos
		for i, v := range lx.values2 {
			a := lx.parseActionValue(lx.positions2[i], v)
			if a != nil {
				lhs.Values = append(lhs.Values, a)
			}
		}
	}

Definition:
	START IDENTIFIER
	{
		//yy:example "%%start source\n\n%%%%"
		//yy:field Pos token.Pos
		//yy:field Value string
		//yy:field Nlist []*Name // For backward compatibility.
	}
|	UNION
	{
		//yy:example "%%union{\n        foo bar\n}\n\n%%%%"
		lhs.Pos = lx.pos
		lhs.Value = lx.value
	}
|	LCURL
	{
		lx.pos2 = lx.pos
		lx.value2 = lx.value
      }
	RCURL
	{
		lhs.Pos = lx.pos2
		lhs.Value = lx.value2
	}
|	ReservedWord Tag NameList
	{
		//yy:example "%%token ARROW \"->\"\n\tIDENT\n%%%%"
		for n := lhs.NameList; n != nil; n = n.NameList {
			lhs.Nlist = append(lhs.Nlist, n.Name)
		}
	}
|	ReservedWord Tag
	{
		//yy:example "%%token <abc>\n%%%%"
	}
|	ERROR_VERBOSE

DefinitionList:
|	DefinitionList Definition
	{
		//yy:example "%%left '+' '-'\n%%left '*' '/'\n%%%%"
		lx.defs = append(lx.defs, lhs.Definition)
	}

LiteralStringOpt:
|	STRING_LITERAL

Name:
	IDENTIFIER LiteralStringOpt
	{
		//yy:field Identifier interface{} // For backward compatibility.
		//yy:field Number int             // For backward compatibility.
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

Precedence:
	{
		//yy:field Identifier interface{} // Name string or literal int.
	}
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

Rule:
	C_IDENTIFIER RuleItemList Precedence
	{
		//yy:field Name *Token
		//yy:field Body []interface{} // For backward compatibility.
		//yy:example "%%%%a:b:{c=$1}{d}%%%%"
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
		//yy:example "%%%%a:{b}{c}%%%%"
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

Specification:
	DefinitionList MARK RuleList Tail
	{
		//yy:field Defs  []*Definition // For backward compatibility.
		//yy:field Rules []*Rule       // For backward compatibility.
		lhs.Defs = lx.defs
		lhs.Rules = lx.rules
		lx.spec = lhs
	}

Tag:
|	'<' IDENTIFIER '>'

Tail:
	MARK
	{
		//yy:field Value string
		lhs.Value = lx.value
	}
|	/* empty */
