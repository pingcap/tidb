// Copyright 2015 The parser Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// CAUTION: Generated file - DO NOT EDIT!

package parser

import (
	"fmt"
)

func ExampleAction() {
	fmt.Println(exampleAST(2, `

%%

a:

	{
	}

`))
	// Output:
	// &parser.Action{
	// · Token: example.y:7:2: '{' "{", Comments: [],
	// · Token2: example.y:8:2: '}' "}", Comments: [],
	// · Pos: 12,
	// · Values: []*parser.ActionValue{ // len 1
	// · · 0: &parser.ActionValue{
	// · · · Pos: 12,
	// · · · Src: "{\n\t}",
	// · · },
	// · },
	// }
}

func ExampleDefinition() {
	fmt.Println(exampleAST(3, `
%start source

%%
`))
	// Output:
	// &parser.Definition{
	// · Token: example.y:2:1: START "%start", Comments: [],
	// · Token2: example.y:2:8: IDENTIFIER "source", Comments: [],
	// }
}

func ExampleDefinition_case1() {
	fmt.Println(exampleAST(4, `
%union{
        foo bar
}

%%
`))
	// Output:
	// &parser.Definition{
	// · Case: 1,
	// · Token: example.y:2:1: UNION "%union", Comments: [],
	// · Pos: 8,
	// · Value: "{\n        foo bar\n}",
	// }
}

func ExampleDefinition_case2() {
	fmt.Println(exampleAST(6, `

%{

%} %error-verbose

`))
	// Output:
	// &parser.Definition{
	// · Case: 2,
	// · Token: example.y:3:1: LCURL "%{", Comments: [],
	// · Token2: example.y:5:1: RCURL "%}", Comments: [],
	// · Pos: 5,
	// · Value: "\n\n",
	// }
}

func ExampleDefinition_case3() {
	fmt.Println(exampleAST(7, `
%token ARROW "->"
	IDENT
%%
`))
	// Output:
	// &parser.Definition{
	// · Case: 3,
	// · NameList: &parser.NameList{
	// · · Name: &parser.Name{
	// · · · LiteralStringOpt: &parser.LiteralStringOpt{
	// · · · · Case: 1,
	// · · · · Token: example.y:2:14: STRING_LITERAL "\"->\"", Comments: [],
	// · · · },
	// · · · Token: example.y:2:8: IDENTIFIER "ARROW", Comments: [],
	// · · · Identifier: "ARROW",
	// · · · Number: -1,
	// · · },
	// · · NameList: &parser.NameList{
	// · · · Case: 1,
	// · · · Name: &parser.Name{
	// · · · · Token: example.y:3:2: IDENTIFIER "IDENT", Comments: [],
	// · · · · Identifier: "IDENT",
	// · · · · Number: -1,
	// · · · },
	// · · },
	// · },
	// · ReservedWord: &parser.ReservedWord{
	// · · Token: example.y:2:1: TOKEN "%token", Comments: [],
	// · },
	// · Nlist: []*parser.Name{ // len 2
	// · · 0: &parser.Name{ /* recursive/repetitive pointee not shown */ },
	// · · 1: &parser.Name{ /* recursive/repetitive pointee not shown */ },
	// · },
	// }
}

func ExampleDefinition_case4() {
	fmt.Println(exampleAST(8, `
%token <abc>
%%
`))
	// Output:
	// &parser.Definition{
	// · Case: 4,
	// · ReservedWord: &parser.ReservedWord{
	// · · Token: example.y:2:1: TOKEN "%token", Comments: [],
	// · },
	// · Tag: &parser.Tag{
	// · · Case: 1,
	// · · Token: example.y:2:8: '<' "<", Comments: [],
	// · · Token2: example.y:2:9: IDENTIFIER "abc", Comments: [],
	// · · Token3: example.y:2:12: '>' ">", Comments: [],
	// · },
	// }
}

func ExampleDefinition_case5() {
	fmt.Println(exampleAST(9, `

%error-verbose %error-verbose

`))
	// Output:
	// &parser.Definition{
	// · Case: 5,
	// · Token: example.y:3:1: ERROR_VERBOSE "%error-verbose", Comments: [],
	// }
}

func ExampleDefinitionList() {
	fmt.Println(exampleAST(10, `

%error-verbose

`) == (*DefinitionList)(nil))
	// Output:
	// true
}

func ExampleDefinitionList_case1() {
	fmt.Println(exampleAST(11, `
%left '+' '-'
%left '*' '/'
%%
`))
	// Output:
	// &parser.DefinitionList{
	// · Case: 1,
	// · Definition: &parser.Definition{
	// · · Case: 3,
	// · · NameList: &parser.NameList{
	// · · · Name: &parser.Name{
	// · · · · Token: example.y:2:7: IDENTIFIER "'+'", Comments: [],
	// · · · · Identifier: 43,
	// · · · · Number: -1,
	// · · · },
	// · · · NameList: &parser.NameList{
	// · · · · Case: 1,
	// · · · · Name: &parser.Name{
	// · · · · · Token: example.y:2:11: IDENTIFIER "'-'", Comments: [],
	// · · · · · Identifier: 45,
	// · · · · · Number: -1,
	// · · · · },
	// · · · },
	// · · },
	// · · ReservedWord: &parser.ReservedWord{
	// · · · Case: 1,
	// · · · Token: example.y:2:1: LEFT "%left", Comments: [],
	// · · },
	// · · Nlist: []*parser.Name{ // len 2
	// · · · 0: &parser.Name{ /* recursive/repetitive pointee not shown */ },
	// · · · 1: &parser.Name{ /* recursive/repetitive pointee not shown */ },
	// · · },
	// · },
	// }
}

func ExampleLiteralStringOpt() {
	fmt.Println(exampleAST(12, `

%left
	a ,

`) == (*LiteralStringOpt)(nil))
	// Output:
	// true
}

func ExampleLiteralStringOpt_case1() {
	fmt.Println(exampleAST(13, `

%left
	a "@b" ,

`))
	// Output:
	// &parser.LiteralStringOpt{
	// · Case: 1,
	// · Token: example.y:4:4: STRING_LITERAL "\"@b\"", Comments: [],
	// }
}

func ExampleName() {
	fmt.Println(exampleAST(14, `

%left
	a ,

`))
	// Output:
	// &parser.Name{
	// · Token: example.y:4:2: IDENTIFIER "a", Comments: [],
	// · Identifier: "a",
	// · Number: -1,
	// }
}

func ExampleName_case1() {
	fmt.Println(exampleAST(15, `

%left
	a 1 ,

`))
	// Output:
	// &parser.Name{
	// · Case: 1,
	// · Token: example.y:4:2: IDENTIFIER "a", Comments: [],
	// · Token2: example.y:4:4: NUMBER "1", Comments: [],
	// · Identifier: "a",
	// · Number: 1,
	// }
}

func ExampleNameList() {
	fmt.Println(exampleAST(16, `

%left
	a ,

`))
	// Output:
	// &parser.NameList{
	// · Name: &parser.Name{
	// · · Token: example.y:4:2: IDENTIFIER "a", Comments: [],
	// · · Identifier: "a",
	// · · Number: -1,
	// · },
	// }
}

func ExampleNameList_case1() {
	fmt.Println(exampleAST(17, `

%left
	a
	b ,

`))
	// Output:
	// &parser.NameList{
	// · Name: &parser.Name{
	// · · Token: example.y:4:2: IDENTIFIER "a", Comments: [],
	// · · Identifier: "a",
	// · · Number: -1,
	// · },
	// · NameList: &parser.NameList{
	// · · Case: 1,
	// · · Name: &parser.Name{
	// · · · Token: example.y:5:2: IDENTIFIER "b", Comments: [],
	// · · · Identifier: "b",
	// · · · Number: -1,
	// · · },
	// · },
	// }
}

func ExampleNameList_case2() {
	fmt.Println(exampleAST(18, `

%left
	a ,
	b ,

`))
	// Output:
	// &parser.NameList{
	// · Name: &parser.Name{
	// · · Token: example.y:4:2: IDENTIFIER "a", Comments: [],
	// · · Identifier: "a",
	// · · Number: -1,
	// · },
	// · NameList: &parser.NameList{
	// · · Case: 2,
	// · · Name: &parser.Name{
	// · · · Token: example.y:5:2: IDENTIFIER "b", Comments: [],
	// · · · Identifier: "b",
	// · · · Number: -1,
	// · · },
	// · · Token: example.y:4:4: ',' ",", Comments: [],
	// · },
	// }
}

func ExamplePrecedence() {
	fmt.Println(exampleAST(19, `

%%

a:
|

`) == (*Precedence)(nil))
	// Output:
	// true
}

func ExamplePrecedence_case1() {
	fmt.Println(exampleAST(20, `

%%

a:
%prec
	b

`))
	// Output:
	// &parser.Precedence{
	// · Case: 1,
	// · Token: example.y:6:1: PREC "%prec", Comments: [],
	// · Token2: example.y:7:2: IDENTIFIER "b", Comments: [],
	// · Identifier: "b",
	// }
}

func ExamplePrecedence_case2() {
	fmt.Println(exampleAST(21, `

%%

a:
%prec
	b
	{
	}

`))
	// Output:
	// &parser.Precedence{
	// · Action: &parser.Action{
	// · · Token: example.y:8:2: '{' "{", Comments: [],
	// · · Token2: example.y:9:2: '}' "}", Comments: [],
	// · · Pos: 20,
	// · · Values: []*parser.ActionValue{ // len 1
	// · · · 0: &parser.ActionValue{
	// · · · · Pos: 20,
	// · · · · Src: "{\n\t}",
	// · · · },
	// · · },
	// · },
	// · Case: 2,
	// · Token: example.y:6:1: PREC "%prec", Comments: [],
	// · Token2: example.y:7:2: IDENTIFIER "b", Comments: [],
	// · Identifier: "b",
	// }
}

func ExamplePrecedence_case3() {
	fmt.Println(exampleAST(22, `

%%

a:
;

`))
	// Output:
	// &parser.Precedence{
	// · Case: 3,
	// · Token: example.y:6:1: ';' ";", Comments: [],
	// }
}

func ExampleReservedWord() {
	fmt.Println(exampleAST(23, `

%token <

`))
	// Output:
	// &parser.ReservedWord{
	// · Token: example.y:3:1: TOKEN "%token", Comments: [],
	// }
}

func ExampleReservedWord_case1() {
	fmt.Println(exampleAST(24, `

%left <

`))
	// Output:
	// &parser.ReservedWord{
	// · Case: 1,
	// · Token: example.y:3:1: LEFT "%left", Comments: [],
	// }
}

func ExampleReservedWord_case2() {
	fmt.Println(exampleAST(25, `

%right <

`))
	// Output:
	// &parser.ReservedWord{
	// · Case: 2,
	// · Token: example.y:3:1: RIGHT "%right", Comments: [],
	// }
}

func ExampleReservedWord_case3() {
	fmt.Println(exampleAST(26, `

%nonassoc <

`))
	// Output:
	// &parser.ReservedWord{
	// · Case: 3,
	// · Token: example.y:3:1: NONASSOC "%nonassoc", Comments: [],
	// }
}

func ExampleReservedWord_case4() {
	fmt.Println(exampleAST(27, `

%type <

`))
	// Output:
	// &parser.ReservedWord{
	// · Case: 4,
	// · Token: example.y:3:1: TYPE "%type", Comments: [],
	// }
}

func ExampleReservedWord_case5() {
	fmt.Println(exampleAST(28, `

%precedence <

`))
	// Output:
	// &parser.ReservedWord{
	// · Case: 5,
	// · Token: example.y:3:1: PRECEDENCE "%precedence", Comments: [],
	// }
}

func ExampleRule() {
	fmt.Println(exampleAST(29, `
%%a:b:{c=$1}{d}%%
`))
	// Output:
	// &parser.Rule{
	// · RuleItemList: &parser.RuleItemList{
	// · · Action: &parser.Action{
	// · · · Token: example.y:2:7: '{' "{", Comments: [],
	// · · · Token2: example.y:2:12: '}' "}", Comments: [],
	// · · · Pos: 14,
	// · · · Values: []*parser.ActionValue{ // len 3
	// · · · · 0: &parser.ActionValue{
	// · · · · · Pos: 8,
	// · · · · · Src: "{c=",
	// · · · · },
	// · · · · 1: &parser.ActionValue{
	// · · · · · Num: 1,
	// · · · · · Pos: 11,
	// · · · · · Src: "$1",
	// · · · · · Type: 3,
	// · · · · },
	// · · · · 2: &parser.ActionValue{
	// · · · · · Pos: 13,
	// · · · · · Src: "}",
	// · · · · },
	// · · · },
	// · · },
	// · · Case: 2,
	// · · RuleItemList: &parser.RuleItemList{
	// · · · Action: &parser.Action{
	// · · · · Token: example.y:2:13: '{' "{", Comments: [],
	// · · · · Token2: example.y:2:15: '}' "}", Comments: [],
	// · · · · Pos: 14,
	// · · · · Values: []*parser.ActionValue{ // len 1
	// · · · · · 0: &parser.ActionValue{
	// · · · · · · Pos: 14,
	// · · · · · · Src: "{d}",
	// · · · · · },
	// · · · · },
	// · · · },
	// · · · Case: 2,
	// · · },
	// · },
	// · Token: example.y:2:5: C_IDENTIFIER "b", Comments: [],
	// · Name: example.y:2:5: C_IDENTIFIER "b", Comments: [],
	// }
}

func ExampleRule_case1() {
	fmt.Println(exampleAST(30, `

%%

a:
|

`))
	// Output:
	// &parser.Rule{
	// · Case: 1,
	// · Token: example.y:6:1: '|' "|", Comments: [],
	// · Name: example.y:5:1: C_IDENTIFIER "a", Comments: [],
	// }
}

func ExampleRuleItemList() {
	fmt.Println(exampleAST(31, `

%%

a:

`) == (*RuleItemList)(nil))
	// Output:
	// true
}

func ExampleRuleItemList_case1() {
	fmt.Println(exampleAST(32, `

%%

a:

	b

`))
	// Output:
	// &parser.RuleItemList{
	// · Case: 1,
	// · Token: example.y:7:2: IDENTIFIER "b", Comments: [],
	// }
}

func ExampleRuleItemList_case2() {
	fmt.Println(exampleAST(33, `

%%

a:

	{
	}

`))
	// Output:
	// &parser.RuleItemList{
	// · Action: &parser.Action{
	// · · Token: example.y:7:2: '{' "{", Comments: [],
	// · · Token2: example.y:8:2: '}' "}", Comments: [],
	// · · Pos: 12,
	// · · Values: []*parser.ActionValue{ // len 1
	// · · · 0: &parser.ActionValue{
	// · · · · Pos: 12,
	// · · · · Src: "{\n\t}",
	// · · · },
	// · · },
	// · },
	// · Case: 2,
	// }
}

func ExampleRuleItemList_case3() {
	fmt.Println(exampleAST(34, `

%%

a:
"@b"

`))
	// Output:
	// &parser.RuleItemList{
	// · Case: 3,
	// · Token: example.y:6:1: STRING_LITERAL "\"@b\"", Comments: [],
	// }
}

func ExampleRuleList() {
	fmt.Println(exampleAST(35, `
%%a:{b}{c}%%
`))
	// Output:
	// &parser.RuleList{
	// · RuleItemList: &parser.RuleItemList{
	// · · Action: &parser.Action{
	// · · · Token: example.y:2:5: '{' "{", Comments: [],
	// · · · Token2: example.y:2:7: '}' "}", Comments: [],
	// · · · Pos: 9,
	// · · · Values: []*parser.ActionValue{ // len 1
	// · · · · 0: &parser.ActionValue{
	// · · · · · Pos: 6,
	// · · · · · Src: "{b}",
	// · · · · },
	// · · · },
	// · · },
	// · · Case: 2,
	// · · RuleItemList: &parser.RuleItemList{
	// · · · Action: &parser.Action{
	// · · · · Token: example.y:2:8: '{' "{", Comments: [],
	// · · · · Token2: example.y:2:10: '}' "}", Comments: [],
	// · · · · Pos: 9,
	// · · · · Values: []*parser.ActionValue{ // len 1
	// · · · · · 0: &parser.ActionValue{
	// · · · · · · Pos: 9,
	// · · · · · · Src: "{c}",
	// · · · · · },
	// · · · · },
	// · · · },
	// · · · Case: 2,
	// · · },
	// · },
	// · Token: example.y:2:3: C_IDENTIFIER "a", Comments: [],
	// }
}

func ExampleRuleList_case1() {
	fmt.Println(exampleAST(36, `

%%

a:
|

`))
	// Output:
	// &parser.RuleList{
	// · RuleList: &parser.RuleList{
	// · · Case: 1,
	// · · Rule: &parser.Rule{
	// · · · Case: 1,
	// · · · Token: example.y:6:1: '|' "|", Comments: [],
	// · · · Name: example.y:5:1: C_IDENTIFIER "a", Comments: [],
	// · · },
	// · },
	// · Token: example.y:5:1: C_IDENTIFIER "a", Comments: [],
	// }
}

func ExampleSpecification() {
	fmt.Println(exampleAST(37, `

%%

a:

`))
	// Output:
	// &parser.Specification{
	// · RuleList: &parser.RuleList{
	// · · Token: example.y:5:1: C_IDENTIFIER "a", Comments: [],
	// · },
	// · Token: example.y:3:1: MARK "%%", Comments: [],
	// · Rules: []*parser.Rule{ // len 1
	// · · 0: &parser.Rule{
	// · · · Token: example.y:5:1: C_IDENTIFIER "a", Comments: [],
	// · · · Name: example.y:5:1: C_IDENTIFIER "a", Comments: [],
	// · · },
	// · },
	// }
}

func ExampleTag() {
	fmt.Println(exampleAST(38, `

%left %error-verbose

`) == (*Tag)(nil))
	// Output:
	// true
}

func ExampleTag_case1() {
	fmt.Println(exampleAST(39, `

%left <
	a > %error-verbose

`))
	// Output:
	// &parser.Tag{
	// · Case: 1,
	// · Token: example.y:3:7: '<' "<", Comments: [],
	// · Token2: example.y:4:2: IDENTIFIER "a", Comments: [],
	// · Token3: example.y:4:4: '>' ">", Comments: [],
	// }
}

func ExampleTail() {
	fmt.Println(exampleAST(40, `

%%

a:


%%

`))
	// Output:
	// &parser.Tail{
	// · Token: example.y:8:1: MARK "%%", Comments: [],
	// · Value: "\n\n",
	// }
}

func ExampleTail_case1() {
	fmt.Println(exampleAST(41, `

%%

a:

`) == (*Tail)(nil))
	// Output:
	// true
}
