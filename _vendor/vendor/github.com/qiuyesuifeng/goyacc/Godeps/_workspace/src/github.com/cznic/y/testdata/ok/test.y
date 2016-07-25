%{

// Copyright 2014 The goyacc Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// NOTE: If this file is parser.go: GENERATED FILE - DO NOT EDIT

package main

import (
	"fmt"
	"go/token"
	"go/scanner"
)

%}

// Note: Instead of yylex, p (type *parser) is used throughout this file. See the Makefile.

%union	{
	list   []node
	node   node
	param  *param
	params []*param
	token  tkn
}

%token	<token>
	ANDAND ANDNOT ASOP
	BODY BREAK
	CASE CHAN COLAS COMM CONST CONTINUE
	DDD DEC DEFAULT DEFER
	ELSE EQ
	FALL FOR FUNC
	GE GO GOTO
	IDENT IF IGNORE IMPORT INC INTERFACE
	LE LITERAL LSH
	MAP
	NE
	OROR
	PACKAGE
	RANGE RETURN RSH
	SELECT STRUCT SWITCH
	TYPE
	UNION
	VAR VARIANT
	XLEFT XRIGHT
	TEST1 TEST2
	'!' '%' '&' '(' ')' '*' '+' '-' '.' '/' ':' ';' '<' '=' '>' '[' '^' '{' '|' '~'

%type	<node>
	BareCompLitExpr
	Case CaseBlock CompLitExpr CompLitType CompoundStmt ConstDecl
	ConstDecl1 ConvType
	DeclName 
	Else ElseIf Embedded Expr ExprOrType
	FuncDecl FuncDecl1 FuncLitDecl FuncLit FuncRetType FuncType ForBody
	ForHeader ForStmt
	IfHeader IfStmt ImportDecl InterfaceMethodDecl InterfaceDecl
	InterfaceType
	Keyval
	LabelName LBrace
	Name NameOrType NewName NonDclStmt NonExprType NonRecvChanType
	oExpr oLiteral oNewName oSimpleStmt OtherType
	PackageDecl PrimaryExpr PrimaryExprNoParen PseudoCall PtrType
	Qualident 
	RangeStmt RecvChanType
	SelectStmt SimpleStmt StructDecl StructType SwitchStmt Symbol
	Type TypeDecl TypeDeclName TypeName
	UnaryExpr UnionType
	VariantType

%type	<list>
	BracedKeyvalList
	CaseBlockList CommonDecl ConstDeclList
	DeclNameList
	ElseIfList ExprList ExprOrTypeList
	FuncBody
	ImportDeclList InterfaceDeclList
	KeyvalList
	LoopBody
	NewNameList
	oExprList
	Statement StmtList StructDeclList
	TypeDeclList
	VarDecl VarDeclList

%type	<param>
	ArgType
	Ddd

%type	<params>
	ArgTypeList
	FuncResult
	oArgTypeListOComma

%left	COMM

%left	OROR
%left	ANDAND
%left	EQ NE LE GE '<' '>'
%left	'+' '-' '|' '^'
%left	'*' '/' '%' '&' LSH RSH ANDNOT

%left	notPackage
%left	PACKAGE

%left	notParen
%left	'('

%left	')'
%left	preferToRightParen

%right	TEST1

%nonassoc
	TEST2

%start	File

%error-verbose

%%

File:
	PackageDecl
	{ // 118
		p.topLevelDecl($1)
	}
	Imports
	TopLevelDeclList

PackageDecl:
	%prec notPackage
	{ // 126
		p.Error("package statement must be first")
		goto ret1
	}
|	PACKAGE Symbol ';'
	{ // 131
		if !p.opts.modeGo && !p.sc.seenRun {
			p.errNode($1, "package statement must be preceded by 'run'")
			goto ret1
		}

		if !p.consider() {
			goto ret0
		}

		$$ = &packageDecl{$1.p(), $2.(*ident)}
	}

Imports:
|	Imports Import ';'

Import:
	IMPORT ImportDecl
	{ // 149
		p.topLevelDecl($2)
	}
|	IMPORT '(' ImportDeclList oSemi ')'
	{ // 153
		p.xdclList($3)
	}
|	IMPORT '(' ')'

ImportDecl:
	LITERAL
	{ // 160
		x, err := newImport(p, (*ident)(nil), newLiteral(p, $1.p(), $1.tok, $1.lit))
		if err != nil {
			goto ret1
		}

		$$ = x
	}
|	Symbol LITERAL
	{ // 169
		x, err := newImport(p, $1, newLiteral(p, $2.p(), $2.tok, $2.lit))
		if err != nil {
			goto ret1
		}

		$$ = x
	}
|	'.' LITERAL
	{ // 178
		x, err := newImport(p, &ident{$1.p(), ".", nil}, newLiteral(p, $2.p(), $2.tok, $2.lit))
		if err != nil {
			goto ret1
		}

		$$ = x
	}

ImportDeclList:
	ImportDecl
	{ // 189
		$$ = []node{$1}
	}
|	ImportDeclList ';' ImportDecl
	{ // 193
		$$ = append($1, $3)
	}

TopLevelDecl:
|	CommonDecl
	{ // 199
		p.xdclList($1)
	}
|	FuncDecl
	{ // 203
		p.topLevelDecl($1)
	}
|	NonDclStmt
	{ // 207
		p.errNode($1, "non-declaration statement outside function body");
	}
|	error

CommonDecl:
	VAR VarDecl
	{ // 214
		$$ = $2
	}
|	VAR '(' VarDeclList oSemi ')'
	{ // 218
		$$ = $3
	}
|	VAR '(' ')'
	{ // 222
		$$ = nil
	}
|	Const ConstDecl
	{ // 226
		$$ = newConstDecls(p, []node{$2})
	}
|	Const '(' ConstDecl oSemi ')'
	{ // 230
		$$ = newConstDecls(p, []node{$3})
	}
|	Const '(' ConstDecl ';' ConstDeclList oSemi ')'
	{ // 234
		$$ = newConstDecls(p, append([]node{$3}, $5...))
	}
|	Const '(' ')'
	{ // 238
		$$ = nil
	}
|	TYPE TypeDecl
	{ // 242
		$$ = []node{$2}
	}
|	TYPE '(' TypeDeclList oSemi ')'
	{ // 246
		$$ = $3
	}
|	TYPE '(' ')'
	{ // 250
		$$ = nil
	}

Const:
	CONST
	{ // 256
		p.constExpr, p.constIota, p.constType = nil, 0, nil
	}

VarDecl:
	DeclNameList Type
	{ // 262
		$$ = newVarDecls($1, $2, nil)
	}
|	DeclNameList Type '=' ExprList
	{ // 266
		$$ = newVarDecls($1, $2, $4)
	}
|	DeclNameList '=' ExprList
	{ // 270
		$$ = newVarDecls($1, nil, $3)
	}

ConstDecl:
	DeclNameList Type '=' ExprList
	{ // 276
		$$ = newConstSpec(p, $1, $2, $4)
	}
|	DeclNameList '=' ExprList
	{ // 280
		$$ = newConstSpec(p, $1, nil, $3)
	}

ConstDecl1:
	ConstDecl
|	DeclNameList Type
	{ // 287
		$$ = newConstSpec(p, $1, nil, nil)
		p.errNode($2, "const declaration cannot have type without expression")
	}
|	DeclNameList
	{ // 292
		$$ = newConstSpec(p, $1, nil, nil)
	}

TypeDeclName:
	Symbol

TypeDecl:
	TypeDeclName Type
	{ // 301
		$$ = &typeDecl{$1.p(), $1.(*ident), $2}
	}

SimpleStmt:
	Expr
|	Expr ASOP Expr
	{ // 308
		$$ = &assignment{$2.p(), $2.tok, []node{$1}, []node{$3}}
	}
|	ExprList '=' ExprList
	{ // 312
		$$ = &assignment{$2.p(), $2.tok, $1, $3}
	}
|	ExprList COLAS ExprList
	{ // 316
		$$ = &shortVarDecl{$2.p(), $1, $3}
	}
|	Expr INC
	{ // 320
		$$ = &incDecStmt{$2.p(), $1, $2.tok}
	}
|	Expr DEC
	{ // 324
		$$ = &incDecStmt{$2.p(), $1, $2.tok}
	}

Case:
	CASE ExprOrTypeList ':'
	{ // 330
		p.newScope($1.p())
		$$ = &switchCase{$1.p(), $2, nil}
	}
|	CASE ExprOrTypeList '=' Expr ':'
	{ // 335
		p.newScope($1.p())
		$$ = &switchCase{$1.p(), []node{&assignment{$3.p(), $3.tok, $2, []node{$4}}}, nil}
	}
|	CASE ExprOrTypeList COLAS Expr ':'
	{ // 340
		p.newScope($1.p())
		$$ = &switchCase{$1.p(), []node{&assignment{$3.p(), $3.tok, $2, []node{$4}}}, nil}
	}
|	DEFAULT ':'
	{ // 345
		p.newScope($1.p())
		$$ = &switchCase{pos: $1.p()}
	}

CompoundStmt:
	'{'
	{ // 352
		p.newScope($1.p())
	}
	StmtList '}'
	{ // 356
		$$ = &compoundStament{$1.p(), $3}
		p.popScope()
	}

CaseBlock:
	Case
	{ // 363
		// If the last token read by the lexer was consumed as part of
		// the case, clear it (parser has cleared yychar). If the last
		// token read by the lexer was the lookahead leave it alone
		// (parser has it cached in yychar). This is so that the
		// StmtList action doesn't look at the case tokens if the
		// StmtList is empty.
		p.yyLast = yychar
	}
	StmtList
	{ // 373
		// This is the only place in the language where a statement
		// list is not allowed to drop the final semicolon, because
		// it's the only place where a statement list is not followed
		// by a closing brace. Handle the error for pedantry.

		// Find the final token of the statement list. p.yyLast is
		// lookahead; p.yyPrev is last of StmtList

		if p.yyPrev > 0 && p.yyPrev != ';' && p.yyLast != '}' {
			p.errPos(p.prevPos, "missing statement after label")
		}

		$1.(*switchCase).body = $3
		$$ = $1
		p.popScope()
	}

CaseBlockList:
	{ // 392
		$$ = nil
	}
|	CaseBlockList CaseBlock
	{ // 396
		$$ = append($1, $2)
	}

LoopBody:
	BODY
	{ // 402
		p.newScope($1.p())
		p.inFor++
	}
	StmtList '}'
	{ // 407
		$$ = $3
		p.popScope()
		p.inFor--
	}

RangeStmt:
	ExprList '=' RANGE Expr
	{ // 415
		$$ = &forStmt{rang: &assignment{$2.p(), $2.tok, $1, []node{$4}}}
	}
|	ExprList COLAS RANGE Expr
	{ // 419
		$$ = &forStmt{rang: &assignment{$2.p(), $2.tok, $1, []node{$4}}}
	}

ForHeader:
	oSimpleStmt ';' oSimpleStmt ';' oSimpleStmt
	{ // 425
		$$ = &forStmt{init: $1, cond: $3, post: $5}
	}
|	oSimpleStmt
	{ // 429
		$$ = &forStmt{cond: $1}
	}
|	RangeStmt

ForBody:
	ForHeader LoopBody
	{ // 436
		$1.(*forStmt).body = $2
		$$ = $1
	}

ForStmt:
	FOR
	{ // 443
		p.newScope($1.p())
	}
	ForBody
	{ // 447
		$3.(*forStmt).pos = $1.p()
		$$ = $3
		p.popScope()
	}

IfHeader:
	oSimpleStmt
	{ // 455
		$$ = &ifStmt{cond: $1}
	}
|	oSimpleStmt ';' oSimpleStmt
	{ // 459
		$$ = &ifStmt{init: $1, cond: $3}
	}

IfStmt:
	IF
	{ // 465
		p.newScope($1.p())
		p.inIf++
	}
	IfHeader LoopBody
	{ // 470
		p.inIf--
	}
	ElseIfList Else
	{ // 474
		x := $3.(*ifStmt)
		if x.cond == nil {
			p.errNode($1, "missing condition in if statement")
		}
		l := make([]*ifStmt, len($6))
		for i, v := range $6 {
			l[i] = v.(*ifStmt)
		}
		x.pos, x.body, x.elif, x.els = $1.p(), $4, l, $7.(*compoundStament)
		$$ = x
		p.popScope()
	}

ElseIf:
	ELSE IF IfHeader
	{ // 490
		p.newScope($2.p())
		p.inIf++
	}
	LoopBody
	{ // 495
		x := $3.(*ifStmt)
		x.pos, x.body = $2.p(), $5
		$$ = x
		p.popScope()
		p.inIf--
	}

ElseIfList:
	{ // 504
		$$ = nil
	}
|	ElseIfList ElseIf
	{ // 508
		$$ = append($1, $2)
	}

Else:
	{ // 513
		$$ = (*compoundStament)(nil)
	}
|	ELSE
	{ // 517
		p.inFor++
		p.inIf++
	}
	CompoundStmt
	{ // 522
		$$ = $3
		p.inFor--
		p.inIf--
	}

SwitchStmt:
	SWITCH
	{ // 530
		p.newScope($1.p())
		p.inSwitch++
	}
	IfHeader BODY CaseBlockList '}'
	{ // 535
		var default0 node
		l := make([]*switchCase, len($5))
		for i, v := range $5 {
			sw := v.(*switchCase)
			l[i] = sw
			if len(sw.expr) == 0 {
				if default0 != nil {
					p.errNode(sw, "multiple defaults in switch (first at %s)", p.file.Position(default0.Pos()))
					continue
				}

				default0 = sw
			}
		}
		x := $3.(*ifStmt)
		$$ = &switchStmt{$1.p(), x.init, x.cond, l}
		p.popScope()
		p.inSwitch--
	}

SelectStmt:
	SELECT
	{ // 558
		p.inSwitch++
	}
	BODY CaseBlockList '}'
	{ // 562
		x := &selectStmt{pos: $1.p()}
		var default0 node
		for _, v := range $4 {
			sw := v.(*switchCase)
			switch len(sw.expr) {
			case 0: // default:
				x.cases = append(x.cases, &commCase{sw.p(), nil})
				if default0 != nil {
					p.errNode(sw, "multiple defaults in select (first at %s)", p.file.Position(default0.Pos()))
					continue
				}

				default0 = sw
				continue
			case 1:
				// ok
			default:
				p.errNode(sw.expr[1], "select cases cannot be lists")
				continue
			}

			v0 := sw.expr[0]
			switch t := v0.(type) {
			case *assignment:
				if len(t.lhs) > 2 {
					break
				}

				if p, ok := t.rhs[0].(*unOp); ok && p.op == token.ARROW {
					x.cases = append(x.cases, &commCase{v0.p(), v0})
					continue
				}
			case *binOp:
				if t.op != token.ARROW {
					break
				}

				x.cases = append(x.cases, &commCase{v0.p(), v0})
				continue
			case *unOp:
				if t.op != token.ARROW {
					break
				}

				x.cases = append(x.cases, &commCase{v0.p(), v0})
				continue
			}
			p.errNode(v0, "select case must be receive, send or assign recv")
		}
		$$ = x
		p.inSwitch--
	}

Expr:
	UnaryExpr
|	Expr OROR Expr
	{ // 619
		$$ = &binOp{$2.p(), $2.tok, $1, $3}
	}
|	Expr ANDAND Expr
	{ // 623
		$$ = &binOp{$2.p(), $2.tok, $1, $3}
	}
|	Expr EQ Expr
	{ // 627
		$$ = &binOp{$2.p(), $2.tok, $1, $3}
	}
|	Expr NE Expr
	{ // 631
		$$ = &binOp{$2.p(), $2.tok, $1, $3}
	}
|	Expr '<' Expr
	{ // 635
		$$ = &binOp{$2.p(), $2.tok, $1, $3}
	}
|	Expr LE Expr
	{ // 639
		$$ = &binOp{$2.p(), $2.tok, $1, $3}
	}
|	Expr GE Expr
	{ // 643
		$$ = &binOp{$2.p(), $2.tok, $1, $3}
	}
|	Expr '>' Expr
	{ // 647
		$$ = &binOp{$2.p(), $2.tok, $1, $3}
	}
|	Expr '+' Expr
	{ // 651
		$$ = &binOp{$2.p(), $2.tok, $1, $3}
	}
|	Expr '-' Expr
	{ // 655
		$$ = &binOp{$2.p(), $2.tok, $1, $3}
	}
|	Expr '|' Expr
	{ // 659
		$$ = &binOp{$2.p(), $2.tok, $1, $3}
	}
|	Expr '^' Expr
	{ // 663
		$$ = &binOp{$2.p(), $2.tok, $1, $3}
	}
|	Expr '*' Expr
	{ // 667
		$$ = &binOp{$2.p(), $2.tok, $1, $3}
	}
|	Expr '/' Expr
	{ // 671
		$$ = &binOp{$2.p(), $2.tok, $1, $3}
	}
|	Expr '%' Expr
	{ // 675
		$$ = &binOp{$2.p(), $2.tok, $1, $3}
	}
|	Expr '&' Expr
	{ // 679
		$$ = &binOp{$2.p(), $2.tok, $1, $3}
	}
|	Expr ANDNOT Expr
	{ // 683
		$$ = &binOp{$2.p(), $2.tok, $1, $3}
	}
|	Expr LSH Expr
	{ // 687
		$$ = &binOp{$2.p(), $2.tok, $1, $3}
	}
|	Expr RSH Expr
	{ // 691
		$$ = &binOp{$2.p(), $2.tok, $1, $3}
	}
|	Expr COMM Expr
	{ // 695
		$$ = &binOp{$2.p(), $2.tok, $1, $3}
	}

UnaryExpr:
	PrimaryExpr
|	'*' UnaryExpr
	{ // 702
		$$ = &unOp{$1.p(), $1.tok, $2}
	}
|	'&' UnaryExpr
	{ // 706
		$$ = &unOp{$1.p(), $1.tok, $2}
	}
|	'+' UnaryExpr
	{ // 710
		$$ = &unOp{$1.p(), $1.tok, $2}
	}
|	'-' UnaryExpr
	{ // 714
		$$ = &unOp{$1.p(), $1.tok, $2}
	}
|	'!' UnaryExpr
	{ // 718
		$$ = &unOp{$1.p(), $1.tok, $2}
	}
|	'~' UnaryExpr
	{ // 722
		p.errNode($1, "the bitwise complement operator is ^");
		$$ = &unOp{$1.p(), token.XOR, $2}
	}
|	'^' UnaryExpr
	{ // 727
		$$ = &unOp{$1.p(), $1.tok, $2}
	}
|	COMM UnaryExpr
	{ // 731
		$$ = &unOp{$1.p(), $1.tok, $2}
	}

PseudoCall:
	PrimaryExpr '(' ')'
	{ // 737
		$$ = &callOp{$2.p(), $1, nil, false}
	}
|	PrimaryExpr '(' ExprOrTypeList oComma ')'
	{ // 741
		$$ = &callOp{$2.p(), $1, $3, false}
	}
|	PrimaryExpr '(' ExprOrTypeList DDD oComma ')'
	{ // 745
		$$ = &callOp{$2.p(), $1, $3, true}
	}

PrimaryExprNoParen:
	LITERAL
	{ // 751
		$$ = newLiteral(p, $1.p(), $1.tok, $1.lit)
	}
|	Name
|	PrimaryExpr '.' Symbol
	{ // 756
		$$ = &selectOp{$2.p(), $1, $3.(*ident)}
	}
|	PrimaryExpr '.' '(' ExprOrType ')'
	{ // 760
		$$ = &typeAssertion{$3.p(), $1, $4}
	}
|	PrimaryExpr '.' '(' TYPE ')'
	{ // 764
		$$ = &typeSwitch{$4.p(), $1}
	}
|	PrimaryExpr '[' Expr ']'
	{ // 768
		$$ = &indexOp{$2.p(), $1, $3}
	}
|	PrimaryExpr '[' oExpr ':' oExpr ']'
	{ // 772
		$$ = &sliceOp{$2.p(), $1, $3, $5, nil}
	}
|	PrimaryExpr '[' oExpr ':' oExpr ':' oExpr ']'
	{ // 776
		if $5 == nil {
			p.errNode($4, "middle index required in 3-index slice")
		}
		if $7 == nil {
			p.errNode($6, "final index required in 3-index slice")
		}
		$$ = &sliceOp{$2.p(), $1, $3, $5, $7}
	}
|	PseudoCall
|	ConvType '(' ExprList oComma ')'
	{ // 787
		if len($3) > 1 {
			p.errNode($3[1], "syntax error: unexpected expression, expecting )")
		}
		$$ = &convOp{$2.p(), $1, $3[0]}
	}
|	ConvType '(' ')'
	{ // 794
		p.errNode($3, "syntax error: unexpected )")
		$$ = &convOp{$2.p(), $1, nil}
	}
|	CompLitType LBrace StartCompLit BracedKeyvalList '}'
	{ // 799
		$$ = &compLit{$2.p(), $1, elements($4)}
		p.fixlbrace($2)
	}
|	PrimaryExprNoParen '{' StartCompLit BracedKeyvalList '}'
	{ // 804
		$$ = &compLit{$2.p(), $1, elements($4)}
	}
|	'(' ExprOrType ')' '{' StartCompLit BracedKeyvalList '}'
	{ // 808
		$$ = &compLit{$4.p(), $2, elements($6)}
	}
|	FuncLit

StartCompLit:
	{ // 814
		
	}

Keyval:
	Expr ':' CompLitExpr
	{ // 820
		$$ = &element{$1.p(), $1, $3}
	}

BareCompLitExpr:
	Expr
	{ // 826
		$$ = &element{$1.p(), nil, $1}
	}
|	'{' StartCompLit BracedKeyvalList '}'
	{ // 830
		$$ = &element{$1.p(), nil, &compLit{$1.p(), nil, elements($3)}}
	}

CompLitExpr:
	Expr
	{ // 836
		$$ = &element{$1.p(), nil, $1}
	}
|	'{' StartCompLit BracedKeyvalList '}'
	{ // 840
		$$ = &element{$1.p(), nil, &compLit{$1.p(), nil, elements($3)}}
	}

PrimaryExpr:
	PrimaryExprNoParen
|	'(' ExprOrType ')'
	{ // 847
		$$ = &paren{$1.p(), $2}
	}

ExprOrType:
	Expr
|	NonExprType	%prec preferToRightParen

NameOrType:
	Type

LBrace:
	BODY
	{ // 860
		$$ = lbrace{$1.p(), BODY}
	}
|	'{'
	{ // 864
		$$ = lbrace{$1.p(), '{'}
	}

// - field Name of a struct type definition
// - label Name declaration/reference
// - method Name of an interface type defintion
NewName:
	Symbol

// identifier in the identifier list of a var or const declaration
//
DeclName:
	Symbol

oNewName:
	{ // 880
		$$ = (*ident)(nil)
	}
|	NewName

Symbol:
	IDENT
	{ // 887
		$$ = &ident{$1.p(), $1.lit, p.currentScope}
	}

Name:
	Symbol	%prec notParen

LabelName:
	NewName

Ddd:
	DDD
	{ // 899
		p.errPos($1.tpos, "final argument in variadic function missing type")
		$$ = &param{pos: $1.p(), ddd: true}
	}
|	DDD Type
	{ // 904
		$$ = &param{pos: $1.p(), ddd: true, typ: $2}
	}

Type:
	RecvChanType
|	FuncType
|	OtherType
|	PtrType
|	TypeName
|	'(' Type ')'
	{ // 915
		$$ = &paren{$1.p(), $2}
	}

NonExprType:
	RecvChanType
|	FuncType
|	OtherType
|	'*' NonExprType
	{ // 924
		$$ = &ptrType{pos: $1.p(), typ: $2}
	}

NonRecvChanType:
	FuncType
|	OtherType
|	PtrType
|	TypeName
|	'(' Type ')'
	{ // 934
		$$ = &paren{$1.p(), $2}
	}

ConvType:
	FuncType
|	OtherType

CompLitType:
	OtherType

FuncRetType:
	RecvChanType
|	FuncType
|	OtherType
|	PtrType
|	TypeName

TypeName:
	Name
	{ // 954
		$$ = &namedType{pos: $1.p(), name: &qualifiedIdent{$1.p(), nil, $1.(*ident)}}
	}
|	Name '.' Symbol
	{ // 958
		$$ = &namedType{pos: $1.p(), name: &qualifiedIdent{$1.p(), $1.(*ident), $3.(*ident)}}
	}

OtherType:
	'[' oExpr ']' Type
	{ // 964
		switch {
		case $2 != nil:
			$$ = &arrayType{pos: $1.p(), expr: $2, typ: $4}
		default:
			$$ = &sliceType{pos: $1.p(), typ: $4}
		}
	}
|	'[' DDD ']' Type
	{ // 973
		$$ = &arrayType{pos: $1.p(), typ: $4}
	}
|	CHAN NonRecvChanType
	{ // 977
		$$ = &channelType{pos: $1.p(), kind: bidirectionalChannel, typ: $2}
	}
|	CHAN COMM Type
	{ // 981
		$$ = &channelType{pos: $2.p(), kind: sendOnlyChannel, typ: $3}
	}
|	MAP '[' Type ']' Type
	{ // 985
		$$ = &mapType{pos: $1.p(), key: $3, val: $5}
	}
|	StructType
|	UnionType
|	VariantType
|	InterfaceType

PtrType:
	'*' Type
	{ // 995
		$$ = &ptrType{pos: $1.p(), typ: $2}
	}

RecvChanType:
	COMM CHAN Type
	{ // 1001
		$$ = &channelType{pos: $1.p(), kind: readOnlyChannel, typ: $3}
	}

StructType:
	STRUCT LBrace StructDeclList oSemi '}'
	{ // 1007
		$$ = newStructType(p, $2.p(), $1, $3)
		p.fixlbrace($2)
	}
|	STRUCT LBrace '}'
	{ // 1012
		$$ = &structType{pos: $1.p()}
		p.fixlbrace($2)
	}

UnionType:
	UNION LBrace StructDeclList oSemi '}'
	{ // 1019
		x := newStructType(p, $2.p(), $1, $3)
		$$ = &unionType{x.pos, x.fields}
		p.fixlbrace($2)

	}
|	UNION LBrace '}'
	{ // 1026
		$$ = &unionType{$1.p(), nil}
		p.fixlbrace($2)
	}

VariantType:
	VARIANT LBrace StructDeclList oSemi '}'
	{ // 1033
		x := newStructType(p, $2.p(), $1, $3)
		$$ = &variantType{x.pos, x.fields}
		p.fixlbrace($2)
	}
|	VARIANT LBrace '}'
	{ // 1039
		$$ = &variantType{$1.p(), nil}
		p.fixlbrace($2)
	}

InterfaceType:
	INTERFACE LBrace InterfaceDeclList oSemi '}'
	{ // 1046
		x := newInterfaceType(p, $2.p(), $3)
		x.pos = $1.p()
		$$ = x
		p.fixlbrace($2)
	}
|	INTERFACE LBrace '}'
	{ // 1053
		$$ = &interfaceType{pos: $1.p()}
		p.fixlbrace($2)
	}

FuncDecl:
	FUNC FuncDecl1 FuncBody
	{ // 1060
		x := $2.(*funcDecl)
		x.pos, x.body = $1.p(), $3
		$$ = x
		p.checkLabels()
		p.popScope()
	}

FuncDecl1:
	Symbol '(' oArgTypeListOComma ')' FuncResult
	{ // 1070
		x := &funcDecl{name: $1.(*ident), typ: newFuncType(p, $2.p(), $2.p(), nil, $3, $5)}
		x.declaredIn = x.typ.scope
		$$ = x
		p.curFnScope = x.declaredIn
	}
|	'(' oArgTypeListOComma ')' Symbol '(' oArgTypeListOComma ')' FuncResult
	{ // 1077
		x := &funcDecl{name: $4.(*ident), typ: newFuncType(p, $1.p(), $1.p(), $2, $6, $8)}
		x.declaredIn = x.typ.scope
		$$ = x
		p.curFnScope = x.declaredIn
	}

FuncType:
	FUNC '(' oArgTypeListOComma ')' FuncResult
	{ // 1086
		$$ = newFuncType(p, $1.p(), $1.p(), nil, $3, $5)
		p.popScope()
	}

FuncBody:
	{ // 1092
		$$ = nil
	}
|	'{' StmtList '}'
	{ // 1096
		$$ = $2
	}

FuncResult:
	%prec notParen
	{ // 1102
		$$ = nil
	}
|	FuncRetType
	{ // 1106
		$$ = []*param{{pos: $1.p(), typ: $1}}
	}
|	'(' oArgTypeListOComma ')'
	{ // 1110
		$$ = $2
	}

FuncLitDecl:
	FuncType
	{ // 1116
		x := $1.(*funcType)
		p.currentScope = x.scope
		x.prevFnScope = p.curFnScope
		p.curFnScope = x.scope
		p.fnLitStack = append(p.fnLitStack, p.inFor, p.inIf, p.inSwitch)
		p.inFor, p.inIf, p.inSwitch = 0, 0, 0
	}

FuncLit:
	FuncLitDecl LBrace StmtList '}'
	{ // 1127
		x := $1.(*funcType)
		f := &funcLit{x.p(), x, $3, x.scope}
		$$ = f
		p.fixlbrace($2)
		p.checkLabels()
		p.curFnScope = x.prevFnScope
		p.popScope()
		n := len(p.fnLitStack)-3
		p.inFor = p.fnLitStack[n]
		p.inIf = p.fnLitStack[n+1]
		p.inSwitch = p.fnLitStack[n+2]
		p.fnLitStack = p.fnLitStack[:n]
	}
|	FuncLitDecl error
	{ // 1142
		p.popScope()
		n := len(p.fnLitStack)-3
		p.inFor = p.fnLitStack[n]
		p.inIf = p.fnLitStack[n+1]
		p.inSwitch = p.fnLitStack[n+2]
		p.fnLitStack = p.fnLitStack[:n]
	}

TopLevelDeclList:
|	TopLevelDeclList TopLevelDecl ';'

VarDeclList:
	VarDecl
	{ // 1156
		$$ = $1
	}
|	VarDeclList ';' VarDecl
	{ // 1160
		$$ = append($1, $3...)
	}

ConstDeclList:
	ConstDecl1
	{ // 1166
		$$ = []node{$1}
	}
|	ConstDeclList ';' ConstDecl1
	{ // 1170
		$$ = append($1, $3)
	}

TypeDeclList:
	TypeDecl
	{ // 1176
		$$ = []node{$1}
	}
|	TypeDeclList ';' TypeDecl
	{ // 1180
		$$ = append($1, $3)
	}

StructDeclList:
	StructDecl
	{ // 1186
		$$ = []node{$1}
	}
|	StructDeclList ';' StructDecl
	{ // 1190
		$$ = append($1, $3)
	}

InterfaceDeclList:
	InterfaceDecl
	{ // 1196
		$$ = []node{$1}
	}
|	InterfaceDeclList ';' InterfaceDecl
	{ // 1200
		$$ = append($1, $3)
	}

StructDecl:
	NewNameList Type oLiteral
	{ // 1206
		$$ = newFields(p, $1, false, $2, $3)
	}
|	Embedded oLiteral
	{ // 1210
		q := $1.(*qualifiedIdent)
		$$ = newFields(p, []node{q.id}, true, &namedType{pos: q.p(), name: q}, $2)
	}
|	'(' Embedded ')' oLiteral
	{ // 1215
		p.errNode($1, "cannot parenthesize embedded type")
		$$ = &fields{}
	}
|	'*' Embedded oLiteral
	{ // 1220
		q := $2.(*qualifiedIdent)
		$$ = newFields(p, []node{q.id}, true, &ptrType{pos: $1.p(), typ: &namedType{pos: q.p(), name: q}}, $3)
	}
|	'(' '*' Embedded ')' oLiteral
	{ // 1225
		p.errNode($1, "cannot parenthesize embedded type")
		$$ = &fields{}
	}
|	'*' '(' Embedded ')' oLiteral
	{ // 1230
		p.errNode($1, "cannot parenthesize embedded type")
		$$ = &fields{}
	}

Qualident:
	IDENT
	{ // 1237
		$$ = &qualifiedIdent{$1.p(), nil, &ident{$1.p(), $1.lit, p.currentScope}}
	}
|	IDENT '.' Symbol
	{ // 1241
		$$ = &qualifiedIdent{$1.p(), &ident{$1.p(), $1.lit, p.currentScope}, $3.(*ident)}
	}

Embedded:
	Qualident

InterfaceDecl:
	NewName InterfaceMethodDecl
	{ // 1250
		$$ = &methodSpec{$1.p(), &qualifiedIdent{$1.p(), nil, $1.(*ident)}, $2.(*funcType)}
	}
|	Qualident
	{ // 1254
		$$ = &methodSpec{pos: $1.p(), typ: &namedType{pos: $1.p(), name: $1.(*qualifiedIdent)}}
	}
|	'(' Qualident ')'
	{ // 1258
		$$ = &methodSpec{pos: $2.p()}
		p.errNode($2, "cannot parenthesize embedded type");
	}

InterfaceMethodDecl:
	'(' oArgTypeListOComma ')' FuncResult
	{ // 1265
		$$ = newFuncType(p, $1.p(), $1.p(), nil, $2, $4)
		p.popScope()
	}

ArgType:
	NameOrType
	{ // 1272
		$$ = &param{pos: $1.p(), typ: $1}
	}
|	Symbol NameOrType
	{ // 1276
		$$ = &param{pos: $1.p(), name: $1.(*ident), typ: $2}
	}
|	Symbol Ddd
	{ // 1280
		x := $2
		x.name, x.ddd = $1.(*ident), true
		$$ = x
	}
|	Ddd

ArgTypeList:
	ArgType
	{ // 1289
		$$ = []*param{$1}
	}
|	ArgTypeList ',' ArgType
	{ // 1293
		$$ = append($1, $3)
	}

oArgTypeListOComma:
	{ // 1298
		$$ = nil
	}
|	ArgTypeList oComma
	{ // 1302
		$$ = $1
	}

Statement:
	{ // 1307
		$$ = nil
	}
|	CompoundStmt
	{ // 1311
		$$ = []node{$1}
	}
|	CommonDecl
	{ // 1315
		sc := p.currentScope
		var err error
		for _, n := range $1 {
			switch x := n.(type) {
			case *constDecl:
				err = sc.declare(p, x.name, n)
			case *varDecl:
				err = sc.declare(p, x.name, n)
			case *typeDecl:
				err = sc.declare(p, x.name, n)
			default:
				panic("internal error 014")
			}
			if err != nil {
				p.errors = append(p.errors, err.(*scanner.Error))
			}
		}
	}
|	NonDclStmt
	{ // 1335
		$$ = []node{$1}
	}
|	error
	{ // 1339
		$$ = nil
	}

NonDclStmt:
	SimpleStmt
|	ForStmt
|	SwitchStmt
|	SelectStmt
|	IfStmt
|	LabelName ':' Statement
	{ // 1350
		id := $1.(*ident)
		$$ = &labeledStmt{$1.p(), id, $3, false}
		idd := *id
		idd.literal += ":"
		if err := p.curFnScope.declare(p, &idd, $$); err != nil {
			p.errors = append(p.errors, err.(*scanner.Error))
		}
	}
|	FALL
	{ // 1360
		if p.inSwitch == 0 {
			p.errNode($1, "fallthrough statement out of place")
		}
		$$ = &fallthroughStmt{$1.p()}
	}
|	BREAK oNewName
	{ // 1367
		if p.inFor-p.inIf <= 0 && p.inSwitch == 0 {
			p.errNode($1, "break is not in a loop")
		}
		id := $2.(*ident)
		$$ = &breakStmt{$1.p(), $2.(*ident)}
		if id != nil {
			idd := *id
			idd.literal = fmt.Sprintf(":%s.%d", id.literal, id.p())
			p.curFnScope.insert(&idd, $$)
		}
	}
|	CONTINUE oNewName
	{ // 1380
		if p.inFor-p.inIf <= 0 {
			p.errNode($1, "continue is not in a loop")
		}
		id := $2.(*ident)
		$$ = &continueStmt{$1.p(), id}
		if id != nil {
			idd := *id
			idd.literal = fmt.Sprintf(":%s.%d", id.literal, id.p())
			p.curFnScope.insert(&idd, $$)
		}
	}
|	GO PseudoCall
	{ // 1393
		$$ = &goStmt{$1.p(), $2.(*callOp)}
	}
|	DEFER PseudoCall
	{ // 1397
		$$ = &deferStmt{$1.p(), $2.(*callOp)}
	}
|	GOTO NewName
	{ // 1401
		id := $2.(*ident)
		$$ = &gotoStmt{$1.p(), id}
		idd := *id
		idd.literal = fmt.Sprintf(":%s.%d", id.literal, id.p())
		p.curFnScope.insert(&idd, $$)
	}
|	GOTO
	{ // 1409
		p.errNode($1, "syntax error: unexpected semicolon or newline, expecting name")
		$$ = &gotoStmt{$1.p(), nil}
	}
|	RETURN oExprList
	{ // 1414
		$$ = &returnStmt{$1.p(), $2}
	}

StmtList:
	Statement
|	StmtList ';' Statement
	{ // 1421
		$$ = append($1, $3...)
	}

// field names of a struct type definition
NewNameList:
	NewName
	{ // 1428
		$$ = []node{$1}
	}
|	NewNameList ',' NewName
	{ // 1432
		$$ = append($1, $3)
	}

// identifier list of the var or const declarations
DeclNameList:
	DeclName
	{ // 1439
		$$ = []node{$1}
	}
|	DeclNameList ',' DeclName
	{ // 1443
		$$ = append($1, $3)
	}

ExprList:
	Expr
	{ // 1449
		$$ = []node{$1}
	}
|	ExprList ',' Expr
	{ // 1453
		$$ = append($1, $3)
	}

ExprOrTypeList:
	ExprOrType
	{ // 1459
		$$ = []node{$1}
	}
|	ExprOrTypeList ',' ExprOrType
	{ // 1463
		$$ = append($1, $3)
	}

KeyvalList:
	Keyval
	{ // 1469
		$$ = []node{$1}
	}
|	BareCompLitExpr
	{ // 1473
		$$ = []node{$1}
	}
|	KeyvalList ',' Keyval
	{ // 1477
		$$ = append($1, $3)
	}
|	KeyvalList ',' BareCompLitExpr
	{ // 1481
		$$ = append($1, $3)
	}

BracedKeyvalList:
	{ // 1486
		$$ = nil
	}
|	KeyvalList oComma
	{ // 1490
		$$ = $1
	}

oSemi:
|	';'

oComma:
|	','

oExpr:
	{ // 1501
		$$ = nil
	}
|	Expr

oExprList:
	{ // 1507
		$$ = nil
	}
|	ExprList

oSimpleStmt:
	{ // 1513
		$$ = nil
	}
|	SimpleStmt

oLiteral:
	{ // 1519
		$$ = node(nil)
	}
|	LITERAL
	{ // 1523
		$$ = newLiteral(p, $1.p(), $1.tok, $1.lit)
	}
