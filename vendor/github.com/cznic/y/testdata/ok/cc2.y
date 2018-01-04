%{

// CAUTION: Generated file, DO NOT EDIT!

%}

%{
// Copyright 2015 The cc Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Based on [0], 6.10.

// CAUTION: Generated file (unless it's pp.y) - DO NOT EDIT!

package cc
%}

%union {
	tok	Token
	toks	PpTokenList
	item	interface{}
}

%token	<item>
	NOELSE

%token	<tok>
	'!'
	'%'
	'&'
	'('
	')'
	'*'
	'+'
	','
	'-'
	'.'
	'/'
	':'
	';'
	'<'
	'='
	'>'
	'?'
	'['
	'\n'
	']'
	'^'
	'{'
	'|'
	'}'
	'~'
	ADDASSIGN
	ANDAND
	ANDASSIGN
	ARROW
	AUTO
	BOOL
	BREAK
	CASE
	CHAR
	CHARCONST
	COMPLEX
	CONST
	CONTINUE
	DDD
	DEC
	DEFAULT
	DIVASSIGN
	DO
	DOUBLE
	ELSE
	ENUM
	EQ
	EXTERN
	FLOAT
	FLOATCONST
	FOR
	GEQ
	GOTO
	IDENTIFIER
	IDENTIFIER_LPAREN
	IF
	INC
	INLINE
	INT
	INTCONST
	LEQ
	LONG
	LONGCHARCONST
	LONGSTRINGLITERAL
	LSH
	LSHASSIGN
	MODASSIGN
	MULASSIGN
	NEQ
	ORASSIGN
	OROR
	PPASSERT
	PPDEFINE
	PPELIF
	PPELSE
	PPENDIF
	PPERROR
	PPHASH_NL
	PPHEADER_NAME
	PPIDENT
	PPIF
	PPIFDEF
	PPIFNDEF
	PPIMPORT
	PPINCLUDE
	PPINCLUDE_NEXT
	PPLINE
	PPNONDIRECTIVE
	PPNUMBER
	PPOTHER
	PPPASTE
	PPPRAGMA
	PPUNASSERT
	PPUNDEF
	PPWARNING
	PREPROCESSINGFILE
	REGISTER
	RESTRICT
	RETURN
	RSH
	RSHASSIGN
	SHORT
	SIGNED
	SIZEOF
	STATIC
	STRINGLITERAL
	STRUCT
	SUBASSIGN
	SWITCH
	TRANSLATIONUNIT
	TYPEDEF
	TYPEDEFNAME
	UNION
	UNSIGNED
	VOID
	VOLATILE
	WHILE
	XORASSIGN

%type	<item>
	AbstractDeclarator
	AbstractDeclaratorOpt
	AdditiveExpression
	AndExpression
	ArgumentExpressionList
	ArgumentExpressionListOpt
	AssignmentExpression
	AssignmentExpressionOpt
	AssignmentOperator
	BlockItem
	BlockItemList
	BlockItemListOpt
	CastExpression
	CompoundStatement
	ConditionalExpression
	Constant
	ConstantExpression
	ControlLine
	Declaration
	DeclarationList
	DeclarationListOpt
	DeclarationSpecifiers
	DeclarationSpecifiersOpt
	Declarator
	DeclaratorOpt
	Designation
	DesignationOpt
	Designator
	DesignatorList
	DirectAbstractDeclarator
	DirectAbstractDeclaratorOpt
	DirectDeclarator
	ElifGroup
	ElifGroupList
	ElifGroupListOpt
	ElseGroup
	ElseGroupOpt
	EndifLine
	EnumSpecifier
	EnumerationConstant
	Enumerator
	EnumeratorList
	EqualityExpression
	ExclusiveOrExpression
	Expression
	ExpressionOpt
	ExpressionStatement
	ExternalDeclaration
	FunctionDefinition
	FunctionSpecifier
	GroupList
	GroupListOpt
	GroupPart
	IdentifierList
	IdentifierListOpt
	IdentifierOpt
	IfGroup
	IfSection
	InclusiveOrExpression
	InitDeclarator
	InitDeclaratorList
	InitDeclaratorListOpt
	Initializer
	InitializerList
	IterationStatement
	JumpStatement
	LabeledStatement
	LogicalAndExpression
	LogicalOrExpression
	MultiplicativeExpression
	ParameterDeclaration
	ParameterList
	ParameterTypeList
	ParameterTypeListOpt
	Pointer
	PointerOpt
	PostfixExpression
	PreprocessingFile
	PrimaryExpression
	RelationalExpression
	SelectionStatement
	ShiftExpression
	SpecifierQualifierList
	SpecifierQualifierListOpt
	Start
	Statement
	StorageClassSpecifier
	StructDeclaration
	StructDeclarationList
	StructDeclarator
	StructDeclaratorList
	StructOrUnion
	StructOrUnionSpecifier
	TranslationUnit
	TypeName
	TypeQualifier
	TypeQualifierList
	TypeQualifierListOpt
	TypeSpecifier
	UnaryExpression
	UnaryOperator

%type	<toks>
	PpTokenList
	PpTokenListOpt
	PpTokens
	ReplacementList
	TextLine

%precedence	NOELSE
%precedence	ELSE

%start Start

%%

AbstractDeclarator:
	Pointer
	{
		$$ = &AbstractDeclarator{
			Pointer: $1.(*Pointer),
		}
	}
|	PointerOpt DirectAbstractDeclarator
	{
		$$ = &AbstractDeclarator{
			Case: 1,
			PointerOpt: $1.(*PointerOpt),
			DirectAbstractDeclarator: $2.(*DirectAbstractDeclarator),
		}
	}

AbstractDeclaratorOpt:
	/* empty */
	{
		$$ = (*AbstractDeclaratorOpt)(nil)
	}
|	AbstractDeclarator
	{
		$$ = &AbstractDeclaratorOpt{
			Case: 1,
			AbstractDeclarator: $1.(*AbstractDeclarator),
		}
	}

AdditiveExpression:
	MultiplicativeExpression
	{
		$$ = &AdditiveExpression{
			MultiplicativeExpression: $1.(*MultiplicativeExpression),
		}
	}
|	AdditiveExpression '+' MultiplicativeExpression
	{
		$$ = &AdditiveExpression{
			Case: 1,
			AdditiveExpression: $1.(*AdditiveExpression),
			Token: $2,
			MultiplicativeExpression: $3.(*MultiplicativeExpression),
		}
	}
|	AdditiveExpression '-' MultiplicativeExpression
	{
		$$ = &AdditiveExpression{
			Case: 2,
			AdditiveExpression: $1.(*AdditiveExpression),
			Token: $2,
			MultiplicativeExpression: $3.(*MultiplicativeExpression),
		}
	}

AndExpression:
	EqualityExpression
	{
		$$ = &AndExpression{
			EqualityExpression: $1.(*EqualityExpression),
		}
	}
|	AndExpression '&' EqualityExpression
	{
		$$ = &AndExpression{
			Case: 1,
			AndExpression: $1.(*AndExpression),
			Token: $2,
			EqualityExpression: $3.(*EqualityExpression),
		}
	}

ArgumentExpressionList:
	AssignmentExpression
	{
		$$ = &ArgumentExpressionList{
			AssignmentExpression: $1.(*AssignmentExpression),
		}
	}
|	ArgumentExpressionList ',' AssignmentExpression
	{
		$$ = &ArgumentExpressionList{
			Case: 1,
			ArgumentExpressionList: $1.(*ArgumentExpressionList),
			Token: $2,
			AssignmentExpression: $3.(*AssignmentExpression),
		}
	}

ArgumentExpressionListOpt:
	/* empty */
	{
		$$ = (*ArgumentExpressionListOpt)(nil)
	}
|	ArgumentExpressionList
	{
		$$ = &ArgumentExpressionListOpt{
			Case: 1,
			ArgumentExpressionList: $1.(*ArgumentExpressionList).reverse(),
		}
	}

AssignmentExpression:
	ConditionalExpression
	{
		$$ = &AssignmentExpression{
			ConditionalExpression: $1.(*ConditionalExpression),
		}
	}
|	UnaryExpression AssignmentOperator AssignmentExpression
	{
		$$ = &AssignmentExpression{
			Case: 1,
			UnaryExpression: $1.(*UnaryExpression),
			AssignmentOperator: $2.(*AssignmentOperator),
			AssignmentExpression: $3.(*AssignmentExpression),
		}
	}

AssignmentExpressionOpt:
	/* empty */
	{
		$$ = (*AssignmentExpressionOpt)(nil)
	}
|	AssignmentExpression
	{
		$$ = &AssignmentExpressionOpt{
			Case: 1,
			AssignmentExpression: $1.(*AssignmentExpression),
		}
	}

AssignmentOperator:
	'='
	{
		$$ = &AssignmentOperator{
			Token: $1,
		}
	}
|	MULASSIGN
	{
		$$ = &AssignmentOperator{
			Case: 1,
			Token: $1,
		}
	}
|	DIVASSIGN
	{
		$$ = &AssignmentOperator{
			Case: 2,
			Token: $1,
		}
	}
|	MODASSIGN
	{
		$$ = &AssignmentOperator{
			Case: 3,
			Token: $1,
		}
	}
|	ADDASSIGN
	{
		$$ = &AssignmentOperator{
			Case: 4,
			Token: $1,
		}
	}
|	SUBASSIGN
	{
		$$ = &AssignmentOperator{
			Case: 5,
			Token: $1,
		}
	}
|	LSHASSIGN
	{
		$$ = &AssignmentOperator{
			Case: 6,
			Token: $1,
		}
	}
|	RSHASSIGN
	{
		$$ = &AssignmentOperator{
			Case: 7,
			Token: $1,
		}
	}
|	ANDASSIGN
	{
		$$ = &AssignmentOperator{
			Case: 8,
			Token: $1,
		}
	}
|	XORASSIGN
	{
		$$ = &AssignmentOperator{
			Case: 9,
			Token: $1,
		}
	}
|	ORASSIGN
	{
		$$ = &AssignmentOperator{
			Case: 10,
			Token: $1,
		}
	}

BlockItem:
	Declaration
	{
		$$ = &BlockItem{
			Declaration: $1.(*Declaration),
		}
	}
|	Statement
	{
		$$ = &BlockItem{
			Case: 1,
			Statement: $1.(*Statement),
		}
	}

BlockItemList:
	BlockItem
	{
		$$ = &BlockItemList{
			BlockItem: $1.(*BlockItem),
		}
	}
|	BlockItemList BlockItem
	{
		$$ = &BlockItemList{
			Case: 1,
			BlockItemList: $1.(*BlockItemList),
			BlockItem: $2.(*BlockItem),
		}
	}

BlockItemListOpt:
	/* empty */
	{
		$$ = (*BlockItemListOpt)(nil)
	}
|	BlockItemList
	{
		$$ = &BlockItemListOpt{
			Case: 1,
			BlockItemList: $1.(*BlockItemList).reverse(),
		}
	}

CastExpression:
	UnaryExpression
	{
		$$ = &CastExpression{
			UnaryExpression: $1.(*UnaryExpression),
		}
	}
|	'(' TypeName ')' CastExpression
	{
		$$ = &CastExpression{
			Case: 1,
			Token: $1,
			TypeName: $2.(*TypeName),
			Token2: $3,
			CastExpression: $4.(*CastExpression),
		}
	}

CompoundStatement:
	'{' BlockItemListOpt '}'
	{
		$$ = &CompoundStatement{
			Token: $1,
			BlockItemListOpt: $2.(*BlockItemListOpt),
			Token2: $3,
		}
	}

ConditionalExpression:
	LogicalOrExpression
	{
		$$ = &ConditionalExpression{
			LogicalOrExpression: $1.(*LogicalOrExpression),
		}
	}
|	LogicalOrExpression '?' Expression ':' ConditionalExpression
	{
		$$ = &ConditionalExpression{
			Case: 1,
			LogicalOrExpression: $1.(*LogicalOrExpression),
			Token: $2,
			Expression: $3.(*Expression),
			Token2: $4,
			ConditionalExpression: $5.(*ConditionalExpression),
		}
	}

Constant:
	CHARCONST
	{
		$$ = &Constant{
			Token: $1,
		}
	}
|	FLOATCONST
	{
		$$ = &Constant{
			Case: 1,
			Token: $1,
		}
	}
|	INTCONST
	{
		$$ = &Constant{
			Case: 2,
			Token: $1,
		}
	}
|	LONGCHARCONST
	{
		$$ = &Constant{
			Case: 3,
			Token: $1,
		}
	}
|	LONGSTRINGLITERAL
	{
		$$ = &Constant{
			Case: 4,
			Token: $1,
		}
	}
|	STRINGLITERAL
	{
		$$ = &Constant{
			Case: 5,
			Token: $1,
		}
	}

ConstantExpression:
	ConditionalExpression
	{
		$$ = &ConstantExpression{
			ConditionalExpression: $1.(*ConditionalExpression),
		}
	}

ControlLine:
	PPDEFINE IDENTIFIER ReplacementList
	{
		$$ = &ControlLine{
			Token: $1,
			Token2: $2,
			ReplacementList: $3,
		}
	}
|	PPDEFINE IDENTIFIER_LPAREN DDD ')' ReplacementList
	{
		$$ = &ControlLine{
			Case: 1,
			Token: $1,
			Token2: $2,
			Token3: $3,
			Token4: $4,
			ReplacementList: $5,
		}
	}
|	PPDEFINE IDENTIFIER_LPAREN IdentifierList ',' DDD ')' ReplacementList
	{
		$$ = &ControlLine{
			Case: 2,
			Token: $1,
			Token2: $2,
			IdentifierList: $3.(*IdentifierList).reverse(),
			Token3: $4,
			Token4: $5,
			Token5: $6,
			ReplacementList: $7,
		}
	}
|	PPDEFINE IDENTIFIER_LPAREN IdentifierListOpt ')' ReplacementList
	{
		$$ = &ControlLine{
			Case: 3,
			Token: $1,
			Token2: $2,
			IdentifierListOpt: $3.(*IdentifierListOpt),
			Token3: $4,
			ReplacementList: $5,
		}
	}
|	PPERROR PpTokenListOpt
	{
		$$ = &ControlLine{
			Case: 4,
			Token: $1,
			PpTokenListOpt: $2,
		}
	}
|	PPHASH_NL
	{
		$$ = &ControlLine{
			Case: 5,
			Token: $1,
		}
	}
|	PPINCLUDE PpTokenList
	{
		$$ = &ControlLine{
			Case: 6,
			Token: $1,
			PpTokenList: $2,
		}
	}
|	PPLINE PpTokenList
	{
		$$ = &ControlLine{
			Case: 7,
			Token: $1,
			PpTokenList: $2,
		}
	}
|	PPPRAGMA PpTokenListOpt
	{
		$$ = &ControlLine{
			Case: 8,
			Token: $1,
			PpTokenListOpt: $2,
		}
	}
|	PPUNDEF IDENTIFIER '\n'
	{
		$$ = &ControlLine{
			Case: 9,
			Token: $1,
			Token2: $2,
			Token3: $3,
		}
	}
|	PPASSERT PpTokenList
	{
		$$ = &ControlLine{
			Case: 10,
			Token: $1,
			PpTokenList: $2,
		}
	}
|	PPDEFINE IDENTIFIER_LPAREN IDENTIFIER DDD ')' ReplacementList
	{
		$$ = &ControlLine{
			Case: 11,
			Token: $1,
			Token2: $2,
			Token3: $3,
			Token4: $4,
			Token5: $5,
			ReplacementList: $6,
		}
	}
|	PPDEFINE IDENTIFIER_LPAREN IdentifierList ',' IDENTIFIER DDD ')' ReplacementList
	{
		$$ = &ControlLine{
			Case: 12,
			Token: $1,
			Token2: $2,
			IdentifierList: $3.(*IdentifierList).reverse(),
			Token3: $4,
			Token4: $5,
			Token5: $6,
			Token6: $7,
			ReplacementList: $8,
		}
	}
|	PPIDENT PpTokenList
	{
		$$ = &ControlLine{
			Case: 13,
			Token: $1,
			PpTokenList: $2,
		}
	}
|	PPIMPORT PpTokenList
	{
		$$ = &ControlLine{
			Case: 14,
			Token: $1,
			PpTokenList: $2,
		}
	}
|	PPINCLUDE_NEXT PpTokenList
	{
		$$ = &ControlLine{
			Case: 15,
			Token: $1,
			PpTokenList: $2,
		}
	}
|	PPUNASSERT PpTokenList
	{
		$$ = &ControlLine{
			Case: 16,
			Token: $1,
			PpTokenList: $2,
		}
	}
|	PPWARNING PpTokenList
	{
		$$ = &ControlLine{
			Case: 17,
			Token: $1,
			PpTokenList: $2,
		}
	}

Declaration:
	DeclarationSpecifiers InitDeclaratorListOpt ';'
	{
		$$ = &Declaration{
			DeclarationSpecifiers: $1.(*DeclarationSpecifiers),
			InitDeclaratorListOpt: $2.(*InitDeclaratorListOpt),
			Token: $3,
		}
	}

DeclarationList:
	Declaration
	{
		$$ = &DeclarationList{
			Declaration: $1.(*Declaration),
		}
	}
|	DeclarationList Declaration
	{
		$$ = &DeclarationList{
			Case: 1,
			DeclarationList: $1.(*DeclarationList),
			Declaration: $2.(*Declaration),
		}
	}

DeclarationListOpt:
	/* empty */
	{
		$$ = (*DeclarationListOpt)(nil)
	}
|	DeclarationList
	{
		$$ = &DeclarationListOpt{
			Case: 1,
			DeclarationList: $1.(*DeclarationList).reverse(),
		}
	}

DeclarationSpecifiers:
	StorageClassSpecifier DeclarationSpecifiersOpt
	{
		$$ = &DeclarationSpecifiers{
			StorageClassSpecifier: $1.(*StorageClassSpecifier),
			DeclarationSpecifiersOpt: $2.(*DeclarationSpecifiersOpt),
		}
	}
|	TypeSpecifier DeclarationSpecifiersOpt
	{
		$$ = &DeclarationSpecifiers{
			Case: 1,
			TypeSpecifier: $1.(*TypeSpecifier),
			DeclarationSpecifiersOpt: $2.(*DeclarationSpecifiersOpt),
		}
	}
|	TypeQualifier DeclarationSpecifiersOpt
	{
		$$ = &DeclarationSpecifiers{
			Case: 2,
			TypeQualifier: $1.(*TypeQualifier),
			DeclarationSpecifiersOpt: $2.(*DeclarationSpecifiersOpt),
		}
	}
|	FunctionSpecifier DeclarationSpecifiersOpt
	{
		$$ = &DeclarationSpecifiers{
			Case: 3,
			FunctionSpecifier: $1.(*FunctionSpecifier),
			DeclarationSpecifiersOpt: $2.(*DeclarationSpecifiersOpt),
		}
	}

DeclarationSpecifiersOpt:
	/* empty */
	{
		$$ = (*DeclarationSpecifiersOpt)(nil)
	}
|	DeclarationSpecifiers
	{
		$$ = &DeclarationSpecifiersOpt{
			Case: 1,
			DeclarationSpecifiers: $1.(*DeclarationSpecifiers),
		}
	}

Declarator:
	PointerOpt DirectDeclarator
	{
		$$ = &Declarator{
			PointerOpt: $1.(*PointerOpt),
			DirectDeclarator: $2.(*DirectDeclarator),
		}
	}

DeclaratorOpt:
	/* empty */
	{
		$$ = (*DeclaratorOpt)(nil)
	}
|	Declarator
	{
		$$ = &DeclaratorOpt{
			Case: 1,
			Declarator: $1.(*Declarator),
		}
	}

Designation:
	DesignatorList '='
	{
		$$ = &Designation{
			DesignatorList: $1.(*DesignatorList).reverse(),
			Token: $2,
		}
	}

DesignationOpt:
	/* empty */
	{
		$$ = (*DesignationOpt)(nil)
	}
|	Designation
	{
		$$ = &DesignationOpt{
			Case: 1,
			Designation: $1.(*Designation),
		}
	}

Designator:
	'[' ConstantExpression ']'
	{
		$$ = &Designator{
			Token: $1,
			ConstantExpression: $2.(*ConstantExpression),
			Token2: $3,
		}
	}
|	'.' IDENTIFIER
	{
		$$ = &Designator{
			Case: 1,
			Token: $1,
			Token2: $2,
		}
	}

DesignatorList:
	Designator
	{
		$$ = &DesignatorList{
			Designator: $1.(*Designator),
		}
	}
|	DesignatorList Designator
	{
		$$ = &DesignatorList{
			Case: 1,
			DesignatorList: $1.(*DesignatorList),
			Designator: $2.(*Designator),
		}
	}

DirectAbstractDeclarator:
	'(' AbstractDeclarator ')'
	{
		$$ = &DirectAbstractDeclarator{
			Token: $1,
			AbstractDeclarator: $2.(*AbstractDeclarator),
			Token2: $3,
		}
	}
|	DirectAbstractDeclaratorOpt '[' AssignmentExpressionOpt ']'
	{
		$$ = &DirectAbstractDeclarator{
			Case: 1,
			DirectAbstractDeclaratorOpt: $1.(*DirectAbstractDeclaratorOpt),
			Token: $2,
			AssignmentExpressionOpt: $3.(*AssignmentExpressionOpt),
			Token2: $4,
		}
	}
|	DirectAbstractDeclaratorOpt '[' TypeQualifierList AssignmentExpressionOpt ']'
	{
		$$ = &DirectAbstractDeclarator{
			Case: 2,
			DirectAbstractDeclaratorOpt: $1.(*DirectAbstractDeclaratorOpt),
			Token: $2,
			TypeQualifierList: $3.(*TypeQualifierList).reverse(),
			AssignmentExpressionOpt: $4.(*AssignmentExpressionOpt),
			Token2: $5,
		}
	}
|	DirectAbstractDeclaratorOpt '[' STATIC TypeQualifierListOpt AssignmentExpression ']'
	{
		$$ = &DirectAbstractDeclarator{
			Case: 3,
			DirectAbstractDeclaratorOpt: $1.(*DirectAbstractDeclaratorOpt),
			Token: $2,
			Token2: $3,
			TypeQualifierListOpt: $4.(*TypeQualifierListOpt),
			AssignmentExpression: $5.(*AssignmentExpression),
			Token3: $6,
		}
	}
|	DirectAbstractDeclaratorOpt '[' TypeQualifierList STATIC AssignmentExpression ']'
	{
		$$ = &DirectAbstractDeclarator{
			Case: 4,
			DirectAbstractDeclaratorOpt: $1.(*DirectAbstractDeclaratorOpt),
			Token: $2,
			TypeQualifierList: $3.(*TypeQualifierList).reverse(),
			Token2: $4,
			AssignmentExpression: $5.(*AssignmentExpression),
			Token3: $6,
		}
	}
|	DirectAbstractDeclaratorOpt '[' '*' ']'
	{
		$$ = &DirectAbstractDeclarator{
			Case: 5,
			DirectAbstractDeclaratorOpt: $1.(*DirectAbstractDeclaratorOpt),
			Token: $2,
			Token2: $3,
			Token3: $4,
		}
	}
|	'(' ParameterTypeListOpt ')'
	{
		$$ = &DirectAbstractDeclarator{
			Case: 6,
			Token: $1,
			ParameterTypeListOpt: $2.(*ParameterTypeListOpt),
			Token2: $3,
		}
	}
|	DirectAbstractDeclarator '(' ParameterTypeListOpt ')'
	{
		$$ = &DirectAbstractDeclarator{
			Case: 7,
			DirectAbstractDeclarator: $1.(*DirectAbstractDeclarator),
			Token: $2,
			ParameterTypeListOpt: $3.(*ParameterTypeListOpt),
			Token2: $4,
		}
	}

DirectAbstractDeclaratorOpt:
	/* empty */
	{
		$$ = (*DirectAbstractDeclaratorOpt)(nil)
	}
|	DirectAbstractDeclarator
	{
		$$ = &DirectAbstractDeclaratorOpt{
			Case: 1,
			DirectAbstractDeclarator: $1.(*DirectAbstractDeclarator),
		}
	}

DirectDeclarator:
	IDENTIFIER
	{
		$$ = &DirectDeclarator{
			Token: $1,
		}
	}
|	'(' Declarator ')'
	{
		$$ = &DirectDeclarator{
			Case: 1,
			Token: $1,
			Declarator: $2.(*Declarator),
			Token2: $3,
		}
	}
|	DirectDeclarator '[' TypeQualifierListOpt AssignmentExpressionOpt ']'
	{
		$$ = &DirectDeclarator{
			Case: 2,
			DirectDeclarator: $1.(*DirectDeclarator),
			Token: $2,
			TypeQualifierListOpt: $3.(*TypeQualifierListOpt),
			AssignmentExpressionOpt: $4.(*AssignmentExpressionOpt),
			Token2: $5,
		}
	}
|	DirectDeclarator '[' STATIC TypeQualifierListOpt AssignmentExpression ']'
	{
		$$ = &DirectDeclarator{
			Case: 3,
			DirectDeclarator: $1.(*DirectDeclarator),
			Token: $2,
			Token2: $3,
			TypeQualifierListOpt: $4.(*TypeQualifierListOpt),
			AssignmentExpression: $5.(*AssignmentExpression),
			Token3: $6,
		}
	}
|	DirectDeclarator '[' TypeQualifierList STATIC AssignmentExpression ']'
	{
		$$ = &DirectDeclarator{
			Case: 4,
			DirectDeclarator: $1.(*DirectDeclarator),
			Token: $2,
			TypeQualifierList: $3.(*TypeQualifierList).reverse(),
			Token2: $4,
			AssignmentExpression: $5.(*AssignmentExpression),
			Token3: $6,
		}
	}
|	DirectDeclarator '[' TypeQualifierListOpt '*' ']'
	{
		$$ = &DirectDeclarator{
			Case: 5,
			DirectDeclarator: $1.(*DirectDeclarator),
			Token: $2,
			TypeQualifierListOpt: $3.(*TypeQualifierListOpt),
			Token2: $4,
			Token3: $5,
		}
	}
|	DirectDeclarator '(' ParameterTypeList ')'
	{
		$$ = &DirectDeclarator{
			Case: 6,
			DirectDeclarator: $1.(*DirectDeclarator),
			Token: $2,
			ParameterTypeList: $3.(*ParameterTypeList),
			Token2: $4,
		}
	}
|	DirectDeclarator '(' IdentifierListOpt ')'
	{
		$$ = &DirectDeclarator{
			Case: 7,
			DirectDeclarator: $1.(*DirectDeclarator),
			Token: $2,
			IdentifierListOpt: $3.(*IdentifierListOpt),
			Token2: $4,
		}
	}

ElifGroup:
	PPELIF PpTokenList GroupListOpt
	{
		$$ = &ElifGroup{
			Token: $1,
			PpTokenList: $2,
			GroupListOpt: $3.(*GroupListOpt),
		}
	}

ElifGroupList:
	ElifGroup
	{
		$$ = &ElifGroupList{
			ElifGroup: $1.(*ElifGroup),
		}
	}
|	ElifGroupList ElifGroup
	{
		$$ = &ElifGroupList{
			Case: 1,
			ElifGroupList: $1.(*ElifGroupList),
			ElifGroup: $2.(*ElifGroup),
		}
	}

ElifGroupListOpt:
	/* empty */
	{
		$$ = (*ElifGroupListOpt)(nil)
	}
|	ElifGroupList
	{
		$$ = &ElifGroupListOpt{
			Case: 1,
			ElifGroupList: $1.(*ElifGroupList).reverse(),
		}
	}

ElseGroup:
	PPELSE '\n' GroupListOpt
	{
		$$ = &ElseGroup{
			Token: $1,
			Token2: $2,
			GroupListOpt: $3.(*GroupListOpt),
		}
	}

ElseGroupOpt:
	/* empty */
	{
		$$ = (*ElseGroupOpt)(nil)
	}
|	ElseGroup
	{
		$$ = &ElseGroupOpt{
			Case: 1,
			ElseGroup: $1.(*ElseGroup),
		}
	}

EndifLine:
	PPENDIF PpTokenListOpt
	{
		$$ = &EndifLine{
			Token: $1,
			PpTokenListOpt: $2,
		}
	}

EnumSpecifier:
	ENUM IdentifierOpt '{' EnumeratorList '}'
	{
		$$ = &EnumSpecifier{
			Token: $1,
			IdentifierOpt: $2.(*IdentifierOpt),
			Token2: $3,
			EnumeratorList: $4.(*EnumeratorList).reverse(),
			Token3: $5,
		}
	}
|	ENUM IdentifierOpt '{' EnumeratorList ',' '}'
	{
		$$ = &EnumSpecifier{
			Case: 1,
			Token: $1,
			IdentifierOpt: $2.(*IdentifierOpt),
			Token2: $3,
			EnumeratorList: $4.(*EnumeratorList).reverse(),
			Token3: $5,
			Token4: $6,
		}
	}
|	ENUM IDENTIFIER
	{
		$$ = &EnumSpecifier{
			Case: 2,
			Token: $1,
			Token2: $2,
		}
	}

EnumerationConstant:
	IDENTIFIER
	{
		$$ = &EnumerationConstant{
			Token: $1,
		}
	}

Enumerator:
	EnumerationConstant
	{
		$$ = &Enumerator{
			EnumerationConstant: $1.(*EnumerationConstant),
		}
	}
|	EnumerationConstant '=' ConstantExpression
	{
		$$ = &Enumerator{
			Case: 1,
			EnumerationConstant: $1.(*EnumerationConstant),
			Token: $2,
			ConstantExpression: $3.(*ConstantExpression),
		}
	}

EnumeratorList:
	Enumerator
	{
		$$ = &EnumeratorList{
			Enumerator: $1.(*Enumerator),
		}
	}
|	EnumeratorList ',' Enumerator
	{
		$$ = &EnumeratorList{
			Case: 1,
			EnumeratorList: $1.(*EnumeratorList),
			Token: $2,
			Enumerator: $3.(*Enumerator),
		}
	}

EqualityExpression:
	RelationalExpression
	{
		$$ = &EqualityExpression{
			RelationalExpression: $1.(*RelationalExpression),
		}
	}
|	EqualityExpression EQ RelationalExpression
	{
		$$ = &EqualityExpression{
			Case: 1,
			EqualityExpression: $1.(*EqualityExpression),
			Token: $2,
			RelationalExpression: $3.(*RelationalExpression),
		}
	}
|	EqualityExpression NEQ RelationalExpression
	{
		$$ = &EqualityExpression{
			Case: 2,
			EqualityExpression: $1.(*EqualityExpression),
			Token: $2,
			RelationalExpression: $3.(*RelationalExpression),
		}
	}

ExclusiveOrExpression:
	AndExpression
	{
		$$ = &ExclusiveOrExpression{
			AndExpression: $1.(*AndExpression),
		}
	}
|	ExclusiveOrExpression '^' AndExpression
	{
		$$ = &ExclusiveOrExpression{
			Case: 1,
			ExclusiveOrExpression: $1.(*ExclusiveOrExpression),
			Token: $2,
			AndExpression: $3.(*AndExpression),
		}
	}

Expression:
	AssignmentExpression
	{
		$$ = &Expression{
			AssignmentExpression: $1.(*AssignmentExpression),
		}
	}
|	Expression ',' AssignmentExpression
	{
		$$ = &Expression{
			Case: 1,
			Expression: $1.(*Expression),
			Token: $2,
			AssignmentExpression: $3.(*AssignmentExpression),
		}
	}

ExpressionOpt:
	/* empty */
	{
		$$ = (*ExpressionOpt)(nil)
	}
|	Expression
	{
		$$ = &ExpressionOpt{
			Case: 1,
			Expression: $1.(*Expression),
		}
	}

ExpressionStatement:
	ExpressionOpt ';'
	{
		$$ = &ExpressionStatement{
			ExpressionOpt: $1.(*ExpressionOpt),
			Token: $2,
		}
	}

ExternalDeclaration:
	FunctionDefinition
	{
		$$ = &ExternalDeclaration{
			FunctionDefinition: $1.(*FunctionDefinition),
		}
	}
|	Declaration
	{
		$$ = &ExternalDeclaration{
			Case: 1,
			Declaration: $1.(*Declaration),
		}
	}

FunctionDefinition:
	DeclarationSpecifiers Declarator DeclarationListOpt CompoundStatement
	{
		$$ = &FunctionDefinition{
			DeclarationSpecifiers: $1.(*DeclarationSpecifiers),
			Declarator: $2.(*Declarator),
			DeclarationListOpt: $3.(*DeclarationListOpt),
			CompoundStatement: $4.(*CompoundStatement),
		}
	}

FunctionSpecifier:
	INLINE
	{
		$$ = &FunctionSpecifier{
			Token: $1,
		}
	}

GroupList:
	GroupPart
	{
		//yy:example "i"
				//yy:nogen

		switch e := $1.(*GroupPart); {
		case e != nil:
			$$ = &GroupList{
				GroupPart: e,
			}
		default:
			$$ = (*GroupList)(nil)
		}
	}
|	GroupList GroupPart
	{
		//yy:example "i\nj"
				//yy:nogen

		switch l, e := $1.(*GroupList), $2.(*GroupPart); {
		case e == nil:
			$$ = l
		default:
			$$ = &GroupList{
				GroupList: l,
				GroupPart: e,
			}
		}
	}

GroupListOpt:
	/* empty */
	{
		$$ = (*GroupListOpt)(nil)
	}
|	GroupList
	{
		$$ = &GroupListOpt{
			Case: 1,
			GroupList: $1.(*GroupList).reverse(),
		}
	}

GroupPart:
	ControlLine
	{
		//yy:nogen
		$$ = &GroupPart{
			ControlLine: $1.(*ControlLine),
		}
	}
|	IfSection
	{
		//yy:nogen
		$$ = &GroupPart{
			IfSection: $1.(*IfSection),
		}
	}
|	PPNONDIRECTIVE PpTokenList
	{
		//yy:nogen
		$$ = &GroupPart{
			Token: $1,
			PpTokenList: $2,
		}
	}
|	TextLine
	{
		//yy:example "void main() {}"
				//yy:nogen

		if $1 == 0 {
			$$ = (*GroupPart)(nil)
			break
		}

		$$ = &GroupPart{
			PpTokenList: $1,
		}
	}

IdentifierList:
	IDENTIFIER
	{
		$$ = &IdentifierList{
			Token: $1,
		}
	}
|	IdentifierList ',' IDENTIFIER
	{
		$$ = &IdentifierList{
			Case: 1,
			IdentifierList: $1.(*IdentifierList),
			Token: $2,
			Token2: $3,
		}
	}

IdentifierListOpt:
	/* empty */
	{
		$$ = (*IdentifierListOpt)(nil)
	}
|	IdentifierList
	{
		$$ = &IdentifierListOpt{
			Case: 1,
			IdentifierList: $1.(*IdentifierList).reverse(),
		}
	}

IdentifierOpt:
	/* empty */
	{
		$$ = (*IdentifierOpt)(nil)
	}
|	IDENTIFIER
	{
		$$ = &IdentifierOpt{
			Case: 1,
			Token: $1,
		}
	}

IfGroup:
	PPIF PpTokenList GroupListOpt
	{
		$$ = &IfGroup{
			Token: $1,
			PpTokenList: $2,
			GroupListOpt: $3.(*GroupListOpt),
		}
	}
|	PPIFDEF IDENTIFIER '\n' GroupListOpt
	{
		$$ = &IfGroup{
			Case: 1,
			Token: $1,
			Token2: $2,
			Token3: $3,
			GroupListOpt: $4.(*GroupListOpt),
		}
	}
|	PPIFNDEF IDENTIFIER '\n' GroupListOpt
	{
		$$ = &IfGroup{
			Case: 2,
			Token: $1,
			Token2: $2,
			Token3: $3,
			GroupListOpt: $4.(*GroupListOpt),
		}
	}

IfSection:
	IfGroup ElifGroupListOpt ElseGroupOpt EndifLine
	{
		$$ = &IfSection{
			IfGroup: $1.(*IfGroup),
			ElifGroupListOpt: $2.(*ElifGroupListOpt),
			ElseGroupOpt: $3.(*ElseGroupOpt),
			EndifLine: $4.(*EndifLine),
		}
	}

InclusiveOrExpression:
	ExclusiveOrExpression
	{
		$$ = &InclusiveOrExpression{
			ExclusiveOrExpression: $1.(*ExclusiveOrExpression),
		}
	}
|	InclusiveOrExpression '|' ExclusiveOrExpression
	{
		$$ = &InclusiveOrExpression{
			Case: 1,
			InclusiveOrExpression: $1.(*InclusiveOrExpression),
			Token: $2,
			ExclusiveOrExpression: $3.(*ExclusiveOrExpression),
		}
	}

InitDeclarator:
	Declarator
	{
		$$ = &InitDeclarator{
			Declarator: $1.(*Declarator),
		}
	}
|	Declarator '=' Initializer
	{
		$$ = &InitDeclarator{
			Case: 1,
			Declarator: $1.(*Declarator),
			Token: $2,
			Initializer: $3.(*Initializer),
		}
	}

InitDeclaratorList:
	InitDeclarator
	{
		$$ = &InitDeclaratorList{
			InitDeclarator: $1.(*InitDeclarator),
		}
	}
|	InitDeclaratorList ',' InitDeclarator
	{
		$$ = &InitDeclaratorList{
			Case: 1,
			InitDeclaratorList: $1.(*InitDeclaratorList),
			Token: $2,
			InitDeclarator: $3.(*InitDeclarator),
		}
	}

InitDeclaratorListOpt:
	/* empty */
	{
		$$ = (*InitDeclaratorListOpt)(nil)
	}
|	InitDeclaratorList
	{
		$$ = &InitDeclaratorListOpt{
			Case: 1,
			InitDeclaratorList: $1.(*InitDeclaratorList).reverse(),
		}
	}

Initializer:
	AssignmentExpression
	{
		$$ = &Initializer{
			AssignmentExpression: $1.(*AssignmentExpression),
		}
	}
|	'{' InitializerList '}'
	{
		$$ = &Initializer{
			Case: 1,
			Token: $1,
			InitializerList: $2.(*InitializerList).reverse(),
			Token2: $3,
		}
	}
|	'{' InitializerList ',' '}'
	{
		$$ = &Initializer{
			Case: 2,
			Token: $1,
			InitializerList: $2.(*InitializerList).reverse(),
			Token2: $3,
			Token3: $4,
		}
	}

InitializerList:
	DesignationOpt Initializer
	{
		$$ = &InitializerList{
			DesignationOpt: $1.(*DesignationOpt),
			Initializer: $2.(*Initializer),
		}
	}
|	InitializerList ',' DesignationOpt Initializer
	{
		$$ = &InitializerList{
			Case: 1,
			InitializerList: $1.(*InitializerList),
			Token: $2,
			DesignationOpt: $3.(*DesignationOpt),
			Initializer: $4.(*Initializer),
		}
	}

IterationStatement:
	WHILE '(' Expression ')' Statement
	{
		$$ = &IterationStatement{
			Token: $1,
			Token2: $2,
			Expression: $3.(*Expression),
			Token3: $4,
			Statement: $5.(*Statement),
		}
	}
|	DO Statement WHILE '(' Expression ')' ';'
	{
		$$ = &IterationStatement{
			Case: 1,
			Token: $1,
			Statement: $2.(*Statement),
			Token2: $3,
			Token3: $4,
			Expression: $5.(*Expression),
			Token4: $6,
			Token5: $7,
		}
	}
|	FOR '(' ExpressionOpt ';' ExpressionOpt ';' ExpressionOpt ')' Statement
	{
		$$ = &IterationStatement{
			Case: 2,
			Token: $1,
			Token2: $2,
			ExpressionOpt: $3.(*ExpressionOpt),
			Token3: $4,
			ExpressionOpt2: $5.(*ExpressionOpt),
			Token4: $6,
			ExpressionOpt3: $7.(*ExpressionOpt),
			Token5: $8,
			Statement: $9.(*Statement),
		}
	}
|	FOR '(' Declaration ExpressionOpt ';' ExpressionOpt ')' Statement
	{
		$$ = &IterationStatement{
			Case: 3,
			Token: $1,
			Token2: $2,
			Declaration: $3.(*Declaration),
			ExpressionOpt: $4.(*ExpressionOpt),
			Token3: $5,
			ExpressionOpt2: $6.(*ExpressionOpt),
			Token4: $7,
			Statement: $8.(*Statement),
		}
	}

JumpStatement:
	GOTO IDENTIFIER ';'
	{
		$$ = &JumpStatement{
			Token: $1,
			Token2: $2,
			Token3: $3,
		}
	}
|	CONTINUE ';'
	{
		$$ = &JumpStatement{
			Case: 1,
			Token: $1,
			Token2: $2,
		}
	}
|	BREAK ';'
	{
		$$ = &JumpStatement{
			Case: 2,
			Token: $1,
			Token2: $2,
		}
	}
|	RETURN ExpressionOpt ';'
	{
		$$ = &JumpStatement{
			Case: 3,
			Token: $1,
			ExpressionOpt: $2.(*ExpressionOpt),
			Token2: $3,
		}
	}

LabeledStatement:
	IDENTIFIER ':' Statement
	{
		$$ = &LabeledStatement{
			Token: $1,
			Token2: $2,
			Statement: $3.(*Statement),
		}
	}
|	CASE ConstantExpression ':' Statement
	{
		$$ = &LabeledStatement{
			Case: 1,
			Token: $1,
			ConstantExpression: $2.(*ConstantExpression),
			Token2: $3,
			Statement: $4.(*Statement),
		}
	}
|	DEFAULT ':' Statement
	{
		$$ = &LabeledStatement{
			Case: 2,
			Token: $1,
			Token2: $2,
			Statement: $3.(*Statement),
		}
	}

LogicalAndExpression:
	InclusiveOrExpression
	{
		$$ = &LogicalAndExpression{
			InclusiveOrExpression: $1.(*InclusiveOrExpression),
		}
	}
|	LogicalAndExpression ANDAND InclusiveOrExpression
	{
		$$ = &LogicalAndExpression{
			Case: 1,
			LogicalAndExpression: $1.(*LogicalAndExpression),
			Token: $2,
			InclusiveOrExpression: $3.(*InclusiveOrExpression),
		}
	}

LogicalOrExpression:
	LogicalAndExpression
	{
		$$ = &LogicalOrExpression{
			LogicalAndExpression: $1.(*LogicalAndExpression),
		}
	}
|	LogicalOrExpression OROR LogicalAndExpression
	{
		$$ = &LogicalOrExpression{
			Case: 1,
			LogicalOrExpression: $1.(*LogicalOrExpression),
			Token: $2,
			LogicalAndExpression: $3.(*LogicalAndExpression),
		}
	}

MultiplicativeExpression:
	CastExpression
	{
		$$ = &MultiplicativeExpression{
			CastExpression: $1.(*CastExpression),
		}
	}
|	MultiplicativeExpression '*' CastExpression
	{
		$$ = &MultiplicativeExpression{
			Case: 1,
			MultiplicativeExpression: $1.(*MultiplicativeExpression),
			Token: $2,
			CastExpression: $3.(*CastExpression),
		}
	}
|	MultiplicativeExpression '/' CastExpression
	{
		$$ = &MultiplicativeExpression{
			Case: 2,
			MultiplicativeExpression: $1.(*MultiplicativeExpression),
			Token: $2,
			CastExpression: $3.(*CastExpression),
		}
	}
|	MultiplicativeExpression '%' CastExpression
	{
		$$ = &MultiplicativeExpression{
			Case: 3,
			MultiplicativeExpression: $1.(*MultiplicativeExpression),
			Token: $2,
			CastExpression: $3.(*CastExpression),
		}
	}

ParameterDeclaration:
	DeclarationSpecifiers Declarator
	{
		$$ = &ParameterDeclaration{
			DeclarationSpecifiers: $1.(*DeclarationSpecifiers),
			Declarator: $2.(*Declarator),
		}
	}
|	DeclarationSpecifiers AbstractDeclaratorOpt
	{
		$$ = &ParameterDeclaration{
			Case: 1,
			DeclarationSpecifiers: $1.(*DeclarationSpecifiers),
			AbstractDeclaratorOpt: $2.(*AbstractDeclaratorOpt),
		}
	}

ParameterList:
	ParameterDeclaration
	{
		$$ = &ParameterList{
			ParameterDeclaration: $1.(*ParameterDeclaration),
		}
	}
|	ParameterList ',' ParameterDeclaration
	{
		$$ = &ParameterList{
			Case: 1,
			ParameterList: $1.(*ParameterList),
			Token: $2,
			ParameterDeclaration: $3.(*ParameterDeclaration),
		}
	}

ParameterTypeList:
	ParameterList
	{
		$$ = &ParameterTypeList{
			ParameterList: $1.(*ParameterList).reverse(),
		}
	}
|	ParameterList ',' DDD
	{
		$$ = &ParameterTypeList{
			Case: 1,
			ParameterList: $1.(*ParameterList).reverse(),
			Token: $2,
			Token2: $3,
		}
	}

ParameterTypeListOpt:
	/* empty */
	{
		$$ = (*ParameterTypeListOpt)(nil)
	}
|	ParameterTypeList
	{
		$$ = &ParameterTypeListOpt{
			Case: 1,
			ParameterTypeList: $1.(*ParameterTypeList),
		}
	}

Pointer:
	'*' TypeQualifierListOpt
	{
		$$ = &Pointer{
			Token: $1,
			TypeQualifierListOpt: $2.(*TypeQualifierListOpt),
		}
	}
|	'*' TypeQualifierListOpt Pointer
	{
		$$ = &Pointer{
			Case: 1,
			Token: $1,
			TypeQualifierListOpt: $2.(*TypeQualifierListOpt),
			Pointer: $3.(*Pointer),
		}
	}

PointerOpt:
	/* empty */
	{
		$$ = (*PointerOpt)(nil)
	}
|	Pointer
	{
		$$ = &PointerOpt{
			Case: 1,
			Pointer: $1.(*Pointer),
		}
	}

PostfixExpression:
	PrimaryExpression
	{
		$$ = &PostfixExpression{
			PrimaryExpression: $1.(*PrimaryExpression),
		}
	}
|	PostfixExpression '[' Expression ']'
	{
		$$ = &PostfixExpression{
			Case: 1,
			PostfixExpression: $1.(*PostfixExpression),
			Token: $2,
			Expression: $3.(*Expression),
			Token2: $4,
		}
	}
|	PostfixExpression '(' ArgumentExpressionListOpt ')'
	{
		$$ = &PostfixExpression{
			Case: 2,
			PostfixExpression: $1.(*PostfixExpression),
			Token: $2,
			ArgumentExpressionListOpt: $3.(*ArgumentExpressionListOpt),
			Token2: $4,
		}
	}
|	PostfixExpression '.' IDENTIFIER
	{
		$$ = &PostfixExpression{
			Case: 3,
			PostfixExpression: $1.(*PostfixExpression),
			Token: $2,
			Token2: $3,
		}
	}
|	PostfixExpression ARROW IDENTIFIER
	{
		$$ = &PostfixExpression{
			Case: 4,
			PostfixExpression: $1.(*PostfixExpression),
			Token: $2,
			Token2: $3,
		}
	}
|	PostfixExpression INC
	{
		$$ = &PostfixExpression{
			Case: 5,
			PostfixExpression: $1.(*PostfixExpression),
			Token: $2,
		}
	}
|	PostfixExpression DEC
	{
		$$ = &PostfixExpression{
			Case: 6,
			PostfixExpression: $1.(*PostfixExpression),
			Token: $2,
		}
	}
|	'(' TypeName ')' '{' InitializerList '}'
	{
		$$ = &PostfixExpression{
			Case: 7,
			Token: $1,
			TypeName: $2.(*TypeName),
			Token2: $3,
			Token3: $4,
			InitializerList: $5.(*InitializerList).reverse(),
			Token4: $6,
		}
	}
|	'(' TypeName ')' '{' InitializerList ',' '}'
	{
		$$ = &PostfixExpression{
			Case: 8,
			Token: $1,
			TypeName: $2.(*TypeName),
			Token2: $3,
			Token3: $4,
			InitializerList: $5.(*InitializerList).reverse(),
			Token4: $6,
			Token5: $7,
		}
	}

PpTokenList:
	PpTokens '\n'
	{
		//yy:noexamples
				//yy:nogen

		lx := yylex.(*ppLexer)
		$$ = PpTokenList(lx.zipX)
		toks.putToken(&lx.zipPos, 0, '\n', 0)
	}

PpTokenListOpt:
	'\n'
	{
		//yy:noexamples
				//yy:nogen

		$$ = 0
	}
|	PpTokenList
	{
		//yy:nogen
	}

PpTokens:
	PPOTHER
	{
		//yy:noexamples
				//yy:nogen

		lx := yylex.(*ppLexer)
		lx.zipX = toks.putToken(&lx.zipPos, $1.c.pos(), $1.c.rune(), $1.val)
	}
|	PpTokens PPOTHER
	{
		//yy:nogen
		lx := yylex.(*ppLexer)
		toks.putToken(&lx.zipPos, $2.c.pos(), $2.c.rune(), $2.val)
	}

PreprocessingFile:
	GroupList
	{
		lx := yylex.(*ppLexer)
		lhs :=  &PreprocessingFile{
			GroupList: $1.(*GroupList).reverse(),
		}
		$$ = lhs
		//yy:example "i"
		lx.ast = lhs
	}

PrimaryExpression:
	IDENTIFIER
	{
		$$ = &PrimaryExpression{
			Token: $1,
		}
	}
|	Constant
	{
		$$ = &PrimaryExpression{
			Case: 1,
			Constant: $1.(*Constant),
		}
	}
|	'(' Expression ')'
	{
		$$ = &PrimaryExpression{
			Case: 2,
			Token: $1,
			Expression: $2.(*Expression),
			Token2: $3,
		}
	}

RelationalExpression:
	ShiftExpression
	{
		$$ = &RelationalExpression{
			ShiftExpression: $1.(*ShiftExpression),
		}
	}
|	RelationalExpression '<' ShiftExpression
	{
		$$ = &RelationalExpression{
			Case: 1,
			RelationalExpression: $1.(*RelationalExpression),
			Token: $2,
			ShiftExpression: $3.(*ShiftExpression),
		}
	}
|	RelationalExpression '>' ShiftExpression
	{
		$$ = &RelationalExpression{
			Case: 2,
			RelationalExpression: $1.(*RelationalExpression),
			Token: $2,
			ShiftExpression: $3.(*ShiftExpression),
		}
	}
|	RelationalExpression LEQ ShiftExpression
	{
		$$ = &RelationalExpression{
			Case: 3,
			RelationalExpression: $1.(*RelationalExpression),
			Token: $2,
			ShiftExpression: $3.(*ShiftExpression),
		}
	}
|	RelationalExpression GEQ ShiftExpression
	{
		$$ = &RelationalExpression{
			Case: 4,
			RelationalExpression: $1.(*RelationalExpression),
			Token: $2,
			ShiftExpression: $3.(*ShiftExpression),
		}
	}

ReplacementList:
	PpTokenListOpt
	{
		//yy:noexamples
				//yy:nogen
	}

SelectionStatement:
	IF '(' Expression ')' Statement %prec NOELSE
	{
		$$ = &SelectionStatement{
			Token: $1,
			Token2: $2,
			Expression: $3.(*Expression),
			Token3: $4,
			Statement: $5.(*Statement),
		}
	}
|	IF '(' Expression ')' Statement ELSE Statement
	{
		$$ = &SelectionStatement{
			Case: 1,
			Token: $1,
			Token2: $2,
			Expression: $3.(*Expression),
			Token3: $4,
			Statement: $5.(*Statement),
			Token4: $6,
			Statement2: $7.(*Statement),
		}
	}
|	SWITCH '(' Expression ')' Statement
	{
		$$ = &SelectionStatement{
			Case: 2,
			Token: $1,
			Token2: $2,
			Expression: $3.(*Expression),
			Token3: $4,
			Statement: $5.(*Statement),
		}
	}

ShiftExpression:
	AdditiveExpression
	{
		$$ = &ShiftExpression{
			AdditiveExpression: $1.(*AdditiveExpression),
		}
	}
|	ShiftExpression LSH AdditiveExpression
	{
		$$ = &ShiftExpression{
			Case: 1,
			ShiftExpression: $1.(*ShiftExpression),
			Token: $2,
			AdditiveExpression: $3.(*AdditiveExpression),
		}
	}
|	ShiftExpression RSH AdditiveExpression
	{
		$$ = &ShiftExpression{
			Case: 2,
			ShiftExpression: $1.(*ShiftExpression),
			Token: $2,
			AdditiveExpression: $3.(*AdditiveExpression),
		}
	}

SpecifierQualifierList:
	TypeSpecifier SpecifierQualifierListOpt
	{
		$$ = &SpecifierQualifierList{
			TypeSpecifier: $1.(*TypeSpecifier),
			SpecifierQualifierListOpt: $2.(*SpecifierQualifierListOpt),
		}
	}
|	TypeQualifier SpecifierQualifierListOpt
	{
		$$ = &SpecifierQualifierList{
			Case: 1,
			TypeQualifier: $1.(*TypeQualifier),
			SpecifierQualifierListOpt: $2.(*SpecifierQualifierListOpt),
		}
	}

SpecifierQualifierListOpt:
	/* empty */
	{
		$$ = (*SpecifierQualifierListOpt)(nil)
	}
|	SpecifierQualifierList
	{
		$$ = &SpecifierQualifierListOpt{
			Case: 1,
			SpecifierQualifierList: $1.(*SpecifierQualifierList),
		}
	}

Start:
	PREPROCESSINGFILE PreprocessingFile
	{
		//yy:noexamples
				//yy:nogen
	}
|	TRANSLATIONUNIT TranslationUnit
	{
		//yy:nogen
	}

Statement:
	LabeledStatement
	{
		$$ = &Statement{
			LabeledStatement: $1.(*LabeledStatement),
		}
	}
|	CompoundStatement
	{
		$$ = &Statement{
			Case: 1,
			CompoundStatement: $1.(*CompoundStatement),
		}
	}
|	ExpressionStatement
	{
		$$ = &Statement{
			Case: 2,
			ExpressionStatement: $1.(*ExpressionStatement),
		}
	}
|	SelectionStatement
	{
		$$ = &Statement{
			Case: 3,
			SelectionStatement: $1.(*SelectionStatement),
		}
	}
|	IterationStatement
	{
		$$ = &Statement{
			Case: 4,
			IterationStatement: $1.(*IterationStatement),
		}
	}
|	JumpStatement
	{
		$$ = &Statement{
			Case: 5,
			JumpStatement: $1.(*JumpStatement),
		}
	}

StorageClassSpecifier:
	TYPEDEF
	{
		$$ = &StorageClassSpecifier{
			Token: $1,
		}
	}
|	EXTERN
	{
		$$ = &StorageClassSpecifier{
			Case: 1,
			Token: $1,
		}
	}
|	STATIC
	{
		$$ = &StorageClassSpecifier{
			Case: 2,
			Token: $1,
		}
	}
|	AUTO
	{
		$$ = &StorageClassSpecifier{
			Case: 3,
			Token: $1,
		}
	}
|	REGISTER
	{
		$$ = &StorageClassSpecifier{
			Case: 4,
			Token: $1,
		}
	}

StructDeclaration:
	SpecifierQualifierList StructDeclaratorList ';'
	{
		$$ = &StructDeclaration{
			SpecifierQualifierList: $1.(*SpecifierQualifierList),
			StructDeclaratorList: $2.(*StructDeclaratorList).reverse(),
			Token: $3,
		}
	}

StructDeclarationList:
	StructDeclaration
	{
		$$ = &StructDeclarationList{
			StructDeclaration: $1.(*StructDeclaration),
		}
	}
|	StructDeclarationList StructDeclaration
	{
		$$ = &StructDeclarationList{
			Case: 1,
			StructDeclarationList: $1.(*StructDeclarationList),
			StructDeclaration: $2.(*StructDeclaration),
		}
	}

StructDeclarator:
	Declarator
	{
		$$ = &StructDeclarator{
			Declarator: $1.(*Declarator),
		}
	}
|	DeclaratorOpt ':' ConstantExpression
	{
		$$ = &StructDeclarator{
			Case: 1,
			DeclaratorOpt: $1.(*DeclaratorOpt),
			Token: $2,
			ConstantExpression: $3.(*ConstantExpression),
		}
	}

StructDeclaratorList:
	StructDeclarator
	{
		$$ = &StructDeclaratorList{
			StructDeclarator: $1.(*StructDeclarator),
		}
	}
|	StructDeclaratorList ',' StructDeclarator
	{
		$$ = &StructDeclaratorList{
			Case: 1,
			StructDeclaratorList: $1.(*StructDeclaratorList),
			Token: $2,
			StructDeclarator: $3.(*StructDeclarator),
		}
	}

StructOrUnion:
	STRUCT
	{
		$$ = &StructOrUnion{
			Token: $1,
		}
	}
|	UNION
	{
		$$ = &StructOrUnion{
			Case: 1,
			Token: $1,
		}
	}

StructOrUnionSpecifier:
	StructOrUnion IdentifierOpt '{' StructDeclarationList '}'
	{
		$$ = &StructOrUnionSpecifier{
			StructOrUnion: $1.(*StructOrUnion),
			IdentifierOpt: $2.(*IdentifierOpt),
			Token: $3,
			StructDeclarationList: $4.(*StructDeclarationList).reverse(),
			Token2: $5,
		}
	}
|	StructOrUnion IDENTIFIER
	{
		$$ = &StructOrUnionSpecifier{
			Case: 1,
			StructOrUnion: $1.(*StructOrUnion),
			Token: $2,
		}
	}

TextLine:
	PpTokenListOpt
	{
		//yy:noexamples
				//yy:nogen
	}

TranslationUnit:
	ExternalDeclaration
	{
		$$ = &TranslationUnit{
			ExternalDeclaration: $1.(*ExternalDeclaration),
		}
	}
|	TranslationUnit ExternalDeclaration
	{
		$$ = &TranslationUnit{
			Case: 1,
			TranslationUnit: $1.(*TranslationUnit),
			ExternalDeclaration: $2.(*ExternalDeclaration),
		}
	}

TypeName:
	SpecifierQualifierList AbstractDeclaratorOpt
	{
		$$ = &TypeName{
			SpecifierQualifierList: $1.(*SpecifierQualifierList),
			AbstractDeclaratorOpt: $2.(*AbstractDeclaratorOpt),
		}
	}

TypeQualifier:
	CONST
	{
		$$ = &TypeQualifier{
			Token: $1,
		}
	}
|	RESTRICT
	{
		$$ = &TypeQualifier{
			Case: 1,
			Token: $1,
		}
	}
|	VOLATILE
	{
		$$ = &TypeQualifier{
			Case: 2,
			Token: $1,
		}
	}

TypeQualifierList:
	TypeQualifier
	{
		$$ = &TypeQualifierList{
			TypeQualifier: $1.(*TypeQualifier),
		}
	}
|	TypeQualifierList TypeQualifier
	{
		$$ = &TypeQualifierList{
			Case: 1,
			TypeQualifierList: $1.(*TypeQualifierList),
			TypeQualifier: $2.(*TypeQualifier),
		}
	}

TypeQualifierListOpt:
	/* empty */
	{
		$$ = (*TypeQualifierListOpt)(nil)
	}
|	TypeQualifierList
	{
		$$ = &TypeQualifierListOpt{
			Case: 1,
			TypeQualifierList: $1.(*TypeQualifierList).reverse(),
		}
	}

TypeSpecifier:
	VOID
	{
		$$ = &TypeSpecifier{
			Token: $1,
		}
	}
|	CHAR
	{
		$$ = &TypeSpecifier{
			Case: 1,
			Token: $1,
		}
	}
|	SHORT
	{
		$$ = &TypeSpecifier{
			Case: 2,
			Token: $1,
		}
	}
|	INT
	{
		$$ = &TypeSpecifier{
			Case: 3,
			Token: $1,
		}
	}
|	LONG
	{
		$$ = &TypeSpecifier{
			Case: 4,
			Token: $1,
		}
	}
|	FLOAT
	{
		$$ = &TypeSpecifier{
			Case: 5,
			Token: $1,
		}
	}
|	DOUBLE
	{
		$$ = &TypeSpecifier{
			Case: 6,
			Token: $1,
		}
	}
|	SIGNED
	{
		$$ = &TypeSpecifier{
			Case: 7,
			Token: $1,
		}
	}
|	UNSIGNED
	{
		$$ = &TypeSpecifier{
			Case: 8,
			Token: $1,
		}
	}
|	BOOL
	{
		$$ = &TypeSpecifier{
			Case: 9,
			Token: $1,
		}
	}
|	COMPLEX
	{
		$$ = &TypeSpecifier{
			Case: 10,
			Token: $1,
		}
	}
|	StructOrUnionSpecifier
	{
		$$ = &TypeSpecifier{
			Case: 11,
			StructOrUnionSpecifier: $1.(*StructOrUnionSpecifier),
		}
	}
|	EnumSpecifier
	{
		$$ = &TypeSpecifier{
			Case: 12,
			EnumSpecifier: $1.(*EnumSpecifier),
		}
	}
|	TYPEDEFNAME
	{
		$$ = &TypeSpecifier{
			Case: 13,
			Token: $1,
		}
	}

UnaryExpression:
	PostfixExpression
	{
		$$ = &UnaryExpression{
			PostfixExpression: $1.(*PostfixExpression),
		}
	}
|	INC UnaryExpression
	{
		$$ = &UnaryExpression{
			Case: 1,
			Token: $1,
			UnaryExpression: $2.(*UnaryExpression),
		}
	}
|	DEC UnaryExpression
	{
		$$ = &UnaryExpression{
			Case: 2,
			Token: $1,
			UnaryExpression: $2.(*UnaryExpression),
		}
	}
|	UnaryOperator CastExpression
	{
		$$ = &UnaryExpression{
			Case: 3,
			UnaryOperator: $1.(*UnaryOperator),
			CastExpression: $2.(*CastExpression),
		}
	}
|	SIZEOF UnaryExpression
	{
		$$ = &UnaryExpression{
			Case: 4,
			Token: $1,
			UnaryExpression: $2.(*UnaryExpression),
		}
	}
|	SIZEOF '(' TypeName ')'
	{
		$$ = &UnaryExpression{
			Case: 5,
			Token: $1,
			Token2: $2,
			TypeName: $3.(*TypeName),
			Token3: $4,
		}
	}

UnaryOperator:
	'&'
	{
		$$ = &UnaryOperator{
			Token: $1,
		}
	}
|	'*'
	{
		$$ = &UnaryOperator{
			Case: 1,
			Token: $1,
		}
	}
|	'+'
	{
		$$ = &UnaryOperator{
			Case: 2,
			Token: $1,
		}
	}
|	'-'
	{
		$$ = &UnaryOperator{
			Case: 3,
			Token: $1,
		}
	}
|	'~'
	{
		$$ = &UnaryOperator{
			Case: 4,
			Token: $1,
		}
	}
|	'!'
	{
		$$ = &UnaryOperator{
			Case: 5,
			Token: $1,
		}
	}
