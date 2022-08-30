%{
// Copyright 2022 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package parser

import (
	"strings"

	"github.com/pingcap/tidb/parser/ast"
)

%}

%union {
	offset int // offset
	item interface{}
	ident string
	expr string
	statement string
}

%token	<ident>

	/*yy:token "%c"     */
	pIdentifier "identifier"

	/*yy:token "\"%c\"" */
	pStringLit "string literal"
	pInvalid   "a special token never used by parser, used by lexer to indicate error"
	pAndand    "&&"
	pPipes     "||"

	/* The following tokens belong to parameterizeTokenMap. Notice: make sure these tokens are contained in parameterizeTokenMap. */
	pAs        "AS"
	pAnd       "AND"
	pFrom      "FROM"
	pIntType   "INT"
	pSelectKwd "SELECT"
	pWhere     "WHERE"
	pFalseKwd  "FALSE"
	pTrueKwd   "TRUE"
	pNull      "NULL"
	pOr        "OR"

%token	<item>

	/*yy:token "1.%d"   */
	pFloatLit "floating-point literal"

	/*yy:token "%d"     */
	pIntLit       "integer literal"
	pAndnot       "&^"
	pAssignmentEq ":="
	pEq           "="
	pGe           ">="
	pLe           "<="
	pNeq          "!="
	pNeqSynonym   "<>"
	pNulleq       "<=>"

%type	<expr>
	Expression    "expression"
	BoolPri       "boolean primary expression"
	BitExpr       "bit expression"
	PredicateExpr "Predicate expression factor"
	SimpleExpr    "simple expression"
	SimpleIdent   "Simple Identifier expression"
	StringLiteral "text literal"
	Literal       "literal value"
	TableRef      "table reference"

%type	<statement>
	Statement  "statement"
	SelectStmt "SELECT statement"

%type	<item>
	ColumnName          "column name"
	CompareOp           "Compare opcode"
	EscapedTableRef     "escaped table reference"
	Field               "field expression"
	FieldList           "field expression list"
	StatementList       "statement list"
	SelectStmtFieldList "SELECT statement field list"
	SelectStmtFromTable "SELECT statement from table"
	SelectStmtBasic     "SELECT statement from constant value"
	TableAsName         "table alias name"
	TableAsNameOpt      "table alias name optional"
	TableFactor         "table factor"
	TableName           "Table name"
	TableRefs           "table references"
	TableRefsClause     "Table references clause"
	WhereClause         "WHERE clause"
	WhereClauseOptional "Optional WHERE clause"
	logAnd              "logical and operator"
	logOr               "logical or operator"

%type	<ident>
	Identifier "identifier or unreserved keyword"

%precedence pEmpty
%precedence pLowerThanStringLitToken
%precedence pStringLit
%precedence pSelectKwd
%left pPipes
%left pOr
%left pAndand pAnd
%left '|'
%left '&'
%left '-' '+'
%left '*' '/' '%'

%start	Start

%%

Start:
	StatementList

StatementList:
	Statement
	{
		if $1 != "" {
			s := $1
			parser.result = append(parser.result, s)
		}
	}

ColumnName:
	Identifier
	{
		$$ = $1
	}
|	Identifier '.' Identifier
	{
		var builder strings.Builder
		builder.WriteString($1)
		builder.WriteString(".")
		builder.WriteString($3)
		$$ = builder.String()
	}
|	Identifier '.' Identifier '.' Identifier
	{
		var builder strings.Builder
		builder.WriteString($1)
		builder.WriteString(".")
		builder.WriteString($3)
		builder.WriteString(".")
		builder.WriteString($5)
		$$ = builder.String()
	}

FieldList:
	Field
|	FieldList ',' Field
	{
		var builder strings.Builder
		builder.WriteString($1.(string))
		builder.WriteString(".")
		builder.WriteString($3.(string))
		$$ = builder.String()
	}

Field:
	'*' %prec '*'
	{
		$$ = "*"
	}
|	Identifier '.' '*' %prec '*'
	{
		var builder strings.Builder
		builder.WriteString($1)
		builder.WriteString(".")
		builder.WriteString("*")
		$$ = builder.String()
	}
|	Identifier '.' Identifier '.' '*' %prec '*'
	{
		var builder strings.Builder
		builder.WriteString($1)
		builder.WriteString(".")
		builder.WriteString($3)
		builder.WriteString(".")
		builder.WriteString("*")
		$$ = builder.String()
	}

SelectStmt:
	SelectStmtFromTable
	{
		$$ = $1.(string)
	}

SelectStmtFromTable:
	SelectStmtBasic "FROM" TableRefsClause WhereClauseOptional
	{
		var builder strings.Builder
		builder.WriteString($1.(string))
		builder.WriteString($2)
		builder.WriteString($3.(string))
		builder.WriteString($4.(string))
		$$ = builder.String()
	}

SelectStmtBasic:
	"SELECT" SelectStmtFieldList
	{
		var builder strings.Builder
		builder.WriteString($1)
		builder.WriteString($2.(string))
		$$ = builder.String()
	}

SelectStmtFieldList:
	FieldList
	{
		$$ = $1
	}

TableRefsClause:
	TableRefs

TableRefs:
	EscapedTableRef

EscapedTableRef:
	TableRef
	{
		$$ = $1
	}

TableRef:
	TableFactor
	{
		$$ = $1.(string)
	}

TableFactor:
	TableName TableAsNameOpt
	{
		var builder strings.Builder
		builder.WriteString($1.(string))
		builder.WriteString($2.(string))
		$$ = builder.String()
	}

TableAsNameOpt:
	%prec pEmpty
	{
		$$ = ""
	}
|	TableAsName
	{
		$$ = $1
	}

TableAsName:
	Identifier
	{
		$$ = $1
	}
|	"AS" Identifier
	{
		var builder strings.Builder
		builder.WriteString($1)
		builder.WriteString($2)
		$$ = builder.String()
	}

TableName:
	Identifier
	{
		$$ = $1
	}
|	Identifier '.' Identifier
	{
		var builder strings.Builder
		builder.WriteString($1)
		builder.WriteString(".")
		builder.WriteString($3)
		$$ = builder.String()
	}

WhereClauseOptional:
	{
		$$ = ""
	}
|	WhereClause

WhereClause:
	"WHERE" Expression
	{
		var builder strings.Builder
		builder.WriteString($1)
		builder.WriteString($2)
		$$ = builder.String()
	}

Expression:
	Expression logOr Expression %prec pPipes
	{
		var builder strings.Builder
		builder.WriteString($1)
		builder.WriteString($2.(string))
		builder.WriteString($3)
		$$ = builder.String()
	}
|	Expression logAnd Expression %prec pAndand
	{
		var builder strings.Builder
		builder.WriteString($1)
		builder.WriteString($2.(string))
		builder.WriteString($3)
		$$ = builder.String()
	}
|	BoolPri

BoolPri:
	BoolPri CompareOp PredicateExpr %prec pEq
	{
		var builder strings.Builder
		builder.WriteString($1)
		builder.WriteString($2.(string))
		builder.WriteString($3)
		$$ = builder.String()
	}
|	PredicateExpr

CompareOp:
	">="
	{
		$$ = ">="
	}
|	'>'
	{
		$$ = ">"
	}
|	"<="
	{
		$$ = "<="
	}
|	'<'
	{
		$$ = "<"
	}
|	"!="
	{
		$$ = "!="
	}
|	"<>"
	{
		$$ = "<>"
	}
|	"="
	{
		$$ = "="
	}
|	"<=>"
	{
		$$ = "<=>"
	}

PredicateExpr:
	BitExpr

BitExpr:
	BitExpr '|' BitExpr %prec '|'
	{
		var builder strings.Builder
		builder.WriteString($1)
		builder.WriteString("|")
		builder.WriteString($3)
		$$ = builder.String()
	}
|	BitExpr '&' BitExpr %prec '&'
	{
		var builder strings.Builder
		builder.WriteString($1)
		builder.WriteString("&")
		builder.WriteString($3)
		$$ = builder.String()
	}
|	BitExpr '+' BitExpr %prec '+'
	{
		var builder strings.Builder
		builder.WriteString($1)
		builder.WriteString("+")
		builder.WriteString($3)
		$$ = builder.String()
	}
|	BitExpr '-' BitExpr %prec '-'
	{
		var builder strings.Builder
		builder.WriteString($1)
		builder.WriteString("-")
		builder.WriteString($3)
		$$ = builder.String()
	}
|	BitExpr '*' BitExpr %prec '*'
	{
		var builder strings.Builder
		builder.WriteString($1)
		builder.WriteString("*")
		builder.WriteString($3)
		$$ = builder.String()
	}
|	BitExpr '/' BitExpr %prec '/'
	{
		var builder strings.Builder
		builder.WriteString($1)
		builder.WriteString("/")
		builder.WriteString($3)
		$$ = builder.String()
	}
|	SimpleExpr

SimpleExpr:
	SimpleIdent
	{
		$$ = $1
	}
|	Literal
	{
		$$ = $1
	}

SimpleIdent:
	Identifier
	{
		$$ = $1
	}
|	Identifier '.' Identifier
	{
		var builder strings.Builder
		builder.WriteString($1)
		builder.WriteString(".")
		builder.WriteString($3)
		$$ = builder.String()
	}
|	Identifier '.' Identifier '.' Identifier
	{
		var builder strings.Builder
		builder.WriteString($1)
		builder.WriteString(".")
		builder.WriteString($3)
		builder.WriteString(".")
		builder.WriteString($5)
		$$ = builder.String()
	}

Literal:
	"FALSE"
	{
		s := ast.NewValueExpr(false, parser.charset, parser.collation)
		parser.params = append(parser.params, s)
		$$ = "?"
	}
|	"NULL"
	{
		s := ast.NewValueExpr(nil, parser.charset, parser.collation)
		parser.params = append(parser.params, s)
		$$ = "?"
	}
|	"TRUE"
	{
		s := ast.NewValueExpr(true, parser.charset, parser.collation)
		parser.params = append(parser.params, s)
		$$ = "?"
	}
|	pFloatLit
	{
		s := ast.NewValueExpr($1, parser.charset, parser.collation)
		parser.params = append(parser.params, s)
		$$ = "?"
	}
|	pIntLit
	{
		s := ast.NewValueExpr($1, parser.charset, parser.collation)
		parser.params = append(parser.params, s)
		$$ = "?"
	}
|	StringLiteral %prec pLowerThanStringLitToken

StringLiteral:
	pStringLit
	{
		s := ast.NewValueExpr($1, parser.charset, parser.collation)
		parser.params = append(parser.params, s)
		$$ = "?"
	}

logOr:
	"OR"
	{
		$$ = "OR"
	}

logAnd:
	"&&"
	{
		$$ = "&&"
	}
|	"AND"
	{
		$$ = "AND"
	}

Statement:
	SelectStmt

/**********************************Identifier********************************************/
Identifier:
	pIdentifier
%%
