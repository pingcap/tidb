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

	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/opcode"
	"github.com/pingcap/tidb/parser/auth"
	"github.com/pingcap/tidb/parser/charset"
	"github.com/pingcap/tidb/parser/types"
)

%}

%union {
	offset int // offset
	item interface{}
	ident string
	expr ast.ExprNode
	statement ast.StmtNode
}

%token	<ident>
	/*yy:token "%c"     */
	identifier "identifier"

	/*yy:token "\"%c\"" */
	stringLit          "string literal"
	singleAtIdentifier "identifier with single leading at" /* TODO */
	andand             "&&"
	pipes              "||"

	/* TODO: Useless for the ReservedKeyword in parameterizer */
	/* The following tokens belong to ReservedKeyword. Notice: make sure these tokens are contained in ReservedKeyword. */
	as                "AS"
	charType          "CHAR"
	doubleType        "DOUBLE"
	from              "FROM"
	intType           "INT"
	selectKwd         "SELECT"
	where             "WHERE"
	falseKwd          "FALSE"
	trueKwd           "TRUE"
	null              "NULL"

%token	<item>
	/*yy:token "1.%d"   */
	floatLit "floating-point literal"

	/*yy:token "%d"     */
	intLit "integer literal"

	/*yy:token "%b"     */
	bitLit       "bit literal"
	andnot       "&^"
	assignmentEq ":="
	eq           "="
	ge           ">="
	le           "<="
	neq          "!="
	neqSynonym   "<>"
	nulleq       "<=>"

%type	<expr>
	Expression                      "expression"
	BoolPri                         "boolean primary expression"
	PredicateExpr                   "Predicate expression factor"
	BitExpr                         "bit expression"
	SimpleExpr                      "simple expression"
	SimpleIdent                     "Simple Identifier expression"

	Literal                         "literal value"
	StringLiteral                   "text literal"


%type	<statement>

%type	<item>
	ColumnName                             "column name"
	CompareOp                              "Compare opcode"
	EscapedTableRef                        "escaped table reference"
	Field                                  "field expression"
	FieldList                              "field expression list"
	SelectStmtFieldList                    "SELECT statement field list"
	SelectStmtFromTable                    "SELECT statement from table"
	SelectStmtBasic                        "SELECT statement from constant value"
	TableAsName                            "table alias name"
	TableAsNameOpt                         "table alias name optional"
	TableFactor                            "table factor"
	TableName                              "Table name"
	TableRefs                              "table references"
	TableRefsClause                        "Table references clause"

	WhereClause                            "WHERE clause"
	WhereClauseOptional                    "Optional WHERE clause"
	logAnd            "logical and operator"
	logOr             "logical or operator"

%type	<ident>
	Identifier                      "identifier or unreserved keyword"

%precedence empty
%precedence lowerThanStringLitToken
%precedence stringLit
%precedence selectKwd

%left '|'
%left '&'
%left '-' '+'
%left '*' '/' '%' div mod

%%
ColumnName:
	Identifier
	{
		$$ = &ast.ColumnName{Name: model.NewCIStr($1)}
	}
|	Identifier '.' Identifier
	{
		$$ = &ast.ColumnName{Table: model.NewCIStr($1), Name: model.NewCIStr($3)}
	}
|	Identifier '.' Identifier '.' Identifier
	{
		$$ = &ast.ColumnName{Schema: model.NewCIStr($1), Table: model.NewCIStr($3), Name: model.NewCIStr($5)}
	}

FieldList:
	Field
	{
		field := $1.(*ast.SelectField)
		field.Offset = parser.startOffset(&yyS[yypt])
		$$ = []*ast.SelectField{field}
	}
|	FieldList ',' Field
	{
		fl := $1.([]*ast.SelectField)
		last := fl[len(fl)-1]
		if last.Expr != nil && last.AsName.O == "" {
			lastEnd := parser.endOffset(&yyS[yypt-1])
			last.SetText(parser.lexer.client, parser.src[last.Offset:lastEnd])
		}
		newField := $3.(*ast.SelectField)
		newField.Offset = parser.startOffset(&yyS[yypt])
		$$ = append(fl, newField)
	}

Field:
	'*' %prec '*'
	{
		$$ = &ast.SelectField{WildCard: &ast.WildCardField{}}
	}
|	Identifier '.' '*' %prec '*'
	{
		wildCard := &ast.WildCardField{Table: model.NewCIStr($1)}
		$$ = &ast.SelectField{WildCard: wildCard}
	}
|	Identifier '.' Identifier '.' '*' %prec '*'
	{
		wildCard := &ast.WildCardField{Schema: model.NewCIStr($1), Table: model.NewCIStr($3)}
		$$ = &ast.SelectField{WildCard: wildCard}
	}

SelectStmtFromTable:
	SelectStmtBasic "FROM" TableRefsClause WhereClauseOptional
	{
		st := $1.(*ast.SelectStmt)
		st.From = $3.(*ast.TableRefsClause)
		lastField := st.Fields.Fields[len(st.Fields.Fields)-1]
		if lastField.Expr != nil && lastField.AsName.O == "" {
			lastEnd := parser.endOffset(&yyS[yypt-5])
			lastField.SetText(parser.lexer.client, parser.src[lastField.Offset:lastEnd])
		}
		if $4 != nil {
			st.Where = $4.(ast.ExprNode)
		}
		$$ = st
	}

SelectStmtBasic:
	"SELECT" SelectStmtFieldList
	{
		st := &ast.SelectStmt{
			Fields:         $2.(*ast.FieldList),
			Kind:           ast.SelectStmtKindSelect,
		}
		$$ = st
	}


SelectStmtFieldList:
	FieldList
	{
		$$ = &ast.FieldList{Fields: $1.([]*ast.SelectField)}
	}

TableRefsClause:
	TableRefs
	{
		$$ = &ast.TableRefsClause{TableRefs: $1.(*ast.Join)}
	}

TableRefs:
	EscapedTableRef
	{
		$$ = &ast.Join{Left: $1.(ast.ResultSetNode), Right: nil}
	}

EscapedTableRef:
	TableRef

TableRef:
	TableFactor

TableFactor:
	TableName TableAsNameOpt
	{
		tn := $1.(*ast.TableName)
		$$ = &ast.TableSource{Source: tn, AsName: $2.(model.CIStr)}
	}

TableAsNameOpt:
	%prec empty
	{
		$$ = model.CIStr{}
	}
|	TableAsName

TableAsName:
	Identifier
	{
		$$ = model.NewCIStr($1)
	}
|	"AS" Identifier
	{
		$$ = model.NewCIStr($2)
	}

TableName:
	Identifier
	{
		$$ = &ast.TableName{Name: model.NewCIStr($1)}
	}
|	Identifier '.' Identifier
	{
		$$ = &ast.TableName{Schema: model.NewCIStr($1), Name: model.NewCIStr($3)}
	}

WhereClauseOptional:
	{
		$$ = nil
	}
|	WhereClause

WhereClause:
	"WHERE" Expression
	{
		$$ = $2
	}

Expression:
	Expression logOr Expression %prec pipes
	{
		$$ = &ast.BinaryOperationExpr{Op: opcode.LogicOr, L: $1, R: $3}
	}
|	Expression logAnd Expression %prec andand
	{
		$$ = &ast.BinaryOperationExpr{Op: opcode.LogicAnd, L: $1, R: $3}
	}
|	BoolPri

BoolPri:
	BoolPri CompareOp PredicateExpr %prec eq
	{
		$$ = &ast.BinaryOperationExpr{Op: $2.(opcode.Op), L: $1, R: $3}
	}
|	PredicateExpr

CompareOp:
	">="
	{
		$$ = opcode.GE
	}
|	'>'
	{
		$$ = opcode.GT
	}
|	"<="
	{
		$$ = opcode.LE
	}
|	'<'
	{
		$$ = opcode.LT
	}
|	"!="
	{
		$$ = opcode.NE
	}
|	"<>"
	{
		$$ = opcode.NE
	}
|	"="
	{
		$$ = opcode.EQ
	}
|	"<=>"
	{
		$$ = opcode.NullEQ
	}

PredicateExpr:
	BitExpr

BitExpr:
	BitExpr '|' BitExpr %prec '|'
	{
		$$ = &ast.BinaryOperationExpr{Op: opcode.Or, L: $1, R: $3}
	}
|	BitExpr '&' BitExpr %prec '&'
	{
		$$ = &ast.BinaryOperationExpr{Op: opcode.And, L: $1, R: $3}
	}
|	BitExpr '+' BitExpr %prec '+'
	{
		$$ = &ast.BinaryOperationExpr{Op: opcode.Plus, L: $1, R: $3}
	}
|	BitExpr '-' BitExpr %prec '-'
	{
		$$ = &ast.BinaryOperationExpr{Op: opcode.Minus, L: $1, R: $3}
	}
|	BitExpr '*' BitExpr %prec '*'
	{
		$$ = &ast.BinaryOperationExpr{Op: opcode.Mul, L: $1, R: $3}
	}
|	BitExpr '/' BitExpr %prec '/'
	{
		$$ = &ast.BinaryOperationExpr{Op: opcode.Div, L: $1, R: $3}
	}
|	SimpleExpr

SimpleIdent:
	Identifier
	{
		$$ = &ast.ColumnNameExpr{Name: &ast.ColumnName{
			Name: model.NewCIStr($1),
		}}
	}
|	Identifier '.' Identifier
	{
		$$ = &ast.ColumnNameExpr{Name: &ast.ColumnName{
			Table: model.NewCIStr($1),
			Name:  model.NewCIStr($3),
		}}
	}
|	Identifier '.' Identifier '.' Identifier
	{
		$$ = &ast.ColumnNameExpr{Name: &ast.ColumnName{
			Schema: model.NewCIStr($1),
			Table:  model.NewCIStr($3),
			Name:   model.NewCIStr($5),
		}}
	}

SimpleExpr:
	SimpleIdent
|	Literal

Literal:
	"FALSE"
	{
		$$ = ast.NewValueExpr(false, parser.charset, parser.collation)
	}
|	"NULL"
	{
		$$ = ast.NewValueExpr(nil, parser.charset, parser.collation)
	}
|	"TRUE"
	{
		$$ = ast.NewValueExpr(true, parser.charset, parser.collation)
	}
|	floatLit
	{
		$$ = ast.NewValueExpr($1, parser.charset, parser.collation)
	}
|	intLit
	{
		$$ = ast.NewValueExpr($1, parser.charset, parser.collation)
	}
|	StringLiteral %prec lowerThanStringLitToken

StringLiteral:
	stringLit
	{
		expr := ast.NewValueExpr($1, parser.charset, parser.collation)
		$$ = expr
	}

logOr:
	pipesAsOr
|	"OR"

logAnd:
	"&&"
|	"AND"

/**********************************Identifier********************************************/
Identifier:
	identifier


%%
