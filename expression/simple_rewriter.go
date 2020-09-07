// Copyright 2018 PingCAP, Inc.
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

package expression

import (
	"context"

	"github.com/pingcap/errors"
	"github.com/pingcap/parser"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util"
)

// ParseSimpleExprWithTableInfo parses simple expression string to Expression.
// The expression string must only reference the column in table Info.
func ParseSimpleExprWithTableInfo(ctx sessionctx.Context, exprStr string, tableInfo *model.TableInfo) (Expression, error) {
	exprStr = "select " + exprStr
	var stmts []ast.StmtNode
	var err error
	var warns []error
	if p, ok := ctx.(interface {
		ParseSQL(context.Context, string, string, string) ([]ast.StmtNode, []error, error)
	}); ok {
		stmts, warns, err = p.ParseSQL(context.Background(), exprStr, "", "")
	} else {
		stmts, warns, err = parser.New().Parse(exprStr, "", "")
	}
	for _, warn := range warns {
		ctx.GetSessionVars().StmtCtx.AppendWarning(util.SyntaxWarn(warn))
	}

	if err != nil {
		return nil, errors.Trace(err)
	}
	expr := stmts[0].(*ast.SelectStmt).Fields.Fields[0].Expr
	return RewriteSimpleExprWithTableInfo(ctx, tableInfo, expr)
}

// ParseSimpleExprCastWithTableInfo parses simple expression string to Expression.
// And the expr returns will cast to the target type.
func ParseSimpleExprCastWithTableInfo(ctx sessionctx.Context, exprStr string, tableInfo *model.TableInfo, targetFt *types.FieldType) (Expression, error) {
	e, err := ParseSimpleExprWithTableInfo(ctx, exprStr, tableInfo)
	if err != nil {
		return nil, err
	}
	e = BuildCastFunction(ctx, e, targetFt)
	return e, nil
}

// RewriteSimpleExprWithTableInfo rewrites simple ast.ExprNode to expression.Expression.
func RewriteSimpleExprWithTableInfo(ctx sessionctx.Context, tbl *model.TableInfo, expr ast.ExprNode) (Expression, error) {
	dbName := model.NewCIStr(ctx.GetSessionVars().CurrentDB)
	columns, names, err := ColumnInfos2ColumnsAndNames(ctx, dbName, tbl.Name, tbl.Cols(), tbl)
	if err != nil {
		return nil, err
	}
	e, err := RewriteAstExpr(ctx, expr, NewSchema(columns...), names)
	if err != nil {
		return nil, err
	}
	return e, nil
}

// ParseSimpleExprsWithNames parses simple expression string to Expression.
// The expression string must only reference the column in the given NameSlice.
func ParseSimpleExprsWithNames(ctx sessionctx.Context, exprStr string, schema *Schema, names types.NameSlice) ([]Expression, error) {
	exprStr = "select " + exprStr
	var stmts []ast.StmtNode
	var err error
	var warns []error
	if p, ok := ctx.(interface {
		ParseSQL(context.Context, string, string, string) ([]ast.StmtNode, []error, error)
	}); ok {
		stmts, warns, err = p.ParseSQL(context.Background(), exprStr, "", "")
	} else {
		stmts, warns, err = parser.New().Parse(exprStr, "", "")
	}
	if err != nil {
		return nil, util.SyntaxWarn(err)
	}
	for _, warn := range warns {
		ctx.GetSessionVars().StmtCtx.AppendWarning(util.SyntaxWarn(warn))
	}

	fields := stmts[0].(*ast.SelectStmt).Fields.Fields
	exprs := make([]Expression, 0, len(fields))
	for _, field := range fields {
		expr, err := RewriteSimpleExprWithNames(ctx, field.Expr, schema, names)
		if err != nil {
			return nil, err
		}
		exprs = append(exprs, expr)
	}
	return exprs, nil
}

// RewriteSimpleExprWithNames rewrites simple ast.ExprNode to expression.Expression.
func RewriteSimpleExprWithNames(ctx sessionctx.Context, expr ast.ExprNode, schema *Schema, names []*types.FieldName) (Expression, error) {
	e, err := RewriteAstExpr(ctx, expr, schema, names)
	if err != nil {
		return nil, err
	}
	return e, nil
}

// FindFieldName finds the column name from NameSlice.
func FindFieldName(names types.NameSlice, astCol *ast.ColumnName) (int, error) {
	dbName, tblName, colName := astCol.Schema, astCol.Table, astCol.Name
	idx := -1
	for i, name := range names {
		if (dbName.L == "" || dbName.L == name.DBName.L) &&
			(tblName.L == "" || tblName.L == name.TblName.L) &&
			(colName.L == name.ColName.L) {
			if idx == -1 {
				idx = i
			} else {
				return -1, errNonUniq.GenWithStackByArgs(name.String(), "field list")
			}
		}
	}
	return idx, nil
}

// FindFieldNameIdxByColName finds the index of corresponding name in the given slice. -1 for not found.
func FindFieldNameIdxByColName(names []*types.FieldName, colName string) int {
	for i, name := range names {
		if name.ColName.L == colName {
			return i
		}
	}
	return -1
}
