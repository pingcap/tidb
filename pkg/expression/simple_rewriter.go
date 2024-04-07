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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package expression

import (
	"context"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/intest"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
)

// ParseSimpleExprWithTableInfo now is only used by some external repo like tiflow to make sure they are not break.
// Deprecated: please use ParseSimpleExpr instead.
func ParseSimpleExprWithTableInfo(ctx BuildContext, exprStr string, tableInfo *model.TableInfo) (Expression, error) {
	return ParseSimpleExpr(ctx, exprStr, WithTableInfo("", tableInfo))
}

// ParseSimpleExpr parses simple expression string to Expression.
// The expression string must only reference the column in table Info.
func ParseSimpleExpr(ctx BuildContext, exprStr string, opts ...BuildOption) (Expression, error) {
	if exprStr == "" {
		intest.Assert(false)
		// This should never happen. Return a clear error message in case we have some unexpected bugs.
		return nil, errors.New("expression should not be an empty string")
	}
	exprStr = "select " + exprStr
	var stmts []ast.StmtNode
	var err error
	var warns []error
	if p, ok := ctx.(sqlexec.SQLParser); ok {
		stmts, warns, err = p.ParseSQL(context.Background(), exprStr)
	} else {
		stmts, warns, err = parser.New().ParseSQL(exprStr)
	}
	for _, warn := range warns {
		ctx.GetEvalCtx().AppendWarning(util.SyntaxWarn(warn))
	}

	if err != nil {
		return nil, errors.Trace(err)
	}
	expr := stmts[0].(*ast.SelectStmt).Fields.Fields[0].Expr
	return BuildSimpleExpr(ctx, expr, opts...)
}

// FindFieldName finds the column name from NameSlice.
func FindFieldName(names types.NameSlice, astCol *ast.ColumnName) (int, error) {
	dbName, tblName, colName := astCol.Schema, astCol.Table, astCol.Name
	idx := -1
	for i, name := range names {
		if !name.NotExplicitUsable && (dbName.L == "" || dbName.L == name.DBName.L) &&
			(tblName.L == "" || tblName.L == name.TblName.L) &&
			(colName.L == name.ColName.L) {
			if idx != -1 {
				if names[idx].Redundant || name.Redundant {
					if !name.Redundant {
						idx = i
					}
					continue
				}
				return -1, errNonUniq.GenWithStackByArgs(astCol.String(), "field list")
			}
			idx = i
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
