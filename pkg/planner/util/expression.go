// Copyright 2024 PingCAP, Inc.
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

package util

import (
	"context"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
)

// EvalAstExprWithPlanCtx evaluates ast expression with plan context.
// Different with expression.EvalSimpleAst, it uses planner context and is more powerful to build
// some special expressions like subquery, window function, etc.
// If you only want to evaluate simple expressions, use `expression.EvalSimpleAst` instead.
var EvalAstExprWithPlanCtx func(ctx sessionctx.Context, expr ast.ExprNode) (types.Datum, error)

// RewriteAstExprWithPlanCtx rewrites ast expression directly.
// Different with expression.BuildSimpleExpr, it uses planner context and is more powerful to build
// some special expressions like subquery, window function, etc.
// If you only want to build simple expressions, use `expression.BuildSimpleExpr` instead.
var RewriteAstExprWithPlanCtx func(ctx sessionctx.Context, expr ast.ExprNode,
	schema *expression.Schema, names types.NameSlice, allowCastArray bool) (expression.Expression, error)

// ParseExprWithPlanCtx parses expression string to Expression.
// Different with expression.ParseSimpleExpr, it uses planner context and is more powerful to build
// some special expressions like subquery, window function, etc.
// If you only want to build simple expressions, use `expression.ParseSimpleExpr` instead.
func ParseExprWithPlanCtx(ctx sessionctx.Context, exprStr string,
	schema *expression.Schema, names types.NameSlice) (expression.Expression, error) {
	exprStr = "select " + exprStr
	var stmts []ast.StmtNode
	var err error
	var warns []error
	if p, ok := ctx.(sqlexec.SQLParser); ok {
		stmts, warns, err = p.ParseSQL(context.Background(), exprStr)
	} else {
		stmts, warns, err = parser.New().ParseSQL(exprStr)
	}

	if err != nil {
		return nil, errors.Trace(util.SyntaxWarn(err))
	}

	for _, warn := range warns {
		ctx.GetSessionVars().StmtCtx.AppendWarning(util.SyntaxWarn(warn))
	}

	expr := stmts[0].(*ast.SelectStmt).Fields.Fields[0].Expr
	return RewriteAstExprWithPlanCtx(ctx, expr, schema, names, false)
}
