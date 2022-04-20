package fromtidb

import (
	// Import tidb/planner/core to initialize expression.RewriteAstExpr
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/parser/ast"
	_ "github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/table/tables"
	"github.com/pingcap/tidb/types"
)

func RewriteAstExprWrapper(se sessionctx.Context, expr ast.ExprNode, schema *expression.Schema, names types.NameSlice) (expression.Expression, error) {
	return expression.RewriteAstExpr(se, expr, schema, names)
}

func NewCommonRecordCtxAndSetSession(size int, ctx sessionctx.Context) {
	recordCtx := tables.NewCommonAddRecordCtx(size)
	tables.SetAddRecordCtx(ctx, recordCtx)
}
