package core

import (
	"strings"

	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/format"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/types"
	driver "github.com/pingcap/tidb/types/parser_driver"
)

type paramExtractor struct {
	vals []*driver.ValueExpr
}

func (pv *paramExtractor) Enter(in ast.Node) (out ast.Node, skipChildren bool) {
	switch n := in.(type) {
	case *driver.ValueExpr:
		pv.vals = append(pv.vals, n)
		param := ast.NewParamMarkerExpr(len(pv.vals) - 1)
		return param, true
	}
	return in, false
}

func (pv *paramExtractor) Leave(in ast.Node) (out ast.Node, ok bool) {
	return in, true
}

// ParameterizeAST ...
func ParameterizeAST(sctx sessionctx.Context, stmt ast.StmtNode) (paramSQL string, params []expression.Expression, ok bool) {
	if !sctx.GetSessionVars().EnableGeneralPlanCache {
		return "", nil, false
	}
	if !checkStmtNode4GeneralPlanCache(sctx, stmt) {
		return "", nil, false
	}

	pe := new(paramExtractor)
	stmt.Accept(pe)
	for _, v := range pe.vals {
		var rt types.FieldType
		types.DefaultParamTypeForValue(v.Datum.GetValue(), &rt)
		params = append(params, &expression.Constant{
			Value:   *(v.Datum.Clone()),
			RetType: &rt,
		})
	}

	var buf strings.Builder // TODO: buffer pool
	restoreCtx := format.NewRestoreCtx(format.DefaultRestoreFlags, &buf)
	if err := stmt.Restore(restoreCtx); err != nil {
		panic("???")
	}

	return buf.String(), params, true
}

func checkStmtNode4GeneralPlanCache(sctx sessionctx.Context, stmt ast.StmtNode) bool {
	// Enter only support: select {col} from {single-table} where {cond} and {cond} ...
	// 	and {cond} = {col} {op} {val}
	// 	and {op} = >, <, =
	n, ok := stmt.(*ast.SelectStmt)
	if !ok {
		return false
	}
	if len(n.TableHints) > 0 || n.Distinct == true || n.GroupBy != nil || n.Having != nil ||
		n.WindowSpecs != nil || n.OrderBy != nil || n.Limit != nil || n.LockInfo != nil || n.SelectIntoOpt != nil {
		return false
	}
	if n.From == nil || n.From.TableRefs == nil { // not from a general table
		return false
	}
	if n.From.TableRefs.Right != nil { // no join
		return false
	}
	return true
}
