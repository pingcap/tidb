package core

import (
	"github.com/pingcap/parser/ast"
	"strings"
)

// SetVar expression collector
type SetVarCollectProcessor struct {
	// key: SetVar name, value: true, exists; false, not exists
	// in rewriteVariable.rewriteVariable(),
	//    if the GetVar name in the SetVarMap dont fold
	//    if not, try to fold
	SetVarMap map[string]bool
}

func NewSetVarCollectProcessor() *SetVarCollectProcessor {
	return &SetVarCollectProcessor{
		SetVarMap: make(map[string]bool),
	}
}

func (svv *SetVarCollectProcessor) Enter(inNode ast.Node) (retNode ast.Node, skipChildren bool) {
	switch v := inNode.(type) {
	case *ast.VariableExpr:
		// Variable found
		if v.Value != nil {
			if svv.SetVarMap == nil {
				svv.SetVarMap = make(map[string]bool)
			}
			svv.SetVarMap[strings.ToLower(v.Name)] = true
		}
		return v, false
	case *ast.BinaryOperationExpr:
	case *ast.CompareSubqueryExpr:
		v.L.Accept(svv)
		v.R.Accept(svv)
		return v, false
	case *ast.ExistsSubqueryExpr:
		v.Sel.Accept(svv)
		return v, false
	case *ast.WindowFuncExpr:
		return v, false
	case *ast.PatternInExpr:
		if v.Sel != nil {
			v.Expr.Accept(svv)
			return inNode, false
		}
		// For 10 in ((select * from t)), the parser won't set v.Sel.
		// So we must process this case here.
		x := v.List[0]
		for {
			switch y := x.(type) {
			case *ast.SubqueryExpr:
				v.Sel = y
				v.Expr.Accept(svv)
			case *ast.ParenthesesExpr:
				x = y.Expr
			default:
				return inNode, false
			}
		}
	case *ast.SubqueryExpr:
		v.Query.Accept(svv)
		return inNode, false
	}
	return inNode, false
}

func (svv *SetVarCollectProcessor) Leave(originInNode ast.Node) (retNode ast.Node, ok bool) {
	return originInNode, true
}
