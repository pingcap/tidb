package parser

import "github.com/pingcap/tidb/pkg/parser/ast"

func hasIntoInResultSet(node ast.ResultSetNode) bool {
	switch sel := node.(type) {
	case *ast.SelectStmt:
		return sel.SelectIntoOpt != nil
	case *ast.SetOprStmt:
		return hasIntoInSetOprSelectList(sel.SelectList)
	}
	return false
}

func hasIntoInSetOprSelectList(list *ast.SetOprSelectList) bool {
	for _, node := range list.Selects {
		switch x := node.(type) {
		case *ast.SelectStmt:
			if x.SelectIntoOpt != nil {
				return true
			}
		case *ast.SetOprSelectList:
			if hasIntoInSetOprSelectList(x) {
				return true
			}
		}
	}
	return false
}
