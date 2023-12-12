package core

import (
	"github.com/pingcap/tidb/pkg/parser/ast"
)

// checkCalcFoundRows returns whether SQL_CALC_FOUND_ROWS exists in the query and
// whether it is placed in the right place.
func checkCalcFoundRows(node ast.Node) (hasCalcFoundRows bool, hasInvalidPlacement bool) {
	checker := &calcFoundRowsPlacementChecker{}
	node.Accept(checker)
	return checker.hasCalcFoundRows, checker.hasInvalidPlacement
}

// calcFoundRowsPlacementChecker checks whether SQL_CALC_FOUND_ROWS exists in the query.
// It also checks whether SQL_CALC_FOUND_ROWS is placed in the right place.
type calcFoundRowsPlacementChecker struct {
	hasCalcFoundRows    bool
	hasInvalidPlacement bool

	currentSelectOffset int
}

func (checker *calcFoundRowsPlacementChecker) Enter(in ast.Node) (out ast.Node, skipChildren bool) {
	switch node := in.(type) {
	case *ast.SelectStmt:
		checker.currentSelectOffset++
		if node.SelectStmtOpts != nil && node.SelectStmtOpts.CalcFoundRows {
			checker.hasCalcFoundRows = true
			if checker.currentSelectOffset != 1 {
				checker.hasInvalidPlacement = true
				return in, true
			}
		}
	}
	return in, false
}

func (checker *calcFoundRowsPlacementChecker) Leave(in ast.Node) (out ast.Node, ok bool) {
	return in, !checker.hasInvalidPlacement
}
