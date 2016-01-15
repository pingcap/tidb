package plan

import "github.com/pingcap/tidb/ast"

func computeFilterRate(expr ast.ExprNode) float64 {
	switch x := expr.(type) {
	case *ast.BinaryOperationExpr:
		return buildFromBinop(x)
	case *ast.PatternInExpr:
		return r.buildFromIn(x)
	case *ast.ParenthesesExpr:
		return r.build(x.Expr)
	case *ast.BetweenExpr:
		return r.buildFromBetween(x)
	case *ast.IsNullExpr:
		return r.buildFromIsNull(x)
	case *ast.IsTruthExpr:
		return r.buildFromIsTruth(x)
	case *ast.PatternLikeExpr:
		return r.buildFromPatternLike(x)
	case *ast.ColumnNameExpr:
		return r.buildFromColumnName(x)
	}
	computer := filterRateComputer{rate: 1}
	for _, con := range conditions {
		con.Accept(&computer)
	}
	return computer.rate
}

func computeFilterRateInBinop(expr *ast.BinaryOperationExpr) float64 {

}