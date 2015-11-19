package optimizer

import (
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/optimizer/plan"
)

type valueRange struct {
	low interface{}
	lowExcl bool
	high interface{}
	highExcl bool
}

func buildValueRanges(expr ast.ExprNode) []valueRange {
	switch x := expr.(type) {
	case *ast.BinaryOperationExpr:
		_ = x
	}
	return nil
}

func mergeValueRanges(a, b []valueRange) []valueRange {
	return nil
}

func filterValueRanges(a, b []valueRange) []valueRange {
	return nil
}

func buildIndexRanges(ranges []valueRange) []*plan.IndexRange {
	return nil
}

func appendIndexRanges(origin *plan.IndexRange, ranges []valueRange) []*plan.IndexRange {
	return nil
}