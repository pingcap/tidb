// +build ignore

package G1

import "go/ast"

func example() {
	_ = ast.BadExpr{From: 123, To: 456} // match
	_ = ast.BadExpr{123, 456}           // no match
	_ = ast.BadExpr{From: 123}          // no match
	_ = ast.BadExpr{To: 456}            // no match
}
