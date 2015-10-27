package optimizer
import "github.com/pingcap/tidb/ast"

// typeComputer is an ast Visitor that
// Compute types for ast.ExprNode.
type typeComputer struct {
	err error
}

func (v *typeComputer) Enter(in ast.Node) (out ast.Node, skipChildren bool) {
	return
}

func (v *typeComputer) Leave(in ast.Node) (out ast.Node, ok bool) {
	return
}