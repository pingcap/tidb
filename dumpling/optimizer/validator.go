package optimizer
import "github.com/pingcap/tidb/ast"

// validator is an ast.Visitor that validates
// ast parsed from parser.
type validator struct {
	err error
}

func (v *validator) Enter(in ast.Node) (out ast.Node, skipChildren bool) {
	return
}

func (v *validator) Leave(in ast.Node) (out ast.Node, ok bool) {
	return
}