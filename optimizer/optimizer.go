package optimizer

import (
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/stmt"
)

// Compile compiles a ast.Node into a executable statement.
func Compile(node ast.Node) (stmt.Statement, error) {
	switch v := node.(type) {
	case *ast.SetStmt:
		return compileSet(v)
	}
	return nil, nil
}

func compileSet(aset *ast.SetStmt) (stmt.Statement, error) {
	return nil, nil
}
