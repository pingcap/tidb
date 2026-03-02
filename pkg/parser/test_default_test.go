package parser

import (
	"testing"

	"github.com/pingcap/tidb/pkg/parser/ast"
)

func TestDefaultExprSyntax(t *testing.T) {
	p := New()
	q := "INSERT INTO t1 (a) SELECT b FROM t2 ON DUPLICATE KEY UPDATE a=DEFAULT(b);"
	stmt, warns, err := p.Parse(q, "", "")
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}
	t.Logf("Parsed successfully: %T, warns: %v", stmt[0], warns)
	ins := stmt[0].(*ast.InsertStmt)
	for _, asgn := range ins.OnDuplicate {
		t.Logf("Assignment Expr: %T", asgn.Expr)
	}
}
