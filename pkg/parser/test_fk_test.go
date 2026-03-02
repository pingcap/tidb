package parser

import (
	"testing"

	"github.com/pingcap/tidb/pkg/parser/ast"
)

func TestFKSubquerySyntax(t *testing.T) {
	p := New()
	q := "select 1 from `child` where `a` is not null and (`a`) not in (select `a` from `parent` ) limit 1"
	stmt, warns, err := p.Parse(q, "", "")
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}
	t.Logf("Parsed successfully: %T, warns: %v", stmt[0], warns)

	// verify the AST
	sel := stmt[0].(*ast.SelectStmt)
	if sel.Limit == nil {
		t.Error("Limit is nil")
	}
}
