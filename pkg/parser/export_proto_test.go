package parser_test

import (
	"fmt"
	"testing"

	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
)

func TestExportTableParsePrototype(t *testing.T) {
	p := parser.New()
	stmts, _, err := p.ParseSQL("EXPORT TABLE db.t TO 's3://bucket/path' FORMAT 'csv' WITH file_size='256MiB', thread=8, detached")
	if err != nil {
		t.Fatal(err)
	}
	st := stmts[0].(*ast.ExportTableStmt)
	fmt.Printf("table=%s.%s path=%s format=%s opts=%d\n", st.Table.Schema, st.Table.Name, st.Path, *st.Format, len(st.Options))
}
