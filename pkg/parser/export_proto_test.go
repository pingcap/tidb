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

func TestExportSchemaParsePrototype(t *testing.T) {
	p := parser.New()
	stmts, _, err := p.ParseSQL("EXPORT SCHEMA db1, db2 TO 's3://bucket/path' FORMAT 'csv' WITH thread=8, detached")
	if err != nil {
		t.Fatal(err)
	}
	st := stmts[0].(*ast.ExportSchemaStmt)
	fmt.Printf("schemas=%v path=%s format=%s opts=%d\n", st.Schemas, st.Path, *st.Format, len(st.Options))

	// "SCHEMA" is already an alias for "DATABASE" (see misc.go's keyword
	// table), so the DATABASE spelling must parse identically.
	stmts, _, err = p.ParseSQL("EXPORT DATABASE db TO 's3://bucket/path'")
	if err != nil {
		t.Fatal(err)
	}
	st = stmts[0].(*ast.ExportSchemaStmt)
	fmt.Printf("schemas=%v path=%s\n", st.Schemas, st.Path)
}
