package ast_test

import (
	"bytes"
	"fmt"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/parser"
)

var _ = Suite(&testAstFormatSuite{})

type testAstFormatSuite struct {
}

func getDefaultCharsetAndCollate() (string, string) {
	return "utf8", "utf8_bin"
}

func (ts *testAstFormatSuite) TestAstFormat(c *C) {
	var testcases = []struct {
		input  string
		output string
	}{
		{`"Hello, world"`, `"Hello, world"`},
		{`'Hello, world'`, `"Hello, world"`},
		{`f between 30 and 50`, `f BETWEEN 30 AND 50`},
		{`3 + 5`, `3 + 5`},
		{`"hello world"    >=    'hello world'`, `"hello world" >= "hello world"`},
		// {"123", "123"},
		// {`"hello world"`, `"hello world"`},
		// {`'hello world'`, `'hello world'`},
	}
	for _, tt := range testcases {
		expr := fmt.Sprintf("select %s", tt.input)
		charset, collation := getDefaultCharsetAndCollate()
		stmts, err := parser.New().Parse(expr, charset, collation)
		node := stmts[0].(*ast.SelectStmt).Fields.Fields[0].Expr
		c.Assert(err, IsNil)

		writer := bytes.NewBufferString("")
		node.Format(writer)
		c.Assert(writer.String(), Equals, tt.output)
	}
}
