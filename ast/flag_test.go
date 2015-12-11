package ast_test

import (
	"testing"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/parser"
)

func TestT(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testFlagSuite{})

type testFlagSuite struct {
}

func (ts *testFlagSuite) TestFlag(c *C) {
	cases := []struct {
		expr string
		flag uint64
	}{
		{
			expr: "1 between 0 and 2",
			flag: ast.FlagConstant,
		},
		{
			expr: "case 1 when 1 then 1 else 0 end",
			flag: ast.FlagConstant,
		},
		{
			expr: "case 1 when 1 then 1 else 0 end",
			flag: ast.FlagConstant,
		},
		{
			expr: "1 = ANY (select 1) OR exists (select 1)",
			flag: ast.FlagHasSubquery,
		},
		{
			expr: "1 in (1) or 1 is true or null is null or 'abc' like 'abc' or 'abc' rlike 'abc'",
			flag: ast.FlagConstant,
		},
		{
			expr: "row (1, 1) = row (1, 1)",
			flag: ast.FlagConstant,
		},
		{
			expr: "(1 + a) > ?",
			flag: ast.FlagHasReference | ast.FlagHasParamMarker,
		},
		{
			expr: "trim('abc ')",
			flag: ast.FlagHasFunc,
		},
		{
			expr: "now() + EXTRACT(YEAR FROM '2009-07-02') + CAST(1 AS UNSIGNED)",
			flag: ast.FlagHasFunc,
		},
		{
			expr: "substring('abc', 1)",
			flag: ast.FlagHasFunc,
		},
		{
			expr: "sum(a)",
			flag: ast.FlagHasAggregateFunc | ast.FlagHasReference,
		},
		{
			expr: "(select 1) as a",
			flag: ast.FlagHasSubquery,
		},
		{
			expr: "@auto_commit",
			flag: ast.FlagHasVariable,
		},
	}
	for _, ca := range cases {
		lexer := parser.NewLexer("select " + ca.expr)
		parser.YYParse(lexer)
		stmt := lexer.Stmts()[0].(*ast.SelectStmt)
		ast.SetFlag(stmt)
		expr := stmt.Fields.Fields[0].Expr
		c.Assert(expr.GetFlag(), Equals, ca.flag, Commentf("For %s", ca.expr))
	}
}
