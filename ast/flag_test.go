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

func (ts *testFlagSuite) TestHasAggFlag(c *C) {
	expr := &ast.BetweenExpr{}
	cases := []struct {
		flag   uint64
		hasAgg bool
	}{
		{ast.FlagHasAggregateFunc, true},
		{ast.FlagHasAggregateFunc | ast.FlagHasVariable, true},
		{ast.FlagHasVariable, false},
	}
	for _, ca := range cases {
		expr.SetFlag(ca.flag)
		c.Assert(ast.HasAggFlag(expr), Equals, ca.hasAgg)
	}
}

func (ts *testFlagSuite) TestFlag(c *C) {
	cases := []struct {
		expr string
		flag uint64
	}{
		{
			"1 between 0 and 2",
			ast.FlagConstant,
		},
		{
			"case 1 when 1 then 1 else 0 end",
			ast.FlagConstant,
		},
		{
			"case 1 when 1 then 1 else 0 end",
			ast.FlagConstant,
		},
		{
			"1 = ANY (select 1) OR exists (select 1)",
			ast.FlagHasSubquery,
		},
		{
			"1 in (1) or 1 is true or null is null or 'abc' like 'abc' or 'abc' rlike 'abc'",
			ast.FlagConstant,
		},
		{
			"row (1, 1) = row (1, 1)",
			ast.FlagConstant,
		},
		{
			"(1 + a) > ?",
			ast.FlagHasReference | ast.FlagHasParamMarker,
		},
		{
			"trim('abc ')",
			ast.FlagHasFunc,
		},
		{
			"now() + EXTRACT(YEAR FROM '2009-07-02') + CAST(1 AS UNSIGNED)",
			ast.FlagHasFunc,
		},
		{
			"substring('abc', 1)",
			ast.FlagHasFunc,
		},
		{
			"sum(a)",
			ast.FlagHasAggregateFunc | ast.FlagHasReference,
		},
		{
			"(select 1) as a",
			ast.FlagHasSubquery,
		},
		{
			"@auto_commit",
			ast.FlagHasVariable,
		},
		{
			"default(a)",
			ast.FlagHasDefault,
		},
	}
	for _, ca := range cases {
		stmt, err := parser.ParseOneStmt("select "+ca.expr, "", "")
		c.Assert(err, IsNil)
		selectStmt := stmt.(*ast.SelectStmt)
		ast.SetFlag(selectStmt)
		expr := selectStmt.Fields.Fields[0].Expr
		c.Assert(expr.GetFlag(), Equals, ca.flag, Commentf("For %s", ca.expr))
	}
}
