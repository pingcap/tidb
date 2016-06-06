package ast

import (
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/mysql"
)

var _ = Suite(&testFunctionsSuite{})

type testFunctionsSuite struct {
}

func (ts *testFunctionsSuite) TestAggregateFuncExtractor(c *C) {
	var expr Node

	extractor := &AggregateFuncExtractor{}
	expr = &AggregateFuncExpr{}
	expr.Accept(extractor)
	c.Assert(extractor.AggFuncs, HasLen, 1)
	c.Assert(extractor.AggFuncs[0], Equals, expr)

	extractor = &AggregateFuncExtractor{}
	expr = &FuncCallExpr{
		FnName: model.NewCIStr("DATE_ARITH"),
	}
	expr.Accept(extractor)
	c.Assert(extractor.AggFuncs, HasLen, 0)

	extractor = &AggregateFuncExtractor{}
	r := &AggregateFuncExpr{}
	expr = &BinaryOperationExpr{
		L: &FuncCallExpr{
			FnName: model.NewCIStr("DATE_ARITH"),
		},
		R: r,
	}
	expr.Accept(extractor)
	c.Assert(extractor.AggFuncs, HasLen, 1)
	c.Assert(extractor.AggFuncs[0], Equals, r)

	// convert ColumnNameExpr to AggregateFuncExpr
	extractor = &AggregateFuncExtractor{}
	expr = &ColumnNameExpr{}
	f := &SelectField{Expr: expr.(ExprNode)}
	f.Accept(extractor)
	c.Assert(extractor.AggFuncs, HasLen, 1)
	e := extractor.AggFuncs[0]
	c.Assert(e, NotNil)
	c.Assert(e.F, Equals, AggFuncFirstRow)

	// select exists(select count(c) from t) from t
	// subquery contains aggregate function
	expr1 := &AggregateFuncExpr{}
	field1 := &SelectField{Expr: expr1}
	fields1 := &FieldList{Fields: []*SelectField{field1}}
	subSel := &SelectStmt{Fields: fields1}

	subExpr := &ExistsSubqueryExpr{
		Sel: &SubqueryExpr{Query: subSel},
	}
	field := &SelectField{Expr: subExpr}
	fields := &FieldList{Fields: []*SelectField{field}}
	sel := &SelectStmt{Fields: fields}

	extractor = &AggregateFuncExtractor{}
	sel.Accept(extractor)
	c.Assert(extractor.AggFuncs, HasLen, 0)
	extractor = &AggregateFuncExtractor{}
	subSel.Accept(extractor)
	c.Assert(extractor.AggFuncs, HasLen, 1)
}

func (ts *testFunctionsSuite) TestAggFuncCount(c *C) {
	args := make([]ExprNode, 1)
	// count with distinct
	agg := &AggregateFuncExpr{
		Args:     args,
		F:        AggFuncCount,
		Distinct: true,
	}
	agg.CurrentGroup = "xx"
	expr := NewValueExpr(1)
	expr1 := NewValueExpr(nil)
	expr2 := NewValueExpr(1)
	exprs := []ExprNode{expr, expr1, expr2}
	for _, e := range exprs {
		args[0] = e
		agg.Update()
	}
	ctx := agg.GetContext()
	c.Assert(ctx.Count, Equals, int64(1))
	// count without distinct
	agg = &AggregateFuncExpr{
		Args: args,
		F:    AggFuncCount,
	}
	agg.CurrentGroup = "xx"
	expr = NewValueExpr(1)
	expr1 = NewValueExpr(nil)
	expr2 = NewValueExpr(1)
	exprs = []ExprNode{expr, expr1, expr2}
	for _, e := range exprs {
		args[0] = e
		agg.Update()
	}
	ctx = agg.GetContext()
	c.Assert(ctx.Count, Equals, int64(2))
}

func (ts *testFunctionsSuite) TestAggFuncSum(c *C) {
	args := make([]ExprNode, 1)
	// sum with distinct
	agg := &AggregateFuncExpr{
		Args:     args,
		F:        AggFuncSum,
		Distinct: true,
	}
	agg.CurrentGroup = "xx"
	expr := NewValueExpr(1)
	expr1 := NewValueExpr(nil)
	expr2 := NewValueExpr(1)
	exprs := []ExprNode{expr, expr1, expr2}
	for _, e := range exprs {
		args[0] = e
		agg.Update()
	}
	ctx := agg.GetContext()
	expect, _ := mysql.ConvertToDecimal(1)
	v, ok := ctx.Value.(mysql.Decimal)
	c.Assert(ok, IsTrue)
	c.Assert(v.Equals(expect), IsTrue)
	// sum without distinct
	agg = &AggregateFuncExpr{
		Args: args,
		F:    AggFuncSum,
	}
	agg.CurrentGroup = "xx"
	expr = NewValueExpr(2)
	expr1 = NewValueExpr(nil)
	expr2 = NewValueExpr(2)
	exprs = []ExprNode{expr, expr1, expr2}
	for _, e := range exprs {
		args[0] = e
		agg.Update()
	}
	ctx = agg.GetContext()
	expect, _ = mysql.ConvertToDecimal(4)
	v, ok = ctx.Value.(mysql.Decimal)
	c.Assert(ok, IsTrue)
	c.Assert(v.Equals(expect), IsTrue)
}
