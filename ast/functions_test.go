package ast

import (
	. "github.com/pingcap/check"
)

var _ = Suite(&testFunctionsSuite{})

type testFunctionsSuite struct {
}

func (ts *testFunctionsSuite) TestAggFunctionDetector(c *C) {
	var expr Node

	ad := &AggFuncDetector{}
	expr = &AggregateFuncExpr{}
	expr.Accept(ad)
	c.Assert(ad.HasAggFunc, IsTrue)

	ad = &AggFuncDetector{}
	expr = &FuncDateArithExpr{}
	expr.Accept(ad)
	c.Assert(ad.HasAggFunc, IsFalse)

	ad = &AggFuncDetector{}
	expr = &BinaryOperationExpr{
		L: &FuncDateArithExpr{},
		R: &AggregateFuncExpr{},
	}
	expr.Accept(ad)
	c.Assert(ad.HasAggFunc, IsTrue)
}
