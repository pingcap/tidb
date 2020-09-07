package aggfuncs_test

import (
	. "github.com/pingcap/check"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/executor/aggfuncs"
	"github.com/pingcap/tidb/types"
)

func (s *testSuite) TestMergePartialResult4Varpop(c *C) {
	tests := []aggTest{
		buildAggTester(ast.AggFuncVarPop, mysql.TypeDouble, 5, types.NewFloat64Datum(float64(2)), types.NewFloat64Datum(float64(2)/float64(3)), types.NewFloat64Datum(float64(59)/float64(8)-float64(19*19)/float64(8*8))),
	}
	for _, test := range tests {
		s.testMergePartialResult(c, test)
	}

	tests2 := []aggTest{
		buildAggTester(ast.AggFuncVarPop, mysql.TypeDouble, 5, types.NewFloat64Datum(float64(2)), types.NewFloat64Datum(float64(2)/float64(3)), types.NewFloat64Datum(2)),
	}
	for _, test := range tests2 {
		s.testMergePartialResultWithDistinct(c, test)
	}
}

func (s *testSuite) TestVarpop(c *C) {
	tests := []aggTest{
		buildAggTester(ast.AggFuncVarPop, mysql.TypeDouble, 5, nil, types.NewFloat64Datum(float64(2))),
	}
	for _, test := range tests {
		s.testAggFunc(c, test)
	}
}

func (s *testSuite) TestMemVarpop(c *C) {
	tests := []aggMemTest{
		buildAggMemTester(ast.AggFuncVarPop, mysql.TypeDouble, 5,
			aggfuncs.DefPartialResult4VarPopFloat64Size, defaultUpdateMemDeltaGens, false),
		buildAggMemTester(ast.AggFuncVarPop, mysql.TypeDouble, 5,
			aggfuncs.DefPartialResult4VarPopDistinctFloat64Size, distinctUpdateMemDeltaGens, true),
	}
	for _, test := range tests {
		s.testAggMemFunc(c, test)
	}
}
