package aggfuncs_test

import (
	. "github.com/pingcap/check"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/v4/types"
)

func (s *testSuite) TestMergePartialResult4Varpop(c *C) {
	tests := []aggTest{
		buildAggTester(ast.AggFuncVarPop, mysql.TypeDouble, 5, types.NewFloat64Datum(float64(2)), types.NewFloat64Datum(float64(2)/float64(3)), types.NewFloat64Datum(float64(59)/float64(8)-float64(19*19)/float64(8*8))),
	}
	for _, test := range tests {
		s.testMergePartialResult(c, test)
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
