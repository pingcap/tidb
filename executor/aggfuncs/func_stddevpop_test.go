package aggfuncs_test

import (
	. "github.com/pingcap/check"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/mysql"
)

func (s *testSuite) TestMergePartialResult4Stddevpop(c *C) {
	tests := []aggTest{
		buildAggTester(ast.AggFuncStddevPop, mysql.TypeDouble, 5, 1.4142135623730951, 0.816496580927726, 1.3169567191065923),
	}
	for _, test := range tests {
		s.testMergePartialResult(c, test)
	}
}

func (s *testSuite) TestStddevpop(c *C) {
	tests := []aggTest{
		buildAggTester(ast.AggFuncStddevPop, mysql.TypeDouble, 5, nil, 1.4142135623730951),
	}
	for _, test := range tests {
		s.testAggFunc(c, test)
	}
}
