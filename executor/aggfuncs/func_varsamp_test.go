package aggfuncs_test

import (
	. "github.com/pingcap/check"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/mysql"
)

func (s *testSuite) TestMergePartialResult4Varsamp(c *C) {
	tests := []aggTest{
		buildAggTester(ast.AggFuncVarSamp, mysql.TypeDouble, 5, 2.5, 1, 1.9821428571428572),
	}
	for _, test := range tests {
		s.testMergePartialResult(c, test)
	}
}

func (s *testSuite) TestVarsamp(c *C) {
	tests := []aggTest{
		buildAggTester(ast.AggFuncVarSamp, mysql.TypeDouble, 5, nil, 2.2222222222222223),
	}
	for _, test := range tests {
		s.testAggFunc(c, test)
	}
}
