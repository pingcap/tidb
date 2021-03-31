package aggfuncs_test

import (
	. "github.com/pingcap/check"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/mysql"
)

func (s *testSuite) TestMergePartialResult4Stddevsamp(c *C) {
	tests := []aggTest{
		buildAggTester(ast.AggFuncStddevSamp, mysql.TypeDouble, 5, 1.5811388300841898, 1, 1.407885953173359),
	}
	for _, test := range tests {
		s.testMergePartialResult(c, test)
	}
}

func (s *testSuite) TestStddevsamp(c *C) {
	tests := []aggTest{
		buildAggTester(ast.AggFuncStddevSamp, mysql.TypeDouble, 5, nil, 1.5811388300841898),
	}
	for _, test := range tests {
		s.testAggFunc(c, test)
	}
}
