package aggfuncs_test

import (
	"testing"

	. "github.com/pingcap/check"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/mysql"
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

func BenchmarkVarPop(b *testing.B) {
	s := testSuite{}
	s.SetUpSuite(nil)

	rowNum := 50000
	tests := []aggTest{
		buildAggTester(ast.AggFuncVarPop, mysql.TypeDouble, rowNum, nil, ""),
	}
	for _, test := range tests {
		s.benchmarkAggFunc(b, test)
	}
}
