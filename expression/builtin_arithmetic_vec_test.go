package expression

import (
	"testing"

	. "github.com/pingcap/check"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/tidb/types"
)

var vecBuiltinArithmeticCases = map[string][]vecExprBenchCase{
	ast.Plus: {
		{types.ETInt, []types.EvalType{types.ETInt, types.ETInt}, []dataGenerator{&rangeInt64Gener{0, 100000}, &rangeInt64Gener{0, 100000}}},
	},
}

func (s *testEvaluatorSuite) TestVectorizedBuiltinArithmeticEvalOneVec(c *C) {
	testVectorizedEvalOneVec(c, vecBuiltinArithmeticCases)
}

func (s *testEvaluatorSuite) TestVectorizedBuiltinArithmeticFunc(c *C) {
	testVectorizedBuiltinFunc(c, vecBuiltinArithmeticCases)
}

func BenchmarkVectorizedBuiltinArithmeticEvalOneVec(b *testing.B) {
	benchmarkVectorizedEvalOneVec(b, vecBuiltinArithmeticCases)
}

func BenchmarkVectorizedBuiltinArithmeticFunc(b *testing.B) {
	benchmarkVectorizedBuiltinFunc(b, vecBuiltinArithmeticCases)
}
