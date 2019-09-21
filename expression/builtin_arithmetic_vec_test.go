// Copyright 2019 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package expression

import (
	"github.com/pingcap/tidb/types"
	"math"
	"testing"

	. "github.com/pingcap/check"
	"github.com/pingcap/parser/ast"
)

var vecBuiltinArithmeticCases = map[string][]vecExprBenchCase{
	ast.LE:     {},
	ast.Minus:  {},
	ast.Div:    {},
	ast.IntDiv: {},
	ast.Mod:    {},
	ast.Or:     {},
	ast.Mul:    {},
	ast.Round:  {},
	ast.And:    {},
	ast.Plus: {{types.ETInt, []types.EvalType{types.ETInt, types.ETInt}, []dataGenerator{
		&rangeInt64Gener{begin: math.MinInt64 / 2, end: math.MaxInt64 / 2},
		&rangeInt64Gener{begin: math.MinInt64 / 2, end: math.MaxInt64 / 2}},
	},
		{types.ETDecimal, []types.EvalType{types.ETDecimal, types.ETDecimal}, nil},
		{types.ETReal, []types.EvalType{types.ETReal, types.ETReal}, nil},},
	ast.NE: {},
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
