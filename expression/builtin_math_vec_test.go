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
	"testing"

	. "github.com/pingcap/check"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/tidb/types"
)

var vecBuiltinMathCases = map[string][]vecExprBenchCase{
	ast.Log10: {
		{types.ETReal, []types.EvalType{types.ETReal}, nil},
	},
	ast.Sqrt: {
		{types.ETReal, []types.EvalType{types.ETReal}, nil},
	},
	ast.Abs: {
		{types.ETDecimal, []types.EvalType{types.ETDecimal}, nil},
	},
	ast.Round: {
		{types.ETDecimal, []types.EvalType{types.ETDecimal}, nil},
	},
}

func (s *testEvaluatorSuite) TestVectorizedBuiltinMathEvalOneVec(c *C) {
	testVectorizedEvalOneVec(c, vecBuiltinMathCases)
}

func (s *testEvaluatorSuite) TestVectorizedBuiltinMathFunc(c *C) {
	testVectorizedBuiltinFunc(c, vecBuiltinMathCases)
}

func BenchmarkVectorizedBuiltinMathEvalOneVec(b *testing.B) {
	benchmarkVectorizedEvalOneVec(b, vecBuiltinMathCases)
}

func BenchmarkVectorizedBuiltinMathFunc(b *testing.B) {
	benchmarkVectorizedBuiltinFunc(b, vecBuiltinMathCases)
}
