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

var vecBuiltinStringCases = map[string][]vecExprBenchCase{
	ast.Repeat: {
		{types.ETString, []types.EvalType{types.ETString, types.ETInt}, []dataGenerator{&randLenStrGener{10, 20}, &rangeInt64Gener{-10, 10}}},
	},
	ast.Lower: {
		{types.ETString, []types.EvalType{types.ETString}, nil},
	},
	ast.IsNull: {
		{types.ETInt, []types.EvalType{types.ETString}, []dataGenerator{&randLenStrGener{10, 20}}},
		{types.ETInt, []types.EvalType{types.ETString}, []dataGenerator{&defaultGener{0.2, types.ETString}}},
	},
}

func (s *testEvaluatorSuite) TestVectorizedBuiltinStringEvalOneVec(c *C) {
	testVectorizedEvalOneVec(c, vecBuiltinStringCases)
}

func (s *testEvaluatorSuite) TestVectorizedBuiltinStringFunc(c *C) {
	testVectorizedBuiltinFunc(c, vecBuiltinStringCases)
}

func BenchmarkVectorizedBuiltinStringEvalOneVec(b *testing.B) {
	benchmarkVectorizedEvalOneVec(b, vecBuiltinStringCases)
}

func BenchmarkVectorizedBuiltinStringFunc(b *testing.B) {
	benchmarkVectorizedBuiltinFunc(b, vecBuiltinStringCases)
}
