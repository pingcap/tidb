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

var vecBuiltinCastCases = map[string][]vecExprBenchCase{
	ast.Cast: {
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETInt}},
		{retEvalType: types.ETReal, childrenTypes: []types.EvalType{types.ETInt}},
		{retEvalType: types.ETDuration, childrenTypes: []types.EvalType{types.ETInt}, geners: []dataGenerator{new(randDurInt)}},
		{retEvalType: types.ETReal, childrenTypes: []types.EvalType{types.ETReal}},
	},
}

func (s *testEvaluatorSuite) TestVectorizedBuiltinCastEvalOneVec(c *C) {
	testVectorizedEvalOneVec(c, vecBuiltinCastCases)
}

func (s *testEvaluatorSuite) TestVectorizedBuiltinCastFunc(c *C) {
	testVectorizedBuiltinFunc(c, vecBuiltinCastCases)
}

func BenchmarkVectorizedBuiltinCastEvalOneVec(b *testing.B) {
	benchmarkVectorizedEvalOneVec(b, vecBuiltinCastCases)
}

func BenchmarkVectorizedBuiltinCastFunc(b *testing.B) {
	benchmarkVectorizedBuiltinFunc(b, vecBuiltinCastCases)
}
