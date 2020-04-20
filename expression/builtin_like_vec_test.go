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

var vecBuiltinLikeCases = map[string][]vecExprBenchCase{
	ast.Like: {
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETString, types.ETString, types.ETInt},
			geners: []dataGenerator{nil, nil, newRangeInt64Gener(int('\\'), int('\\')+1)},
		},
	},
	ast.Regexp: {
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETString, types.ETString}},
	},
}

func (s *testEvaluatorSuite) TestVectorizedBuiltinLikeFunc(c *C) {
	testVectorizedBuiltinFunc(c, vecBuiltinLikeCases)
}

func BenchmarkVectorizedBuiltinLikeFunc(b *testing.B) {
	benchmarkVectorizedBuiltinFunc(b, vecBuiltinLikeCases)
}
