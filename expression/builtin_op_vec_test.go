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

var vecBuiltinOpCases = map[string][]vecExprBenchCase{
	ast.IsTruth:   {},
	ast.IsFalsity: {},
	ast.LogicOr: {
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETInt, types.ETInt}, geners: makeBinaryLogicOpDataGens()},
	},
	ast.LogicXor: {},
	ast.Xor:      {},
	ast.LogicAnd: {
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETInt, types.ETInt}, geners: makeBinaryLogicOpDataGens()},
	},
	ast.UnaryNot:   {},
	ast.UnaryMinus: {},
	ast.IsNull:     {},
}

// givenValsGener returns the items sequentially from the slice given at
// the construction time. If this slice is exhausted, it falls back to
// the fallback generator.
type givenValsGener struct {
	given    []interface{}
	idx      int
	fallback dataGenerator
}

func (g *givenValsGener) gen() interface{} {
	if g.idx >= len(g.given) {
		return g.fallback.gen()
	}
	v := g.given[g.idx]
	g.idx++
	return v
}

func makeGivenValsOrDefaultGener(vals []interface{}, eType types.EvalType) *givenValsGener {
	g := &givenValsGener{}
	g.given = vals
	g.fallback = &defaultGener{0.2, eType}
	return g
}

func makeBinaryLogicOpDataGens() []dataGenerator {
	// TODO: This data generator currently only applies to AND and OR operators,
	// so its name may be too generic.
	pairs := [][]interface{}{
		{nil, nil},
		{0, nil},
		{nil, 0},
		{1, nil},
		{nil, 1},
		{0, 0},
		{0, 1},
		{1, 0},
		{1, 1},
		{-1, 1},
	}

	maybeToInt64 := func(v interface{}) interface{} {
		if v == nil {
			return nil
		}
		return int64(v.(int))
	}

	n := len(pairs)
	arg0s := make([]interface{}, n)
	arg1s := make([]interface{}, n)
	for i, p := range pairs {
		arg0s[i] = maybeToInt64(p[0])
		arg1s[i] = maybeToInt64(p[1])
	}
	return []dataGenerator{
		makeGivenValsOrDefaultGener(arg0s, types.ETInt),
		makeGivenValsOrDefaultGener(arg1s, types.ETInt)}
}

func (s *testEvaluatorSuite) TestVectorizedBuiltinOpFunc(c *C) {
	testVectorizedBuiltinFunc(c, vecBuiltinOpCases)
}

func BenchmarkVectorizedBuiltinOpFunc(b *testing.B) {
	benchmarkVectorizedBuiltinFunc(b, vecBuiltinOpCases)
}
