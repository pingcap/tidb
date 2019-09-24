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

func makeLogicAndDataGens() []dataGenerator {
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
	vals0 := make([]interface{}, n)
	vals1 := make([]interface{}, n)
	for i, p := range pairs {
		vals0[i] = maybeToInt64(p[0])
		vals1[i] = maybeToInt64(p[1])
	}
	return []dataGenerator{
		makeGivenValsOrDefaultGener(vals0, types.ETInt),
		makeGivenValsOrDefaultGener(vals1, types.ETInt)}
}

var vecBuiltinOpCases = map[string][]vecExprBenchCase{
	ast.LogicAnd: {
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETInt, types.ETInt}, geners: makeLogicAndDataGens()},
	},
}

func (s *testEvaluatorSuite) TestVectorizedBuiltinOpEvalOneVec(c *C) {
	testVectorizedEvalOneVec(c, vecBuiltinOpCases)
}

func (s *testEvaluatorSuite) TestVectorizedBuiltinOpFunc(c *C) {
	testVectorizedBuiltinFunc(c, vecBuiltinOpCases)
}

func BenchmarkVectorizedBuiltinOpEvalOneVec(b *testing.B) {
	benchmarkVectorizedEvalOneVec(b, vecBuiltinOpCases)
}

func BenchmarkVectorizedBuiltinOpFunc(b *testing.B) {
	benchmarkVectorizedBuiltinFunc(b, vecBuiltinOpCases)
}
