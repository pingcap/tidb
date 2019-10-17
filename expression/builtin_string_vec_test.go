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
	"math/rand"
	"testing"

	. "github.com/pingcap/check"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/tidb/types"
)

type randSpaceStrGener struct {
	lenBegin int
	lenEnd   int
}

func (g *randSpaceStrGener) gen() interface{} {
	n := rand.Intn(g.lenEnd-g.lenBegin) + g.lenBegin
	buf := make([]byte, n)
	for i := range buf {
		x := rand.Intn(150)
		if x < 10 {
			buf[i] = byte('0' + x)
		} else if x-10 < 26 {
			buf[i] = byte('a' + x - 10)
		} else if x < 62 {
			buf[i] = byte('A' + x - 10 - 26)
		} else {
			buf[i] = byte(' ')
		}
	}
	return string(buf)
}

var vecBuiltinStringCases = map[string][]vecExprBenchCase{
	ast.Length: {
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETString}, geners: []dataGenerator{&defaultGener{0.2, types.ETString}}},
	},
	ast.ASCII: {
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETString}, geners: []dataGenerator{&defaultGener{0.2, types.ETString}}},
	},
	ast.Concat:         {},
	ast.ConcatWS:       {},
	ast.Convert:        {},
	ast.Substring:      {},
	ast.SubstringIndex: {},
	ast.Locate: {
		{
			retEvalType:   types.ETInt,
			childrenTypes: []types.EvalType{types.ETString, types.ETString},
			geners:        []dataGenerator{&randLenStrGener{0, 10}, &randLenStrGener{0, 20}},
		},
		{
			retEvalType:   types.ETInt,
			childrenTypes: []types.EvalType{types.ETString, types.ETString},
			geners:        []dataGenerator{&randLenStrGener{1, 2}, &randLenStrGener{0, 20}},
		},
		{
			retEvalType:   types.ETInt,
			childrenTypes: []types.EvalType{types.ETString, types.ETString, types.ETInt},
			geners:        []dataGenerator{&randLenStrGener{0, 10}, &randLenStrGener{0, 20}, &rangeInt64Gener{-10, 20}},
		},
		{
			retEvalType:   types.ETInt,
			childrenTypes: []types.EvalType{types.ETString, types.ETString, types.ETInt},
			geners:        []dataGenerator{&randLenStrGener{1, 2}, &randLenStrGener{0, 10}, &rangeInt64Gener{0, 8}},
		},
	},
	ast.Hex: {},
	ast.Unhex: {
		{retEvalType: types.ETString, childrenTypes: []types.EvalType{types.ETString}, geners: []dataGenerator{&randHexStrGener{10, 100}}},
	},
	ast.Trim: {
		{retEvalType: types.ETString, childrenTypes: []types.EvalType{types.ETString}, geners: []dataGenerator{&randSpaceStrGener{10, 100}}},
		{retEvalType: types.ETString, childrenTypes: []types.EvalType{types.ETString, types.ETString}, geners: []dataGenerator{&randLenStrGener{10, 20}, &randLenStrGener{5, 25}}},
	},
	ast.LTrim: {},
	ast.RTrim: {
		{retEvalType: types.ETString, childrenTypes: []types.EvalType{types.ETString}, geners: []dataGenerator{&randSpaceStrGener{10, 100}}},
	},
	ast.Lpad: {
		{
			retEvalType:   types.ETString,
			childrenTypes: []types.EvalType{types.ETString, types.ETInt, types.ETString},
			geners:        []dataGenerator{&randLenStrGener{0, 20}, &rangeInt64Gener{0, 20}, &randLenStrGener{0, 10}},
		},
		{
			retEvalType:   types.ETString,
			childrenTypes: []types.EvalType{types.ETString, types.ETInt, types.ETString},
			geners:        []dataGenerator{&defaultGener{0.2, types.ETString}, &defaultGener{0.2, types.ETInt}, &defaultGener{0.2, types.ETString}},
		},
	},
	ast.Rpad:      {},
	ast.BitLength: {},
	ast.FindInSet: {},
	ast.Field:     {},
	ast.MakeSet:   {},
	ast.Oct: {
		{retEvalType: types.ETString, childrenTypes: []types.EvalType{types.ETInt}},
	},
	ast.Ord:   {},
	ast.Quote: {},
	ast.Bin: {
		{retEvalType: types.ETString, childrenTypes: []types.EvalType{types.ETInt}},
	},
	ast.ToBase64: {
		{retEvalType: types.ETString, childrenTypes: []types.EvalType{types.ETString}, geners: []dataGenerator{&randLenStrGener{0, 10}}},
	},
	ast.FromBase64: {
		{retEvalType: types.ETString, childrenTypes: []types.EvalType{types.ETString}, geners: []dataGenerator{&randLenStrGener{10, 100}}},
	},
	ast.ExportSet: {},
	ast.Repeat: {
		{retEvalType: types.ETString, childrenTypes: []types.EvalType{types.ETString, types.ETInt}, geners: []dataGenerator{&randLenStrGener{10, 20}, &rangeInt64Gener{-10, 10}}},
	},
	ast.Lower: {
		{retEvalType: types.ETString, childrenTypes: []types.EvalType{types.ETString}},
	},
	ast.IsNull: {
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETString}, geners: []dataGenerator{&randLenStrGener{10, 20}}},
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETString}, geners: []dataGenerator{&defaultGener{0.2, types.ETString}}},
	},
	ast.Upper: {
		{retEvalType: types.ETString, childrenTypes: []types.EvalType{types.ETString}},
	},
	ast.Right: {
		{retEvalType: types.ETString, childrenTypes: []types.EvalType{types.ETString, types.ETInt}},
	},
	ast.Left: {
		{retEvalType: types.ETString, childrenTypes: []types.EvalType{types.ETString, types.ETInt}},
	},
	ast.Space: {
		{retEvalType: types.ETString, childrenTypes: []types.EvalType{types.ETInt}, geners: []dataGenerator{&rangeInt64Gener{-10, 2000}}},
		{retEvalType: types.ETString, childrenTypes: []types.EvalType{types.ETInt}, geners: []dataGenerator{&rangeInt64Gener{5, 10}}},
	},
	ast.Reverse: {
		{retEvalType: types.ETString, childrenTypes: []types.EvalType{types.ETString}, geners: []dataGenerator{&randLenStrGener{10, 20}}},
		{retEvalType: types.ETString, childrenTypes: []types.EvalType{types.ETString}, geners: []dataGenerator{&defaultGener{0.2, types.ETString}}},
	},
	ast.Replace: {
		{retEvalType: types.ETString, childrenTypes: []types.EvalType{types.ETString, types.ETString, types.ETString}, geners: []dataGenerator{&randLenStrGener{10, 20}, &randLenStrGener{0, 10}, &randLenStrGener{0, 10}}},
	},
	ast.InsertFunc: {
		{retEvalType: types.ETString, childrenTypes: []types.EvalType{types.ETString, types.ETInt, types.ETInt, types.ETString}, geners: []dataGenerator{&randLenStrGener{10, 20}, &rangeInt64Gener{-10, 20}, &rangeInt64Gener{0, 100}, &randLenStrGener{0, 10}}},
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
