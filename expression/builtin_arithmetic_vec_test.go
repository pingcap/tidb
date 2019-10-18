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
	"math"
	"testing"

	. "github.com/pingcap/check"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/types"
)

var vecBuiltinArithmeticCases = map[string][]vecExprBenchCase{
	ast.LE: {},
	ast.Minus: {
		{retEvalType: types.ETReal, childrenTypes: []types.EvalType{types.ETReal, types.ETReal}},
		{retEvalType: types.ETDecimal, childrenTypes: []types.EvalType{types.ETDecimal, types.ETDecimal}},
	},
	ast.Div: {
		{retEvalType: types.ETReal, childrenTypes: []types.EvalType{types.ETReal, types.ETReal}},
		{retEvalType: types.ETReal, childrenTypes: []types.EvalType{types.ETReal, types.ETReal}, geners: []dataGenerator{nil, &rangeRealGener{0, 0, 0}}},
		{retEvalType: types.ETDecimal, childrenTypes: []types.EvalType{types.ETDecimal, types.ETDecimal}},
		{retEvalType: types.ETDecimal, childrenTypes: []types.EvalType{types.ETDecimal, types.ETDecimal}, geners: []dataGenerator{nil, &rangeDecimalGener{0, 0, 0.2}}},
	},
	ast.IntDiv: {},
	ast.Mod: {
		{retEvalType: types.ETReal, childrenTypes: []types.EvalType{types.ETReal, types.ETReal}},
		{retEvalType: types.ETReal, childrenTypes: []types.EvalType{types.ETReal, types.ETReal}, geners: []dataGenerator{nil, &rangeRealGener{0, 0, 0}}},
		{retEvalType: types.ETReal, childrenTypes: []types.EvalType{types.ETReal, types.ETReal}, geners: []dataGenerator{&rangeRealGener{0, 0, 0}, nil}},
		{retEvalType: types.ETReal, childrenTypes: []types.EvalType{types.ETReal, types.ETReal}, geners: []dataGenerator{nil, &rangeRealGener{0, 0, 1}}},
		{retEvalType: types.ETDecimal, childrenTypes: []types.EvalType{types.ETDecimal, types.ETDecimal}},
		{retEvalType: types.ETDecimal, childrenTypes: []types.EvalType{types.ETDecimal, types.ETDecimal}, geners: []dataGenerator{nil, &rangeDecimalGener{0, 0, 0}}},
		{retEvalType: types.ETDecimal, childrenTypes: []types.EvalType{types.ETDecimal, types.ETDecimal}, geners: []dataGenerator{&rangeDecimalGener{0, 0, 0}, nil}},
		{retEvalType: types.ETDecimal, childrenTypes: []types.EvalType{types.ETDecimal, types.ETDecimal}, geners: []dataGenerator{nil, &rangeDecimalGener{0, 0, 1}}},
	},
	ast.Or: {},
	ast.Mul: {
		{retEvalType: types.ETReal, childrenTypes: []types.EvalType{types.ETReal, types.ETReal}},
		{retEvalType: types.ETDecimal, childrenTypes: []types.EvalType{types.ETDecimal, types.ETDecimal}},
	},
	ast.Round: {},
	ast.And:   {},
	ast.Plus: {
		{retEvalType: types.ETReal, childrenTypes: []types.EvalType{types.ETReal, types.ETReal}},
		{retEvalType: types.ETDecimal, childrenTypes: []types.EvalType{types.ETDecimal, types.ETDecimal}},
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETInt, types.ETInt},
			geners: []dataGenerator{
				&rangeInt64Gener{begin: math.MinInt64 / 2, end: math.MaxInt64 / 2},
				&rangeInt64Gener{begin: math.MinInt64 / 2, end: math.MaxInt64 / 2},
			},
		},
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETInt, types.ETInt},
			childrenFieldTypes: []*types.FieldType{{Tp: mysql.TypeLonglong, Flag: mysql.UnsignedFlag},
				{Tp: mysql.TypeLonglong, Flag: mysql.UnsignedFlag}},
			geners: []dataGenerator{
				&rangeInt64Gener{begin: 0, end: math.MaxInt64},
				&rangeInt64Gener{begin: 0, end: math.MaxInt64},
			},
		},
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETInt, types.ETInt},
			childrenFieldTypes: []*types.FieldType{{Tp: mysql.TypeLonglong},
				{Tp: mysql.TypeLonglong, Flag: mysql.UnsignedFlag}},
			geners: []dataGenerator{
				&rangeInt64Gener{begin: 0, end: math.MaxInt64},
				&rangeInt64Gener{begin: 0, end: math.MaxInt64},
			},
		},
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETInt, types.ETInt},
			childrenFieldTypes: []*types.FieldType{{Tp: mysql.TypeLonglong, Flag: mysql.UnsignedFlag},
				{Tp: mysql.TypeLonglong}},
			geners: []dataGenerator{
				&rangeInt64Gener{begin: 0, end: math.MaxInt64},
				&rangeInt64Gener{begin: 0, end: math.MaxInt64},
			},
		},
	},
	ast.NE: {},
}

func (s *testEvaluatorSuite) TestVectorizedBuiltinArithmeticFunc(c *C) {
	testVectorizedBuiltinFunc(c, vecBuiltinArithmeticCases)
}

func BenchmarkVectorizedBuiltinArithmeticFunc(b *testing.B) {
	benchmarkVectorizedBuiltinFunc(b, vecBuiltinArithmeticCases)
}
