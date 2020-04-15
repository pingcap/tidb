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
	"github.com/pingcap/tidb/v4/types"
)

var vecBuiltinArithmeticCases = map[string][]vecExprBenchCase{
	ast.LE: {},
	ast.Minus: {
		{retEvalType: types.ETReal, childrenTypes: []types.EvalType{types.ETReal, types.ETReal}},
		{retEvalType: types.ETDecimal, childrenTypes: []types.EvalType{types.ETDecimal, types.ETDecimal}},
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETInt, types.ETInt}, geners: []dataGenerator{newRangeInt64Gener(-100000, 100000), newRangeInt64Gener(-100000, 100000)}},
	},
	ast.Div: {
		{retEvalType: types.ETReal, childrenTypes: []types.EvalType{types.ETReal, types.ETReal}},
		{retEvalType: types.ETReal, childrenTypes: []types.EvalType{types.ETReal, types.ETReal}, geners: []dataGenerator{nil, newRangeRealGener(0, 0, 0)}},
		{retEvalType: types.ETDecimal, childrenTypes: []types.EvalType{types.ETDecimal, types.ETDecimal}},
		{retEvalType: types.ETDecimal, childrenTypes: []types.EvalType{types.ETDecimal, types.ETDecimal}, geners: []dataGenerator{nil, newRangeDecimalGener(0, 0, 0.2)}},
	},
	ast.IntDiv: {
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETDecimal, types.ETDecimal}},
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETDecimal, types.ETDecimal}, geners: []dataGenerator{nil, newRangeDecimalGener(0, 0, 0.2)}},
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETDecimal, types.ETDecimal},
			childrenFieldTypes: []*types.FieldType{{Tp: mysql.TypeNewDecimal, Flag: mysql.UnsignedFlag}, nil},
			geners:             []dataGenerator{newRangeDecimalGener(0, 10000, 0.2), newRangeDecimalGener(0, 10000, 0.2)},
		},
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETDecimal, types.ETDecimal},
			childrenFieldTypes: []*types.FieldType{nil, {Tp: mysql.TypeNewDecimal, Flag: mysql.UnsignedFlag}},
			geners:             []dataGenerator{newRangeDecimalGener(0, 10000, 0.2), newRangeDecimalGener(0, 10000, 0.2)},
		},
		// when the final result is at (-1, 0], it should be return 0 instead of the error
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETDecimal, types.ETDecimal},
			childrenFieldTypes: []*types.FieldType{nil, {Tp: mysql.TypeNewDecimal, Flag: mysql.UnsignedFlag}},
			geners:             []dataGenerator{newRangeDecimalGener(-100, -1, 0.2), newRangeDecimalGener(1000, 2000, 0.2)},
		},
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETInt, types.ETInt},
			geners: []dataGenerator{
				newRangeInt64Gener(math.MinInt64/2, math.MaxInt64/2),
				newRangeInt64Gener(math.MinInt64/2, math.MaxInt64/2),
			},
		},
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETInt, types.ETInt},
			childrenFieldTypes: []*types.FieldType{
				{Tp: mysql.TypeLonglong, Flag: mysql.UnsignedFlag},
				{Tp: mysql.TypeLonglong, Flag: mysql.UnsignedFlag}},
			geners: []dataGenerator{
				newRangeInt64Gener(0, math.MaxInt64),
				newRangeInt64Gener(0, math.MaxInt64),
			},
		},
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETInt, types.ETInt},
			childrenFieldTypes: []*types.FieldType{
				{Tp: mysql.TypeLonglong},
				{Tp: mysql.TypeLonglong, Flag: mysql.UnsignedFlag}},
			geners: []dataGenerator{
				newRangeInt64Gener(0, math.MaxInt64),
				newRangeInt64Gener(0, math.MaxInt64),
			},
		},
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETInt, types.ETInt},
			childrenFieldTypes: []*types.FieldType{
				{Tp: mysql.TypeLonglong, Flag: mysql.UnsignedFlag},
				{Tp: mysql.TypeLonglong}},
			geners: []dataGenerator{
				newRangeInt64Gener(0, math.MaxInt64),
				newRangeInt64Gener(0, math.MaxInt64),
			},
		},
	},
	ast.Mod: {
		{retEvalType: types.ETReal, childrenTypes: []types.EvalType{types.ETReal, types.ETReal}},
		{retEvalType: types.ETReal, childrenTypes: []types.EvalType{types.ETReal, types.ETReal}, geners: []dataGenerator{nil, newRangeRealGener(0, 0, 0)}},
		{retEvalType: types.ETReal, childrenTypes: []types.EvalType{types.ETReal, types.ETReal}, geners: []dataGenerator{newRangeRealGener(0, 0, 0), nil}},
		{retEvalType: types.ETReal, childrenTypes: []types.EvalType{types.ETReal, types.ETReal}, geners: []dataGenerator{nil, newRangeRealGener(0, 0, 1)}},
		{retEvalType: types.ETDecimal, childrenTypes: []types.EvalType{types.ETDecimal, types.ETDecimal}},
		{retEvalType: types.ETDecimal, childrenTypes: []types.EvalType{types.ETDecimal, types.ETDecimal}, geners: []dataGenerator{nil, newRangeDecimalGener(0, 0, 0)}},
		{retEvalType: types.ETDecimal, childrenTypes: []types.EvalType{types.ETDecimal, types.ETDecimal}, geners: []dataGenerator{newRangeDecimalGener(0, 0, 0), nil}},
		{retEvalType: types.ETDecimal, childrenTypes: []types.EvalType{types.ETDecimal, types.ETDecimal}, geners: []dataGenerator{nil, newRangeDecimalGener(0, 0, 1)}},
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETInt, types.ETInt}, geners: []dataGenerator{nil, newRangeInt64Gener(0, 1)}},
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETInt, types.ETInt}, geners: []dataGenerator{newRangeInt64Gener(0, 1), nil}},
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETInt, types.ETInt},
			geners: []dataGenerator{
				newRangeInt64Gener(math.MinInt64/2, math.MaxInt64/2),
				newRangeInt64Gener(math.MinInt64/2, math.MaxInt64/2),
			},
		},
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETInt, types.ETInt},
			childrenFieldTypes: []*types.FieldType{{Tp: mysql.TypeLonglong, Flag: mysql.UnsignedFlag},
				{Tp: mysql.TypeLonglong, Flag: mysql.UnsignedFlag}},
			geners: []dataGenerator{
				newRangeInt64Gener(0, math.MaxInt64),
				newRangeInt64Gener(0, math.MaxInt64),
			},
		},
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETInt, types.ETInt},
			childrenFieldTypes: []*types.FieldType{{Tp: mysql.TypeLonglong},
				{Tp: mysql.TypeLonglong, Flag: mysql.UnsignedFlag}},
			geners: []dataGenerator{
				newRangeInt64Gener(math.MinInt64/2, math.MaxInt64/2),
				newRangeInt64Gener(0, math.MaxInt64),
			},
		},
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETInt, types.ETInt},
			childrenFieldTypes: []*types.FieldType{{Tp: mysql.TypeLonglong, Flag: mysql.UnsignedFlag},
				{Tp: mysql.TypeLonglong}},
			geners: []dataGenerator{
				newRangeInt64Gener(0, math.MaxInt64),
				newRangeInt64Gener(math.MinInt64/2, math.MaxInt64/2),
			},
		},
	},
	ast.Or: {},
	ast.Mul: {
		{retEvalType: types.ETReal, childrenTypes: []types.EvalType{types.ETReal, types.ETReal}},
		{retEvalType: types.ETDecimal, childrenTypes: []types.EvalType{types.ETDecimal, types.ETDecimal}},
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETInt, types.ETInt}, geners: []dataGenerator{newRangeInt64Gener(-10000, 10000), newRangeInt64Gener(-10000, 10000)}},
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETInt, types.ETInt}, childrenFieldTypes: []*types.FieldType{{Tp: mysql.TypeInt24, Flag: mysql.UnsignedFlag}, {Tp: mysql.TypeLonglong, Flag: mysql.UnsignedFlag}},
			geners: []dataGenerator{
				newRangeInt64Gener(0, 10000),
				newRangeInt64Gener(0, 10000),
			},
		},
	},
	ast.Round: {},
	ast.And:   {},
	ast.Plus: {
		{retEvalType: types.ETReal, childrenTypes: []types.EvalType{types.ETReal, types.ETReal}},
		{retEvalType: types.ETDecimal, childrenTypes: []types.EvalType{types.ETDecimal, types.ETDecimal}},
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETInt, types.ETInt},
			geners: []dataGenerator{
				newRangeInt64Gener(math.MinInt64/2, math.MaxInt64/2),
				newRangeInt64Gener(math.MinInt64/2, math.MaxInt64/2),
			},
		},
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETInt, types.ETInt},
			childrenFieldTypes: []*types.FieldType{{Tp: mysql.TypeLonglong, Flag: mysql.UnsignedFlag},
				{Tp: mysql.TypeLonglong, Flag: mysql.UnsignedFlag}},
			geners: []dataGenerator{
				newRangeInt64Gener(0, math.MaxInt64),
				newRangeInt64Gener(0, math.MaxInt64),
			},
		},
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETInt, types.ETInt},
			childrenFieldTypes: []*types.FieldType{{Tp: mysql.TypeLonglong},
				{Tp: mysql.TypeLonglong, Flag: mysql.UnsignedFlag}},
			geners: []dataGenerator{
				newRangeInt64Gener(0, math.MaxInt64),
				newRangeInt64Gener(0, math.MaxInt64),
			},
		},
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETInt, types.ETInt},
			childrenFieldTypes: []*types.FieldType{{Tp: mysql.TypeLonglong, Flag: mysql.UnsignedFlag},
				{Tp: mysql.TypeLonglong}},
			geners: []dataGenerator{
				newRangeInt64Gener(0, math.MaxInt64),
				newRangeInt64Gener(0, math.MaxInt64),
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
