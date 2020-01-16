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
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/types"
)

var vecBuiltinMathCases = map[string][]vecExprBenchCase{
	ast.Conv: {
		{
			retEvalType:   types.ETString,
			childrenTypes: []types.EvalType{types.ETString, types.ETInt, types.ETInt},
		},
	},
	ast.Sign: {
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETReal}},
	},
	ast.Log: {
		{retEvalType: types.ETReal, childrenTypes: []types.EvalType{types.ETReal}},
		{retEvalType: types.ETReal, childrenTypes: []types.EvalType{types.ETReal, types.ETReal}, geners: []dataGenerator{nil, nil}},
	},
	ast.Log10: {
		{retEvalType: types.ETReal, childrenTypes: []types.EvalType{types.ETReal}},
	},
	ast.Log2: {
		{retEvalType: types.ETReal, childrenTypes: []types.EvalType{types.ETReal}},
	},
	ast.Sqrt: {
		{retEvalType: types.ETReal, childrenTypes: []types.EvalType{types.ETReal}},
	},
	ast.Acos: {
		{retEvalType: types.ETReal, childrenTypes: []types.EvalType{types.ETReal}},
	},
	ast.Asin: {
		{retEvalType: types.ETReal, childrenTypes: []types.EvalType{types.ETReal}},
	},
	ast.Atan: {
		{retEvalType: types.ETReal, childrenTypes: []types.EvalType{types.ETReal}},
	},
	ast.Atan2: {
		{retEvalType: types.ETReal, childrenTypes: []types.EvalType{types.ETReal, types.ETReal}},
	},
	ast.Cos: {
		{retEvalType: types.ETReal, childrenTypes: []types.EvalType{types.ETReal}},
	},
	ast.Exp: {
		{retEvalType: types.ETReal, childrenTypes: []types.EvalType{types.ETReal}, geners: []dataGenerator{newRangeRealGener(-1, 1, 0.2)}},
	},
	ast.Degrees: {
		{retEvalType: types.ETReal, childrenTypes: []types.EvalType{types.ETReal}},
	},
	ast.Cot: {
		{retEvalType: types.ETReal, childrenTypes: []types.EvalType{types.ETReal}},
	},
	ast.Radians: {
		{retEvalType: types.ETReal, childrenTypes: []types.EvalType{types.ETReal}},
	},
	ast.Sin: {
		{retEvalType: types.ETReal, childrenTypes: []types.EvalType{types.ETReal}},
	},
	ast.Tan: {
		{retEvalType: types.ETReal, childrenTypes: []types.EvalType{types.ETReal}},
	},
	ast.Abs: {
		{retEvalType: types.ETDecimal, childrenTypes: []types.EvalType{types.ETDecimal}},
		{retEvalType: types.ETReal, childrenTypes: []types.EvalType{types.ETReal}},
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETInt}},
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETInt}, childrenFieldTypes: []*types.FieldType{{Tp: mysql.TypeInt24, Flag: mysql.UnsignedFlag}}},
	},
	ast.Round: {
		{retEvalType: types.ETReal, childrenTypes: []types.EvalType{types.ETReal}},
		{retEvalType: types.ETReal, childrenTypes: []types.EvalType{types.ETReal, types.ETInt}, geners: []dataGenerator{nil, newRangeInt64Gener(-100, 100)}},
		{retEvalType: types.ETDecimal, childrenTypes: []types.EvalType{types.ETDecimal}},
		{retEvalType: types.ETDecimal, childrenTypes: []types.EvalType{types.ETDecimal, types.ETInt}},
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETInt}},
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETInt, types.ETInt}, geners: []dataGenerator{nil, newRangeInt64Gener(-100, 100)}},
	},
	ast.Pow: {
		{retEvalType: types.ETReal, childrenTypes: []types.EvalType{types.ETReal, types.ETReal}, geners: []dataGenerator{newRangeRealGener(0, 10, 0.5), newRangeRealGener(0, 100, 0.5)}},
	},
	ast.Floor: {
		{retEvalType: types.ETReal, childrenTypes: []types.EvalType{types.ETReal}, geners: nil},
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETInt}, childrenFieldTypes: []*types.FieldType{{Tp: mysql.TypeInt24}}, geners: nil},
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETDecimal}, geners: nil},
		{retEvalType: types.ETDecimal, childrenTypes: []types.EvalType{types.ETInt}, geners: nil},
		{retEvalType: types.ETDecimal, childrenTypes: []types.EvalType{types.ETInt}, childrenFieldTypes: []*types.FieldType{{Tp: mysql.TypeLonglong, Flag: mysql.UnsignedFlag}}, geners: nil},
		{retEvalType: types.ETDecimal, childrenTypes: []types.EvalType{types.ETDecimal}, childrenFieldTypes: []*types.FieldType{{Tp: mysql.TypeNewDecimal, Flen: 32, Decimal: 2}}, geners: nil},
	},
	ast.Ceil: {
		{retEvalType: types.ETReal, childrenTypes: []types.EvalType{types.ETReal}, geners: nil},
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETInt}, childrenFieldTypes: []*types.FieldType{{Tp: mysql.TypeInt24}}, geners: nil},
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETDecimal}, geners: nil},
		{retEvalType: types.ETDecimal, childrenTypes: []types.EvalType{types.ETInt}, geners: nil},
		{retEvalType: types.ETDecimal, childrenTypes: []types.EvalType{types.ETDecimal}, childrenFieldTypes: []*types.FieldType{{Tp: mysql.TypeNewDecimal, Flen: 32, Decimal: 2}}, geners: nil},
	},
	ast.PI: {
		{retEvalType: types.ETReal},
	},
	ast.Truncate: {
		{retEvalType: types.ETReal, childrenTypes: []types.EvalType{types.ETReal, types.ETInt}, geners: []dataGenerator{nil, newRangeInt64Gener(-10, 10)}},
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETInt, types.ETInt}, geners: []dataGenerator{nil, newRangeInt64Gener(-10, 10)}},
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETInt, types.ETInt}, childrenFieldTypes: []*types.FieldType{{Tp: mysql.TypeInt24, Flag: mysql.UnsignedFlag}}, geners: []dataGenerator{nil, newRangeInt64Gener(-10, 10)}},
		{retEvalType: types.ETDecimal, childrenTypes: []types.EvalType{types.ETDecimal, types.ETInt}},
	},
	ast.Rand: {
		{retEvalType: types.ETReal, childrenTypes: []types.EvalType{types.ETInt}, geners: []dataGenerator{newDefaultGener(0, types.ETInt)}},
	},
	ast.CRC32: {
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETString}},
	},
}

var vecBuiltinMathCases1 = map[string][]vecExprBenchCase{
	ast.Rand: {
		{retEvalType: types.ETReal},
	},
}

func (s *testEvaluatorSuite) TestVectorizedBuiltinMathEvalOneVec(c *C) {
	testVectorizedEvalOneVec(c, vecBuiltinMathCases)
}

func (s *testEvaluatorSuite) TestVectorizedBuiltinMathFunc(c *C) {
	testVectorizedBuiltinFunc(c, vecBuiltinMathCases)
}

func (s *testEvaluatorSuite) TestVectorizedBuiltinMathFuncForRand(c *C) {
	testVectorizedBuiltinFuncForRand(c, vecBuiltinMathCases1)
}

func BenchmarkVectorizedBuiltinMathEvalOneVec(b *testing.B) {
	benchmarkVectorizedEvalOneVec(b, vecBuiltinMathCases)
}

func BenchmarkVectorizedBuiltinMathFunc(b *testing.B) {
	benchmarkVectorizedBuiltinFunc(b, vecBuiltinMathCases)
}
