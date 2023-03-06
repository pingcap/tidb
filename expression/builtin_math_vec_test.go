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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package expression

import (
	"testing"

	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/types"
)

var vecBuiltinMathCases = map[string][]vecExprBenchCase{
	/* TODO: Because of https://github.com/pingcap/tidb/issues/5817, we don't enable it now.
	ast.Conv: {
		{
			retEvalType:   types.ETString,
			childrenTypes: []types.EvalType{types.ETString, types.ETInt, types.ETInt},
		},
	},
	*/
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
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETInt}, childrenFieldTypes: []*types.FieldType{
			types.NewFieldTypeBuilder().SetType(mysql.TypeInt24).SetFlag(mysql.UnsignedFlag).BuildP(),
		}},
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
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETInt}, childrenFieldTypes: []*types.FieldType{
			types.NewFieldTypeBuilder().SetType(mysql.TypeInt24).BuildP(),
		}, geners: nil},
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETDecimal}, geners: nil},
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETInt}, geners: nil},
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETInt}, childrenFieldTypes: []*types.FieldType{
			types.NewFieldTypeBuilder().SetType(mysql.TypeLonglong).SetFlag(mysql.UnsignedFlag).BuildP(),
		}, geners: nil},
		{retEvalType: types.ETDecimal, childrenTypes: []types.EvalType{types.ETDecimal}, childrenFieldTypes: []*types.FieldType{
			types.NewFieldTypeBuilder().SetType(mysql.TypeNewDecimal).SetFlen(32).SetDecimal(2).BuildP(),
		}, geners: nil},
	},
	ast.Ceil: {
		{retEvalType: types.ETReal, childrenTypes: []types.EvalType{types.ETReal}, geners: nil},
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETInt}, childrenFieldTypes: []*types.FieldType{
			types.NewFieldTypeBuilder().SetType(mysql.TypeInt24).BuildP(),
		}, geners: nil},
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETDecimal}, geners: nil},
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETInt}, geners: nil},
		{retEvalType: types.ETDecimal, childrenTypes: []types.EvalType{types.ETDecimal}, childrenFieldTypes: []*types.FieldType{
			types.NewFieldTypeBuilder().SetType(mysql.TypeNewDecimal).SetFlen(32).SetDecimal(2).BuildP(),
		}, geners: nil},
	},
	ast.PI: {
		{retEvalType: types.ETReal},
	},
	ast.Truncate: {
		{retEvalType: types.ETReal, childrenTypes: []types.EvalType{types.ETReal, types.ETInt}, geners: []dataGenerator{nil, newRangeInt64Gener(-10, 10)}},
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETInt, types.ETInt}, geners: []dataGenerator{nil, newRangeInt64Gener(-10, 10)}},
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETInt, types.ETInt}, childrenFieldTypes: []*types.FieldType{
			types.NewFieldTypeBuilder().SetType(mysql.TypeInt24).SetFlag(mysql.UnsignedFlag).BuildP(),
		}, geners: []dataGenerator{nil, newRangeInt64Gener(-10, 10)}},
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

func TestVectorizedBuiltinMathEvalOneVec(t *testing.T) {
	testVectorizedEvalOneVec(t, vecBuiltinMathCases)
}

func TestVectorizedBuiltinMathFunc(t *testing.T) {
	testVectorizedBuiltinFunc(t, vecBuiltinMathCases)
}

func TestVectorizedBuiltinMathFuncForRand(t *testing.T) {
	testVectorizedBuiltinFuncForRand(t, vecBuiltinMathCases1)
}

func BenchmarkVectorizedBuiltinMathEvalOneVec(b *testing.B) {
	benchmarkVectorizedEvalOneVec(b, vecBuiltinMathCases)
}

func BenchmarkVectorizedBuiltinMathFunc(b *testing.B) {
	benchmarkVectorizedBuiltinFunc(b, vecBuiltinMathCases)
}
