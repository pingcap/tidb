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
	"math"
	"testing"

	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/mock"
	"github.com/stretchr/testify/require"
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
			childrenFieldTypes: []*types.FieldType{
				types.NewFieldTypeBuilder().SetType(mysql.TypeNewDecimal).SetFlag(mysql.UnsignedFlag).BuildP(), nil},
			geners: []dataGenerator{newRangeDecimalGener(0, 10000, 0.2), newRangeDecimalGener(0, 10000, 0.2)},
		},
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETDecimal, types.ETDecimal},
			childrenFieldTypes: []*types.FieldType{nil, types.NewFieldTypeBuilder().SetType(mysql.TypeNewDecimal).SetFlag(mysql.UnsignedFlag).BuildP()},
			geners:             []dataGenerator{newRangeDecimalGener(0, 10000, 0.2), newRangeDecimalGener(0, 10000, 0.2)},
		},
		// when the final result is at (-1, 0], it should be return 0 instead of the error
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETDecimal, types.ETDecimal},
			childrenFieldTypes: []*types.FieldType{nil, types.NewFieldTypeBuilder().SetType(mysql.TypeNewDecimal).SetFlag(mysql.UnsignedFlag).BuildP()},
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
				types.NewFieldTypeBuilder().SetType(mysql.TypeLonglong).SetFlag(mysql.UnsignedFlag).BuildP(),
				types.NewFieldTypeBuilder().SetType(mysql.TypeLonglong).SetFlag(mysql.UnsignedFlag).BuildP()},
			geners: []dataGenerator{
				newRangeInt64Gener(0, math.MaxInt64),
				newRangeInt64Gener(0, math.MaxInt64),
			},
		},
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETInt, types.ETInt},
			childrenFieldTypes: []*types.FieldType{
				types.NewFieldType(mysql.TypeLonglong),
				types.NewFieldTypeBuilder().SetType(mysql.TypeLonglong).SetFlag(mysql.UnsignedFlag).BuildP()},
			geners: []dataGenerator{
				newRangeInt64Gener(0, math.MaxInt64),
				newRangeInt64Gener(0, math.MaxInt64),
			},
		},
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETInt, types.ETInt},
			childrenFieldTypes: []*types.FieldType{
				types.NewFieldTypeBuilder().SetType(mysql.TypeLonglong).SetFlag(mysql.UnsignedFlag).BuildP(),
				types.NewFieldType(mysql.TypeLonglong)},
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
			childrenFieldTypes: []*types.FieldType{
				types.NewFieldTypeBuilder().SetType(mysql.TypeLonglong).SetFlag(mysql.UnsignedFlag).BuildP(),
				types.NewFieldTypeBuilder().SetType(mysql.TypeLonglong).SetFlag(mysql.UnsignedFlag).BuildP()},
			geners: []dataGenerator{
				newRangeInt64Gener(0, math.MaxInt64),
				newRangeInt64Gener(0, math.MaxInt64),
			},
		},
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETInt, types.ETInt},
			childrenFieldTypes: []*types.FieldType{
				types.NewFieldType(mysql.TypeLonglong),
				types.NewFieldTypeBuilder().SetType(mysql.TypeLonglong).SetFlag(mysql.UnsignedFlag).BuildP()},
			geners: []dataGenerator{
				newRangeInt64Gener(math.MinInt64/2, math.MaxInt64/2),
				newRangeInt64Gener(0, math.MaxInt64),
			},
		},
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETInt, types.ETInt},
			childrenFieldTypes: []*types.FieldType{
				types.NewFieldTypeBuilder().SetType(mysql.TypeLonglong).SetFlag(mysql.UnsignedFlag).BuildP(),
				types.NewFieldType(mysql.TypeLonglong)},
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
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETInt, types.ETInt},
			childrenFieldTypes: []*types.FieldType{
				types.NewFieldTypeBuilder().SetType(mysql.TypeInt24).SetFlag(mysql.UnsignedFlag).BuildP(),
				types.NewFieldTypeBuilder().SetType(mysql.TypeLonglong).SetFlag(mysql.UnsignedFlag).BuildP()},
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
			childrenFieldTypes: []*types.FieldType{
				types.NewFieldTypeBuilder().SetType(mysql.TypeLonglong).SetFlag(mysql.UnsignedFlag).BuildP(),
				types.NewFieldTypeBuilder().SetType(mysql.TypeLonglong).SetFlag(mysql.UnsignedFlag).BuildP()},
			geners: []dataGenerator{
				newRangeInt64Gener(0, math.MaxInt64),
				newRangeInt64Gener(0, math.MaxInt64),
			},
		},
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETInt, types.ETInt},
			childrenFieldTypes: []*types.FieldType{
				types.NewFieldType(mysql.TypeLonglong),
				types.NewFieldTypeBuilder().SetType(mysql.TypeLonglong).SetFlag(mysql.UnsignedFlag).BuildP()},
			geners: []dataGenerator{
				newRangeInt64Gener(0, math.MaxInt64),
				newRangeInt64Gener(0, math.MaxInt64),
			},
		},
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETInt, types.ETInt},
			childrenFieldTypes: []*types.FieldType{
				types.NewFieldTypeBuilder().SetType(mysql.TypeLonglong).SetFlag(mysql.UnsignedFlag).BuildP(),
				types.NewFieldType(mysql.TypeLonglong)},
			geners: []dataGenerator{
				newRangeInt64Gener(0, math.MaxInt64),
				newRangeInt64Gener(0, math.MaxInt64),
			},
		},
	},
	ast.NE: {},
}

func TestVectorizedBuiltinArithmeticFunc(t *testing.T) {
	testVectorizedBuiltinFunc(t, vecBuiltinArithmeticCases)
}

func TestVectorizedDecimalErrOverflow(t *testing.T) {
	ctx := mock.NewContext()
	testCases := []struct {
		args     []float64
		funcName string
		errStr   string
	}{
		{args: []float64{8.1e80, 8.1e80}, funcName: ast.Plus,
			errStr: "[types:1690]DECIMAL value is out of range in '(Column#0 + Column#0)'",
		},
		{args: []float64{8.1e80, -8.1e80}, funcName: ast.Minus,
			errStr: "[types:1690]DECIMAL value is out of range in '(Column#0 - Column#0)'",
		},
		{args: []float64{8.1e80, 8.1e80}, funcName: ast.Mul,
			errStr: "[types:1690]DECIMAL value is out of range in '(Column#0 * Column#0)'",
		},
		{args: []float64{8.1e80, 0.1}, funcName: ast.Div,
			errStr: "[types:1690]DECIMAL value is out of range in '(Column#0 / Column#0)'",
		},
	}

	for _, tt := range testCases {
		fts := []*types.FieldType{eType2FieldType(types.ETDecimal), eType2FieldType(types.ETDecimal)}
		input := chunk.New(fts, 1, 1)
		dec1, dec2 := new(types.MyDecimal), new(types.MyDecimal)
		err := dec1.FromFloat64(tt.args[0])
		require.NoError(t, err)
		err = dec2.FromFloat64(tt.args[1])
		require.NoError(t, err)
		input.AppendMyDecimal(0, dec1)
		input.AppendMyDecimal(1, dec2)
		cols := []Expression{&Column{Index: 0, RetType: fts[0]}, &Column{Index: 1, RetType: fts[1]}}
		baseFunc, err := funcs[tt.funcName].getFunction(ctx, cols)
		require.NoError(t, err)
		result := chunk.NewColumn(eType2FieldType(types.ETDecimal), 1)
		err = baseFunc.vecEvalDecimal(input, result)
		require.EqualError(t, err, tt.errStr)
	}
}

func BenchmarkVectorizedBuiltinArithmeticFunc(b *testing.B) {
	benchmarkVectorizedBuiltinFunc(b, vecBuiltinArithmeticCases)
}
