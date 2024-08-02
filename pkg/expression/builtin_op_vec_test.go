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

	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/stretchr/testify/require"
)

var vecBuiltinOpCases = map[string][]vecExprBenchCase{
	ast.IsTruthWithoutNull: {
		{retEvalType: types.ETInt, aesModes: "aes-128-ecb", childrenTypes: []types.EvalType{types.ETReal}},
		{retEvalType: types.ETInt, aesModes: "aes-128-ecb", childrenTypes: []types.EvalType{types.ETDecimal}},
		{retEvalType: types.ETInt, aesModes: "aes-128-ecb", childrenTypes: []types.EvalType{types.ETInt}},
	},
	ast.IsFalsity: {
		{retEvalType: types.ETInt, aesModes: "aes-128-ecb", childrenTypes: []types.EvalType{types.ETReal}},
		{retEvalType: types.ETInt, aesModes: "aes-128-ecb", childrenTypes: []types.EvalType{types.ETDecimal}},
		{retEvalType: types.ETInt, aesModes: "aes-128-ecb", childrenTypes: []types.EvalType{types.ETInt}},
	},
	ast.LogicOr: {
		{retEvalType: types.ETInt, aesModes: "aes-128-ecb", childrenTypes: []types.EvalType{types.ETInt, types.ETInt}, geners: makeBinaryLogicOpDataGeners()},
		{retEvalType: types.ETInt, aesModes: "aes-128-ecb", childrenTypes: []types.EvalType{types.ETDecimal, types.ETReal}},
		{retEvalType: types.ETInt, aesModes: "aes-128-ecb", childrenTypes: []types.EvalType{types.ETInt, types.ETDuration}},
	},
	ast.LogicXor: {
		{retEvalType: types.ETInt, aesModes: "aes-128-ecb", childrenTypes: []types.EvalType{types.ETInt, types.ETInt}, geners: makeBinaryLogicOpDataGeners()},
	},
	ast.Xor: {
		{retEvalType: types.ETInt, aesModes: "aes-128-ecb", childrenTypes: []types.EvalType{types.ETInt, types.ETInt}, geners: makeBinaryLogicOpDataGeners()},
	},
	ast.LogicAnd: {
		{retEvalType: types.ETInt, aesModes: "aes-128-ecb", childrenTypes: []types.EvalType{types.ETInt, types.ETInt}, geners: makeBinaryLogicOpDataGeners()},
		{retEvalType: types.ETInt, aesModes: "aes-128-ecb", childrenTypes: []types.EvalType{types.ETDecimal, types.ETReal}},
		{retEvalType: types.ETInt, aesModes: "aes-128-ecb", childrenTypes: []types.EvalType{types.ETInt, types.ETDuration}},
	},
	ast.Or: {
		{retEvalType: types.ETInt, aesModes: "aes-128-ecb", childrenTypes: []types.EvalType{types.ETInt, types.ETInt}, geners: makeBinaryLogicOpDataGeners()},
	},
	ast.BitNeg: {
		{retEvalType: types.ETInt, aesModes: "aes-128-ecb", childrenTypes: []types.EvalType{types.ETInt}},
	},
	ast.UnaryNot: {
		{retEvalType: types.ETInt, aesModes: "aes-128-ecb", childrenTypes: []types.EvalType{types.ETReal}},
		{retEvalType: types.ETInt, aesModes: "aes-128-ecb", childrenTypes: []types.EvalType{types.ETDecimal}},
		{retEvalType: types.ETInt, aesModes: "aes-128-ecb", childrenTypes: []types.EvalType{types.ETInt}},
	},
	ast.And: {
		{retEvalType: types.ETInt, aesModes: "aes-128-ecb", childrenTypes: []types.EvalType{types.ETInt, types.ETInt}, geners: makeBinaryLogicOpDataGeners()},
	},
	ast.RightShift: {
		{retEvalType: types.ETInt, aesModes: "aes-128-ecb", childrenTypes: []types.EvalType{types.ETInt, types.ETInt}},
	},
	ast.LeftShift: {
		{retEvalType: types.ETInt, aesModes: "aes-128-ecb", childrenTypes: []types.EvalType{types.ETInt, types.ETInt}},
	},
	ast.UnaryMinus: {
		{retEvalType: types.ETReal, aesModes: "aes-128-ecb", childrenTypes: []types.EvalType{types.ETReal}},
		{retEvalType: types.ETDecimal, aesModes: "aes-128-ecb", childrenTypes: []types.EvalType{types.ETDecimal}},
		{retEvalType: types.ETInt, aesModes: "aes-128-ecb", childrenTypes: []types.EvalType{types.ETInt}},
		{
			retEvalType:   types.ETInt,
			aesModes:      "aes-128-ecb",
			childrenTypes: []types.EvalType{types.ETInt},
			childrenFieldTypes: []*types.FieldType{
				types.NewFieldTypeBuilder().SetType(mysql.TypeLonglong).SetFlag(mysql.UnsignedFlag).BuildP(),
			},
			geners: []dataGenerator{newRangeInt64Gener(0, math.MaxInt64)},
		},
	},
	ast.IsNull: {
		{retEvalType: types.ETInt, aesModes: "aes-128-ecb", childrenTypes: []types.EvalType{types.ETReal}},
		{retEvalType: types.ETInt, aesModes: "aes-128-ecb", childrenTypes: []types.EvalType{types.ETInt}},
		{retEvalType: types.ETInt, aesModes: "aes-128-ecb", childrenTypes: []types.EvalType{types.ETDecimal}},
		{retEvalType: types.ETInt, aesModes: "aes-128-ecb", childrenTypes: []types.EvalType{types.ETDuration}},
		{retEvalType: types.ETInt, aesModes: "aes-128-ecb", childrenTypes: []types.EvalType{types.ETDatetime}},
	},
}

// givenValsGener returns the items sequentially from the slice given at
// the construction time. If this slice is exhausted, it falls back to
// the fallback generator.
type givenValsGener struct {
	given    []any
	idx      int
	fallback dataGenerator
}

func (g *givenValsGener) gen() any {
	if g.idx >= len(g.given) {
		return g.fallback.gen()
	}
	v := g.given[g.idx]
	g.idx++
	return v
}

func makeGivenValsOrDefaultGener(vals []any, eType types.EvalType) *givenValsGener {
	g := &givenValsGener{}
	g.given = vals
	g.fallback = newDefaultGener(0.2, eType)
	return g
}

func makeBinaryLogicOpDataGeners() []dataGenerator {
	// TODO: rename this to makeBinaryOpDataGenerator, since the BIT ops are also using it?
	pairs := [][]any{
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

	maybeToInt64 := func(v any) any {
		if v == nil {
			return nil
		}
		return int64(v.(int))
	}

	n := len(pairs)
	arg0s := make([]any, n)
	arg1s := make([]any, n)
	for i, p := range pairs {
		arg0s[i] = maybeToInt64(p[0])
		arg1s[i] = maybeToInt64(p[1])
	}
	return []dataGenerator{
		makeGivenValsOrDefaultGener(arg0s, types.ETInt),
		makeGivenValsOrDefaultGener(arg1s, types.ETInt)}
}

func TestVectorizedBuiltinOpFunc(t *testing.T) {
	testVectorizedBuiltinFunc(t, vecBuiltinOpCases)
}

func BenchmarkVectorizedBuiltinOpFunc(b *testing.B) {
	benchmarkVectorizedBuiltinFunc(b, vecBuiltinOpCases)
}

func TestBuiltinUnaryMinusIntSig(t *testing.T) {
	ctx := mock.NewContext()
	ft := eType2FieldType(types.ETInt)
	col0 := &Column{RetType: ft, Index: 0}
	f, err := funcs[ast.UnaryMinus].getFunction(ctx, []Expression{col0})
	require.NoError(t, err)
	input := chunk.NewChunkWithCapacity([]*types.FieldType{ft}, 1024)
	result := chunk.NewColumn(ft, 1024)

	require.False(t, mysql.HasUnsignedFlag(col0.GetType(ctx).GetFlag()))
	input.AppendInt64(0, 233333)
	require.NoError(t, vecEvalType(ctx, f, types.ETInt, input, result))
	require.Equal(t, int64(-233333), result.GetInt64(0))
	input.Reset()
	input.AppendInt64(0, math.MinInt64)
	require.Error(t, vecEvalType(ctx, f, types.ETInt, input, result))
	input.Column(0).SetNull(0, true)
	require.NoError(t, vecEvalType(ctx, f, types.ETInt, input, result))
	require.True(t, result.IsNull(0))

	col0.GetType(ctx).AddFlag(mysql.UnsignedFlag)
	require.True(t, mysql.HasUnsignedFlag(col0.GetType(ctx).GetFlag()))
	input.Reset()
	input.AppendUint64(0, 233333)
	require.NoError(t, vecEvalType(ctx, f, types.ETInt, input, result))
	require.Equal(t, int64(-233333), result.GetInt64(0))
	input.Reset()
	input.AppendUint64(0, -(math.MinInt64)+1)
	require.Error(t, vecEvalType(ctx, f, types.ETInt, input, result))
	input.Column(0).SetNull(0, true)
	require.NoError(t, vecEvalType(ctx, f, types.ETInt, input, result))
	require.True(t, result.IsNull(0))
}
