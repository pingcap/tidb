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
	"fmt"
	"math/rand"
	"testing"

	"github.com/pingcap/tidb/pkg/expression/exprctx"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/stretchr/testify/require"
)

func dateTimeFromString(s string) types.Time {
	t, err := types.ParseDate(types.DefaultStmtNoWarningContext, s)
	if err != nil {
		panic(err)
	}
	return t
}

var vecBuiltinOtherCases = map[string][]vecExprBenchCase{
	ast.SetVar: {
		{retEvalType: types.ETString, childrenTypes: []types.EvalType{types.ETString, types.ETString}},
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETString, types.ETInt}},
		{retEvalType: types.ETReal, childrenTypes: []types.EvalType{types.ETString, types.ETReal}},
		{retEvalType: types.ETDecimal, childrenTypes: []types.EvalType{types.ETString, types.ETDecimal}},
	},
	ast.GetVar: {
		{retEvalType: types.ETString, childrenTypes: []types.EvalType{types.ETString}},
	},
	ast.In:       {},
	ast.BitCount: {{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETInt}}},
	ast.GetParam: {
		{
			retEvalType: types.ETString, childrenTypes: []types.EvalType{types.ETInt},
			geners: []dataGenerator{newRangeInt64Gener(0, 10)},
		},
	},
}

func TestVectorizedBuiltinOtherFunc(t *testing.T) {
	testVectorizedBuiltinFunc(t, vecBuiltinOtherCases)
}

func BenchmarkVectorizedBuiltinOtherFunc(b *testing.B) {
	benchmarkVectorizedBuiltinFunc(b, vecBuiltinOtherCases)
}

func TestInDecimal(t *testing.T) {
	ctx := mock.NewContext()
	ft := eType2FieldType(types.ETDecimal)
	col0 := &Column{RetType: ft, Index: 0}
	col1 := &Column{RetType: ft, Index: 1}
	inFunc, err := funcs[ast.In].getFunction(ctx, []Expression{col0, col1})
	require.NoError(t, err)

	input := chunk.NewChunkWithCapacity([]*types.FieldType{ft, ft}, 1024)
	for i := range 1024 {
		d0 := new(types.MyDecimal)
		d1 := new(types.MyDecimal)
		v := fmt.Sprintf("%d.%d", rand.Intn(1000), rand.Int31())
		require.Nil(t, d0.FromString([]byte(v)))
		v += "00"
		require.Nil(t, d1.FromString([]byte(v)))
		input.Column(0).AppendMyDecimal(d0)
		input.Column(1).AppendMyDecimal(d1)
		require.NotEqual(t, input.Column(0).GetDecimal(i).GetDigitsFrac(), input.Column(1).GetDecimal(i).GetDigitsFrac())
	}
	result := chunk.NewColumn(ft, 1024)
	require.NoError(t, vecEvalType(ctx, inFunc, types.ETInt, input, result))
	for range 1024 {
		require.Equal(t, int64(1), result.GetInt64(0))
	}
}

func TestGetParamVec(t *testing.T) {
	ctx := createContext(t)
	params := []types.Datum{
		types.NewIntDatum(123),
		types.NewStringDatum("abc"),
	}
	ctx.GetSessionVars().PlanCacheParams.Append(params...)
	ft := eType2FieldType(types.ETInt)
	col := &Column{RetType: ft, Index: 0}
	fn, err := funcs[ast.GetParam].getFunction(ctx, []Expression{col})
	require.NoError(t, err)

	input := chunk.NewChunkWithCapacity([]*types.FieldType{ft}, 3)
	for i := range params {
		input.Column(0).AppendInt64(int64(i))
	}
	result := chunk.NewColumn(ft, 3)
	require.NoError(t, vecEvalType(ctx, fn, types.ETString, input, result))
	require.Equal(t, len(params), result.Rows())
	for i := range result.Rows() {
		require.False(t, result.IsNull(i))
		val := result.GetString(i)
		str, err := params[i].ToString()
		require.NoError(t, err)
		require.Equal(t, str, val)
	}

	input = chunk.NewChunkWithCapacity([]*types.FieldType{ft}, 3)
	input.Column(0).AppendInt64(1)
	input.Column(0).AppendInt64(2)
	input.Column(0).AppendInt64(int64(len(params)))
	require.Equal(t, exprctx.ErrParamIndexExceedParamCounts, vecEvalType(ctx, fn, types.ETString, input, result))
}
