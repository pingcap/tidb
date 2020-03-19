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
	"fmt"
	"math/rand"
	"testing"

	. "github.com/pingcap/check"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/mock"
)

func dateTimeFromString(s string) types.Time {
	t, err := types.ParseDate(nil, s)
	if err != nil {
		panic(err)
	}
	return t
}

var vecBuiltinOtherCases = map[string][]vecExprBenchCase{
	ast.SetVar: {
		{retEvalType: types.ETString, childrenTypes: []types.EvalType{types.ETString, types.ETString}},
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

func (s *testEvaluatorSuite) TestVectorizedBuiltinOtherFunc(c *C) {
	testVectorizedBuiltinFunc(c, vecBuiltinOtherCases)
}

func BenchmarkVectorizedBuiltinOtherFunc(b *testing.B) {
	benchmarkVectorizedBuiltinFunc(b, vecBuiltinOtherCases)
}

func (s *testEvaluatorSuite) TestInDecimal(c *C) {
	ctx := mock.NewContext()
	ft := eType2FieldType(types.ETDecimal)
	col0 := &Column{RetType: ft, Index: 0}
	col1 := &Column{RetType: ft, Index: 1}
	inFunc, err := funcs[ast.In].getFunction(ctx, []Expression{col0, col1})
	c.Assert(err, IsNil)

	input := chunk.NewChunkWithCapacity([]*types.FieldType{ft, ft}, 1024)
	for i := 0; i < 1024; i++ {
		d0 := new(types.MyDecimal)
		d1 := new(types.MyDecimal)
		v := fmt.Sprintf("%d.%d", rand.Intn(1000), rand.Int31())
		c.Assert(d0.FromString([]byte(v)), IsNil)
		v += "00"
		c.Assert(d1.FromString([]byte(v)), IsNil)
		input.Column(0).AppendMyDecimal(d0)
		input.Column(1).AppendMyDecimal(d1)
		c.Assert(input.Column(0).GetDecimal(i).GetDigitsFrac(), Not(Equals), input.Column(1).GetDecimal(i).GetDigitsFrac())
	}
	result := chunk.NewColumn(ft, 1024)
	c.Assert(inFunc.vecEvalInt(input, result), IsNil)
	for i := 0; i < 1024; i++ {
		c.Assert(result.GetInt64(0), Equals, int64(1))
	}
}
