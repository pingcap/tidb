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
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/mock"
)

var vecBuiltinTimeCases = map[string][]vecExprBenchCase{
	ast.Month: {
		{types.ETInt, []types.EvalType{types.ETDatetime}, nil},
	},
	ast.Date: {
		{types.ETDatetime, []types.EvalType{types.ETDatetime}, nil},
	},
}

func (s *testEvaluatorSuite) TestVectorizedBuiltinTimeEvalOneVec(c *C) {
	testVectorizedEvalOneVec(c, vecBuiltinTimeCases)
}

func (s *testEvaluatorSuite) TestVectorizedBuiltinTimeFunc(c *C) {
	testVectorizedBuiltinFunc(c, vecBuiltinTimeCases)
}

func BenchmarkVectorizedBuiltinTimeEvalOneVec(b *testing.B) {
	benchmarkVectorizedEvalOneVec(b, vecBuiltinTimeCases)
}

func BenchmarkVectorizedBuiltinTimeFunc(b *testing.B) {
	benchmarkVectorizedBuiltinFunc(b, vecBuiltinTimeCases)
}

func (s *testEvaluatorSuite) TestVecMonth(c *C) {
	ctx := mock.NewContext()
	ctx.GetSessionVars().SQLMode |= mysql.ModeNoZeroDate
	input := chunk.New([]*types.FieldType{types.NewFieldType(mysql.TypeDatetime)}, 3, 3)
	input.Reset()
	input.AppendTime(0, types.ZeroDate)
	input.AppendNull(0)
	input.AppendTime(0, types.ZeroDate)

	f, _, result := genVecBuiltinFuncBenchCase(ctx, ast.Month, vecExprBenchCase{types.ETInt, []types.EvalType{types.ETDatetime}, nil})
	c.Assert(ctx.GetSessionVars().StrictSQLMode, IsTrue)
	c.Assert(f.vecEvalInt(input, result), IsNil)
	c.Assert(len(ctx.GetSessionVars().StmtCtx.GetWarnings()), Equals, 2)

	ctx.GetSessionVars().StmtCtx.InInsertStmt = true
	c.Assert(f.vecEvalInt(input, result), NotNil)
}
