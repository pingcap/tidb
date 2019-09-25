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
	ast.CurrentTime: {
	},
	ast.Format: {
	},
	ast.DateDiff: {
	},
	ast.LT: {
	},
	ast.Day: {
	},
	ast.Week: {
	},
	ast.YearWeek: {
	},
	ast.DayName: {
	},
	ast.TimeToSec: {
	},
	ast.DayOfMonth: {
	},
	ast.Regexp: {
	},
	ast.SubDate: {
	},
	ast.MakeDate: {
	},
	ast.Lower: {
	},
	ast.DateLiteral: {
	},
	ast.UTCTime: {
	},
	ast.Quarter: {
	},
	ast.GetFormat: {
	},
	ast.StrToDate: {
	},
	ast.TimestampAdd: {
	},
	ast.FromUnixTime: {
	},
	ast.Mul: {
	},
	ast.UTCDate: {
	},
	ast.Convert: {
	},
	ast.SecToTime: {
	},
	ast.UTCTimestamp: {
	},
	ast.Abs: {
	},
	ast.SubTime: {
	},
	ast.Bin: {
	},
	ast.Length: {
	},
	ast.DayOfWeek: {
	},
	ast.MicroSecond: {
	},
	ast.PeriodAdd: {
	},
	ast.CurrentDate: {
	},
	ast.Replace: {
	},
	ast.UnixTimestamp: {
	},
	ast.If: {
	},
	ast.AddDate: {
	},
	ast.Hour: {
	},
	ast.Log: {
	},
	ast.Timestamp: {
	},
	ast.GE: {
	},
	ast.DayOfYear: {
	},
	ast.Round: {
	},
	ast.FromDays: {
	},
	ast.TimeDiff: {
	},
	ast.Upper: {
	},
	ast.AddTime: {
	},
	ast.MakeTime: {
	},
	ast.MonthName: {
	},
	ast.And: {
	},
	ast.TimeFormat: {
	},
	ast.TimestampLiteral: {
	},
	ast.ToSeconds: {
	},
	ast.TimestampDiff: {
	},
	ast.Extract: {
	},
	ast.ToDays: {
	},
	ast.LastDay: {
	},
	ast.ConvertTz: {
	},
	ast.Truncate: {
	},
	ast.Conv: {
	},
	ast.Minute: {
	},
	ast.Second: {
	},
	ast.TimeLiteral: {
	},
	ast.Interval: {
	},
	ast.Now: {
	},
	ast.WeekOfYear: {
	},
	ast.DateFormat: {
	},
	ast.PeriodDiff: {
	},
	ast.Weekday: {
	}, ast.Month: {
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETDatetime}},
	},
	ast.Year: {
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETDatetime}},
	},
	ast.Date: {
		{retEvalType: types.ETDatetime, childrenTypes: []types.EvalType{types.ETDatetime}},
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

	f, _, _, result := genVecBuiltinFuncBenchCase(ctx, ast.Month, vecExprBenchCase{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETDatetime}})
	c.Assert(ctx.GetSessionVars().StrictSQLMode, IsTrue)
	c.Assert(f.vecEvalInt(input, result), IsNil)
	c.Assert(len(ctx.GetSessionVars().StmtCtx.GetWarnings()), Equals, 2)

	ctx.GetSessionVars().StmtCtx.InInsertStmt = true
	c.Assert(f.vecEvalInt(input, result), NotNil)
}
