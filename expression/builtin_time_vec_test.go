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
	ast.DateLiteral: {},
	ast.DateDiff:    {},
	ast.TimeDiff: {
		// builtinNullTimeDiffSig
		{retEvalType: types.ETDuration, childrenTypes: []types.EvalType{types.ETDuration, types.ETDatetime}},
		{retEvalType: types.ETDuration, childrenTypes: []types.EvalType{types.ETDuration, types.ETTimestamp}},
		{retEvalType: types.ETDuration, childrenTypes: []types.EvalType{types.ETDatetime, types.ETDuration}},
		{retEvalType: types.ETDuration, childrenTypes: []types.EvalType{types.ETTimestamp, types.ETDuration}},
		// builtinDurationDurationTimeDiffSig
		{retEvalType: types.ETDuration, childrenTypes: []types.EvalType{types.ETDuration, types.ETDuration}},
		// builtinDurationStringTimeDiffSig
		{retEvalType: types.ETDuration, childrenTypes: []types.EvalType{types.ETDuration, types.ETString}, geners: []dataGenerator{nil, &timeStrGener{Year: 2019, Month: 10}}},
		{retEvalType: types.ETDuration, childrenTypes: []types.EvalType{types.ETDuration, types.ETString}, geners: []dataGenerator{nil, &dateTimeStrGener{Year: 2019, Month: 10}}},
		{retEvalType: types.ETDuration, childrenTypes: []types.EvalType{types.ETDuration, types.ETString}, geners: []dataGenerator{nil, &dateTimeStrGener{Year: 2019, Month: 10, Fsp: 4}}},
		// builtinTimeTimeTimeDiffSig
		{retEvalType: types.ETDuration, childrenTypes: []types.EvalType{types.ETDatetime, types.ETDatetime}, geners: []dataGenerator{&dateTimeGener{Year: 2019, Month: 10}, &dateTimeGener{Year: 2019, Month: 10}}},
		{retEvalType: types.ETDuration, childrenTypes: []types.EvalType{types.ETDatetime, types.ETTimestamp}, geners: []dataGenerator{&dateTimeGener{Year: 2019, Month: 10}, &dateTimeGener{Year: 2019, Month: 10}}},
		{retEvalType: types.ETDuration, childrenTypes: []types.EvalType{types.ETTimestamp, types.ETTimestamp}, geners: []dataGenerator{&dateTimeGener{Year: 2019, Month: 10}, &dateTimeGener{Year: 2019, Month: 10}}},
		{retEvalType: types.ETDuration, childrenTypes: []types.EvalType{types.ETTimestamp, types.ETDatetime}, geners: []dataGenerator{&dateTimeGener{Year: 2019, Month: 10}, &dateTimeGener{Year: 2019, Month: 10}}},
		// builtinTimeStringTimeDiffSig
		{retEvalType: types.ETDuration, childrenTypes: []types.EvalType{types.ETDatetime, types.ETString}, geners: []dataGenerator{&dateTimeGener{Year: 2019, Month: 10}, &timeStrGener{Year: 2019, Month: 10}}},
		{retEvalType: types.ETDuration, childrenTypes: []types.EvalType{types.ETDatetime, types.ETString}, geners: []dataGenerator{&dateTimeGener{Year: 2019, Month: 10}, &dateTimeStrGener{Year: 2019, Month: 10}}},
		{retEvalType: types.ETDuration, childrenTypes: []types.EvalType{types.ETTimestamp, types.ETString}, geners: []dataGenerator{&dateTimeGener{Year: 2019, Month: 10}, &timeStrGener{Year: 2019, Month: 10}}},
		{retEvalType: types.ETDuration, childrenTypes: []types.EvalType{types.ETTimestamp, types.ETString}, geners: []dataGenerator{&dateTimeGener{Year: 2019, Month: 10}, &dateTimeStrGener{Year: 2019, Month: 10}}},
		// builtinStringDurationTimeDiffSig
		{retEvalType: types.ETDuration, childrenTypes: []types.EvalType{types.ETString, types.ETDuration}, geners: []dataGenerator{&timeStrGener{Year: 2019, Month: 10}, nil}},
		{retEvalType: types.ETDuration, childrenTypes: []types.EvalType{types.ETString, types.ETDuration}, geners: []dataGenerator{&dateTimeStrGener{Year: 2019, Month: 10}, nil}},
		// builtinStringTimeTimeDiffSig
		{retEvalType: types.ETDuration, childrenTypes: []types.EvalType{types.ETString, types.ETDatetime}, geners: []dataGenerator{&timeStrGener{Year: 2019, Month: 10}, &dateTimeGener{Year: 2019, Month: 10}}},
		{retEvalType: types.ETDuration, childrenTypes: []types.EvalType{types.ETString, types.ETDatetime}, geners: []dataGenerator{&dateTimeStrGener{Year: 2019, Month: 10}, &dateTimeGener{Year: 2019, Month: 10}}},
		{retEvalType: types.ETDuration, childrenTypes: []types.EvalType{types.ETString, types.ETTimestamp}, geners: []dataGenerator{&timeStrGener{Year: 2019, Month: 10}, &dateTimeGener{Year: 2019, Month: 10}}},
		{retEvalType: types.ETDuration, childrenTypes: []types.EvalType{types.ETString, types.ETTimestamp}, geners: []dataGenerator{&dateTimeStrGener{Year: 2019, Month: 10}, &dateTimeGener{Year: 2019, Month: 10}}},
		// builtinStringStringTimeDiffSig
		{retEvalType: types.ETDuration, childrenTypes: []types.EvalType{types.ETString, types.ETString}, geners: []dataGenerator{&timeStrGener{Year: 2019, Month: 10}, &dateTimeStrGener{Year: 2019, Month: 10}}},
		{retEvalType: types.ETDuration, childrenTypes: []types.EvalType{types.ETString, types.ETString}, geners: []dataGenerator{&dateTimeStrGener{Year: 2019, Month: 10}, &timeStrGener{Year: 2019, Month: 10}}},
	},
	ast.DateFormat: {},
	ast.Hour: {
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETDuration}, geners: []dataGenerator{&rangeDurationGener{0.2}}},
	},
	ast.Minute: {
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETDuration}, geners: []dataGenerator{&rangeDurationGener{0.2}}},
	},
	ast.Second: {
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETDuration}, geners: []dataGenerator{&rangeDurationGener{0.2}}},
	},
	ast.MicroSecond: {},
	ast.Now:         {},
	ast.DayOfWeek: {
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETDatetime}},
	},
	ast.DayOfYear: {},
	ast.Day:       {},
	ast.CurrentTime: {
		{retEvalType: types.ETDuration},
		{retEvalType: types.ETDuration, childrenTypes: []types.EvalType{types.ETInt}, geners: []dataGenerator{&rangeInt64Gener{0, 7}}}, // fsp must be in the range 0 to 6.
	},
	ast.CurrentDate: {
		{retEvalType: types.ETDatetime},
	},
	ast.MakeDate:   {},
	ast.MakeTime:   {},
	ast.PeriodAdd:  {},
	ast.PeriodDiff: {},
	ast.Quarter:    {},
	ast.TimeFormat: {
		{retEvalType: types.ETString, childrenTypes: []types.EvalType{types.ETDuration, types.ETString}, geners: []dataGenerator{&rangeDurationGener{0.5}, &timeFormatGener{0.5}}},
	},
	ast.TimeToSec:        {},
	ast.TimestampAdd:     {},
	ast.TimestampDiff:    {},
	ast.TimestampLiteral: {},
	ast.SubDate:          {},
	ast.AddDate:          {},
	ast.SubTime:          {},
	ast.AddTime: {
		// builtinAddStringAndStringSig, a special case written by hand.
		// arg1 has BinaryFlag here.
		{
			retEvalType:   types.ETString,
			childrenTypes: []types.EvalType{types.ETString, types.ETString},
			childrenFieldTypes: []*types.FieldType{nil, {
				Tp:      mysql.TypeString,
				Flen:    types.UnspecifiedLength,
				Decimal: types.UnspecifiedLength,
				Flag:    mysql.BinaryFlag,
			}},
			geners: []dataGenerator{
				gener{defaultGener{eType: types.ETString, nullRation: 0.2}},
				gener{defaultGener{eType: types.ETString, nullRation: 0.2}},
			},
		},
	},
	ast.Month: {
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETDatetime}},
	},
	ast.Year: {
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETDatetime}},
	},
	ast.Date: {
		{retEvalType: types.ETDatetime, childrenTypes: []types.EvalType{types.ETDatetime}},
	},
	ast.Timestamp: {
		{retEvalType: types.ETDatetime, childrenTypes: []types.EvalType{types.ETString}, geners: []dataGenerator{new(dateTimeStrGener)}},
		{retEvalType: types.ETDatetime, childrenTypes: []types.EvalType{types.ETString}, geners: []dataGenerator{new(timeStrGener)}},
		{retEvalType: types.ETDatetime, childrenTypes: []types.EvalType{types.ETString}, geners: []dataGenerator{new(dateStrGener)}},
		{retEvalType: types.ETDatetime, childrenTypes: []types.EvalType{types.ETString}},
		{retEvalType: types.ETDatetime, childrenTypes: []types.EvalType{types.ETString, types.ETString},
			geners: []dataGenerator{new(dateTimeStrGener), new(dateStrGener)}},
		{retEvalType: types.ETDatetime, childrenTypes: []types.EvalType{types.ETString, types.ETString},
			geners: []dataGenerator{new(dateTimeStrGener), nil}},
		{retEvalType: types.ETDatetime, childrenTypes: []types.EvalType{types.ETString, types.ETString},
			geners: []dataGenerator{nil, new(dateStrGener)}},
	},
	ast.MonthName: {
		{retEvalType: types.ETString, childrenTypes: []types.EvalType{types.ETDatetime}},
	},
	ast.DayOfMonth: {
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETDatetime}},
	},
	ast.UTCDate: {
		{retEvalType: types.ETDatetime},
	},
	ast.Weekday: {
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETDatetime}},
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETDatetime}, geners: []dataGenerator{gener{defaultGener{eType: types.ETDatetime, nullRation: 0.2}}}},
	},
	ast.FromDays: {
		{retEvalType: types.ETDatetime, childrenTypes: []types.EvalType{types.ETInt}},
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
