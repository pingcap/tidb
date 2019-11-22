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
	"math/rand"
	"testing"

	. "github.com/pingcap/check"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/mock"
)

type periodGener struct{}

func (g *periodGener) gen() interface{} {
	return int64((rand.Intn(2500)+1)*100 + rand.Intn(12) + 1)
}

// unitStrGener is used to generate strings which are unit format
type unitStrGener struct{}

func (g *unitStrGener) gen() interface{} {
	units := []string{
		"MICROSECOND",
		"SECOND",
		"MINUTE",
		"HOUR",
		"DAY",
		"WEEK",
		"MONTH",
		"QUARTER",
		"YEAR",
	}

	n := rand.Int() % len(units)
	return units[n]
}

type dateTimeUnitStrGener struct{}

func (g *dateTimeUnitStrGener) gen() interface{} {
	dateTimes := []string{
		"DAY",
		"WEEK",
		"MONTH",
		"QUARTER",
		"YEAR",
		"DAY_MICROSECOND",
		"DAY_SECOND",
		"DAY_MINUTE",
		"DAY_HOUR",
		"YEAR_MONTH",
	}

	n := rand.Int() % len(dateTimes)
	return dateTimes[n]
}

var vecBuiltinTimeCases = map[string][]vecExprBenchCase{
	ast.DateLiteral: {
		{retEvalType: types.ETDatetime, childrenTypes: []types.EvalType{types.ETDatetime},
			constants: []*Constant{{Value: types.NewStringDatum("2019-11-11"), RetType: types.NewFieldType(mysql.TypeString)}},
		},
	},
	ast.DateDiff:   {},
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
	ast.ToSeconds: {
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETDatetime}},
	},
	ast.MicroSecond: {
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETDuration}, geners: []dataGenerator{&rangeDurationGener{0.2}}},
	},
	ast.Now: {
		{retEvalType: types.ETDatetime},
		{retEvalType: types.ETDatetime, childrenTypes: []types.EvalType{types.ETInt},
			geners: []dataGenerator{&rangeInt64Gener{0, 7}},
		},
	},
	ast.DayOfWeek: {
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETDatetime}},
	},
	ast.DayOfYear: {
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETDatetime}},
	},
	ast.Day: {},
	ast.ToDays: {
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETDatetime}},
	},
	ast.CurrentTime: {
		{retEvalType: types.ETDuration},
		{retEvalType: types.ETDuration, childrenTypes: []types.EvalType{types.ETInt}, geners: []dataGenerator{&rangeInt64Gener{0, 7}}}, // fsp must be in the range 0 to 6.
	},
	ast.CurrentDate: {
		{retEvalType: types.ETDatetime},
	},
	ast.MakeDate: {
		{retEvalType: types.ETDatetime, childrenTypes: []types.EvalType{types.ETInt, types.ETInt},
			geners: []dataGenerator{&rangeInt64Gener{0, 2200}, &rangeInt64Gener{0, 365}},
		},
	},
	ast.MakeTime: {},
	ast.PeriodAdd: {
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETInt, types.ETInt}, geners: []dataGenerator{new(periodGener), new(periodGener)}},
	},
	ast.PeriodDiff: {
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETInt, types.ETInt}, geners: []dataGenerator{new(periodGener), new(periodGener)}},
	},
	ast.Quarter: {
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETDatetime}},
	},
	ast.TimeFormat: {
		{retEvalType: types.ETString, childrenTypes: []types.EvalType{types.ETDuration, types.ETString}, geners: []dataGenerator{&rangeDurationGener{0.5}, &timeFormatGener{0.5}}},
	},
	ast.TimeToSec: {
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETDuration}},
	},
	ast.SecToTime: {
		{retEvalType: types.ETDuration, childrenTypes: []types.EvalType{types.ETReal}},
	},
	ast.TimestampAdd: {
		{
			retEvalType:   types.ETString,
			childrenTypes: []types.EvalType{types.ETString, types.ETInt, types.ETDatetime},
			geners:        []dataGenerator{&unitStrGener{}, nil, nil},
		},
	},
	ast.TimestampDiff: {
		{
			retEvalType:   types.ETInt,
			childrenTypes: []types.EvalType{types.ETString, types.ETDatetime, types.ETDatetime},
			geners:        []dataGenerator{&unitStrGener{}, nil, nil}},
	},
	ast.TimestampLiteral: {},
	ast.SubDate:          {},
	ast.AddDate:          {},
	ast.SubTime: {
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
				&timeStrGener{},
				&timeStrGener{},
			},
		},
	},
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
	ast.Week: {
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETDatetime}},
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETDatetime, types.ETInt}},
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
	ast.DayName: {
		{retEvalType: types.ETString, childrenTypes: []types.EvalType{types.ETDatetime}},
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETDatetime}},
		{retEvalType: types.ETReal, childrenTypes: []types.EvalType{types.ETDatetime}},
	},
	ast.UTCDate: {
		{retEvalType: types.ETDatetime},
	},
	ast.UTCTimestamp: {
		{retEvalType: types.ETTimestamp},
		{retEvalType: types.ETTimestamp, childrenTypes: []types.EvalType{types.ETInt}, geners: []dataGenerator{&rangeInt64Gener{begin: 0, end: 7}}},
	},
	ast.UnixTimestamp: {
		{retEvalType: types.ETInt},
	},
	ast.UTCTime: {
		{retEvalType: types.ETDuration},
		{retEvalType: types.ETDuration, childrenTypes: []types.EvalType{types.ETInt}, geners: []dataGenerator{&rangeInt64Gener{begin: 0, end: 7}}},
	},
	ast.Weekday: {
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETDatetime}},
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETDatetime}, geners: []dataGenerator{gener{defaultGener{eType: types.ETDatetime, nullRation: 0.2}}}},
	},
	ast.YearWeek: {
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETDatetime}},
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETDatetime, types.ETInt}},
	},
	ast.WeekOfYear: {
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETDatetime}},
	},
	ast.FromDays: {
		{retEvalType: types.ETDatetime, childrenTypes: []types.EvalType{types.ETInt}},
	},
	ast.FromUnixTime: {
		{retEvalType: types.ETDatetime, childrenTypes: []types.EvalType{types.ETDecimal},
			geners: []dataGenerator{gener{defaultGener{eType: types.ETDecimal, nullRation: 0.9}}},
		},
	},
	ast.StrToDate: {
		{
			retEvalType:   types.ETDatetime,
			childrenTypes: []types.EvalType{types.ETString, types.ETString},
			geners:        []dataGenerator{&timeStrGener{}, &constStrGener{"%y-%m-%d"}},
		},
		{
			retEvalType:   types.ETDatetime,
			childrenTypes: []types.EvalType{types.ETString, types.ETString},
			geners:        []dataGenerator{&timeStrGener{NullRation: 0.3}, nil},
			constants:     []*Constant{nil, {Value: types.NewDatum("%Y-%m-%d"), RetType: types.NewFieldType(mysql.TypeString)}},
		},
		{
			retEvalType:   types.ETDatetime,
			childrenTypes: []types.EvalType{types.ETString, types.ETString},
			geners:        []dataGenerator{&timeStrGener{}, nil},
			// "%y%m%d" is wrong format, STR_TO_DATE should be failed for all rows
			constants: []*Constant{nil, {Value: types.NewDatum("%y%m%d"), RetType: types.NewFieldType(mysql.TypeString)}},
		},
	},
	ast.GetFormat: {
		{
			retEvalType:   types.ETString,
			childrenTypes: []types.EvalType{types.ETString, types.ETString},
			geners:        []dataGenerator{&formatGener{0.2}, &locationGener{0.2}},
		},
	},
	ast.Sysdate: {
		// Because there is a chance that a time error will cause the test to fail,
		// we cannot use the vectorized test framework to test builtinSysDateWithoutFspSig.
		// We test the builtinSysDateWithoutFspSig in TestSysDate function.
		// {retEvalType: types.ETDatetime},
		// {retEvalType: types.ETDatetime, childrenTypes: []types.EvalType{types.ETInt},
		// 	geners: []dataGenerator{&rangeInt64Gener{begin: 0, end: 7}}},
	},
	ast.TiDBParseTso: {
		{
			retEvalType:   types.ETDatetime,
			childrenTypes: []types.EvalType{types.ETInt},
			geners:        []dataGenerator{&rangeInt64Gener{0, math.MaxInt64}},
		},
	},
	ast.LastDay: {
		{retEvalType: types.ETDatetime, childrenTypes: []types.EvalType{types.ETDatetime}},
	},
	ast.Extract: {
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETString, types.ETDatetime}, geners: []dataGenerator{&dateTimeUnitStrGener{}, nil}},
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
