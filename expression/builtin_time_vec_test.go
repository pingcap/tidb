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

type periodGener struct {
	randGen *defaultRandGen
}

func newPeriodGener() *periodGener {
	return &periodGener{newDefaultRandGen()}
}

func (g *periodGener) gen() interface{} {
	return int64((g.randGen.Intn(2500)+1)*100 + g.randGen.Intn(12) + 1)
}

// unitStrGener is used to generate strings which are unit format
type unitStrGener struct {
	randGen *defaultRandGen
}

func newUnitStrGener() *unitStrGener {
	return &unitStrGener{newDefaultRandGen()}
}

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

	n := g.randGen.Intn(len(units))
	return units[n]
}

type dateTimeUnitStrGener struct {
	randGen *defaultRandGen
}

func newDateTimeUnitStrGener() *dateTimeUnitStrGener {
	return &dateTimeUnitStrGener{newDefaultRandGen()}
}

// tzStrGener is used to generate strings which are timezones
type tzStrGener struct{}

func (g *tzStrGener) gen() interface{} {
	tzs := []string{
		"",
		"GMT",
		"MET",
		"+00:00",
		"+10:00",
	}

	n := rand.Int() % len(tzs)
	return tzs[n]
}

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

	n := g.randGen.Intn(len(dateTimes))
	return dateTimes[n]
}

var vecBuiltinTimeCases = map[string][]vecExprBenchCase{
	ast.DateLiteral: {
		{retEvalType: types.ETDatetime, childrenTypes: []types.EvalType{types.ETDatetime},
			constants: []*Constant{{Value: types.NewStringDatum("2019-11-11"), RetType: types.NewFieldType(mysql.TypeString)}},
		},
	},
	ast.TimeLiteral: {
		{retEvalType: types.ETDuration, childrenTypes: []types.EvalType{types.ETString},
			constants: []*Constant{
				{Value: types.NewStringDatum("838:59:59"), RetType: types.NewFieldType(mysql.TypeString)}},
		},
	},
	ast.DateDiff: {
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETDatetime, types.ETDatetime}},
	},
	ast.DateFormat: {
		{
			retEvalType:   types.ETString,
			childrenTypes: []types.EvalType{types.ETString, types.ETString},
			geners:        []dataGenerator{&dateTimeStrGener{randGen: newDefaultRandGen()}, newTimeFormatGener(0.5)},
		},
	},
	ast.Hour: {
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETDuration}, geners: []dataGenerator{newRangeDurationGener(0.2)}},
	},
	ast.Minute: {
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETDuration}, geners: []dataGenerator{newRangeDurationGener(0.2)}},
	},
	ast.Second: {
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETDuration}, geners: []dataGenerator{newRangeDurationGener(0.2)}},
	},
	ast.ToSeconds: {
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETDatetime}},
	},
	ast.MicroSecond: {
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETDuration}, geners: []dataGenerator{newRangeDurationGener(0.2)}},
	},
	ast.Now: {
		{retEvalType: types.ETDatetime},
		{
			retEvalType:   types.ETDatetime,
			childrenTypes: []types.EvalType{types.ETInt},
			geners:        []dataGenerator{newRangeInt64Gener(0, 7)},
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
		{retEvalType: types.ETDuration, childrenTypes: []types.EvalType{types.ETInt}, geners: []dataGenerator{newRangeInt64Gener(0, 7)}}, // fsp must be in the range 0 to 6.
	},
	ast.Time: {
		{retEvalType: types.ETDuration, childrenTypes: []types.EvalType{types.ETString}, geners: []dataGenerator{&dateTimeStrGener{randGen: newDefaultRandGen()}}},
	},
	ast.CurrentDate: {
		{retEvalType: types.ETDatetime},
	},
	ast.MakeDate: {
		{retEvalType: types.ETDatetime, childrenTypes: []types.EvalType{types.ETInt, types.ETInt},
			geners: []dataGenerator{newRangeInt64Gener(0, 2200), newRangeInt64Gener(0, 365)},
		},
	},
	ast.MakeTime: {
		{retEvalType: types.ETDuration, childrenTypes: []types.EvalType{types.ETInt, types.ETInt, types.ETReal}, geners: []dataGenerator{newRangeInt64Gener(-1000, 1000)}},
		{
			retEvalType: types.ETDuration, childrenTypes: []types.EvalType{types.ETInt, types.ETInt, types.ETReal},
			childrenFieldTypes: []*types.FieldType{{Tp: mysql.TypeLonglong, Flag: mysql.UnsignedFlag}},
			geners:             []dataGenerator{newRangeInt64Gener(-1000, 1000)},
		},
		{retEvalType: types.ETDuration, childrenTypes: []types.EvalType{types.ETReal, types.ETReal, types.ETReal}, geners: []dataGenerator{newRangeRealGener(-1000.0, 1000.0, 0.1)}},
	},
	ast.PeriodAdd: {
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETInt, types.ETInt}, geners: []dataGenerator{newPeriodGener(), newPeriodGener()}},
	},
	ast.PeriodDiff: {
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETInt, types.ETInt}, geners: []dataGenerator{newPeriodGener(), newPeriodGener()}},
	},
	ast.Quarter: {
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETDatetime}},
	},
	ast.TimeFormat: {
		{retEvalType: types.ETString, childrenTypes: []types.EvalType{types.ETDuration, types.ETString}, geners: []dataGenerator{newRangeDurationGener(0.5), newTimeFormatGener(0.5)}},
	},
	ast.TimeToSec: {
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETDuration}},
	},
	ast.SecToTime: {
		{retEvalType: types.ETDuration, childrenTypes: []types.EvalType{types.ETReal}},
	},
	// This test case may fail due to the issue: https://github.com/pingcap/tidb/issues/13638.
	// We remove this case to stabilize CI, and will reopen this when we fix the issue above.
	//ast.TimestampAdd: {
	//	{
	//		retEvalType:   types.ETString,
	//		childrenTypes: []types.EvalType{types.ETString, types.ETInt, types.ETDatetime},
	//		geners:        []dataGenerator{&unitStrGener{newDefaultRandGen()}, nil, nil},
	//	},
	//},
	ast.UnixTimestamp: {
		{
			retEvalType:   types.ETInt,
			childrenTypes: []types.EvalType{types.ETDatetime},
			childrenFieldTypes: []*types.FieldType{
				{
					Tp:      mysql.TypeDatetime,
					Flen:    types.UnspecifiedLength,
					Decimal: 0,
					Flag:    mysql.BinaryFlag,
				},
			},
			geners: []dataGenerator{&dateTimeGener{Fsp: 0, randGen: newDefaultRandGen()}},
		},
		{retEvalType: types.ETDecimal, childrenTypes: []types.EvalType{types.ETTimestamp}},
		{retEvalType: types.ETInt},
	},
	ast.TimestampDiff: {
		{
			retEvalType:   types.ETInt,
			childrenTypes: []types.EvalType{types.ETString, types.ETDatetime, types.ETDatetime},
			geners:        []dataGenerator{newUnitStrGener(), nil, nil}},
	},
	ast.TimestampLiteral: {
		{retEvalType: types.ETTimestamp, childrenTypes: []types.EvalType{types.ETString},
			constants: []*Constant{{Value: types.NewStringDatum("2019-12-04 00:00:00"), RetType: types.NewFieldType(mysql.TypeString)}},
		},
	},
	ast.SubDate: {},
	ast.AddDate: {},
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
				&dateStrGener{randGen: newDefaultRandGen()},
				&dateStrGener{randGen: newDefaultRandGen()},
			},
		},
		// builtinSubTimeStringNullSig
		{
			retEvalType:        types.ETString,
			childrenTypes:      []types.EvalType{types.ETDatetime, types.ETDatetime},
			childrenFieldTypes: []*types.FieldType{types.NewFieldType(mysql.TypeDate), types.NewFieldType(mysql.TypeDatetime)},
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
				gener{*newDefaultGener(0.2, types.ETString)},
				gener{*newDefaultGener(0.2, types.ETString)},
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
		{retEvalType: types.ETDatetime, childrenTypes: []types.EvalType{types.ETString}, geners: []dataGenerator{&dateTimeStrGener{randGen: newDefaultRandGen()}}},
		{retEvalType: types.ETDatetime, childrenTypes: []types.EvalType{types.ETString}, geners: []dataGenerator{&dateStrGener{randGen: newDefaultRandGen()}}},
		{retEvalType: types.ETDatetime, childrenTypes: []types.EvalType{types.ETString}, geners: []dataGenerator{&timeStrGener{randGen: newDefaultRandGen()}}},
		{retEvalType: types.ETDatetime, childrenTypes: []types.EvalType{types.ETString}},
		{retEvalType: types.ETDatetime, childrenTypes: []types.EvalType{types.ETString, types.ETString},
			geners: []dataGenerator{&dateTimeStrGener{randGen: newDefaultRandGen()}, &timeStrGener{randGen: newDefaultRandGen()}}},
		{retEvalType: types.ETDatetime, childrenTypes: []types.EvalType{types.ETString, types.ETString},
			geners: []dataGenerator{&dateTimeStrGener{randGen: newDefaultRandGen()}, nil}},
		{retEvalType: types.ETDatetime, childrenTypes: []types.EvalType{types.ETString, types.ETString},
			geners: []dataGenerator{nil, &timeStrGener{randGen: newDefaultRandGen()}}},
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
		{retEvalType: types.ETTimestamp, childrenTypes: []types.EvalType{types.ETInt}, geners: []dataGenerator{newRangeInt64Gener(0, 7)}},
	},
	ast.UTCTime: {
		{retEvalType: types.ETDuration},
		{retEvalType: types.ETDuration, childrenTypes: []types.EvalType{types.ETInt}, geners: []dataGenerator{newRangeInt64Gener(0, 7)}},
	},
	ast.Weekday: {
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETDatetime}},
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETDatetime}, geners: []dataGenerator{gener{*newDefaultGener(0.2, types.ETDatetime)}}},
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
			geners: []dataGenerator{gener{*newDefaultGener(0.9, types.ETDecimal)}},
		},
	},
	ast.StrToDate: {
		{
			retEvalType:   types.ETDatetime,
			childrenTypes: []types.EvalType{types.ETString, types.ETString},
			geners:        []dataGenerator{&dateStrGener{randGen: newDefaultRandGen()}, &constStrGener{"%y-%m-%d"}},
		},
		{
			retEvalType:   types.ETDatetime,
			childrenTypes: []types.EvalType{types.ETString, types.ETString},
			geners:        []dataGenerator{&dateStrGener{NullRation: 0.3, randGen: newDefaultRandGen()}, nil},
			constants:     []*Constant{nil, {Value: types.NewDatum("%Y-%m-%d"), RetType: types.NewFieldType(mysql.TypeString)}},
		},
		{
			retEvalType:   types.ETDatetime,
			childrenTypes: []types.EvalType{types.ETString, types.ETString},
			geners:        []dataGenerator{&dateStrGener{randGen: newDefaultRandGen()}, nil},
			// "%y%m%d" is wrong format, STR_TO_DATE should be failed for all rows
			constants: []*Constant{nil, {Value: types.NewDatum("%y%m%d"), RetType: types.NewFieldType(mysql.TypeString)}},
		},
		{
			retEvalType:   types.ETDuration,
			childrenTypes: []types.EvalType{types.ETString, types.ETString},
			geners:        []dataGenerator{&timeStrGener{nullRation: 0.3, randGen: newDefaultRandGen()}, nil},
			constants:     []*Constant{nil, {Value: types.NewDatum("%H:%i:%s"), RetType: types.NewFieldType(mysql.TypeString)}},
		},
		{
			retEvalType:   types.ETDuration,
			childrenTypes: []types.EvalType{types.ETString, types.ETString},
			geners:        []dataGenerator{&timeStrGener{nullRation: 0.3, randGen: newDefaultRandGen()}, nil},
			// "%H%i%s" is wrong format, STR_TO_DATE should be failed for all rows
			constants: []*Constant{nil, {Value: types.NewDatum("%H%i%s"), RetType: types.NewFieldType(mysql.TypeString)}},
		},
	},
	ast.GetFormat: {
		{
			retEvalType:   types.ETString,
			childrenTypes: []types.EvalType{types.ETString, types.ETString},
			geners:        []dataGenerator{newFormatGener(0.2), newLocationGener(0.2)},
		},
	},
	ast.Sysdate: {
		// Because there is a chance that a time error will cause the test to fail,
		// we cannot use the vectorized test framework to test builtinSysDateWithoutFspSig.
		// We test the builtinSysDateWithoutFspSig in TestSysDate function.
		// {retEvalType: types.ETDatetime},
		// {retEvalType: types.ETDatetime, childrenTypes: []types.EvalType{types.ETInt},
		// 	geners: []dataGenerator{newRangeInt64Gener(0, 7)}},
	},
	ast.TiDBParseTso: {
		{
			retEvalType:   types.ETDatetime,
			childrenTypes: []types.EvalType{types.ETInt},
			geners:        []dataGenerator{newRangeInt64Gener(0, math.MaxInt64)},
		},
	},
	ast.LastDay: {
		{retEvalType: types.ETDatetime, childrenTypes: []types.EvalType{types.ETDatetime}},
	},
	ast.Extract: {
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETString, types.ETDatetime}, geners: []dataGenerator{newDateTimeUnitStrGener(), nil}},
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETString, types.ETDuration},
			constants: []*Constant{{Value: types.NewStringDatum("MICROSECOND"), RetType: types.NewFieldType(mysql.TypeString)}},
		},
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETString, types.ETDuration},
			constants: []*Constant{{Value: types.NewStringDatum("SECOND"), RetType: types.NewFieldType(mysql.TypeString)}},
		},
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETString, types.ETDuration},
			constants: []*Constant{{Value: types.NewStringDatum("MINUTE"), RetType: types.NewFieldType(mysql.TypeString)}},
		},
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETString, types.ETDuration},
			constants: []*Constant{{Value: types.NewStringDatum("HOUR"), RetType: types.NewFieldType(mysql.TypeString)}},
		},
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETString, types.ETDuration},
			constants: []*Constant{{Value: types.NewStringDatum("SECOND_MICROSECOND"), RetType: types.NewFieldType(mysql.TypeString)}},
		},
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETString, types.ETDuration},
			constants: []*Constant{{Value: types.NewStringDatum("MINUTE_MICROSECOND"), RetType: types.NewFieldType(mysql.TypeString)}},
		},
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETString, types.ETDuration},
			constants: []*Constant{{Value: types.NewStringDatum("MINUTE_SECOND"), RetType: types.NewFieldType(mysql.TypeString)}},
		},
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETString, types.ETDuration},
			constants: []*Constant{{Value: types.NewStringDatum("HOUR_MICROSECOND"), RetType: types.NewFieldType(mysql.TypeString)}},
		},
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETString, types.ETDuration},
			constants: []*Constant{{Value: types.NewStringDatum("HOUR_SECOND"), RetType: types.NewFieldType(mysql.TypeString)}},
		},
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETString, types.ETDuration},
			constants: []*Constant{{Value: types.NewStringDatum("HOUR_MINUTE"), RetType: types.NewFieldType(mysql.TypeString)}},
		},
	},
	ast.ConvertTz: {
		{retEvalType: types.ETDatetime, childrenTypes: []types.EvalType{types.ETDatetime, types.ETString, types.ETString},
			geners: []dataGenerator{nil, newNullWrappedGener(0.2, &tzStrGener{}), newNullWrappedGener(0.2, &tzStrGener{})}},
	},
}

func (s *testVectorizeSuite2) TestVectorizedBuiltinTimeEvalOneVec(c *C) {
	testVectorizedEvalOneVec(c, vecBuiltinTimeCases)
}

func (s *testVectorizeSuite2) TestVectorizedBuiltinTimeFunc(c *C) {
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
	ctx.GetSessionVars().StmtCtx.TruncateAsWarning = true
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
	ctx.GetSessionVars().StmtCtx.TruncateAsWarning = false
	c.Assert(f.vecEvalInt(input, result), NotNil)
}
