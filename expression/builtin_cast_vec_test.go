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
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/types/json"
)

var vecBuiltinCastCases = map[string][]vecExprBenchCase{
	ast.Cast: {
		{retEvalType: types.ETDecimal, childrenTypes: []types.EvalType{types.ETJson}, geners: []dataGenerator{&decimalJSONGener{}}},
		{retEvalType: types.ETDecimal, childrenTypes: []types.EvalType{types.ETString}, geners: []dataGenerator{&decimalStringGener{}}},
		{retEvalType: types.ETDecimal, childrenTypes: []types.EvalType{types.ETReal}},
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETInt}},
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETReal}},
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETDecimal}},
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETJson}},
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETDatetime}},
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETDuration}},
		{
			retEvalType:   types.ETInt,
			childrenTypes: []types.EvalType{types.ETString},
			geners:        []dataGenerator{&numStrGener{rangeInt64Gener{math.MinInt64 + 1, 0}}},
		},
		{
			retEvalType:   types.ETInt,
			childrenTypes: []types.EvalType{types.ETString},
			geners:        []dataGenerator{&numStrGener{rangeInt64Gener{0, math.MaxInt64}}},
		},
		{retEvalType: types.ETReal, childrenTypes: []types.EvalType{types.ETInt}},
		{retEvalType: types.ETDuration, childrenTypes: []types.EvalType{types.ETInt}, geners: []dataGenerator{new(randDurInt)}},
		{retEvalType: types.ETDuration, childrenTypes: []types.EvalType{types.ETReal}, geners: []dataGenerator{new(randDurReal)}},
		{retEvalType: types.ETDuration, childrenTypes: []types.EvalType{types.ETDecimal}, geners: []dataGenerator{new(randDurDecimal)}},
		{retEvalType: types.ETReal, childrenTypes: []types.EvalType{types.ETReal}},
		{retEvalType: types.ETReal, childrenTypes: []types.EvalType{types.ETJson}},
		{retEvalType: types.ETReal, childrenTypes: []types.EvalType{types.ETDecimal}},
		{retEvalType: types.ETReal, childrenTypes: []types.EvalType{types.ETDatetime}},
		{retEvalType: types.ETReal, childrenTypes: []types.EvalType{types.ETDuration}, geners: []dataGenerator{&rangeDurationGener{nullRation: 0.5}}},
		{retEvalType: types.ETDuration, childrenTypes: []types.EvalType{types.ETDatetime},
			geners: []dataGenerator{&dateTimeGenerWithFsp{
				defaultGener: defaultGener{nullRation: 0.2, eType: types.ETDatetime},
				fsp:          1,
			}},
		},
		{retEvalType: types.ETString, childrenTypes: []types.EvalType{types.ETInt}},
		{retEvalType: types.ETDecimal, childrenTypes: []types.EvalType{types.ETDatetime}},
		{retEvalType: types.ETString, childrenTypes: []types.EvalType{types.ETString}},
		{retEvalType: types.ETString, childrenTypes: []types.EvalType{types.ETDuration}},
		{retEvalType: types.ETString, childrenTypes: []types.EvalType{types.ETDatetime}},
		{retEvalType: types.ETString, childrenTypes: []types.EvalType{types.ETTimestamp}},
		{retEvalType: types.ETString, childrenTypes: []types.EvalType{types.ETReal}},
		{retEvalType: types.ETString, childrenTypes: []types.EvalType{types.ETJson}},
		{retEvalType: types.ETString, childrenTypes: []types.EvalType{types.ETDecimal}},
		{retEvalType: types.ETDuration, childrenTypes: []types.EvalType{types.ETJson}, geners: []dataGenerator{&randJSONDuration{}}},
		{retEvalType: types.ETDuration, childrenTypes: []types.EvalType{types.ETDuration}},
		{retEvalType: types.ETJson, childrenTypes: []types.EvalType{types.ETInt}},
		{retEvalType: types.ETJson, childrenTypes: []types.EvalType{types.ETReal}},
		{retEvalType: types.ETJson, childrenTypes: []types.EvalType{types.ETDuration}},
		{retEvalType: types.ETJson, childrenTypes: []types.EvalType{types.ETDatetime}},
		{retEvalType: types.ETJson, childrenTypes: []types.EvalType{types.ETJson}},
		{retEvalType: types.ETJson, childrenTypes: []types.EvalType{types.ETString}, geners: []dataGenerator{&jsonStringGener{}}},
		{retEvalType: types.ETJson, childrenTypes: []types.EvalType{types.ETDecimal}},
		{retEvalType: types.ETDatetime, childrenTypes: []types.EvalType{types.ETJson}, geners: []dataGenerator{&datetimeJSONGener{}}},
		{retEvalType: types.ETDatetime, childrenTypes: []types.EvalType{types.ETReal}},
		{retEvalType: types.ETDatetime, childrenTypes: []types.EvalType{types.ETDecimal}},
		{retEvalType: types.ETDatetime, childrenTypes: []types.EvalType{types.ETInt}},
		{retEvalType: types.ETDatetime, childrenTypes: []types.EvalType{types.ETString},
			geners: []dataGenerator{
				&dateTimeStrGener{},
				&timeStrGener{},
				&dateStrGener{},
			}},
		{retEvalType: types.ETDatetime, childrenTypes: []types.EvalType{types.ETDuration}},
		{retEvalType: types.ETDatetime, childrenTypes: []types.EvalType{types.ETDatetime}},
		{retEvalType: types.ETDatetime, childrenTypes: []types.EvalType{types.ETTimestamp}},
		{retEvalType: types.ETDatetime, childrenTypes: []types.EvalType{types.ETJson},
			geners: []dataGenerator{
				&jsonTimeGener{},
			}},
		{retEvalType: types.ETDecimal, childrenTypes: []types.EvalType{types.ETDecimal}},
	},
}

type dateTimeGenerWithFsp struct {
	defaultGener
	fsp int8
}

func (g *dateTimeGenerWithFsp) gen() interface{} {
	result := g.defaultGener.gen()
	if t, ok := result.(types.Time); ok {
		t.Fsp = g.fsp
		return t
	}
	return result
}

type randJSONDuration struct{}

func (g *randJSONDuration) gen() interface{} {
	d := types.Duration{
		Duration: time.Duration(time.Duration(rand.Intn(12))*time.Hour + time.Duration(rand.Intn(60))*time.Minute + time.Duration(rand.Intn(60))*time.Second + time.Duration(rand.Intn(1000))*time.Millisecond),
		Fsp:      3}
	return json.CreateBinary(d.String())
}

type datetimeJSONGener struct{}

func (g *datetimeJSONGener) gen() interface{} {
	year := rand.Intn(2200)
	month := rand.Intn(10) + 1
	day := rand.Intn(20) + 1
	hour := rand.Intn(12)
	minute := rand.Intn(60)
	second := rand.Intn(60)
	microsecond := rand.Intn(1000000)
	d := types.Time{
		Time: types.FromDate(year, month, day, hour, minute, second, microsecond),
		Fsp:  3,
	}
	return json.CreateBinary(d.String())
}

func (s *testEvaluatorSuite) TestVectorizedBuiltinCastEvalOneVec(c *C) {
	testVectorizedEvalOneVec(c, vecBuiltinCastCases)
}

func (s *testEvaluatorSuite) TestVectorizedBuiltinCastFunc(c *C) {
	testVectorizedBuiltinFunc(c, vecBuiltinCastCases)
}

func BenchmarkVectorizedBuiltinCastEvalOneVec(b *testing.B) {
	benchmarkVectorizedEvalOneVec(b, vecBuiltinCastCases)
}

func BenchmarkVectorizedBuiltinCastFunc(b *testing.B) {
	benchmarkVectorizedBuiltinFunc(b, vecBuiltinCastCases)
}
