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
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/types/json"
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
	ast.BitCount: {},
	ast.In: {
		{
			retEvalType: types.ETInt,
			childrenTypes: []types.EvalType{
				types.ETInt,
				types.ETInt, types.ETInt, types.ETInt, types.ETInt,
				types.ETInt,
			},
			constants: []*Constant{
				nil,
				nil, nil, nil, nil,
				{Value: types.NewDatum(1), RetType: types.NewFieldType(mysql.TypeInt24)},
			},
			geners: []dataGenerator{&rangeInt64Gener{1, 2}, nil, nil, nil, nil},
		},
		{
			retEvalType: types.ETInt,
			childrenTypes: []types.EvalType{
				types.ETString,
				types.ETString, types.ETString, types.ETString, types.ETString,
				types.ETString, //types.ETString, types.ETString, types.ETString,
			},
			constants: []*Constant{
				nil,
				nil, nil, nil, nil,
				{Value: types.NewStringDatum("aaaaaaaaaa"), RetType: types.NewFieldType(mysql.TypeString)},
				//{Value: types.NewDatum("bbbbbbbbbb"), RetType: types.NewFieldType(mysql.TypeString)},
				//{Value: types.NewDatum("cccccccccc"), RetType: types.NewFieldType(mysql.TypeString)},
				//{Value: types.NewDatum("dddddddddd"), RetType: types.NewFieldType(mysql.TypeString)},
			},
			geners: []dataGenerator{&constStrGener{"aaaaaaaaaa"}, nil, nil, nil, nil},
		},
		{
			retEvalType: types.ETInt,
			childrenTypes: []types.EvalType{
				types.ETDatetime,
				types.ETDatetime, types.ETDatetime,
			},
			constants: []*Constant{
				nil,
				{Value: types.NewTimeDatum(dateTimeFromString("2019-01-01")), RetType: types.NewFieldType(mysql.TypeDatetime)},
				{Value: types.NewTimeDatum(dateTimeFromString("2019-01-01")), RetType: types.NewFieldType(mysql.TypeDatetime)},
			},
		},
		{
			retEvalType: types.ETInt,
			childrenTypes: []types.EvalType{
				types.ETJson,
				types.ETJson, //types.ETJson,
			},
			constants: []*Constant{
				nil,
				{Value: types.NewJSONDatum(json.CreateBinary("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")), RetType: types.NewFieldType(mysql.TypeJSON)},
				//{Value: types.NewJSONDatum(json.CreateBinary("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")), RetType: types.NewFieldType(mysql.TypeJSON)},
			},
		},
		{
			retEvalType: types.ETInt,
			childrenTypes: []types.EvalType{
				types.ETDuration,
				types.ETDuration, types.ETDuration,
			},
			constants: []*Constant{
				nil,
				{Value: types.NewDurationDatum(types.Duration{Duration: time.Duration(1000)}), RetType: types.NewFieldType(mysql.TypeDuration)},
				{Value: types.NewDurationDatum(types.Duration{Duration: time.Duration(2000)}), RetType: types.NewFieldType(mysql.TypeDuration)},
			},
		},
		{
			retEvalType: types.ETInt,
			childrenTypes: []types.EvalType{
				types.ETReal,
				types.ETReal, types.ETReal,
			},
			constants: []*Constant{
				nil,
				{Value: types.NewFloat64Datum(0.1), RetType: types.NewFieldType(mysql.TypeFloat)},
				{Value: types.NewFloat64Datum(0.2), RetType: types.NewFieldType(mysql.TypeFloat)},
			},
		},
	},
	ast.GetParam: {
		{
			retEvalType: types.ETString, childrenTypes: []types.EvalType{types.ETInt},
			geners: []dataGenerator{&rangeInt64Gener{0, 10}},
		},
	},
}

func (s *testEvaluatorSuite) TestVectorizedBuiltinOtherFunc(c *C) {
	testVectorizedBuiltinFunc(c, vecBuiltinOtherCases)
}

func BenchmarkVectorizedBuiltinOtherFunc(b *testing.B) {
	benchmarkVectorizedBuiltinFunc(b, vecBuiltinOtherCases)
}
