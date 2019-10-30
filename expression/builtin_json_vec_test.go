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
	"github.com/pingcap/tidb/types"
)

var vecBuiltinJSONCases = map[string][]vecExprBenchCase{
	ast.JSONKeys: {
		{retEvalType: types.ETJson, childrenTypes: []types.EvalType{types.ETJson}},
	},
	ast.JSONArrayAppend:  {},
	ast.JSONContainsPath: {},
	ast.JSONExtract:      {},
	ast.JSONLength:       {},
	ast.JSONType:         {{retEvalType: types.ETString, childrenTypes: []types.EvalType{types.ETJson}}},
	ast.JSONArray: {
		{retEvalType: types.ETJson, childrenTypes: []types.EvalType{types.ETJson}},
		{retEvalType: types.ETJson, childrenTypes: []types.EvalType{types.ETJson, types.ETJson}},
		{retEvalType: types.ETJson, childrenTypes: []types.EvalType{types.ETJson, types.ETJson, types.ETJson}},
	},
	ast.JSONArrayInsert: {},
	ast.JSONContains: {
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETJson, types.ETJson}},
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETJson, types.ETJson, types.ETString}, geners: []dataGenerator{nil, nil, &constStrGener{"$.abc"}}},
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETJson, types.ETJson, types.ETString}, geners: []dataGenerator{nil, nil, &constStrGener{"$.key"}}},
	},
	ast.JSONObject: {
		{
			retEvalType: types.ETJson,
			childrenTypes: []types.EvalType{
				types.ETString, types.ETJson,
				types.ETString, types.ETJson,
				types.ETString, types.ETJson,
				types.ETString, types.ETJson,
				types.ETString, types.ETJson,
				types.ETString, types.ETJson,
				types.ETString, types.ETJson,
				types.ETString, types.ETJson,
				types.ETString, types.ETJson,
				types.ETString, types.ETJson,
				types.ETString, types.ETJson,
				types.ETString, types.ETJson,
			},
			geners: []dataGenerator{
				&randLenStrGener{10, 20}, nil,
				&randLenStrGener{10, 20}, nil,
				&randLenStrGener{10, 20}, nil,
				&randLenStrGener{10, 20}, nil,
				&randLenStrGener{10, 20}, nil,
				&randLenStrGener{10, 20}, nil,
				&randLenStrGener{10, 20}, nil,
				&randLenStrGener{10, 20}, nil,
				&randLenStrGener{10, 20}, nil,
				&randLenStrGener{10, 20}, nil,
				&randLenStrGener{10, 20}, nil,
				&randLenStrGener{10, 20}, nil,
			},
		},
	},
	ast.JSONSet:     {},
	ast.JSONSearch:  {},
	ast.JSONReplace: {},
	ast.JSONDepth:   {{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETJson}}},
	ast.JSONUnquote: {
		{retEvalType: types.ETString, childrenTypes: []types.EvalType{types.ETString}, geners: []dataGenerator{&jsonStringGener{}}},
	},
	ast.JSONRemove: {},
	ast.JSONMerge:  {{retEvalType: types.ETJson, childrenTypes: []types.EvalType{types.ETJson, types.ETJson, types.ETJson, types.ETJson, types.ETJson}}},
	ast.JSONInsert: {},
	ast.JSONQuote: {
		{retEvalType: types.ETString, childrenTypes: []types.EvalType{types.ETJson}},
	},
}

func (s *testEvaluatorSuite) TestVectorizedBuiltinJSONFunc(c *C) {
	testVectorizedBuiltinFunc(c, vecBuiltinJSONCases)
}

func BenchmarkVectorizedBuiltinJSONFunc(b *testing.B) {
	benchmarkVectorizedBuiltinFunc(b, vecBuiltinJSONCases)
}
