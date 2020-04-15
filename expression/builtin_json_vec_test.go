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
		{retEvalType: types.ETJson, childrenTypes: []types.EvalType{types.ETJson, types.ETString}, geners: []dataGenerator{&constJSONGener{"{\"a\": {\"c\": 3}, \"b\": 2}"}, &constStrGener{"$.a"}}},
	},
	ast.JSONArrayAppend: {
		{retEvalType: types.ETJson, childrenTypes: []types.EvalType{types.ETJson, types.ETString, types.ETJson},
			geners: []dataGenerator{newNullWrappedGener(0.1, &constJSONGener{"{\"a\": {\"c\": 3}, \"b\": 2}"}),
				newNullWrappedGener(0.1, &constStrGener{"$.a"}),
				newNullWrappedGener(0.1, &constJSONGener{"{\"b\": 2}"})}},
		{retEvalType: types.ETJson, childrenTypes: []types.EvalType{types.ETJson, types.ETString, types.ETJson, types.ETString, types.ETJson},
			geners: []dataGenerator{newNullWrappedGener(0.1, &constJSONGener{"{\"a\": {\"c\": 3}, \"b\": 2}"}),
				newNullWrappedGener(0.1, &constStrGener{"$.a"}),
				newNullWrappedGener(0.1, &constJSONGener{"{\"b\": 2}"}),
				newNullWrappedGener(0.1, &constStrGener{"$.b"}),
				newNullWrappedGener(0.1, &constJSONGener{"{\"x\": 3}"}),
			}},
		{retEvalType: types.ETJson, childrenTypes: []types.EvalType{types.ETJson, types.ETString, types.ETJson, types.ETString, types.ETJson},
			geners: []dataGenerator{newNullWrappedGener(0.1, &constJSONGener{"{\"a\": {\"c\": 3}, \"b\": 2}"}),
				newNullWrappedGener(0.1, &constStrGener{"$.a"}),
				newNullWrappedGener(0.1, &constJSONGener{"{\"b\": 2}"}),
				newNullWrappedGener(0.1, &constStrGener{"$.x"}), // not exists
				newNullWrappedGener(0.1, &constJSONGener{"{\"x\": 3}"}),
			}},
	},
	ast.JSONContainsPath: {
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETJson, types.ETString, types.ETString}, geners: []dataGenerator{&constJSONGener{"{\"a\": {\"c\": {\"d\": 4}}, \"b\": 2}"}, &constStrGener{"one"}, &constStrGener{"$.c"}}},
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETJson, types.ETString, types.ETString}, geners: []dataGenerator{&constJSONGener{"{\"a\": {\"c\": {\"d\": 4}}, \"b\": 2}"}, &constStrGener{"all"}, &constStrGener{"$.a"}, &constStrGener{"$.c"}}},
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETJson, types.ETString, types.ETString}, geners: []dataGenerator{&constJSONGener{"{\"a\": {\"c\": {\"d\": 4}}, \"b\": 2}"}, &constStrGener{"one"}, &constStrGener{"$.*"}}},
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETJson, types.ETString, types.ETString}, geners: []dataGenerator{&constJSONGener{"{\"a\": {\"c\": {\"d\": 4}}, \"b\": 2}"}, &constStrGener{"aLl"}, &constStrGener{"$.a"}, &constStrGener{"$.e"}}},
	},
	ast.JSONExtract: {
		{retEvalType: types.ETJson, childrenTypes: []types.EvalType{types.ETJson, types.ETString}, geners: []dataGenerator{nil, &constStrGener{"$.key"}}},
		{retEvalType: types.ETJson, childrenTypes: []types.EvalType{types.ETJson, types.ETString, types.ETString}, geners: []dataGenerator{nil, &constStrGener{"$.key"}, &constStrGener{"$[0]"}}},
	},
	ast.JSONLength: {
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETJson}},
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETJson, types.ETString}, geners: []dataGenerator{nil, &constStrGener{"$.key"}}},
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETJson, types.ETString}, geners: []dataGenerator{nil, &constStrGener{"$.abc"}}},
	},
	ast.JSONType: {{retEvalType: types.ETString, childrenTypes: []types.EvalType{types.ETJson}}},
	ast.JSONArray: {
		{retEvalType: types.ETJson, childrenTypes: []types.EvalType{types.ETJson}},
		{retEvalType: types.ETJson, childrenTypes: []types.EvalType{types.ETJson, types.ETJson}},
		{retEvalType: types.ETJson, childrenTypes: []types.EvalType{types.ETJson, types.ETJson, types.ETJson}},
	},
	ast.JSONArrayInsert: {
		{retEvalType: types.ETJson, childrenTypes: []types.EvalType{types.ETJson, types.ETString, types.ETJson, types.ETString, types.ETJson}, geners: []dataGenerator{&constJSONGener{"[\"a\", {\"b\": [1, 2]}, [3, 4]]"}, &constStrGener{"$[1]"}, nil, &constStrGener{"$[2][1]"}, nil}},
		{retEvalType: types.ETJson, childrenTypes: []types.EvalType{types.ETJson, types.ETString, types.ETJson, types.ETString, types.ETJson}, geners: []dataGenerator{&constJSONGener{"[\"a\", {\"b\": [1, 2]}, [3, 4]]"}, &constStrGener{"$[1]"}, &constJSONGener{"[1,2,3,4,5]"}, &constStrGener{"$[2][1]"}, &constJSONGener{"{\"abc\":1,\"def\":2,\"ghi\":[1,2,3,4]}"}}},
	},
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
				newRandLenStrGener(10, 20), nil,
				newRandLenStrGener(10, 20), nil,
				newRandLenStrGener(10, 20), nil,
				newRandLenStrGener(10, 20), nil,
				newRandLenStrGener(10, 20), nil,
				newRandLenStrGener(10, 20), nil,
				newRandLenStrGener(10, 20), nil,
				newRandLenStrGener(10, 20), nil,
				newRandLenStrGener(10, 20), nil,
				newRandLenStrGener(10, 20), nil,
				newRandLenStrGener(10, 20), nil,
				newRandLenStrGener(10, 20), nil,
			},
		},
	},
	ast.JSONSet: {
		{retEvalType: types.ETJson, childrenTypes: []types.EvalType{types.ETJson, types.ETString, types.ETJson, types.ETString, types.ETJson}, geners: []dataGenerator{nil, &constStrGener{"$.key"}, nil, &constStrGener{"$.aaa"}, nil}},
	},
	ast.JSONSearch: {},
	ast.JSONReplace: {
		{retEvalType: types.ETJson, childrenTypes: []types.EvalType{types.ETJson, types.ETString, types.ETJson}, geners: []dataGenerator{nil, &constStrGener{"$.key"}, nil}},
	},
	ast.JSONStorageSize: {{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETJson}}},
	ast.JSONDepth:       {{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETJson}}},
	ast.JSONUnquote: {
		{retEvalType: types.ETString, childrenTypes: []types.EvalType{types.ETString}, geners: []dataGenerator{newJSONStringGener()}},
	},
	ast.JSONRemove: {
		{retEvalType: types.ETJson, childrenTypes: []types.EvalType{types.ETJson, types.ETString}, geners: []dataGenerator{nil, &constStrGener{"$.key"}}},
	},
	ast.JSONMerge: {{retEvalType: types.ETJson, childrenTypes: []types.EvalType{types.ETJson, types.ETJson, types.ETJson, types.ETJson, types.ETJson}}},
	ast.JSONInsert: {
		{retEvalType: types.ETJson, childrenTypes: []types.EvalType{types.ETJson, types.ETString, types.ETJson, types.ETString, types.ETJson}, geners: []dataGenerator{nil, &constStrGener{"$.aaa"}, nil, &constStrGener{"$.bbb"}, nil}},
	},
	ast.JSONQuote: {
		{retEvalType: types.ETString, childrenTypes: []types.EvalType{types.ETString}},
	},
}

func (s *testVectorizeSuite2) TestVectorizedBuiltinJSONFunc(c *C) {
	testVectorizedBuiltinFunc(c, vecBuiltinJSONCases)
}

func BenchmarkVectorizedBuiltinJSONFunc(b *testing.B) {
	benchmarkVectorizedBuiltinFunc(b, vecBuiltinJSONCases)
}
