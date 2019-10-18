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
	ast.JSONType:         {},
	ast.JSONArray:        {},
	ast.JSONArrayInsert:  {},
	ast.JSONContains:     {},
	ast.JSONObject:       {},
	ast.JSONSet:          {},
	ast.JSONSearch:       {},
	ast.JSONReplace:      {},
	ast.JSONDepth:        {},
	ast.JSONUnquote:      {},
	ast.JSONRemove:       {},
	ast.JSONMerge:        {},
	ast.JSONInsert:       {},
	ast.JSONQuote:        {},
}

func (s *testEvaluatorSuite) TestVectorizedBuiltinJSONFunc(c *C) {
	testVectorizedBuiltinFunc(c, vecBuiltinJSONCases)
}

func BenchmarkVectorizedBuiltinJSONFunc(b *testing.B) {
	benchmarkVectorizedBuiltinFunc(b, vecBuiltinJSONCases)
}
