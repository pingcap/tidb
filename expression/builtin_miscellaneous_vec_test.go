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

var vecBuiltinMiscellaneousCases = map[string][]vecExprBenchCase{
	ast.NameConst: {
	},
	ast.LT: {
	},
	ast.Inet6Aton: {
	},
	ast.Time: {
	},
	ast.IsIPv6: {
	},
	ast.Charset: {
	},
	ast.Mod: {
	},
	ast.ReleaseLock: {
	},
	ast.Bin: {
	},
	ast.Length: {
	},
	ast.Collation: {
	},
	ast.Sleep: {
	},
	ast.UUID: {
	},
	ast.Date: {
	},
	ast.Timestamp: {
	},
	ast.Inet6Ntoa: {
	},
	ast.And: {
	},
	ast.InetAton: {
	},
	ast.AnyValue: {
	},
	ast.Second: {
	},
	ast.IsIPv4Mapped: {
	},
	ast.Now: {
	},
	ast.IsIPv4Compat: {
	},
	ast.LE: {
	},	ast.InetNtoa: {
		{retEvalType: types.ETString, childrenTypes: []types.EvalType{types.ETInt}},
	},
	ast.IsIPv4: {
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETString}},
	},
}

func (s *testEvaluatorSuite) TestVectorizedBuiltinMiscellaneousCasesEvalOneVec(c *C) {
	testVectorizedEvalOneVec(c, vecBuiltinMiscellaneousCases)
}

func (s *testEvaluatorSuite) TestVectorizedBuiltinMiscellaneousCasesFunc(c *C) {
	testVectorizedBuiltinFunc(c, vecBuiltinMiscellaneousCases)
}

func BenchmarkVectorizedBuiltinMiscellaneousCasesEvalOneVec(b *testing.B) {
	benchmarkVectorizedEvalOneVec(b, vecBuiltinMiscellaneousCases)
}

func BenchmarkVectorizedBuiltinMiscellaneousCasesFunc(b *testing.B) {
	benchmarkVectorizedBuiltinFunc(b, vecBuiltinMiscellaneousCases)
}
