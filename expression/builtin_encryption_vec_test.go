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

var vecBuiltinEncryptionCases = map[string][]vecExprBenchCase{
	ast.AesEncrypt: {},
	ast.Uncompress: {
		{retEvalType: types.ETString, childrenTypes: []types.EvalType{types.ETString}},
	},
	ast.AesDecrypt: {},
	ast.Compress: {
		{retEvalType: types.ETString, childrenTypes: []types.EvalType{types.ETString}},
	},
	ast.MD5: {
		{retEvalType: types.ETString, childrenTypes: []types.EvalType{types.ETString}},
	},
	ast.SHA: {
		{retEvalType: types.ETString, childrenTypes: []types.EvalType{types.ETString}},
	},
	ast.RandomBytes:        {},
	ast.UncompressedLength: {},
	ast.SHA1: {
		{retEvalType: types.ETString, childrenTypes: []types.EvalType{types.ETString}},
	},
	ast.PasswordFunc: {
		{retEvalType: types.ETString, childrenTypes: []types.EvalType{types.ETString}, geners: []dataGenerator{&randLenStrGener{10, 20}}},
	},
	ast.SHA2: {
		{retEvalType: types.ETString, childrenTypes: []types.EvalType{types.ETString, types.ETInt}, geners: []dataGenerator{&randLenStrGener{10, 20}, &rangeInt64Gener{SHA0, SHA0 + 1}}},
		{retEvalType: types.ETString, childrenTypes: []types.EvalType{types.ETString, types.ETInt}, geners: []dataGenerator{&randLenStrGener{10, 20}, &rangeInt64Gener{SHA224, SHA224 + 1}}},
		{retEvalType: types.ETString, childrenTypes: []types.EvalType{types.ETString, types.ETInt}, geners: []dataGenerator{&randLenStrGener{10, 20}, &rangeInt64Gener{SHA256, SHA256 + 1}}},
		{retEvalType: types.ETString, childrenTypes: []types.EvalType{types.ETString, types.ETInt}, geners: []dataGenerator{&randLenStrGener{10, 20}, &rangeInt64Gener{SHA384, SHA384 + 1}}},
		{retEvalType: types.ETString, childrenTypes: []types.EvalType{types.ETString, types.ETInt}, geners: []dataGenerator{&randLenStrGener{10, 20}, &rangeInt64Gener{SHA512, SHA512 + 1}}},
	},
	ast.Encode: {
		{retEvalType: types.ETString, childrenTypes: []types.EvalType{types.ETString, types.ETString}},
	},
	ast.Decode: {
		{retEvalType: types.ETString, childrenTypes: []types.EvalType{types.ETString, types.ETString}, geners: []dataGenerator{&randLenStrGener{10, 20}}},
	},
}

func (s *testEvaluatorSuite) TestVectorizedBuiltinEncryptionFunc(c *C) {
	testVectorizedBuiltinFunc(c, vecBuiltinEncryptionCases)
}

func BenchmarkVectorizedBuiltinEncryptionFunc(b *testing.B) {
	benchmarkVectorizedBuiltinFunc(b, vecBuiltinEncryptionCases)
}
