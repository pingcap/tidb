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
	ast.AesEncrypt:         {},
	ast.Uncompress:         {},
	ast.AesDecrypt:         {},
	ast.Compress:           {},
	ast.MD5:                {},
	ast.SHA:                {},
	ast.RandomBytes:        {},
	ast.UncompressedLength: {},
	ast.SHA1:               {},
	ast.PasswordFunc:       {},
	ast.SHA2:               {},
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
