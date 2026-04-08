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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package expression

import (
	"testing"

	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/types"
)

var vecBuiltinEncryptionCases = map[string][]vecExprBenchCase{
	ast.AesEncrypt: {
		{retEvalType: types.ETString, childrenTypes: []types.EvalType{types.ETString, types.ETString}, aesModes: "aes-128-ecb"},
		{retEvalType: types.ETString, childrenTypes: []types.EvalType{types.ETString, types.ETString, types.ETString}, geners: []dataGenerator{nil, nil, &constStrGener{"iv"}}, aesModes: "aes-128-ecb"},
		{retEvalType: types.ETString, childrenTypes: []types.EvalType{types.ETString, types.ETString, types.ETString}, geners: []dataGenerator{nil, nil, newRandLenStrGener(16, 17)}, aesModes: "aes-128-cbc"},
		{retEvalType: types.ETString, childrenTypes: []types.EvalType{types.ETString, types.ETString, types.ETString}, geners: []dataGenerator{nil, nil, newRandLenStrGener(16, 17)}, aesModes: "aes-128-ofb"},
		{retEvalType: types.ETString, childrenTypes: []types.EvalType{types.ETString, types.ETString, types.ETString}, geners: []dataGenerator{nil, nil, newRandLenStrGener(16, 17)}, aesModes: "aes-128-cfb"},
	},
	ast.Uncompress: {
		{retEvalType: types.ETString, childrenTypes: []types.EvalType{types.ETString}, aesModes: "aes-128-ecb"},
	},
	ast.AesDecrypt: {
		{retEvalType: types.ETString, childrenTypes: []types.EvalType{types.ETString, types.ETString}, aesModes: "aes-128-ecb"},
		{retEvalType: types.ETString, childrenTypes: []types.EvalType{types.ETString, types.ETString, types.ETString}, geners: []dataGenerator{nil, nil, &constStrGener{"iv"}}, aesModes: "aes-128-ecb"},
		{retEvalType: types.ETString, childrenTypes: []types.EvalType{types.ETString, types.ETString, types.ETString}, geners: []dataGenerator{nil, nil, newRandLenStrGener(16, 17)}, aesModes: "aes-128-cbc"},
		{retEvalType: types.ETString, childrenTypes: []types.EvalType{types.ETString, types.ETString, types.ETString}, geners: []dataGenerator{nil, nil, newRandLenStrGener(16, 17)}, aesModes: "aes-128-ofb"},
		{retEvalType: types.ETString, childrenTypes: []types.EvalType{types.ETString, types.ETString, types.ETString}, geners: []dataGenerator{nil, nil, newRandLenStrGener(16, 17)}, aesModes: "aes-128-cfb"},
	},
	ast.Compress: {
		{retEvalType: types.ETString, childrenTypes: []types.EvalType{types.ETString}, aesModes: "aes-128-ecb"},
	},
	ast.MD5: {
		{retEvalType: types.ETString, childrenTypes: []types.EvalType{types.ETString}, aesModes: "aes-128-ecb"},
	},
	ast.SHA: {
		{retEvalType: types.ETString, childrenTypes: []types.EvalType{types.ETString}, aesModes: "aes-128-ecb"},
	},
	ast.RandomBytes: {},
	ast.UncompressedLength: {
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETString}, aesModes: "aes-128-ecb"},
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETString}, aesModes: "aes-128-ecb", geners: []dataGenerator{newRandLenStrGener(1, 5)}},
	},
	ast.SHA1: {
		{retEvalType: types.ETString, childrenTypes: []types.EvalType{types.ETString}, aesModes: "aes-128-ecb"},
	},
	ast.PasswordFunc: {
		{retEvalType: types.ETString, childrenTypes: []types.EvalType{types.ETString}, aesModes: "aes-128-ecb", geners: []dataGenerator{newRandLenStrGener(10, 20)}},
	},
	ast.SHA2: {
		{retEvalType: types.ETString, childrenTypes: []types.EvalType{types.ETString, types.ETInt}, aesModes: "aes-128-ecb", geners: []dataGenerator{newRandLenStrGener(10, 20), newRangeInt64Gener(SHA0, SHA0+1)}},
		{retEvalType: types.ETString, childrenTypes: []types.EvalType{types.ETString, types.ETInt}, aesModes: "aes-128-ecb", geners: []dataGenerator{newRandLenStrGener(10, 20), newRangeInt64Gener(SHA224, SHA224+1)}},
		{retEvalType: types.ETString, childrenTypes: []types.EvalType{types.ETString, types.ETInt}, aesModes: "aes-128-ecb", geners: []dataGenerator{newRandLenStrGener(10, 20), newRangeInt64Gener(SHA256, SHA256+1)}},
		{retEvalType: types.ETString, childrenTypes: []types.EvalType{types.ETString, types.ETInt}, aesModes: "aes-128-ecb", geners: []dataGenerator{newRandLenStrGener(10, 20), newRangeInt64Gener(SHA384, SHA384+1)}},
		{retEvalType: types.ETString, childrenTypes: []types.EvalType{types.ETString, types.ETInt}, aesModes: "aes-128-ecb", geners: []dataGenerator{newRandLenStrGener(10, 20), newRangeInt64Gener(SHA512, SHA512+1)}},
	},
	ast.SM3: {
		{retEvalType: types.ETString, childrenTypes: []types.EvalType{types.ETString}, aesModes: "aes-128-ecb"},
	},
	ast.Encode: {
		{retEvalType: types.ETString, childrenTypes: []types.EvalType{types.ETString, types.ETString}, aesModes: "aes-128-ecb"},
	},
	ast.Decode: {
		{retEvalType: types.ETString, childrenTypes: []types.EvalType{types.ETString, types.ETString}, geners: []dataGenerator{newRandLenStrGener(10, 20)}, aesModes: "aes-128-ecb"},
	},
	ast.ValidatePasswordStrength: {
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETString}, aesModes: "aes-128-ecb"},
	},
}

func TestVectorizedBuiltinEncryptionFunc(t *testing.T) {
	testVectorizedBuiltinFunc(t, vecBuiltinEncryptionCases)
}

func BenchmarkVectorizedBuiltinEncryptionFunc(b *testing.B) {
	benchmarkVectorizedBuiltinFunc(b, vecBuiltinEncryptionCases)
}
