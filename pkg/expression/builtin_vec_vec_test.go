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

func newVector(str string) types.VectorFloat32 {
	vec, _ := types.ParseVectorFloat32(str)
	return vec
}

type vectorFloat32Gener struct {
	vec types.VectorFloat32
}

func (g *vectorFloat32Gener) gen() interface{} {
	return g.vec
}

type vectorFloat32TextGener struct {
	vec string
}

func (g *vectorFloat32TextGener) gen() interface{} {
	return g.vec
}

var vecBuiltinVecCases = map[string][]vecExprBenchCase{
	ast.VecDims: {
		{
			retEvalType:   types.ETInt,
			childrenTypes: []types.EvalType{types.ETVectorFloat32},
			geners:        []dataGenerator{&vectorFloat32Gener{newVector("[1.1,2.2]")}}},
	},
	ast.VecL1Distance: {
		{
			retEvalType:   types.ETReal,
			childrenTypes: []types.EvalType{types.ETVectorFloat32, types.ETVectorFloat32},
			geners:        []dataGenerator{&vectorFloat32Gener{newVector("[1.1,2.2]")}, &vectorFloat32Gener{newVector("[2.2,3.3]")}}},
	},
	ast.VecL2Distance: {
		{
			retEvalType:   types.ETReal,
			childrenTypes: []types.EvalType{types.ETVectorFloat32, types.ETVectorFloat32},
			geners:        []dataGenerator{&vectorFloat32Gener{newVector("[1.1,2.2]")}, &vectorFloat32Gener{newVector("[2.2,3.3]")}}},
	},
	ast.VecCosineDistance: {
		{
			retEvalType:   types.ETReal,
			childrenTypes: []types.EvalType{types.ETVectorFloat32, types.ETVectorFloat32},
			geners:        []dataGenerator{&vectorFloat32Gener{newVector("[1.1,2.2]")}, &vectorFloat32Gener{newVector("[2.2,3.3]")}}},
	},
	ast.VecNegativeInnerProduct: {
		{
			retEvalType:   types.ETReal,
			childrenTypes: []types.EvalType{types.ETVectorFloat32, types.ETVectorFloat32},
			geners:        []dataGenerator{&vectorFloat32Gener{newVector("[1.1,2.2]")}, &vectorFloat32Gener{newVector("[2.2,3.3]")}}},
	},
	ast.VecL2Norm: {
		{
			retEvalType:   types.ETReal,
			childrenTypes: []types.EvalType{types.ETVectorFloat32},
			geners:        []dataGenerator{&vectorFloat32Gener{newVector("[1.1,2.2]")}}},
	},
	ast.VecAsText: {
		{
			retEvalType:   types.ETString,
			childrenTypes: []types.EvalType{types.ETVectorFloat32},
			geners:        []dataGenerator{&vectorFloat32Gener{newVector("[1.1,2.2]")}}},
	},
	ast.VecFromText: {
		{
			retEvalType:   types.ETVectorFloat32,
			childrenTypes: []types.EvalType{types.ETString},
			geners:        []dataGenerator{&vectorFloat32TextGener{"[1.1,2.2,3.3]"}}},
	},
}

func TestVectorizedBuiltinVecFunc(t *testing.T) {
	testVectorizedBuiltinFunc(t, vecBuiltinVecCases)
}

func BenchmarkVectorizedBuiltinVecFunc(b *testing.B) {
	benchmarkVectorizedBuiltinFunc(b, vecBuiltinVecCases)
}
