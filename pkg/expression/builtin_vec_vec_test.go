// Copyright 2024 PingCAP, Inc.
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
	"math"
	"testing"

	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
)

// create a vector include NaN in values.
func createVectorTestSample(vector []float32) types.VectorFloat32 {
	vec := types.InitVectorFloat32(len(vector))
	copy(vec.Elements(), vector)
	return vec
}

var vecBuiltinVecCases = map[string][]vecExprBenchCase{
	ast.VecDims: {
		{
			retEvalType:   types.ETInt,
			childrenTypes: []types.EvalType{types.ETVectorFloat32},
			geners:        []dataGenerator{newVectorFloat32RandGener(3)},
		},
		// Null vector test, need 'Null' result.
		{
			retEvalType:   types.ETInt,
			childrenTypes: []types.EvalType{types.ETVectorFloat32},
			geners:        []dataGenerator{newNullWrappedGener(1.0, &vectorFloat32RandGener{3, newDefaultRandGen()})},
		},
	},
	ast.VecL1Distance: {
		{
			retEvalType:   types.ETReal,
			childrenTypes: []types.EvalType{types.ETVectorFloat32, types.ETVectorFloat32},
			geners: []dataGenerator{
				newVectorFloat32RandGener(3),
				newVectorFloat32RandGener(3),
			},
		},
		// Null vector test, need 'Null' result.
		{
			retEvalType:   types.ETReal,
			childrenTypes: []types.EvalType{types.ETVectorFloat32, types.ETVectorFloat32},
			geners: []dataGenerator{
				newVectorFloat32RandGener(3),
				newNullWrappedGener(1.0, &vectorFloat32RandGener{3, newDefaultRandGen()}),
			},
		},
		{
			retEvalType:   types.ETReal,
			childrenTypes: []types.EvalType{types.ETVectorFloat32, types.ETVectorFloat32},
			geners: []dataGenerator{
				newNullWrappedGener(1.0, &vectorFloat32RandGener{3, newDefaultRandGen()}),
				newNullWrappedGener(1.0, &vectorFloat32RandGener{3, newDefaultRandGen()}),
			},
		},
		// NaN vector test, need 'Null' result.
		{
			retEvalType:   types.ETReal,
			childrenTypes: []types.EvalType{types.ETVectorFloat32, types.ETVectorFloat32},
			constants: []*Constant{
				{Value: types.NewVectorFloat32Datum(createVectorTestSample([]float32{float32(math.Inf(1))})), RetType: types.NewFieldType(mysql.TypeTiDBVectorFloat32)},
				{Value: types.NewVectorFloat32Datum(createVectorTestSample([]float32{float32(math.Inf(1))})), RetType: types.NewFieldType(mysql.TypeTiDBVectorFloat32)},
			},
		},
	},
	ast.VecL2Distance: {
		{
			retEvalType:   types.ETReal,
			childrenTypes: []types.EvalType{types.ETVectorFloat32, types.ETVectorFloat32},
			geners: []dataGenerator{
				newVectorFloat32RandGener(3),
				newVectorFloat32RandGener(3),
			},
		},
		// Null vector test, need 'Null' result.
		{
			retEvalType:   types.ETReal,
			childrenTypes: []types.EvalType{types.ETVectorFloat32, types.ETVectorFloat32},
			geners: []dataGenerator{
				newVectorFloat32RandGener(3),
				newNullWrappedGener(1.0, &vectorFloat32RandGener{3, newDefaultRandGen()}),
			},
		},
		{
			retEvalType:   types.ETReal,
			childrenTypes: []types.EvalType{types.ETVectorFloat32, types.ETVectorFloat32},
			geners: []dataGenerator{
				newNullWrappedGener(1.0, &vectorFloat32RandGener{3, newDefaultRandGen()}),
				newNullWrappedGener(1.0, &vectorFloat32RandGener{3, newDefaultRandGen()}),
			},
		},
		// NaN vector test, need 'Null' result.
		{
			retEvalType:   types.ETReal,
			childrenTypes: []types.EvalType{types.ETVectorFloat32, types.ETVectorFloat32},
			constants: []*Constant{
				{Value: types.NewVectorFloat32Datum(createVectorTestSample([]float32{float32(math.Inf(1))})), RetType: types.NewFieldType(mysql.TypeTiDBVectorFloat32)},
				{Value: types.NewVectorFloat32Datum(createVectorTestSample([]float32{float32(math.Inf(1))})), RetType: types.NewFieldType(mysql.TypeTiDBVectorFloat32)},
			},
		},
	},
	ast.VecCosineDistance: {
		{
			retEvalType:   types.ETReal,
			childrenTypes: []types.EvalType{types.ETVectorFloat32, types.ETVectorFloat32},
			geners: []dataGenerator{
				newVectorFloat32RandGener(3),
				newVectorFloat32RandGener(3),
			},
		},
		// Null vector test, need 'Null' result.
		{
			retEvalType:   types.ETReal,
			childrenTypes: []types.EvalType{types.ETVectorFloat32, types.ETVectorFloat32},
			geners: []dataGenerator{
				newVectorFloat32RandGener(3),
				newNullWrappedGener(1.0, &vectorFloat32RandGener{3, newDefaultRandGen()}),
			},
		},
		{
			retEvalType:   types.ETReal,
			childrenTypes: []types.EvalType{types.ETVectorFloat32, types.ETVectorFloat32},
			geners: []dataGenerator{
				newNullWrappedGener(1.0, &vectorFloat32RandGener{3, newDefaultRandGen()}),
				newNullWrappedGener(1.0, &vectorFloat32RandGener{3, newDefaultRandGen()}),
			},
		},
		// NaN vector test, need 'Null' result.
		{
			retEvalType:   types.ETReal,
			childrenTypes: []types.EvalType{types.ETVectorFloat32, types.ETVectorFloat32},
			constants: []*Constant{
				{Value: types.NewVectorFloat32Datum(createVectorTestSample([]float32{float32(math.NaN())})), RetType: types.NewFieldType(mysql.TypeTiDBVectorFloat32)},
				{Value: types.NewVectorFloat32Datum(createVectorTestSample([]float32{float32(1.1)})), RetType: types.NewFieldType(mysql.TypeTiDBVectorFloat32)},
			},
		},
	},
	ast.VecNegativeInnerProduct: {
		{
			retEvalType:   types.ETReal,
			childrenTypes: []types.EvalType{types.ETVectorFloat32, types.ETVectorFloat32},
			geners: []dataGenerator{
				newVectorFloat32RandGener(3),
				newVectorFloat32RandGener(3),
			},
		},
		// Null vector test, need 'Null' result.
		{
			retEvalType:   types.ETReal,
			childrenTypes: []types.EvalType{types.ETVectorFloat32, types.ETVectorFloat32},
			geners: []dataGenerator{
				newVectorFloat32RandGener(3),
				newNullWrappedGener(1.0, &vectorFloat32RandGener{3, newDefaultRandGen()}),
			},
		},
		{
			retEvalType:   types.ETReal,
			childrenTypes: []types.EvalType{types.ETVectorFloat32, types.ETVectorFloat32},
			geners: []dataGenerator{
				newNullWrappedGener(1.0, &vectorFloat32RandGener{3, newDefaultRandGen()}),
				newNullWrappedGener(1.0, &vectorFloat32RandGener{3, newDefaultRandGen()}),
			},
		},
		// NaN vector test, need 'Null' result.
		{
			retEvalType:   types.ETReal,
			childrenTypes: []types.EvalType{types.ETVectorFloat32, types.ETVectorFloat32},
			constants: []*Constant{
				{Value: types.NewVectorFloat32Datum(createVectorTestSample([]float32{float32(math.NaN())})), RetType: types.NewFieldType(mysql.TypeTiDBVectorFloat32)},
				{Value: types.NewVectorFloat32Datum(createVectorTestSample([]float32{float32(1.1)})), RetType: types.NewFieldType(mysql.TypeTiDBVectorFloat32)},
			},
		},
	},
	ast.VecL2Norm: {
		{
			retEvalType:   types.ETReal,
			childrenTypes: []types.EvalType{types.ETVectorFloat32},
			geners:        []dataGenerator{newVectorFloat32RandGener(3)},
		},
		// Null vector test, need 'Null' result.
		{
			retEvalType:   types.ETReal,
			childrenTypes: []types.EvalType{types.ETVectorFloat32},
			geners:        []dataGenerator{newNullWrappedGener(1.0, &vectorFloat32RandGener{3, newDefaultRandGen()})},
		},
		// NaN vector test, need 'Null' result.
		{
			retEvalType:   types.ETReal,
			childrenTypes: []types.EvalType{types.ETVectorFloat32},
			constants: []*Constant{
				{Value: types.NewVectorFloat32Datum(createVectorTestSample([]float32{float32(math.NaN())})), RetType: types.NewFieldType(mysql.TypeTiDBVectorFloat32)},
			},
		},
	},
	ast.VecAsText: {
		{
			retEvalType:   types.ETString,
			childrenTypes: []types.EvalType{types.ETVectorFloat32},
			geners:        []dataGenerator{newVectorFloat32RandGener(3)},
		},
		// Null vector test, need 'Null' result.
		{
			retEvalType:   types.ETString,
			childrenTypes: []types.EvalType{types.ETVectorFloat32},
			geners:        []dataGenerator{newNullWrappedGener(1.0, &vectorFloat32RandGener{3, newDefaultRandGen()})},
		},
	},
	ast.VecFromText: {
		{
			retEvalType:   types.ETVectorFloat32,
			childrenTypes: []types.EvalType{types.ETString},
			geners:        []dataGenerator{&constStrGener{"[1.1,2.2,3.3]"}},
		},
		// Null string test, need 'Null' result.
		{
			retEvalType:   types.ETVectorFloat32,
			childrenTypes: []types.EvalType{types.ETString},
			geners:        []dataGenerator{newNullWrappedGener(1.1, &constStrGener{"[1.1,2.2,3.3]"})},
		},
	},
}

func TestVectorizedBuiltinVecFunc(t *testing.T) {
	testVectorizedBuiltinFunc(t, vecBuiltinVecCases)
}

func BenchmarkVectorizedBuiltinVecFunc(b *testing.B) {
	benchmarkVectorizedBuiltinFunc(b, vecBuiltinVecCases)
}
