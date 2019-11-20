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
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/types"
)

var vecBuiltinOtherCases = map[string][]vecExprBenchCase{
	ast.SetVar: {
		{retEvalType: types.ETString, childrenTypes: []types.EvalType{types.ETString, types.ETString}},
	},
	ast.GetVar: {
		{retEvalType: types.ETString, childrenTypes: []types.EvalType{types.ETString}},
	},
	ast.BitCount: {},
	ast.GetParam: {},
	ast.In: {
		{
			retEvalType: types.ETInt,
			childrenTypes: []types.EvalType{
				types.ETInt,
				types.ETInt, types.ETInt, types.ETInt, types.ETInt,
				types.ETInt, //types.ETInt, types.ETInt, types.ETInt,
				// types.ETInt, types.ETInt, types.ETInt, types.ETInt,
				// types.ETInt, types.ETInt, types.ETInt, types.ETInt,
				// types.ETInt, types.ETInt, types.ETInt, types.ETInt,
			},
			constants: []*Constant{
				nil,
				nil, nil, nil, nil,
				{Value: types.NewDatum(1), RetType: types.NewFieldType(mysql.TypeInt24)},
				//&Constant{Value: types.NewDatum(2), RetType: types.NewFieldType(mysql.TypeInt24)},
				//&Constant{Value: types.NewDatum(3), RetType: types.NewFieldType(mysql.TypeInt24)},
				//&Constant{Value: types.NewDatum(4), RetType: types.NewFieldType(mysql.TypeInt24)},
				//&Constant{Value: types.NewDatum(5), RetType: types.NewFieldType(mysql.TypeInt24)},
				//&Constant{Value: types.NewDatum(6), RetType: types.NewFieldType(mysql.TypeInt24)},
				//&Constant{Value: types.NewDatum(7), RetType: types.NewFieldType(mysql.TypeInt24)},
				//&Constant{Value: types.NewDatum(8), RetType: types.NewFieldType(mysql.TypeInt24)},
				// &Constant{Value: types.NewDatum(9), RetType: types.NewFieldType(mysql.TypeInt24)},
				// &Constant{Value: types.NewDatum(10), RetType: types.NewFieldType(mysql.TypeInt24)},
				// &Constant{Value: types.NewDatum(11), RetType: types.NewFieldType(mysql.TypeInt24)},
				// &Constant{Value: types.NewDatum(12), RetType: types.NewFieldType(mysql.TypeInt24)},
				// &Constant{Value: types.NewDatum(13), RetType: types.NewFieldType(mysql.TypeInt24)},
				// &Constant{Value: types.NewDatum(14), RetType: types.NewFieldType(mysql.TypeInt24)},
				// &Constant{Value: types.NewDatum(15), RetType: types.NewFieldType(mysql.TypeInt24)},
				// &Constant{Value: types.NewDatum(16), RetType: types.NewFieldType(mysql.TypeInt24)},
			},
			geners: []dataGenerator{&rangeInt64Gener{1, 2}, nil, nil, nil, nil},
		},
	},
}

func (s *testEvaluatorSuite) TestVectorizedBuiltinOtherFunc(c *C) {
	testVectorizedBuiltinFunc(c, vecBuiltinOtherCases)
}

func BenchmarkVectorizedBuiltinOtherFunc(b *testing.B) {
	benchmarkVectorizedBuiltinFunc(b, vecBuiltinOtherCases)
}
