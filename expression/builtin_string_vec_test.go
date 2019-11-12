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
	"math/rand"
	"testing"

	. "github.com/pingcap/check"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/charset"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/types"
)

type randSpaceStrGener struct {
	lenBegin int
	lenEnd   int
}

func (g *randSpaceStrGener) gen() interface{} {
	n := rand.Intn(g.lenEnd-g.lenBegin) + g.lenBegin
	buf := make([]byte, n)
	for i := range buf {
		x := rand.Intn(150)
		if x < 10 {
			buf[i] = byte('0' + x)
		} else if x-10 < 26 {
			buf[i] = byte('a' + x - 10)
		} else if x < 62 {
			buf[i] = byte('A' + x - 10 - 26)
		} else {
			buf[i] = byte(' ')
		}
	}
	return string(buf)
}

var vecBuiltinStringCases = map[string][]vecExprBenchCase{
	ast.Length: {
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETString}, geners: []dataGenerator{&defaultGener{0.2, types.ETString}}},
	},
	ast.ASCII: {
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETString}, geners: []dataGenerator{&defaultGener{0.2, types.ETString}}},
	},
	ast.Concat: {
		{retEvalType: types.ETString, childrenTypes: []types.EvalType{types.ETString, types.ETString, types.ETString}},
	},
	ast.ConcatWS: {},
	ast.Convert:  {},
	ast.Substring: {
		{
			retEvalType:   types.ETString,
			childrenTypes: []types.EvalType{types.ETString, types.ETInt},
			geners:        []dataGenerator{&randLenStrGener{0, 20}, &rangeInt64Gener{-25, 25}},
		},
		{
			retEvalType:   types.ETString,
			childrenTypes: []types.EvalType{types.ETString, types.ETInt, types.ETInt},
			geners:        []dataGenerator{&randLenStrGener{0, 20}, &rangeInt64Gener{-25, 25}, &rangeInt64Gener{-25, 25}},
		},
	},
	ast.SubstringIndex: {
		{
			retEvalType:   types.ETString,
			childrenTypes: []types.EvalType{types.ETString, types.ETString, types.ETInt},
			geners:        []dataGenerator{&randLenStrGener{0, 20}, &randLenStrGener{0, 2}, &rangeInt64Gener{-4, 4}},
		},
	},
	ast.Locate: {
		{
			retEvalType:   types.ETInt,
			childrenTypes: []types.EvalType{types.ETString, types.ETString},
			geners:        []dataGenerator{&randLenStrGener{0, 10}, &randLenStrGener{0, 20}},
		},
		{
			retEvalType:   types.ETInt,
			childrenTypes: []types.EvalType{types.ETString, types.ETString},
			geners:        []dataGenerator{&randLenStrGener{1, 2}, &randLenStrGener{0, 20}},
		},
		{
			retEvalType:   types.ETInt,
			childrenTypes: []types.EvalType{types.ETString, types.ETString, types.ETInt},
			geners:        []dataGenerator{&randLenStrGener{0, 10}, &randLenStrGener{0, 20}, &rangeInt64Gener{-10, 20}},
		},
		{
			retEvalType:   types.ETInt,
			childrenTypes: []types.EvalType{types.ETString, types.ETString, types.ETInt},
			geners:        []dataGenerator{&randLenStrGener{1, 2}, &randLenStrGener{0, 10}, &rangeInt64Gener{0, 8}},
		},
	},
	ast.Hex: {
		{retEvalType: types.ETString, childrenTypes: []types.EvalType{types.ETString}, geners: []dataGenerator{&randHexStrGener{10, 100}}},
		{retEvalType: types.ETString, childrenTypes: []types.EvalType{types.ETInt}},
	},
	ast.Unhex: {
		{retEvalType: types.ETString, childrenTypes: []types.EvalType{types.ETString}, geners: []dataGenerator{&randHexStrGener{10, 100}}},
	},
	ast.Trim: {
		{retEvalType: types.ETString, childrenTypes: []types.EvalType{types.ETString}, geners: []dataGenerator{&randSpaceStrGener{10, 100}}},
		{retEvalType: types.ETString, childrenTypes: []types.EvalType{types.ETString, types.ETString}, geners: []dataGenerator{&randLenStrGener{10, 20}, &randLenStrGener{5, 25}}},
	},
	ast.LTrim: {
		{retEvalType: types.ETString, childrenTypes: []types.EvalType{types.ETString}, geners: []dataGenerator{&randSpaceStrGener{10, 100}}},
	},
	ast.RTrim: {
		{retEvalType: types.ETString, childrenTypes: []types.EvalType{types.ETString}, geners: []dataGenerator{&randSpaceStrGener{10, 100}}},
	},
	ast.Lpad: {
		{
			retEvalType:   types.ETString,
			childrenTypes: []types.EvalType{types.ETString, types.ETInt, types.ETString},
			geners:        []dataGenerator{&randLenStrGener{0, 20}, &rangeInt64Gener{168435456, 368435456}, &randLenStrGener{0, 10}},
		},
		{
			retEvalType:   types.ETString,
			childrenTypes: []types.EvalType{types.ETString, types.ETInt, types.ETString},
			geners:        []dataGenerator{&defaultGener{0.2, types.ETString}, &defaultGener{0.2, types.ETInt}, &defaultGener{0.2, types.ETString}},
		},
	},
	ast.Rpad: {
		{
			retEvalType:   types.ETString,
			childrenTypes: []types.EvalType{types.ETString, types.ETInt, types.ETString},
			geners:        []dataGenerator{&randLenStrGener{0, 20}, &rangeInt64Gener{168435456, 368435456}, &randLenStrGener{0, 10}},
		},
		{
			retEvalType:   types.ETString,
			childrenTypes: []types.EvalType{types.ETString, types.ETInt, types.ETString},
			geners:        []dataGenerator{&defaultGener{0.2, types.ETString}, &defaultGener{0.2, types.ETInt}, &defaultGener{0.2, types.ETString}},
		},
		{
			retEvalType:        types.ETString,
			childrenTypes:      []types.EvalType{types.ETString, types.ETInt, types.ETString},
			childrenFieldTypes: []*types.FieldType{{Tp: mysql.TypeString, Flag: mysql.BinaryFlag, Collate: charset.CollationBin}},
			geners:             []dataGenerator{&randLenStrGener{0, 20}, &rangeInt64Gener{168435456, 368435456}, &randLenStrGener{0, 10}},
		},
		{
			retEvalType:        types.ETString,
			childrenTypes:      []types.EvalType{types.ETString, types.ETInt, types.ETString},
			childrenFieldTypes: []*types.FieldType{{Tp: mysql.TypeString, Flag: mysql.BinaryFlag, Collate: charset.CollationBin}},
			geners:             []dataGenerator{&defaultGener{0.2, types.ETString}, &defaultGener{0.2, types.ETInt}, &defaultGener{0.2, types.ETString}},
		},
	},
	ast.CharLength: {
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETString}},
	},
	ast.BitLength: {
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETString}},
	},
	ast.FindInSet: {},
	ast.Field:     {},
	ast.MakeSet: {
		{retEvalType: types.ETString, childrenTypes: []types.EvalType{types.ETInt, types.ETString, types.ETString, types.ETString, types.ETString, types.ETString, types.ETString, types.ETString, types.ETString}},
	},
	ast.Oct: {
		{retEvalType: types.ETString, childrenTypes: []types.EvalType{types.ETInt}},
		{retEvalType: types.ETString, childrenTypes: []types.EvalType{types.ETString}, geners: []dataGenerator{&numStrGener{rangeInt64Gener{-10, 10}}}},
	},
	ast.Quote: {
		{retEvalType: types.ETString, childrenTypes: []types.EvalType{types.ETString}},
	},
	ast.Ord: {
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETString}},
	},
	ast.Bin: {
		{retEvalType: types.ETString, childrenTypes: []types.EvalType{types.ETInt}},
	},
	ast.ToBase64: {
		{retEvalType: types.ETString, childrenTypes: []types.EvalType{types.ETString}, geners: []dataGenerator{&randLenStrGener{0, 10}}},
	},
	ast.FromBase64: {
		{retEvalType: types.ETString, childrenTypes: []types.EvalType{types.ETString}, geners: []dataGenerator{&randLenStrGener{10, 100}}},
	},
	ast.ExportSet: {},
	ast.Repeat: {
		{retEvalType: types.ETString, childrenTypes: []types.EvalType{types.ETString, types.ETInt}, geners: []dataGenerator{&randLenStrGener{10, 20}, &rangeInt64Gener{-10, 10}}},
	},
	ast.Lower: {
		{retEvalType: types.ETString, childrenTypes: []types.EvalType{types.ETString}},
	},
	ast.IsNull: {
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETString}, geners: []dataGenerator{&randLenStrGener{10, 20}}},
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETString}, geners: []dataGenerator{&defaultGener{0.2, types.ETString}}},
	},
	ast.Upper: {
		{retEvalType: types.ETString, childrenTypes: []types.EvalType{types.ETString}},
	},
	ast.Right: {
		{retEvalType: types.ETString, childrenTypes: []types.EvalType{types.ETString, types.ETInt}},
		// need to add BinaryFlag for the Binary func
		{retEvalType: types.ETString, childrenTypes: []types.EvalType{types.ETString, types.ETInt},
			childrenFieldTypes: []*types.FieldType{{Tp: mysql.TypeString, Flag: mysql.BinaryFlag, Collate: charset.CollationBin},
				{Tp: mysql.TypeLonglong}},
			geners: []dataGenerator{
				&randLenStrGener{10, 20},
				&rangeInt64Gener{begin: -10, end: 20},
			},
		},
	},
	ast.Left: {
		{retEvalType: types.ETString, childrenTypes: []types.EvalType{types.ETString, types.ETInt}},
		// need to add BinaryFlag for the Binary func
		{retEvalType: types.ETString, childrenTypes: []types.EvalType{types.ETString, types.ETInt},
			childrenFieldTypes: []*types.FieldType{{Tp: mysql.TypeString, Flag: mysql.BinaryFlag, Collate: charset.CollationBin},
				{Tp: mysql.TypeLonglong}},
			geners: []dataGenerator{
				&randLenStrGener{10, 20},
				&rangeInt64Gener{begin: -10, end: 20},
			},
		},
	},
	ast.Space: {
		{retEvalType: types.ETString, childrenTypes: []types.EvalType{types.ETInt}, geners: []dataGenerator{&rangeInt64Gener{-10, 2000}}},
		{retEvalType: types.ETString, childrenTypes: []types.EvalType{types.ETInt}, geners: []dataGenerator{&rangeInt64Gener{5, 10}}},
	},
	ast.Reverse: {
		{retEvalType: types.ETString, childrenTypes: []types.EvalType{types.ETString}, geners: []dataGenerator{&randLenStrGener{10, 20}}},
		{retEvalType: types.ETString, childrenTypes: []types.EvalType{types.ETString}, geners: []dataGenerator{&defaultGener{0.2, types.ETString}}},
		// need to add BinaryFlag for the Binary func
		{retEvalType: types.ETString, childrenTypes: []types.EvalType{types.ETString},
			childrenFieldTypes: []*types.FieldType{{Tp: mysql.TypeString, Flag: mysql.BinaryFlag, Collate: charset.CollationBin}},
		},
	},
	ast.Replace: {
		{retEvalType: types.ETString, childrenTypes: []types.EvalType{types.ETString, types.ETString, types.ETString}, geners: []dataGenerator{&randLenStrGener{10, 20}, &randLenStrGener{0, 10}, &randLenStrGener{0, 10}}},
	},
	ast.InsertFunc: {
		{retEvalType: types.ETString, childrenTypes: []types.EvalType{types.ETString, types.ETInt, types.ETInt, types.ETString}, geners: []dataGenerator{&randLenStrGener{10, 20}, &rangeInt64Gener{-10, 20}, &rangeInt64Gener{0, 100}, &randLenStrGener{0, 10}}},
	},
	ast.Elt: {
		{retEvalType: types.ETString, childrenTypes: []types.EvalType{types.ETInt, types.ETString, types.ETString, types.ETString}, geners: []dataGenerator{&rangeInt64Gener{-1, 5}}},
	},
	ast.FromUnixTime: {
		{retEvalType: types.ETString, childrenTypes: []types.EvalType{types.ETDecimal, types.ETString},
			geners: []dataGenerator{
				gener{defaultGener{eType: types.ETDecimal, nullRation: 0.9}},
				&constStrGener{"%y-%m-%d"},
			},
		},
	},
}

func (s *testEvaluatorSuite) TestVectorizedBuiltinStringEvalOneVec(c *C) {
	testVectorizedEvalOneVec(c, vecBuiltinStringCases)
}

func (s *testEvaluatorSuite) TestVectorizedBuiltinStringFunc(c *C) {
	testVectorizedBuiltinFunc(c, vecBuiltinStringCases)
}

func BenchmarkVectorizedBuiltinStringEvalOneVec(b *testing.B) {
	benchmarkVectorizedEvalOneVec(b, vecBuiltinStringCases)
}

func BenchmarkVectorizedBuiltinStringFunc(b *testing.B) {
	benchmarkVectorizedBuiltinFunc(b, vecBuiltinStringCases)
}
