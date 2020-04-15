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
	"github.com/pingcap/tidb/v4/types"
	"github.com/pingcap/tidb/v4/util/chunk"
	"github.com/pingcap/tidb/v4/util/mock"
)

func genVecBuiltinRegexpBenchCaseForConstants() (baseFunc builtinFunc, childrenFieldTypes []*types.FieldType, input *chunk.Chunk, output *chunk.Column) {
	const (
		numArgs = 2
		batchSz = 1024
		rePat   = `\A[A-Za-z]{3,5}\d{1,5}[[:alpha:]]*\z`
	)

	childrenFieldTypes = make([]*types.FieldType, numArgs)
	for i := 0; i < numArgs; i++ {
		childrenFieldTypes[i] = eType2FieldType(types.ETString)
	}

	input = chunk.New(childrenFieldTypes, batchSz, batchSz)
	// Fill the first arg with some random string
	fillColumnWithGener(types.ETString, input, 0, newRandLenStrGener(10, 20))
	// It seems like we still need to fill this column, otherwise row.GetDatumRow() will crash
	fillColumnWithGener(types.ETString, input, 1, &constStrGener{s: rePat})

	args := make([]Expression, numArgs)
	args[0] = &Column{Index: 0, RetType: childrenFieldTypes[0]}
	args[1] = DatumToConstant(types.NewStringDatum(rePat), mysql.TypeString)

	var err error
	baseFunc, err = funcs[ast.Regexp].getFunction(mock.NewContext(), args)
	if err != nil {
		panic(err)
	}

	output = chunk.NewColumn(eType2FieldType(types.ETInt), batchSz)
	// Mess up the output to make sure vecEvalXXX to call ResizeXXX/ReserveXXX itself.
	output.AppendNull()
	return
}

func (s *testEvaluatorSuite) TestVectorizedBuiltinRegexpForConstants(c *C) {
	bf, childrenFieldTypes, input, output := genVecBuiltinRegexpBenchCaseForConstants()
	err := bf.vecEvalInt(input, output)
	c.Assert(err, IsNil)
	i64s := output.Int64s()

	it := chunk.NewIterator4Chunk(input)
	i := 0
	commentf := func(row int) CommentInterface {
		return Commentf("func: builtinRegexpUTF8Sig, row: %v, rowData: %v", row, input.GetRow(row).GetDatumRow(childrenFieldTypes))
	}
	for row := it.Begin(); row != it.End(); row = it.Next() {
		val, isNull, err := bf.evalInt(row)
		c.Assert(err, IsNil)
		c.Assert(isNull, Equals, output.IsNull(i), commentf(i))
		if !isNull {
			c.Assert(val, Equals, i64s[i], commentf(i))
		}
		i++
	}
}

func BenchmarkVectorizedBuiltinRegexpForConstants(b *testing.B) {
	bf, _, input, output := genVecBuiltinRegexpBenchCaseForConstants()
	b.Run("builtinRegexpUTF8Sig-Constants-VecBuiltinFunc", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if err := bf.vecEvalInt(input, output); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("builtinRegexpUTF8Sig-Constants-NonVecBuiltinFunc", func(b *testing.B) {
		b.ResetTimer()
		it := chunk.NewIterator4Chunk(input)
		for i := 0; i < b.N; i++ {
			output.Reset(types.ETInt)
			for row := it.Begin(); row != it.End(); row = it.Next() {
				v, isNull, err := bf.evalInt(row)
				if err != nil {
					b.Fatal(err)
				}
				if isNull {
					output.AppendNull()
				} else {
					output.AppendInt64(v)
				}
			}
		}
	})
}
