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
	"fmt"
	"testing"

	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/stretchr/testify/require"
)

func genVecBuiltinRegexpBenchCaseForConstants(ctx BuildContext) (baseFunc builtinFunc, childrenFieldTypes []*types.FieldType, input *chunk.Chunk, output *chunk.Column) {
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
	args[1] = DatumToConstant(types.NewStringDatum(rePat), mysql.TypeString, 0)

	var err error
	baseFunc, err = funcs[ast.Regexp].getFunction(ctx, args)
	if err != nil {
		panic(err)
	}

	output = chunk.NewColumn(eType2FieldType(types.ETInt), batchSz)
	// Mess up the output to make sure vecEvalXXX to call ResizeXXX/ReserveXXX itself.
	output.AppendNull()
	return
}

func TestVectorizedBuiltinRegexpForConstants(t *testing.T) {
	ctx := mock.NewContext()
	bf, childrenFieldTypes, input, output := genVecBuiltinRegexpBenchCaseForConstants(ctx)
	err := vecEvalType(ctx, bf, types.ETInt, input, output)
	require.NoError(t, err)
	i64s := output.Int64s()

	it := chunk.NewIterator4Chunk(input)
	i := 0
	commentf := func(row int) string {
		return fmt.Sprintf("func: builtinRegexpUTF8Sig, row: %v, rowData: %v", row, input.GetRow(row).GetDatumRow(childrenFieldTypes))
	}
	for row := it.Begin(); row != it.End(); row = it.Next() {
		val, err := evalBuiltinFunc(bf, ctx, row)
		require.NoError(t, err)
		require.Equal(t, output.IsNull(i), val.IsNull(), commentf(i))
		if !val.IsNull() {
			require.Equal(t, types.KindInt64, val.Kind(), commentf(i))
			require.Equal(t, i64s[i], val.GetInt64(), commentf(i))
		}
		i++
	}
}

func BenchmarkVectorizedBuiltinRegexpForConstants(b *testing.B) {
	ctx := mock.NewContext()
	bf, _, input, output := genVecBuiltinRegexpBenchCaseForConstants(ctx)
	b.Run("builtinRegexpUTF8Sig-Constants-VecBuiltinFunc", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if err := bf.vecEvalInt(ctx, input, output); err != nil {
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
				v, isNull, err := bf.evalInt(ctx, row)
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
