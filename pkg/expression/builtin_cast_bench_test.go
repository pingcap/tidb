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
	"math/rand"
	"testing"

	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/mock"
)

func genCastIntAsInt(ctx BuildContext) (*builtinCastIntAsIntSig, *chunk.Chunk, *chunk.Column) {
	col := &Column{RetType: types.NewFieldType(mysql.TypeLonglong), Index: 0}
	baseFunc, err := newBaseBuiltinFunc(ctx, "", []Expression{col}, types.NewFieldType(mysql.TypeLonglong))
	if err != nil {
		panic(err)
	}
	baseCast := newBaseBuiltinCastFunc(baseFunc, false)
	cast := &builtinCastIntAsIntSig{baseCast}
	input := chunk.NewChunkWithCapacity([]*types.FieldType{types.NewFieldType(mysql.TypeLonglong)}, 1024)
	for i := 0; i < 1024; i++ {
		input.AppendInt64(0, rand.Int63n(10000)-5000)
	}
	result := chunk.NewColumn(types.NewFieldType(mysql.TypeLonglong), 1024)
	return cast, input, result
}

func BenchmarkCastIntAsIntRow(b *testing.B) {
	ctx := mock.NewContext()
	cast, input, _ := genCastIntAsInt(ctx)
	it := chunk.NewIterator4Chunk(input)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for row := it.Begin(); row != it.End(); row = it.Next() {
			if _, _, err := cast.evalInt(ctx, row); err != nil {
				b.Fatal(err)
			}
		}
	}
}

func BenchmarkCastIntAsIntVec(b *testing.B) {
	ctx := mock.NewContext()
	cast, input, result := genCastIntAsInt(ctx)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := cast.vecEvalInt(ctx, input, result); err != nil {
			b.Fatal(err)
		}
	}
}
