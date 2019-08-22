// Copyright 2017 PingCAP, Inc.
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

	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/mock"
)

func BenchmarkCastIntAsInt(b *testing.B) {
	col := &Column{RetType: types.NewFieldType(mysql.TypeLonglong), Index: 0}
	baseFunc := newBaseBuiltinFunc(mock.NewContext(), []Expression{col})
	baseCast := newBaseBuiltinCastFunc(baseFunc, false)
	cast := builtinCastIntAsIntSig{baseCast}

	input := chunk.NewChunkWithCapacity([]*types.FieldType{types.NewFieldType(mysql.TypeLonglong)}, 1024)
	for i := 0; i < 1024; i++ {
		input.AppendInt64(0, int64(i))
	}
	result := chunk.NewColumn(types.NewFieldType(mysql.TypeLonglong), 1024)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := cast.vecEvalInt(input, result); err != nil {
			b.Fatal(err)
		}
	}
}
