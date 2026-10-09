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

package chunk

import (
	"testing"

	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
)

// BenchmarkDataInDiskByRowsGetRowAlloc benchmarks heap allocations during
// repeated GetRow calls on spilled data.  Run with:
//
//	go test -bench BenchmarkDataInDiskByRowsGetRowAlloc -benchmem -count 5 ./pkg/util/chunk/
//
// Lower allocs/op indicates the pool is effective.
func BenchmarkDataInDiskByRowsGetRowAlloc(b *testing.B) {
	const numRows = 1024
	fieldTypes := []*types.FieldType{types.NewFieldType(mysql.TypeLonglong)}

	// Write test data to disk.
	l := NewDataInDiskByRows(fieldTypes)
	defer l.Close()

	chk := NewChunkWithCapacity(fieldTypes, numRows)
	for i := 0; i < numRows; i++ {
		chk.AppendInt64(0, int64(i))
	}
	if err := l.Add(chk); err != nil {
		b.Fatal(err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := l.GetRow(RowPtr{ChkIdx: 0, RowIdx: uint32(i % numRows)})
		if err != nil {
			b.Fatal(err)
		}
	}
}

