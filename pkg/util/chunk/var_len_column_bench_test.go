// Copyright 2026 PingCAP, Inc.
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
	"fmt"
	"strings"
	"testing"
)

var (
	benchmarkNewVarLenColumnSink  *Column
	benchmarkRenewVarLenChunkSink *Chunk
)

func BenchmarkNewVarLenColumnTPCC(b *testing.B) {
	for _, capacity := range []int{0, 1, InitialCapacity, 64, 1024} {
		b.Run(fmt.Sprintf("capacity=%d", capacity), func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				benchmarkNewVarLenColumnSink = newVarLenColumn(capacity)
			}
			b.StopTimer()
			validateVarLenColumnBenchmark(b, benchmarkNewVarLenColumnSink, capacity, 0, 0)
		})
	}
}

func BenchmarkVarLenColumnBuildTPCC(b *testing.B) {
	cases := []struct {
		name     string
		rows     int
		valueLen int
	}{
		{name: "empty"},
		{name: "rows=1/value=8", rows: 1, valueLen: 8},
		{name: "rows=32/value=8", rows: 32, valueLen: 8},
		{name: "rows=33/value=8", rows: 33, valueLen: 8},
		{name: "rows=32/value=64", rows: 32, valueLen: 64},
	}

	for _, bc := range cases {
		b.Run(bc.name, func(b *testing.B) {
			value := strings.Repeat("x", bc.valueLen)
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				col := newVarLenColumn(InitialCapacity)
				for range bc.rows {
					col.AppendString(value)
				}
				benchmarkNewVarLenColumnSink = col
			}
			b.StopTimer()
			validateVarLenColumnBenchmark(
				b,
				benchmarkNewVarLenColumnSink,
				InitialCapacity,
				bc.rows,
				bc.rows*bc.valueLen,
			)
		})
	}
}

func BenchmarkRenewVarLenColumnsTPCC(b *testing.B) {
	for _, columnCount := range []int{1, 4, 12} {
		b.Run(fmt.Sprintf("columns=%d", columnCount), func(b *testing.B) {
			columns := make([]*Column, columnCount)
			for i := range columns {
				columns[i] = newVarLenColumn(InitialCapacity)
				columns[i].AppendString("abcdefgh")
			}
			source := &Chunk{
				columns:      columns,
				capacity:     InitialCapacity,
				requiredRows: 1024,
			}

			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				benchmarkRenewVarLenChunkSink = Renew(source, 1024)
			}
			b.StopTimer()
			if got := benchmarkRenewVarLenChunkSink.NumCols(); got != columnCount {
				b.Fatalf("unexpected column count: %d", got)
			}
			for _, col := range benchmarkRenewVarLenChunkSink.columns {
				validateVarLenColumnBenchmark(b, col, InitialCapacity, 0, 0)
			}
		})
	}
}

func validateVarLenColumnBenchmark(
	b *testing.B,
	col *Column,
	initialCapacity int,
	rows int,
	dataLen int,
) {
	b.Helper()
	if col == nil {
		b.Fatal("nil column")
	}
	if col.length != rows || len(col.offsets) != rows+1 || len(col.data) != dataLen {
		b.Fatalf(
			"unexpected column shape: rows=%d offsets=%d data=%d",
			col.length,
			len(col.offsets),
			len(col.data),
		)
	}
	if rows == 0 {
		if got, want := cap(col.offsets), initialCapacity+1; got != want {
			b.Fatalf("unexpected offsets capacity: got %d want %d", got, want)
		}
		if got, want := cap(col.data), estimatedElemLen*initialCapacity; got != want {
			b.Fatalf("unexpected data capacity: got %d want %d", got, want)
		}
		if got, want := cap(col.nullBitmap), (initialCapacity+7)/8; got != want {
			b.Fatalf("unexpected null bitmap capacity: got %d want %d", got, want)
		}
	}
}

func TestVarLenColumnStorageIndependenceAndGrowth(t *testing.T) {
	first := newVarLenColumn(InitialCapacity)
	second := newVarLenColumn(InitialCapacity)

	for i := range InitialCapacity + 1 {
		first.AppendString(fmt.Sprintf("value-%02d", i))
	}
	second.AppendNull()

	if got := first.GetString(InitialCapacity); got != "value-32" {
		t.Fatalf("unexpected value after offsets growth: %q", got)
	}
	if first.IsNull(0) {
		t.Fatal("first column unexpectedly contains a null")
	}
	if !second.IsNull(0) {
		t.Fatal("second column lost its null")
	}
	if first.offsets[0] != 0 || second.offsets[0] != 0 {
		t.Fatal("initial offsets changed")
	}
	if &first.offsets[0] == &second.offsets[0] {
		t.Fatal("variable-length columns share offsets storage")
	}
	if &first.nullBitmap[0] == &second.nullBitmap[0] {
		t.Fatal("variable-length columns share null-bitmap storage")
	}
}
