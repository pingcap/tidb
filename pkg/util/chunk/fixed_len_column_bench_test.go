package chunk

import (
	"fmt"
	"testing"
)

var benchmarkNewFixedLenColumnSink *Column

func BenchmarkNewFixedLenColumnDominantLegacy(b *testing.B) {
	benchmarkFixedLenColumnFactory(b, func() *Column {
		return &Column{
			elemBuf:    make([]byte, sizeInt64),
			data:       make([]byte, 0, getDataMemCap(InitialCapacity, sizeInt64)),
			nullBitmap: make([]byte, 0, getNullBitmapCap(InitialCapacity)),
		}
	})
}

func BenchmarkNewFixedLenColumnDominantCurrent(b *testing.B) {
	benchmarkFixedLenColumnFactory(b, func() *Column {
		return newFixedLenColumn(sizeInt64, InitialCapacity)
	})
}

func benchmarkFixedLenColumnFactory(b *testing.B, factory func() *Column) {
	col := factory()
	validateFixedLenColumnBenchmark(b, col, sizeInt64, InitialCapacity)

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		col = factory()
	}
	b.StopTimer()

	benchmarkNewFixedLenColumnSink = col
	validateFixedLenColumnBenchmark(b, col, sizeInt64, InitialCapacity)
}

func BenchmarkNewFixedLenColumnRandomRanges(b *testing.B) {
	cases := []struct {
		elemLen  int
		capacity int
	}{
		{elemLen: 4, capacity: InitialCapacity},
		{elemLen: 8, capacity: InitialCapacity},
		{elemLen: 16, capacity: InitialCapacity},
		{elemLen: 40, capacity: InitialCapacity},
		{elemLen: 8, capacity: 0},
		{elemLen: 8, capacity: 1024},
	}

	for _, tc := range cases {
		name := fmt.Sprintf("elem=%d/cap=%d", tc.elemLen, tc.capacity)
		b.Run(name, func(b *testing.B) {
			col := newFixedLenColumn(tc.elemLen, tc.capacity)
			validateFixedLenColumnBenchmark(b, col, tc.elemLen, tc.capacity)

			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				col = newFixedLenColumn(tc.elemLen, tc.capacity)
			}
			b.StopTimer()

			benchmarkNewFixedLenColumnSink = col
			validateFixedLenColumnBenchmark(b, col, tc.elemLen, tc.capacity)
		})
	}
}

func validateFixedLenColumnBenchmark(
	b *testing.B,
	col *Column,
	elemLen int,
	capacity int,
) {
	b.Helper()
	if len(col.elemBuf) != elemLen || cap(col.elemBuf) != elemLen {
		b.Fatalf("unexpected element buffer: len=%d cap=%d", len(col.elemBuf), cap(col.elemBuf))
	}
	if len(col.data) != 0 || int64(cap(col.data)) != getDataMemCap(capacity, elemLen) {
		b.Fatalf("unexpected data buffer: len=%d cap=%d", len(col.data), cap(col.data))
	}
	if len(col.nullBitmap) != 0 || int64(cap(col.nullBitmap)) != getNullBitmapCap(capacity) {
		b.Fatalf("unexpected null bitmap: len=%d cap=%d", len(col.nullBitmap), cap(col.nullBitmap))
	}
}

func TestFixedLen8ColumnStorageIndependence(t *testing.T) {
	first := newFixedLenColumn(sizeInt64, InitialCapacity)
	second := newFixedLenColumn(sizeInt64, InitialCapacity)

	first.elemBuf[0] = 1
	first.data = append(first.data, 2)
	first.nullBitmap = append(first.nullBitmap, 3)

	if second.elemBuf[0] != 0 || len(second.data) != 0 || len(second.nullBitmap) != 0 {
		t.Fatal("fixed-length columns alias")
	}
	if first.elemBuf[0] != 1 {
		t.Fatal("data or null-bitmap mutation changed the element buffer")
	}
}
