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

package rowcodec_test

import (
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/benchdaily"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/rowcodec"
)

func BenchmarkDecodeWideRowToChunk(b *testing.B) {
	makeBytes := func(n int) []byte {
		bs := make([]byte, n)
		for i := range bs {
			bs[i] = byte('a' + (i % 26))
		}
		return bs
	}

	const (
		intCols      = 48
		bytesCols    = 12
		timeCols     = 4
		totalCols    = intCols + bytesCols + timeCols
		batchSize    = 64
		timestampFsp = 0
	)

	benchCases := []struct {
		name     string
		bytesLen int
	}{
		{name: "small_bytes_32", bytesLen: 32},
		{name: "big_bytes_1024", bytesLen: 1024},
	}

	for _, tc := range benchCases {
		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()

			colIDs := make([]int64, totalCols)
			values := make([]types.Datum, totalCols)
			fieldTypes := make([]*types.FieldType, totalCols)
			cols := make([]rowcodec.ColInfo, totalCols)

			// Int columns.
			for i := 0; i < intCols; i++ {
				colIDs[i] = int64(i + 1)
				values[i].SetInt64(int64(i))
				ft := types.NewFieldType(mysql.TypeLonglong)
				fieldTypes[i] = ft
				cols[i] = rowcodec.ColInfo{ID: colIDs[i], Ft: ft}
			}

			// Bytes columns.
			payload := makeBytes(tc.bytesLen)
			for i := 0; i < bytesCols; i++ {
				idx := intCols + i
				colIDs[idx] = int64(idx + 1)
				values[idx].SetBytes(payload)
				ft := types.NewFieldType(mysql.TypeVarchar)
				fieldTypes[idx] = ft
				cols[idx] = rowcodec.ColInfo{ID: colIDs[idx], Ft: ft}
			}

			// Timestamp columns (exercise FromPackedUint + optional TZ conversion).
			baseCore := types.FromDate(2024, 1, 2, 3, 4, 5, 0)
			baseTS := types.NewTime(baseCore, mysql.TypeTimestamp, timestampFsp)
			for i := 0; i < timeCols; i++ {
				idx := intCols + bytesCols + i
				colIDs[idx] = int64(idx + 1)
				values[idx].SetMysqlTime(baseTS)
				ft := types.NewFieldType(mysql.TypeTimestamp)
				ft.SetDecimal(timestampFsp)
				fieldTypes[idx] = ft
				cols[idx] = rowcodec.ColInfo{ID: colIDs[idx], Ft: ft}
			}

			var enc rowcodec.Encoder
			rowData, err := enc.Encode(time.Local, colIDs, values, nil, nil)
			if err != nil {
				b.Fatal(err)
			}

			decoder := rowcodec.NewChunkDecoder(cols, []int64{-1}, nil, time.Local)
			chk := chunk.NewChunkWithCapacity(fieldTypes, batchSize)

			b.ResetTimer()
			for range b.N {
				chk.Reset()
				for r := 0; r < batchSize; r++ {
					if err := decoder.DecodeToChunk(rowData, kv.IntHandle(r), chk); err != nil {
						b.Fatal(err)
					}
				}
			}
		})
	}
}

func BenchmarkChecksum(b *testing.B) {
	b.ReportAllocs()
	datums := types.MakeDatums(1, "abc", 1.1)
	tp1 := types.NewFieldType(mysql.TypeLong)
	tp2 := types.NewFieldType(mysql.TypeVarchar)
	tp3 := types.NewFieldType(mysql.TypeDouble)
	cols := []rowcodec.ColData{
		{&model.ColumnInfo{ID: 1, FieldType: *tp1}, &datums[0]},
		{&model.ColumnInfo{ID: 2, FieldType: *tp2}, &datums[1]},
		{&model.ColumnInfo{ID: 3, FieldType: *tp3}, &datums[2]},
	}
	row := rowcodec.RowData{Cols: cols}
	for range b.N {
		_, err := row.Checksum(time.Local)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkEncode(b *testing.B) {
	b.ReportAllocs()
	oldRow := types.MakeDatums(1, "abc", 1.1)
	var xb rowcodec.Encoder
	var buf []byte
	colIDs := []int64{1, 2, 3}
	var err error
	for range b.N {
		buf = buf[:0]
		buf, err = xb.Encode(nil, colIDs, oldRow, nil, buf)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkEncodeFromOldRow(b *testing.B) {
	b.ReportAllocs()
	oldRow := types.MakeDatums(1, "abc", 1.1)
	oldRowData, err := tablecodec.EncodeOldRow(nil, oldRow, []int64{1, 2, 3}, nil, nil)
	if err != nil {
		b.Fatal(err)
	}
	var xb rowcodec.Encoder
	var buf []byte
	for range b.N {
		buf, err = rowcodec.EncodeFromOldRow(&xb, nil, oldRowData, buf)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDecode(b *testing.B) {
	b.ReportAllocs()
	oldRow := types.MakeDatums(1, "abc", 1.1)
	colIDs := []int64{-1, 2, 3}
	tps := []*types.FieldType{
		types.NewFieldType(mysql.TypeLonglong),
		types.NewFieldType(mysql.TypeString),
		types.NewFieldType(mysql.TypeDouble),
	}
	var xb rowcodec.Encoder
	xRowData, err := xb.Encode(nil, colIDs, oldRow, nil, nil)
	if err != nil {
		b.Fatal(err)
	}
	cols := make([]rowcodec.ColInfo, len(tps))
	for i, tp := range tps {
		cols[i] = rowcodec.ColInfo{
			ID: colIDs[i],
			Ft: tp,
		}
	}
	decoder := rowcodec.NewChunkDecoder(cols, []int64{-1}, nil, time.Local)
	chk := chunk.NewChunkWithCapacity(tps, 1)
	for range b.N {
		chk.Reset()
		err = decoder.DecodeToChunk(xRowData, 0, kv.IntHandle(1), chk)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func TestBenchDaily(t *testing.T) {
	benchdaily.Run(
		BenchmarkEncode,
		BenchmarkDecode,
		BenchmarkEncodeFromOldRow,
	)
}
