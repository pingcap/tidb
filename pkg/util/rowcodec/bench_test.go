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
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/benchdaily"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/rowcodec"
)

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
	for i := 0; i < b.N; i++ {
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
	for i := 0; i < b.N; i++ {
		buf = buf[:0]
		buf, err = xb.Encode(nil, colIDs, oldRow, buf)
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
	for i := 0; i < b.N; i++ {
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
	xRowData, err := xb.Encode(nil, colIDs, oldRow, nil)
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
	for i := 0; i < b.N; i++ {
		chk.Reset()
		err = decoder.DecodeToChunk(xRowData, kv.IntHandle(1), chk)
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
