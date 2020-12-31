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

package rowcodec_test

import (
	"testing"
	"time"

	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/rowcodec"
)

func BenchmarkEncode(b *testing.B) {
	b.ReportAllocs()
	oldRow := types.MakeDatums(1, "abc", 1.1)
	var xb rowcodec.Encoder
	var buf []byte
	colIDs := []int64{1, 2, 3}
	var err error
	for i := 0; i < b.N; i++ {
		buf, err = xb.Encode(nil, colIDs, oldRow, buf)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkEncodeFromOldRow(b *testing.B) {
	b.ReportAllocs()
	oldRow := types.MakeDatums(1, "abc", 1.1)
	oldRowData, err := tablecodec.EncodeOldRow(new(stmtctx.StatementContext), oldRow, []int64{1, 2, 3}, nil, nil)
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
