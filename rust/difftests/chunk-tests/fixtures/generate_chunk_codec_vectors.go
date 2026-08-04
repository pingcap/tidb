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
// See the License for the specific language governing permissions and
// limitations under the License.

//go:build ignore

// This generator drives the REAL `pkg/util/chunk` Codec -- the chunk<->bytes
// exchange format a coprocessor response carries under `EncodeType_TypeChunk`
// -- and prints one line per case:
//
//	<case>\t<hex of Codec.Encode(chk)>
//
// Its stdout is the reviewed fixture stored in chunk_codec_vectors.tsv.
// Reproduce with, from the repository root:
//
//	go run rust/difftests/chunk-tests/fixtures/generate_chunk_codec_vectors.go \
//	  > rust/difftests/chunk-tests/fixtures/chunk_codec_vectors.tsv

package main

import (
	"encoding/hex"
	"fmt"
	"time"

	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
)

func ft(tp byte) *types.FieldType { return types.NewFieldType(tp) }

func emit(name string, fields []*types.FieldType, chk *chunk.Chunk) {
	buf := chunk.NewCodec(fields).Encode(chk)
	fmt.Printf("%s\t%s\n", name, hex.EncodeToString(buf))
}

func mustDec(s string) *types.MyDecimal {
	var d types.MyDecimal
	if err := d.FromString([]byte(s)); err != nil {
		panic(err)
	}
	return &d
}

func mustJSON(s string) types.BinaryJSON {
	j, err := types.ParseBinaryJSONFromString(s)
	if err != nil {
		panic(err)
	}
	return j
}

func main() {
	// 1. A fixed-length column with a null in the middle. The null row's data
	//    bytes are the stale scratch element, which the encoding carries.
	{
		fields := []*types.FieldType{ft(mysql.TypeLonglong)}
		chk := chunk.NewChunkWithCapacity(fields, 8)
		chk.AppendInt64(0, 1)
		chk.AppendNull(0)
		chk.AppendInt64(0, -2)
		emit("int64_with_null", fields, chk)
	}

	// 2. A var-length column: the offsets array is part of the image.
	{
		fields := []*types.FieldType{ft(mysql.TypeVarchar)}
		chk := chunk.NewChunkWithCapacity(fields, 8)
		chk.AppendString(0, "ab")
		chk.AppendNull(0)
		chk.AppendString(0, "")
		chk.AppendString(0, "cdefg")
		emit("varchar_with_null", fields, chk)
	}

	// 3. No nulls at all: the null bitmap is omitted entirely.
	{
		fields := []*types.FieldType{ft(mysql.TypeLonglong)}
		chk := chunk.NewChunkWithCapacity(fields, 8)
		for i := range 5 {
			chk.AppendInt64(0, int64(i))
		}
		emit("no_nulls", fields, chk)
	}

	// 4. Every row null, and enough rows that the bitmap spans two bytes.
	{
		fields := []*types.FieldType{ft(mysql.TypeVarchar)}
		chk := chunk.NewChunkWithCapacity(fields, 16)
		for range 9 {
			chk.AppendNull(0)
		}
		emit("all_null_two_bitmap_bytes", fields, chk)
	}

	// 5. Zero rows.
	{
		fields := []*types.FieldType{ft(mysql.TypeLonglong), ft(mysql.TypeVarchar)}
		emit("zero_rows", fields, chunk.NewChunkWithCapacity(fields, 8))
	}

	// 6. One column per fixed-vs-variable shape `getFixedLen` distinguishes.
	{
		fields := []*types.FieldType{
			ft(mysql.TypeTiny),
			ft(mysql.TypeFloat),
			ft(mysql.TypeDouble),
			ft(mysql.TypeYear),
			ft(mysql.TypeDuration),
			ft(mysql.TypeNewDecimal),
			ft(mysql.TypeDatetime),
			ft(mysql.TypeVarchar),
			ft(mysql.TypeBlob),
			ft(mysql.TypeJSON),
			ft(mysql.TypeEnum),
			ft(mysql.TypeSet),
			ft(mysql.TypeBit),
		}
		chk := chunk.NewChunkWithCapacity(fields, 8)
		for i := range 3 {
			k := int64(i)
			chk.AppendInt64(0, k)
			chk.AppendFloat32(1, float32(k)+0.5)
			chk.AppendFloat64(2, float64(k)+0.25)
			chk.AppendInt64(3, 2000+k)
			chk.AppendDuration(4, types.Duration{Duration: time.Duration((k + 1) * 1e9), Fsp: 0})
			chk.AppendMyDecimal(5, mustDec(fmt.Sprintf("%d.25", i)))
			chk.AppendTime(6, types.NewTime(types.FromDate(2024, 3, 17, 4, 5, i, 0), mysql.TypeDatetime, 0))
			chk.AppendString(7, fmt.Sprintf("v%d", i))
			chk.AppendBytes(8, []byte{byte(i), byte(i + 1)})
			chk.AppendJSON(9, mustJSON(fmt.Sprintf("%d", i)))
			chk.AppendEnum(10, types.Enum{Name: "e", Value: uint64(i)})
			chk.AppendSet(11, types.Set{Name: "s", Value: uint64(i)})
			chk.AppendBytes(12, []byte{byte(0x80 + i)})
		}
		// A trailing null in each column, so every shape carries a bitmap.
		for i := range fields {
			chk.AppendNull(i)
		}
		emit("all_shapes", fields, chk)
	}
}
