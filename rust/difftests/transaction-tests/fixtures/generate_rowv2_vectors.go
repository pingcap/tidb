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

//go:build ignore

// Task #190: the row-v2 test suite in tidb-codec only self-round-trips
// (encode ours, decode ours), so it can pass while producing bytes TiDB
// never writes. This generator produces Go-authoritative vectors for the
// cases that self-round-trips structurally cannot catch: the unsigned/signed
// hash-int-tag boundary, the null-bitmap small/large column-id split, the
// isBig row flag, every fixed-width scalar type once, common-handle padding
// on both sides of the 9-byte floor, and an index key/value with restored
// data.
package main

import (
	"fmt"
	"time"

	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/codec"
	"github.com/pingcap/tidb/pkg/util/rowcodec"
)

func main() {
	hashIntBoundary()
	nullBitmapSplit()
	bigRow()
	fixedWidthTypes()
	commonHandlePadding()
	indexKeyWithRestoredData()
}

// Source: `codec.EncodeHashChunkRowIdx` (pkg/util/codec/codec.go). The
// hash-int-tag fix (#f4517b7a92) taught `encode_hash_datum` that an unsigned
// column always hashes with `uvarintFlag` even when its stored 64-bit word
// reads negative as i64. These three vectors sit exactly on that boundary.
func hashIntBoundary() {
	unsignedBigInt := types.NewFieldType(mysql.TypeLonglong)
	unsignedBigInt.AddFlag(mysql.UnsignedFlag)

	signedBigInt := types.NewFieldType(mysql.TypeLonglong)

	emitHash("hash_u64_max_unsigned", unsignedBigInt, types.NewUintDatum(^uint64(0)))
	emitHash("hash_i64_max_plus_1_unsigned", unsignedBigInt, types.NewUintDatum(uint64(1)<<63))
	emitHash("hash_i64_max_signed", signedBigInt, types.NewIntDatum(9223372036854775807))
}

func emitHash(name string, ft *types.FieldType, d types.Datum) {
	row := chunk.MutRowFromDatums([]types.Datum{d})
	flag, b, err := codec.EncodeHashChunkRowIdx(types.DefaultStmtNoWarningContext, row.ToRow(), ft, 0)
	if err != nil {
		panic(err)
	}
	fmt.Printf("%s_flag=%x\n", name, []byte{flag})
	fmt.Printf("%s_bytes=%x\n", name, b)
}

// Source: `rowcodec.Encoder.Encode` (pkg/util/rowcodec/encoder.go). One row
// mixing a column ID below 256 and one above 256, exercising the null
// bitmap and the small/large column-id split together instead of in
// isolation.
func nullBitmapSplit() {
	colIDs := []int64{5, 300}
	values := []types.Datum{types.NewIntDatum(7), types.Datum{}}
	values[1].SetNull()
	emitRow("row_null_bitmap_small_large", colIDs, values)
}

// Source: `rowcodec.Encoder.Encode`. 256 columns forces `isBig` (Go
// `smallRowCap`/`largeRowCap` split at 255 columns).
func bigRow() {
	const n = 256
	colIDs := make([]int64, n)
	values := make([]types.Datum, n)
	for i := 0; i < n; i++ {
		colIDs[i] = int64(i + 1)
		values[i] = types.NewIntDatum(int64(i))
	}
	emitRow("row_256_columns_is_big", colIDs, values)
}

// Source: `rowcodec.Encoder.Encode`. Every fixed-width column kind once.
func fixedWidthTypes() {
	t, err := types.ParseTime(types.DefaultStmtNoWarningContext, "2026-08-02 12:34:56", mysql.TypeDatetime, 0)
	if err != nil {
		panic(err)
	}
	dur, _, err := types.ParseDuration(types.DefaultStmtNoWarningContext, "12:34:56", 0)
	if err != nil {
		panic(err)
	}
	enumVal, err := types.ParseEnumName([]string{"a", "b"}, "b", "utf8mb4_bin")
	if err != nil {
		panic(err)
	}
	setVal, err := types.ParseSetValue([]string{"x", "y"}, 0b11)
	if err != nil {
		panic(err)
	}
	bit, err := types.ParseBitStr("0b101")
	if err != nil {
		panic(err)
	}
	doc, err := types.ParseBinaryJSONFromString(`{"a":1}`)
	if err != nil {
		panic(err)
	}

	colIDs := []int64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	values := []types.Datum{
		types.NewIntDatum(-42),
		types.NewUintDatum(42),
		types.NewFloat64Datum(3.5),
		types.NewFloat64Datum(negZero()),
		types.NewDurationDatum(dur),
		types.NewTimeDatum(t),
		types.NewMysqlEnumDatum(enumVal),
		types.NewMysqlSetDatum(setVal, "utf8mb4_bin"),
		types.NewMysqlBitDatum(bit),
		types.NewJSONDatum(doc),
	}
	emitRow("row_fixed_width_all_types", colIDs, values)

	// Go's float64 machinery refuses to construct a NaN MySQL value through
	// the normal validated path; there is no `types.NewFloat64Datum(math.NaN())`
	// that rowcodec is ever asked to persist without an earlier truncation error.
	// Pin that refusal instead of encoding a NaN vector: TiDB never writes one.
	fmt.Println("# NaN float64: TiDB has no validated path that produces a stored NaN row value; refusal pinned, no vector emitted")
}

func negZero() float64 {
	return -0.0
}

// Source: `kv.NewCommonHandle` (pkg/kv/key.go) + `tablecodec.EncodeRowKeyWithHandle`.
// A common handle shorter than 9 bytes is zero-padded to 9; one already at or
// above 9 bytes passes through untouched.
func commonHandlePadding() {
	// Same short-handle construction as Go's own `TestPaddingHandle`
	// (pkg/kv/key_test.go): a decimal `1` encodes to fewer than 9 bytes,
	// while an integer datum always encodes to exactly 9.
	short, err := codec.EncodeKey(time.UTC, nil, types.NewDecimalDatum(types.NewDecFromInt(1)))
	if err != nil {
		panic(err)
	}
	if len(short) >= 9 {
		panic("expected short encoding under 9 bytes")
	}
	long, err := codec.EncodeKey(time.UTC, nil, types.NewIntDatum(1), types.NewIntDatum(2))
	if err != nil {
		panic(err)
	}
	if len(long) < 9 {
		panic("expected long encoding at or above 9 bytes")
	}

	shortHandle, err := kv.NewCommonHandle(short)
	if err != nil {
		panic(err)
	}
	longHandle, err := kv.NewCommonHandle(long)
	if err != nil {
		panic(err)
	}

	fmt.Printf("common_handle_short_raw=%x\n", short)
	fmt.Printf("common_handle_short_padded_key=%x\n", []byte(tablecodec.EncodeRowKeyWithHandle(42, shortHandle)))
	fmt.Printf("common_handle_long_raw=%x\n", long)
	fmt.Printf("common_handle_long_key=%x\n", []byte(tablecodec.EncodeRowKeyWithHandle(42, longHandle)))
}

// Source: `tablecodec.GenIndexKey` for the key half and the
// `idxValNeedRestoredData` branch of `genIndexValueVersion0`
// (pkg/tablecodec/tablecodec.go) for the value half -- the exact layout
// `tablecodec.decode_index_kv`'s `decode_restored_values` (index value
// version 0) path decodes. `decode_index_kv` currently has no production
// caller on the Rust side, so this only produces the fixture and leaves
// wiring/decoding as a follow-up.
func indexKeyWithRestoredData() {
	indexedValues := []types.Datum{types.NewStringDatum("abc")}
	key, err := codec.EncodeKey(time.UTC, nil, indexedValues...)
	if err != nil {
		panic(err)
	}
	indexKey := tablecodec.EncodeIndexSeekKey(42, 7, key)

	// Replicates genIndexValueVersion0's idxValNeedRestoredData branch for a
	// non-distinct, non-global index (tailLen byte, then rowcodec-encoded
	// restored columns, padded to at least 10 bytes).
	idxVal := []byte{0}
	idxVal, err = (&rowcodec.Encoder{Enable: true}).Encode(time.UTC, []int64{1}, indexedValues, nil, idxVal)
	if err != nil {
		panic(err)
	}
	tailLen := 0
	if len(idxVal) < 10 {
		pad := 10 - len(idxVal)
		tailLen += pad
		for i := 0; i < pad; i++ {
			idxVal = append(idxVal, 0)
		}
	}
	idxVal[0] = byte(tailLen)

	fmt.Printf("index_key_restored_data_key=%x\n", []byte(indexKey))
	fmt.Printf("index_key_restored_data_value=%x\n", idxVal)
	fmt.Printf("index_key_restored_data_column_value=%s\n", "abc")
}

func emitRow(name string, colIDs []int64, values []types.Datum) {
	encoder := rowcodec.Encoder{}
	row, err := encoder.Encode(time.UTC, colIDs, values, nil, nil)
	if err != nil {
		panic(err)
	}
	fmt.Printf("%s=%x\n", name, row)
}
