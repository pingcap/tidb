// Copyright 2016 PingCAP, Inc.
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

package tablecodec

import (
	"encoding/binary"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/benchdaily"
	"github.com/pingcap/tidb/pkg/util/codec"
	"github.com/pingcap/tidb/pkg/util/rowcodec"
)

func BenchmarkEncodeRowKeyWithHandle(b *testing.B) {
	for i := 0; i < b.N; i++ {
		EncodeRowKeyWithHandle(100, kv.IntHandle(100))
	}
}

func BenchmarkEncodeEndKey(b *testing.B) {
	for i := 0; i < b.N; i++ {
		EncodeRowKeyWithHandle(100, kv.IntHandle(100))
		EncodeRowKeyWithHandle(100, kv.IntHandle(101))
	}
}

// BenchmarkEncodeRowKeyWithPrefixNex tests the performance of encoding row key with prefixNext
// PrefixNext() is slow than using EncodeRowKeyWithHandle.
// BenchmarkEncodeEndKey-4		20000000	        97.2 ns/op
// BenchmarkEncodeRowKeyWithPrefixNex-4	10000000	       121 ns/op
func BenchmarkEncodeRowKeyWithPrefixNex(b *testing.B) {
	for i := 0; i < b.N; i++ {
		sk := EncodeRowKeyWithHandle(100, kv.IntHandle(100))
		sk.PrefixNext()
	}
}

func BenchmarkDecodeRowKey(b *testing.B) {
	rowKey := EncodeRowKeyWithHandle(100, kv.IntHandle(100))
	for i := 0; i < b.N; i++ {
		_, err := DecodeRowKey(rowKey)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDecodeIndexKeyIntHandle(b *testing.B) {
	var idxVal []byte
	// When handle values greater than 255, it will have a memory alloc.
	idxVal = append(idxVal, EncodeHandleInUniqueIndexValue(kv.IntHandle(256), false)...)

	for i := 0; i < b.N; i++ {
		DecodeHandleInIndexValue(idxVal)
	}
}

func BenchmarkDecodeIndexKeyCommonHandle(b *testing.B) {
	var idxVal []byte
	idxVal = append(idxVal, 0)
	// index version
	idxVal = append(idxVal, IndexVersionFlag)
	idxVal = append(idxVal, byte(1))

	// common handle
	encoded, _ := codec.EncodeKey(time.UTC, nil, types.MakeDatums(1, 2)...)
	h, _ := kv.NewCommonHandle(encoded)
	idxVal = encodeCommonHandle(idxVal, h)

	for i := 0; i < b.N; i++ {
		DecodeHandleInIndexValue(idxVal)
	}
}

func BenchmarkDecodeIndexKVGeneral(b *testing.B) {
	// Benchmark version-0 unique int handle index (no restored data).
	// This exercises the general path: CutIndexKeyTo + reEncodeHandleTo.
	colValues := []types.Datum{types.NewIntDatum(42), types.NewIntDatum(100)}
	encodedCols, _ := codec.EncodeKey(time.UTC, nil, colValues...)
	key := EncodeIndexSeekKey(1, 1, encodedCols)

	// Build version 0 value with int handle in tail (unique index).
	var value []byte
	value = append(value, 8) // tailLen = 8
	value = append(value, 0, 0)
	var hBuf [8]byte
	binary.BigEndian.PutUint64(hBuf[:], uint64(7))
	value = append(value, hBuf[:]...)

	colsLen := 2
	columns := []rowcodec.ColInfo{
		{ID: 1, Ft: types.NewFieldType(mysql.TypeLonglong)},
		{ID: 2, Ft: types.NewFieldType(mysql.TypeLonglong)},
		{ID: 3, Ft: types.NewFieldType(mysql.TypeLonglong)},
	}

	b.Run("WithPreAlloc", func(b *testing.B) {
		preAlloc := make([][]byte, colsLen, colsLen+1)
		var buf [9]byte
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			preAlloc = preAlloc[:colsLen:colsLen+1]
			_, err := DecodeIndexKVEx(key, value, colsLen, HandleDefault, columns, buf[:0], preAlloc)
			if err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("WithoutPreAlloc", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, err := DecodeIndexKV(key, value, colsLen, HandleDefault, columns)
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkDecodeIndexKVGeneralNonUnique(b *testing.B) {
	// Benchmark version-0 non-unique int handle index (handle in key suffix).
	handleVal := int64(7)
	colValues := []types.Datum{types.NewIntDatum(42), types.NewIntDatum(100)}
	allDatums := append(colValues, types.NewIntDatum(handleVal))
	encodedAll, _ := codec.EncodeKey(time.UTC, nil, allDatums...)
	key := EncodeIndexSeekKey(1, 1, encodedAll)

	// Build version 0 non-unique value (no handle in value, padded > 9 bytes).
	var value []byte
	value = append(value, 0) // tailLen = 0
	value = append(value, make([]byte, 9)...)

	colsLen := 2
	columns := []rowcodec.ColInfo{
		{ID: 1, Ft: types.NewFieldType(mysql.TypeLonglong)},
		{ID: 2, Ft: types.NewFieldType(mysql.TypeLonglong)},
		{ID: 3, Ft: types.NewFieldType(mysql.TypeLonglong)},
	}

	b.Run("WithPreAlloc", func(b *testing.B) {
		preAlloc := make([][]byte, colsLen, colsLen+1)
		var buf [9]byte
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			preAlloc = preAlloc[:colsLen:colsLen+1]
			_, err := DecodeIndexKVEx(key, value, colsLen, HandleDefault, columns, buf[:0], preAlloc)
			if err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("WithoutPreAlloc", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, err := DecodeIndexKV(key, value, colsLen, HandleDefault, columns)
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkDecodeRestoredValues(b *testing.B) {
	// Build restored values data using rowcodec.Encoder.
	colIDs := []int64{1, 2, 3}
	datums := []types.Datum{
		types.NewIntDatum(42),
		types.NewBytesDatum([]byte("hello world")),
		types.NewUintDatum(999),
	}
	rd := rowcodec.Encoder{Enable: true}
	restoredBytes, err := rd.Encode(time.UTC, colIDs, datums, nil, nil)
	if err != nil {
		b.Fatal(err)
	}

	// Build version 0 value with restored data + int handle.
	// restoredBytes starts with CodecVer (=RestoreDataFlag), so splitIndexValueForIndexValueVersion0
	// will correctly detect it as restored values.
	var value []byte
	value = append(value, 8) // tailLen = 8
	value = append(value, restoredBytes...)
	var hBuf [8]byte
	binary.BigEndian.PutUint64(hBuf[:], uint64(7))
	value = append(value, hBuf[:]...)

	uft := types.NewFieldType(mysql.TypeLonglong)
	uft.AddFlag(mysql.UnsignedFlag)
	columns := []rowcodec.ColInfo{
		{ID: 1, Ft: types.NewFieldType(mysql.TypeLonglong)},
		{ID: 2, Ft: types.NewFieldType(mysql.TypeVarchar)},
		{ID: 3, Ft: uft},
		{ID: 4, Ft: types.NewFieldType(mysql.TypeLonglong)}, // handle
	}

	colValues := []types.Datum{
		types.NewIntDatum(42),
		types.NewBytesDatum([]byte("hello world")),
		types.NewUintDatum(999),
	}
	encodedCols, _ := codec.EncodeKey(time.UTC, nil, colValues...)
	key := EncodeIndexSeekKey(1, 1, encodedCols)

	colsLen := 3

	b.Run("Original", func(b *testing.B) {
		preAlloc := make([][]byte, colsLen, colsLen+1)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			preAlloc = preAlloc[:colsLen:colsLen+1]
			_, err := DecodeIndexKVEx(key, value, colsLen, HandleDefault, columns, nil, preAlloc)
			if err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("WithIndexRestoredDecoder", func(b *testing.B) {
		preAlloc := make([][]byte, colsLen, colsLen+1)
		dec := NewIndexRestoredDecoder(columns[:colsLen])
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			preAlloc = preAlloc[:colsLen:colsLen+1]
			_, err := DecodeIndexKVEx(key, value, colsLen, HandleDefault, columns, nil, preAlloc, dec)
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

func TestBenchDaily(t *testing.T) {
	benchdaily.Run(
		BenchmarkEncodeRowKeyWithHandle,
		BenchmarkEncodeEndKey,
		BenchmarkEncodeRowKeyWithPrefixNex,
		BenchmarkDecodeRowKey,
		BenchmarkHasTablePrefix,
		BenchmarkHasTablePrefixBuiltin,
		BenchmarkEncodeValue,
	)
}
