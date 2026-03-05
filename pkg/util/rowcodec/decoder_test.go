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

package rowcodec

import (
	"encoding/binary"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/stretchr/testify/require"
)

func TestChunkDecoderColMapping(t *testing.T) {
	ftNullable := types.NewFieldType(mysql.TypeLonglong)
	ftNotNull1 := types.NewFieldType(mysql.TypeLonglong)
	ftNotNull1.SetFlag(ftNotNull1.GetFlag() | mysql.NotNullFlag)
	ftNotNull2 := types.NewFieldType(mysql.TypeLonglong)
	ftNotNull2.SetFlag(ftNotNull2.GetFlag() | mysql.NotNullFlag)

	cols := []ColInfo{
		{ID: 1, Ft: ftNullable}, // nullable
		{ID: 2, Ft: ftNotNull1}, // not-null
		{ID: 3, Ft: ftNotNull2}, // not-null
	}
	fts := []*types.FieldType{ftNullable, ftNotNull1, ftNotNull2}

	var encoder Encoder
	colIDs := []int64{1, 2, 3}

	// Row1: all columns are not null, build the mapping.
	row1, err := encoder.Encode(time.UTC, colIDs, []types.Datum{
		types.NewIntDatum(10),
		types.NewIntDatum(20),
		types.NewIntDatum(30),
	}, nil, nil)
	require.NoError(t, err)

	// Row2: column 1 becomes NULL, shifting the not-null segment indices.
	var nullDatum types.Datum
	nullDatum.SetNull()
	row2, err := encoder.Encode(time.UTC, colIDs, []types.Datum{
		nullDatum,
		types.NewIntDatum(22),
		types.NewIntDatum(33),
	}, nil, nil)
	require.NoError(t, err)

	// Row3: column 1 becomes not null again.
	row3, err := encoder.Encode(time.UTC, colIDs, []types.Datum{
		types.NewIntDatum(11),
		types.NewIntDatum(23),
		types.NewIntDatum(34),
	}, nil, nil)
	require.NoError(t, err)

	decoder := NewChunkDecoder(cols, []int64{-1}, nil, time.UTC)
	chk := chunk.New(fts, 0, 3)

	require.NoError(t, decoder.DecodeToChunk(row1, kv.IntHandle(-1), chk))
	require.True(t, decoder.mappingInited)
	require.Equal(t, 3, decoder.mappingRowCols)
	require.Len(t, decoder.colMapping, 3)
	require.Equal(t, -1, decoder.colMapping[0]) // nullable column should not be cached
	require.GreaterOrEqual(t, decoder.colMapping[1], 0)
	require.GreaterOrEqual(t, decoder.colMapping[2], 0)

	mappingAfterRow1 := append([]int(nil), decoder.colMapping...)

	require.NoError(t, decoder.DecodeToChunk(row2, kv.IntHandle(-1), chk))
	require.Equal(t, mappingAfterRow1, decoder.colMapping) // no rebuild; rowCols unchanged

	require.NoError(t, decoder.DecodeToChunk(row3, kv.IntHandle(-1), chk))

	require.Equal(t, 3, chk.NumRows())

	r1 := chk.GetRow(0)
	require.False(t, r1.IsNull(0))
	require.Equal(t, int64(10), r1.GetInt64(0))
	require.Equal(t, int64(20), r1.GetInt64(1))
	require.Equal(t, int64(30), r1.GetInt64(2))

	r2 := chk.GetRow(1)
	require.True(t, r2.IsNull(0))
	require.Equal(t, int64(22), r2.GetInt64(1))
	require.Equal(t, int64(33), r2.GetInt64(2))

	r3 := chk.GetRow(2)
	require.False(t, r3.IsNull(0))
	require.Equal(t, int64(11), r3.GetInt64(0))
	require.Equal(t, int64(23), r3.GetInt64(1))
	require.Equal(t, int64(34), r3.GetInt64(2))
}

func TestChunkDecoderColMappingSchemaChange(t *testing.T) {
	ft1 := types.NewFieldType(mysql.TypeLonglong)
	ft1.SetFlag(ft1.GetFlag() | mysql.NotNullFlag)
	ft2 := types.NewFieldType(mysql.TypeLonglong)
	ft2.SetFlag(ft2.GetFlag() | mysql.NotNullFlag)
	ft3 := types.NewFieldType(mysql.TypeLonglong)
	ft3.SetFlag(ft3.GetFlag() | mysql.NotNullFlag)

	cols := []ColInfo{
		{ID: 1, Ft: ft1},
		{ID: 2, Ft: ft2},
		{ID: 3, Ft: ft3},
	}
	fts := []*types.FieldType{ft1, ft2, ft3}

	defDatum := func(i int, chk *chunk.Chunk) error {
		// Default value for column 3 only.
		if i == 2 {
			chk.AppendInt64(i, 999)
			return nil
		}
		chk.AppendNull(i)
		return nil
	}

	var encoder Encoder
	// Row1: old schema, missing column 3.
	row1, err := encoder.Encode(time.UTC, []int64{1, 2}, []types.Datum{
		types.NewIntDatum(10),
		types.NewIntDatum(20),
	}, nil, nil)
	require.NoError(t, err)

	// Row2: new schema includes column 3.
	row2, err := encoder.Encode(time.UTC, []int64{1, 2, 3}, []types.Datum{
		types.NewIntDatum(11),
		types.NewIntDatum(22),
		types.NewIntDatum(33),
	}, nil, nil)
	require.NoError(t, err)

	decoder := NewChunkDecoder(cols, []int64{-1}, defDatum, time.UTC)
	chk := chunk.New(fts, 0, 2)

	require.NoError(t, decoder.DecodeToChunk(row1, kv.IntHandle(-1), chk))
	require.True(t, decoder.mappingInited)
	require.Equal(t, 2, decoder.mappingRowCols)
	require.Equal(t, -1, decoder.colMapping[2]) // col3 not found in old rows

	require.NoError(t, decoder.DecodeToChunk(row2, kv.IntHandle(-1), chk))
	require.Equal(t, 3, decoder.mappingRowCols)         // mapping rebuilt
	require.GreaterOrEqual(t, decoder.colMapping[2], 0) // col3 becomes cacheable

	require.Equal(t, 2, chk.NumRows())

	r1 := chk.GetRow(0)
	require.Equal(t, int64(10), r1.GetInt64(0))
	require.Equal(t, int64(20), r1.GetInt64(1))
	require.Equal(t, int64(999), r1.GetInt64(2)) // default

	r2 := chk.GetRow(1)
	require.Equal(t, int64(11), r2.GetInt64(0))
	require.Equal(t, int64(22), r2.GetInt64(1))
	require.Equal(t, int64(33), r2.GetInt64(2))
}

func TestChunkDecoderCompiledColsCorrectness(t *testing.T) {
	ftInt := types.NewFieldType(mysql.TypeLonglong)
	ftUint := types.NewFieldType(mysql.TypeLonglong)
	ftUint.SetFlag(ftUint.GetFlag() | mysql.UnsignedFlag)
	ftBytes := types.NewFieldType(mysql.TypeVarchar)
	ftDT := types.NewFieldType(mysql.TypeDatetime)
	ftDT.SetDecimal(0)
	ftTS := types.NewFieldType(mysql.TypeTimestamp)
	ftTS.SetDecimal(0)

	cols := []ColInfo{
		{ID: 1, Ft: ftInt},
		{ID: 2, Ft: ftUint},
		{ID: 3, Ft: ftBytes},
		{ID: 4, Ft: ftDT},
		{ID: 5, Ft: ftTS},
	}
	fts := []*types.FieldType{ftInt, ftUint, ftBytes, ftDT, ftTS}
	colIDs := []int64{1, 2, 3, 4, 5}

	dt := types.NewTime(types.FromDate(2024, 1, 2, 3, 4, 5, 0), mysql.TypeDatetime, 0)
	ts := types.NewTime(types.FromDate(2024, 2, 3, 4, 5, 6, 0), mysql.TypeTimestamp, 0)

	type testCase struct {
		name      string
		encodeLoc *time.Location
		decodeLoc *time.Location
		expectTS  types.Time
	}
	testCases := []testCase{
		{
			name:      "loc_nil",
			encodeLoc: nil,
			decodeLoc: nil,
			expectTS:  ts,
		},
		{
			name:      "loc_local",
			encodeLoc: time.Local,
			decodeLoc: time.Local,
			expectTS:  ts,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var encoder Encoder
			rowData, err := encoder.Encode(tc.encodeLoc, colIDs, []types.Datum{
				types.NewIntDatum(123),
				types.NewUintDatum(456),
				types.NewBytesDatum([]byte("abc")),
				types.NewTimeDatum(dt),
				types.NewTimeDatum(ts),
			}, nil, nil)
			require.NoError(t, err)

			decoder := NewChunkDecoder(cols, []int64{-1}, nil, tc.decodeLoc)
			chk := chunk.NewChunkWithCapacity(fts, 1)
			require.NoError(t, decoder.DecodeToChunk(rowData, kv.IntHandle(-1), chk))
			require.Equal(t, 1, chk.NumRows())

			row := chk.GetRow(0)
			require.False(t, row.IsNull(0))
			require.Equal(t, int64(123), row.GetInt64(0))
			require.False(t, row.IsNull(1))
			require.Equal(t, uint64(456), row.GetUint64(1))
			require.False(t, row.IsNull(2))
			require.Equal(t, []byte("abc"), row.GetBytes(2))
			require.False(t, row.IsNull(3))
			require.Equal(t, 0, row.GetTime(3).Compare(dt))
			require.False(t, row.IsNull(4))
			require.Equal(t, 0, row.GetTime(4).Compare(tc.expectTS))
		})
	}
}

func TestEncodeOldDatumArena(t *testing.T) {
	// Verify encodeOldDatumToArena produces identical results to encodeOldDatum.
	dec := &BytesDecoder{}

	tests := []struct {
		name string
		tp   byte
		val  []byte
	}{
		{"Int", IntFlag, func() []byte { var b [8]byte; binary.BigEndian.PutUint64(b[:], uint64(42)); return b[:] }()},
		{"Uint", UintFlag, func() []byte { var b [8]byte; binary.BigEndian.PutUint64(b[:], 999); return b[:] }()},
		{"Bytes", BytesFlag, []byte("hello world")},
		{"Float", FloatFlag, func() []byte { var b [8]byte; binary.BigEndian.PutUint64(b[:], 0x4059000000000000); return b[:] }()},
		{"EmptyBytes", BytesFlag, []byte{}},
		{"NilFlag", NilFlag, nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			expected := dec.encodeOldDatum(tt.tp, tt.val)
			arena := make([]byte, 0, 64)
			result, newArena := encodeOldDatumToArena(tt.tp, tt.val, arena)
			require.Equal(t, expected, []byte(result), "mismatch for %s", tt.name)
			// Verify result is a sub-slice of arena.
			require.Equal(t, len(result), len(newArena)-len(arena))
		})
	}

	// Verify multiple sequential calls share the same arena.
	arena := make([]byte, 0, 256)
	var results [][]byte
	for _, tt := range tests {
		var result []byte
		result, arena = encodeOldDatumToArena(tt.tp, tt.val, arena)
		results = append(results, result)
	}
	// Verify each result matches original.
	for i, tt := range tests {
		expected := dec.encodeOldDatum(tt.tp, tt.val)
		require.Equal(t, expected, []byte(results[i]), "arena sequential mismatch for %s", tt.name)
	}
}

func TestDecodeToBytesNoHandleInto(t *testing.T) {
	// Encode a row with multiple column types.
	var encoder Encoder
	colIDs := []int64{1, 2, 3}
	datums := []types.Datum{
		types.NewIntDatum(42),
		types.NewBytesDatum([]byte("test")),
		types.NewUintDatum(100),
	}
	rowData, err := encoder.Encode(time.UTC, colIDs, datums, nil, nil)
	require.NoError(t, err)

	uft := types.NewFieldType(mysql.TypeLonglong)
	uft.SetFlag(uft.GetFlag() | mysql.UnsignedFlag)
	columns := []ColInfo{
		{ID: 1, Ft: types.NewFieldType(mysql.TypeLonglong)},
		{ID: 2, Ft: types.NewFieldType(mysql.TypeVarchar)},
		{ID: 3, Ft: uft},
	}
	outputOffset := map[int64]int{1: 0, 2: 1, 3: 2}

	dec := NewByteDecoder(columns, []int64{-1}, nil, nil)

	// Original path.
	origValues, err := dec.DecodeToBytesNoHandle(outputOffset, rowData)
	require.NoError(t, err)

	// Into path.
	values := make([][]byte, len(columns))
	arena := make([]byte, 0, 256)
	intoValues, newArena, err := dec.DecodeToBytesNoHandleInto(outputOffset, rowData, values, arena)
	require.NoError(t, err)

	require.Equal(t, len(origValues), len(intoValues))
	for i := range origValues {
		require.Equal(t, origValues[i], intoValues[i], "mismatch at index %d", i)
	}

	// Arena should have grown.
	require.Greater(t, len(newArena), 0)

	// Second call should reuse the same values slice.
	datums2 := []types.Datum{
		types.NewIntDatum(100),
		types.NewBytesDatum([]byte("another")),
		types.NewUintDatum(200),
	}
	rowData2, err := encoder.Encode(time.UTC, colIDs, datums2, nil, nil)
	require.NoError(t, err)

	origValues2, err := dec.DecodeToBytesNoHandle(outputOffset, rowData2)
	require.NoError(t, err)

	arena = arena[:0]
	intoValues2, _, err := dec.DecodeToBytesNoHandleInto(outputOffset, rowData2, values, arena)
	require.NoError(t, err)

	for i := range origValues2 {
		require.Equal(t, origValues2[i], intoValues2[i], "second call mismatch at index %d", i)
	}
}
