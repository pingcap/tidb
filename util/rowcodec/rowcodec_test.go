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
	"math"
	"strings"
	"testing"
	"time"

	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/types/json"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/collate"
	"github.com/pingcap/tidb/util/rowcodec"
	"github.com/stretchr/testify/require"
)

type testData struct {
	id     int64
	ft     *types.FieldType
	dt     types.Datum
	bt     types.Datum
	def    *types.Datum
	handle bool
}

func TestEncodeLargeSmallReuseBug(t *testing.T) {
	t.Parallel()

	// reuse one rowcodec.Encoder.
	var encoder rowcodec.Encoder
	colFt := types.NewFieldType(mysql.TypeString)

	largeColID := int64(300)
	b, err := encoder.Encode(&stmtctx.StatementContext{}, []int64{largeColID}, []types.Datum{types.NewBytesDatum([]byte(""))}, nil)
	require.NoError(t, err)

	bDecoder := rowcodec.NewDatumMapDecoder([]rowcodec.ColInfo{
		{
			ID:         largeColID,
			Ft:         colFt,
			IsPKHandle: false,
		},
	}, nil)
	_, err = bDecoder.DecodeToDatumMap(b, nil)
	require.NoError(t, err)

	colFt = types.NewFieldType(mysql.TypeLonglong)
	smallColID := int64(1)
	b, err = encoder.Encode(&stmtctx.StatementContext{}, []int64{smallColID}, []types.Datum{types.NewIntDatum(2)}, nil)
	require.NoError(t, err)

	bDecoder = rowcodec.NewDatumMapDecoder([]rowcodec.ColInfo{
		{
			ID:         smallColID,
			Ft:         colFt,
			IsPKHandle: false,
		},
	}, nil)
	m, err := bDecoder.DecodeToDatumMap(b, nil)
	require.NoError(t, err)
	v := m[smallColID]
	require.Equal(t, int64(2), v.GetInt64())
}

func TestDecodeRowWithHandle(t *testing.T) {
	t.Parallel()

	handleID := int64(-1)
	handleValue := int64(10000)

	encodeAndDecodeHandle := func(t *testing.T, testData []testData) {
		// transform test data into input.
		colIDs := make([]int64, 0, len(testData))
		dts := make([]types.Datum, 0, len(testData))
		fts := make([]*types.FieldType, 0, len(testData))
		cols := make([]rowcodec.ColInfo, 0, len(testData))
		handleColFtMap := make(map[int64]*types.FieldType)
		for _, d := range testData {
			if d.handle {
				handleColFtMap[handleID] = d.ft
			} else {
				colIDs = append(colIDs, d.id)
				dts = append(dts, d.dt)
			}
			fts = append(fts, d.ft)
			cols = append(cols, rowcodec.ColInfo{
				ID:         d.id,
				IsPKHandle: d.handle,
				Ft:         d.ft,
			})
		}

		// test encode input.
		var encoder rowcodec.Encoder
		sc := new(stmtctx.StatementContext)
		sc.TimeZone = time.UTC
		newRow, err := encoder.Encode(sc, colIDs, dts, nil)
		require.NoError(t, err)

		// decode to datum map.
		mDecoder := rowcodec.NewDatumMapDecoder(cols, sc.TimeZone)
		dm, err := mDecoder.DecodeToDatumMap(newRow, nil)
		require.NoError(t, err)
		dm, err = tablecodec.DecodeHandleToDatumMap(kv.IntHandle(handleValue),
			[]int64{handleID}, handleColFtMap, sc.TimeZone, dm)
		require.NoError(t, err)
		for _, td := range testData {
			d, exists := dm[td.id]
			require.True(t, exists)
			require.Equal(t, td.dt, d)
		}

		// decode to chunk.
		cDecoder := rowcodec.NewChunkDecoder(cols, []int64{-1}, nil, sc.TimeZone)
		chk := chunk.New(fts, 1, 1)
		err = cDecoder.DecodeToChunk(newRow, kv.IntHandle(handleValue), chk)
		require.NoError(t, err)
		chkRow := chk.GetRow(0)
		cdt := chkRow.GetDatumRow(fts)
		for i, td := range testData {
			d := cdt[i]
			if d.Kind() == types.KindMysqlDecimal {
				require.Equal(t, td.bt.GetMysqlDecimal(), d.GetMysqlDecimal())
			} else {
				require.Equal(t, td.bt, d)
			}
		}

		// decode to old row bytes.
		colOffset := make(map[int64]int)
		for i, td := range testData {
			colOffset[td.id] = i
		}
		bDecoder := rowcodec.NewByteDecoder(cols, []int64{-1}, nil, nil)
		oldRow, err := bDecoder.DecodeToBytes(colOffset, kv.IntHandle(handleValue), newRow, nil)
		require.NoError(t, err)
		for i, td := range testData {
			remain, d, err := codec.DecodeOne(oldRow[i])
			require.NoError(t, err)
			require.Equal(t, 0, len(remain))
			if d.Kind() == types.KindMysqlDecimal {
				require.Equal(t, td.bt.GetMysqlDecimal(), d.GetMysqlDecimal())
			} else {
				require.Equal(t, td.bt, d)
			}
		}
	}

	// encode & decode signed int.
	testDataSigned := []testData{
		{
			handleID,
			types.NewFieldType(mysql.TypeLonglong),
			types.NewIntDatum(handleValue),
			types.NewIntDatum(handleValue),
			nil,
			true,
		},
		{
			10,
			types.NewFieldType(mysql.TypeLonglong),
			types.NewIntDatum(1),
			types.NewIntDatum(1),
			nil,
			false,
		},
	}
	encodeAndDecodeHandle(t, testDataSigned)

	// encode & decode unsigned int.
	testDataUnsigned := []testData{
		{
			handleID,
			withUnsigned(types.NewFieldType(mysql.TypeLonglong)),
			types.NewUintDatum(uint64(handleValue)),
			types.NewUintDatum(uint64(handleValue)), // decode as bytes will uint if unsigned.
			nil,
			true,
		},
		{
			10,
			types.NewFieldType(mysql.TypeLonglong),
			types.NewIntDatum(1),
			types.NewIntDatum(1),
			nil,
			false,
		},
	}
	encodeAndDecodeHandle(t, testDataUnsigned)
}

func TestEncodeKindNullDatum(t *testing.T) {
	t.Parallel()

	var encoder rowcodec.Encoder
	sc := new(stmtctx.StatementContext)
	sc.TimeZone = time.UTC
	colIDs := []int64{
		1,
		2,
	}
	var nilDt types.Datum
	nilDt.SetNull()
	dts := []types.Datum{nilDt, types.NewIntDatum(2)}
	ft := types.NewFieldType(mysql.TypeLonglong)
	fts := []*types.FieldType{ft, ft}
	newRow, err := encoder.Encode(sc, colIDs, dts, nil)
	require.NoError(t, err)

	cols := []rowcodec.ColInfo{{
		ID: 1,
		Ft: ft,
	},
		{
			ID: 2,
			Ft: ft,
		}}
	cDecoder := rowcodec.NewChunkDecoder(cols, []int64{-1}, nil, sc.TimeZone)
	chk := chunk.New(fts, 1, 1)
	err = cDecoder.DecodeToChunk(newRow, kv.IntHandle(-1), chk)
	require.NoError(t, err)
	chkRow := chk.GetRow(0)
	cdt := chkRow.GetDatumRow(fts)
	require.True(t, cdt[0].IsNull())
	require.Equal(t, int64(2), cdt[1].GetInt64())
}

func TestDecodeDecimalFspNotMatch(t *testing.T) {
	t.Parallel()

	var encoder rowcodec.Encoder
	sc := new(stmtctx.StatementContext)
	sc.TimeZone = time.UTC
	colIDs := []int64{
		1,
	}
	dec := withFrac(4)(withLen(6)(types.NewDecimalDatum(types.NewDecFromStringForTest("11.9900"))))
	dts := []types.Datum{dec}
	ft := types.NewFieldType(mysql.TypeNewDecimal)
	ft.Decimal = 4
	fts := []*types.FieldType{ft}
	newRow, err := encoder.Encode(sc, colIDs, dts, nil)
	require.NoError(t, err)

	// decode to chunk.
	ft = types.NewFieldType(mysql.TypeNewDecimal)
	ft.Decimal = 3
	cols := make([]rowcodec.ColInfo, 0)
	cols = append(cols, rowcodec.ColInfo{
		ID: 1,
		Ft: ft,
	})
	cDecoder := rowcodec.NewChunkDecoder(cols, []int64{-1}, nil, sc.TimeZone)
	chk := chunk.New(fts, 1, 1)
	err = cDecoder.DecodeToChunk(newRow, kv.IntHandle(-1), chk)
	require.NoError(t, err)
	chkRow := chk.GetRow(0)
	cdt := chkRow.GetDatumRow(fts)
	dec = withFrac(3)(withLen(6)(types.NewDecimalDatum(types.NewDecFromStringForTest("11.990"))))
	require.Equal(t, dec.GetMysqlDecimal().String(), cdt[0].GetMysqlDecimal().String())
}

func TestTypesNewRowCodec(t *testing.T) {
	t.Parallel()

	getJSONDatum := func(value string) types.Datum {
		j, err := json.ParseBinaryFromString(value)
		require.NoError(t, err)
		var d types.Datum
		d.SetMysqlJSON(j)
		return d
	}
	getSetDatum := func(name string, value uint64) types.Datum {
		var d types.Datum
		d.SetMysqlSet(types.Set{Name: name, Value: value}, mysql.DefaultCollationName)
		return d
	}
	getTime := func(value string) types.Time {
		timeVal, err := types.ParseTime(&stmtctx.StatementContext{TimeZone: time.UTC}, value, mysql.TypeTimestamp, 6)
		require.NoError(t, err)
		return timeVal
	}

	var encoder rowcodec.Encoder
	encodeAndDecode := func(t *testing.T, testData []testData) {
		// transform test data into input.
		colIDs := make([]int64, 0, len(testData))
		dts := make([]types.Datum, 0, len(testData))
		fts := make([]*types.FieldType, 0, len(testData))
		cols := make([]rowcodec.ColInfo, 0, len(testData))
		for i := range testData {
			t := testData[i]
			colIDs = append(colIDs, t.id)
			dts = append(dts, t.dt)
			fts = append(fts, t.ft)
			cols = append(cols, rowcodec.ColInfo{
				ID:         t.id,
				IsPKHandle: t.handle,
				Ft:         t.ft,
			})
		}

		// test encode input.
		sc := new(stmtctx.StatementContext)
		sc.TimeZone = time.UTC
		newRow, err := encoder.Encode(sc, colIDs, dts, nil)
		require.NoError(t, err)

		// decode to datum map.
		mDecoder := rowcodec.NewDatumMapDecoder(cols, sc.TimeZone)
		dm, err := mDecoder.DecodeToDatumMap(newRow, nil)
		require.NoError(t, err)
		for _, td := range testData {
			d, exists := dm[td.id]
			require.True(t, exists)
			require.Equal(t, td.dt, d)
		}

		// decode to chunk.
		cDecoder := rowcodec.NewChunkDecoder(cols, []int64{-1}, nil, sc.TimeZone)
		chk := chunk.New(fts, 1, 1)
		err = cDecoder.DecodeToChunk(newRow, kv.IntHandle(-1), chk)
		require.NoError(t, err)
		chkRow := chk.GetRow(0)
		cdt := chkRow.GetDatumRow(fts)
		for i, td := range testData {
			d := cdt[i]
			if d.Kind() == types.KindMysqlDecimal {
				require.Equal(t, td.bt.GetMysqlDecimal(), d.GetMysqlDecimal())
			} else {
				require.Equal(t, td.dt, d)
			}
		}

		// decode to old row bytes.
		colOffset := make(map[int64]int)
		for i, td := range testData {
			colOffset[td.id] = i
		}
		bDecoder := rowcodec.NewByteDecoder(cols, []int64{-1}, nil, nil)
		oldRow, err := bDecoder.DecodeToBytes(colOffset, kv.IntHandle(-1), newRow, nil)
		require.NoError(t, err)
		for i, td := range testData {
			remain, d, err := codec.DecodeOne(oldRow[i])
			require.NoError(t, err)
			require.Equal(t, 0, len(remain))
			if d.Kind() == types.KindMysqlDecimal {
				require.Equal(t, td.bt.GetMysqlDecimal(), d.GetMysqlDecimal())
			} else if d.Kind() == types.KindBytes {
				require.Equal(t, td.bt.GetBytes(), d.GetBytes())
			} else {
				require.Equal(t, td.bt, d)
			}
		}
	}

	testData := []testData{
		{
			1,
			types.NewFieldType(mysql.TypeLonglong),
			types.NewIntDatum(1),
			types.NewIntDatum(1),
			nil,
			false,
		},
		{
			22,
			withUnsigned(types.NewFieldType(mysql.TypeShort)),
			types.NewUintDatum(1),
			types.NewUintDatum(1),
			nil,
			false,
		},
		{
			3,
			types.NewFieldType(mysql.TypeDouble),
			types.NewFloat64Datum(2),
			types.NewFloat64Datum(2),
			nil,
			false,
		},
		{
			24,
			types.NewFieldTypeWithCollation(mysql.TypeBlob, mysql.DefaultCollationName, types.UnspecifiedLength),
			types.NewStringDatum("abc"),
			types.NewStringDatum("abc"),
			nil,
			false,
		},
		{
			25,
			&types.FieldType{Tp: mysql.TypeString, Collate: mysql.DefaultCollationName},
			types.NewStringDatum("ab"),
			types.NewBytesDatum([]byte("ab")),
			nil,
			false,
		},
		{
			5,
			withFsp(6)(types.NewFieldType(mysql.TypeTimestamp)),
			types.NewTimeDatum(getTime("2011-11-10 11:11:11.999999")),
			types.NewUintDatum(1840446893366133311),
			nil,
			false,
		},
		{
			16,
			withFsp(0)(types.NewFieldType(mysql.TypeDuration)),
			types.NewDurationDatum(getDuration("4:00:00")),
			types.NewIntDatum(14400000000000),
			nil,
			false,
		},
		{
			8,
			types.NewFieldType(mysql.TypeNewDecimal),
			withFrac(4)(withLen(6)(types.NewDecimalDatum(types.NewDecFromStringForTest("11.9900")))),
			withFrac(4)(withLen(6)(types.NewDecimalDatum(types.NewDecFromStringForTest("11.9900")))),
			nil,
			false,
		},
		{
			12,
			types.NewFieldType(mysql.TypeYear),
			types.NewIntDatum(1999),
			types.NewIntDatum(1999),
			nil,
			false,
		},
		{
			9,
			withEnumElems("y", "n")(types.NewFieldTypeWithCollation(mysql.TypeEnum, mysql.DefaultCollationName, collate.DefaultLen)),
			types.NewMysqlEnumDatum(types.Enum{Name: "n", Value: 2}),
			types.NewUintDatum(2),
			nil,
			false,
		},
		{
			14,
			types.NewFieldType(mysql.TypeJSON),
			getJSONDatum(`{"a":2}`),
			getJSONDatum(`{"a":2}`),
			nil,
			false,
		},
		{
			11,
			types.NewFieldType(mysql.TypeNull),
			types.NewDatum(nil),
			types.NewDatum(nil),
			nil,
			false,
		},
		{
			2,
			types.NewFieldType(mysql.TypeNull),
			types.NewDatum(nil),
			types.NewDatum(nil),
			nil,
			false,
		},
		{
			100,
			types.NewFieldType(mysql.TypeNull),
			types.NewDatum(nil),
			types.NewDatum(nil),
			nil,
			false,
		},
		{
			116,
			types.NewFieldType(mysql.TypeFloat),
			types.NewFloat32Datum(6),
			types.NewFloat64Datum(6),
			nil,
			false,
		},
		{
			117,
			withEnumElems("n1", "n2")(types.NewFieldTypeWithCollation(mysql.TypeSet, mysql.DefaultCollationName, collate.DefaultLen)),
			getSetDatum("n1", 1),
			types.NewUintDatum(1),
			nil,
			false,
		},
		{
			118,
			withFlen(24)(types.NewFieldType(mysql.TypeBit)), // 3 bit
			types.NewMysqlBitDatum(types.NewBinaryLiteralFromUint(3223600, 3)),
			types.NewUintDatum(3223600),
			nil,
			false,
		},
		{
			119,
			&types.FieldType{Tp: mysql.TypeVarString, Collate: mysql.DefaultCollationName},
			types.NewStringDatum(""),
			types.NewBytesDatum([]byte("")),
			nil,
			false,
		},
	}

	// test small
	encodeAndDecode(t, testData)

	// test large colID
	testData[0].id = 300
	encodeAndDecode(t, testData)
	testData[0].id = 1

	// test large data
	testData[3].dt = types.NewStringDatum(strings.Repeat("a", math.MaxUint16+1))
	testData[3].bt = types.NewStringDatum(strings.Repeat("a", math.MaxUint16+1))
	encodeAndDecode(t, testData)
}

func TestNilAndDefault(t *testing.T) {
	t.Parallel()

	encodeAndDecode := func(t *testing.T, testData []testData) {
		// transform test data into input.
		colIDs := make([]int64, 0, len(testData))
		dts := make([]types.Datum, 0, len(testData))
		cols := make([]rowcodec.ColInfo, 0, len(testData))
		fts := make([]*types.FieldType, 0, len(testData))
		for i := range testData {
			t := testData[i]
			if t.def == nil {
				colIDs = append(colIDs, t.id)
				dts = append(dts, t.dt)
			}
			fts = append(fts, t.ft)
			cols = append(cols, rowcodec.ColInfo{
				ID:         t.id,
				IsPKHandle: t.handle,
				Ft:         t.ft,
			})
		}
		ddf := func(i int, chk *chunk.Chunk) error {
			t := testData[i]
			if t.def == nil {
				chk.AppendNull(i)
				return nil
			}
			chk.AppendDatum(i, t.def)
			return nil
		}
		bdf := func(i int) ([]byte, error) {
			t := testData[i]
			if t.def == nil {
				return nil, nil
			}
			return getOldDatumByte(*t.def), nil
		}
		// test encode input.
		var encoder rowcodec.Encoder
		sc := new(stmtctx.StatementContext)
		sc.TimeZone = time.UTC
		newRow, err := encoder.Encode(sc, colIDs, dts, nil)
		require.NoError(t, err)

		// decode to datum map.
		mDecoder := rowcodec.NewDatumMapDecoder(cols, sc.TimeZone)
		dm, err := mDecoder.DecodeToDatumMap(newRow, nil)
		require.NoError(t, err)
		for _, td := range testData {
			d, exists := dm[td.id]
			if td.def != nil {
				// for datum should not fill default value.
				require.False(t, exists)
			} else {
				require.True(t, exists)
				require.Equal(t, td.bt, d)
			}
		}

		// decode to chunk.
		chk := chunk.New(fts, 1, 1)
		cDecoder := rowcodec.NewChunkDecoder(cols, []int64{-1}, ddf, sc.TimeZone)
		err = cDecoder.DecodeToChunk(newRow, kv.IntHandle(-1), chk)
		require.NoError(t, err)
		chkRow := chk.GetRow(0)
		cdt := chkRow.GetDatumRow(fts)
		for i, td := range testData {
			d := cdt[i]
			if d.Kind() == types.KindMysqlDecimal {
				require.Equal(t, td.bt.GetMysqlDecimal(), d.GetMysqlDecimal())
			} else {
				require.Equal(t, td.bt, d)
			}
		}

		chk = chunk.New(fts, 1, 1)
		cDecoder = rowcodec.NewChunkDecoder(cols, []int64{-1}, nil, sc.TimeZone)
		err = cDecoder.DecodeToChunk(newRow, kv.IntHandle(-1), chk)
		require.NoError(t, err)
		chkRow = chk.GetRow(0)
		cdt = chkRow.GetDatumRow(fts)
		for i := range testData {
			if i == 0 {
				continue
			}
			require.True(t, cdt[i].IsNull())
		}

		// decode to old row bytes.
		colOffset := make(map[int64]int)
		for i, td := range testData {
			colOffset[td.id] = i
		}
		bDecoder := rowcodec.NewByteDecoder(cols, []int64{-1}, bdf, sc.TimeZone)
		oldRow, err := bDecoder.DecodeToBytes(colOffset, kv.IntHandle(-1), newRow, nil)
		require.NoError(t, err)
		for i, td := range testData {
			remain, d, err := codec.DecodeOne(oldRow[i])
			require.NoError(t, err)
			require.Equal(t, 0, len(remain))
			if d.Kind() == types.KindMysqlDecimal {
				require.Equal(t, td.bt.GetMysqlDecimal(), d.GetMysqlDecimal())
			} else {
				require.Equal(t, td.bt, d)
			}
		}
	}
	dtNilData := []testData{
		{
			1,
			types.NewFieldType(mysql.TypeLonglong),
			types.NewIntDatum(1),
			types.NewIntDatum(1),
			nil,
			false,
		},
		{
			2,
			withUnsigned(types.NewFieldType(mysql.TypeLonglong)),
			types.NewUintDatum(1),
			types.NewUintDatum(9),
			getDatumPoint(types.NewUintDatum(9)),
			false,
		},
	}
	encodeAndDecode(t, dtNilData)
}

func TestVarintCompatibility(t *testing.T) {
	t.Parallel()

	encodeAndDecodeByte := func(t *testing.T, testData []testData) {
		// transform test data into input.
		colIDs := make([]int64, 0, len(testData))
		dts := make([]types.Datum, 0, len(testData))
		cols := make([]rowcodec.ColInfo, 0, len(testData))
		for i := range testData {
			t := testData[i]
			colIDs = append(colIDs, t.id)
			dts = append(dts, t.dt)
			cols = append(cols, rowcodec.ColInfo{
				ID:         t.id,
				IsPKHandle: t.handle,
				Ft:         t.ft,
			})
		}

		// test encode input.
		var encoder rowcodec.Encoder
		sc := new(stmtctx.StatementContext)
		sc.TimeZone = time.UTC
		newRow, err := encoder.Encode(sc, colIDs, dts, nil)
		require.NoError(t, err)
		decoder := rowcodec.NewByteDecoder(cols, []int64{-1}, nil, sc.TimeZone)
		// decode to old row bytes.
		colOffset := make(map[int64]int)
		for i, td := range testData {
			colOffset[td.id] = i
		}
		oldRow, err := decoder.DecodeToBytes(colOffset, kv.IntHandle(1), newRow, nil)
		require.NoError(t, err)
		for i, td := range testData {
			oldVarint, err := tablecodec.EncodeValue(nil, nil, td.bt) // tablecodec will encode as varint/varuint
			require.NoError(t, err)
			require.Equal(t, oldRow[i], oldVarint)
		}
	}

	testDataValue := []testData{
		{
			1,
			types.NewFieldType(mysql.TypeLonglong),
			types.NewIntDatum(1),
			types.NewIntDatum(1),
			nil,
			false,
		},
		{
			2,
			withUnsigned(types.NewFieldType(mysql.TypeLonglong)),
			types.NewUintDatum(1),
			types.NewUintDatum(1),
			nil,
			false,
		},
	}
	encodeAndDecodeByte(t, testDataValue)
}

func TestCodecUtil(t *testing.T) {
	t.Parallel()

	colIDs := []int64{1, 2, 3, 4}
	tps := make([]*types.FieldType, 4)
	for i := 0; i < 3; i++ {
		tps[i] = types.NewFieldType(mysql.TypeLonglong)
	}
	tps[3] = types.NewFieldType(mysql.TypeNull)
	sc := new(stmtctx.StatementContext)
	oldRow, err := tablecodec.EncodeOldRow(sc, types.MakeDatums(1, 2, 3, nil), colIDs, nil, nil)
	require.NoError(t, err)
	var (
		rb     rowcodec.Encoder
		newRow []byte
	)
	newRow, err = rowcodec.EncodeFromOldRow(&rb, nil, oldRow, nil)
	require.NoError(t, err)
	require.True(t, rowcodec.IsNewFormat(newRow))
	require.False(t, rowcodec.IsNewFormat(oldRow))

	// test stringer for decoder.
	var cols = make([]rowcodec.ColInfo, 0, len(tps))
	for i, ft := range tps {
		cols = append(cols, rowcodec.ColInfo{
			ID:         colIDs[i],
			IsPKHandle: false,
			Ft:         ft,
		})
	}
	d := rowcodec.NewDecoder(cols, []int64{-1}, nil)

	// test ColumnIsNull
	isNil, err := d.ColumnIsNull(newRow, 4, nil)
	require.NoError(t, err)
	require.True(t, isNil)
	isNil, err = d.ColumnIsNull(newRow, 1, nil)
	require.NoError(t, err)
	require.False(t, isNil)
	isNil, err = d.ColumnIsNull(newRow, 5, nil)
	require.NoError(t, err)
	require.True(t, isNil)
	isNil, err = d.ColumnIsNull(newRow, 5, []byte{1})
	require.NoError(t, err)
	require.False(t, isNil)

	// test isRowKey
	require.False(t, rowcodec.IsRowKey([]byte{'b', 't'}))
	require.False(t, rowcodec.IsRowKey([]byte{'t', 'r'}))
}

func TestOldRowCodec(t *testing.T) {
	t.Parallel()

	colIDs := []int64{1, 2, 3, 4}
	tps := make([]*types.FieldType, 4)
	for i := 0; i < 3; i++ {
		tps[i] = types.NewFieldType(mysql.TypeLonglong)
	}
	tps[3] = types.NewFieldType(mysql.TypeNull)
	sc := new(stmtctx.StatementContext)
	oldRow, err := tablecodec.EncodeOldRow(sc, types.MakeDatums(1, 2, 3, nil), colIDs, nil, nil)
	require.NoError(t, err)

	var (
		rb     rowcodec.Encoder
		newRow []byte
	)
	newRow, err = rowcodec.EncodeFromOldRow(&rb, nil, oldRow, nil)
	require.NoError(t, err)

	cols := make([]rowcodec.ColInfo, len(tps))
	for i, tp := range tps {
		cols[i] = rowcodec.ColInfo{
			ID: colIDs[i],
			Ft: tp,
		}
	}
	rd := rowcodec.NewChunkDecoder(cols, []int64{-1}, nil, time.Local)
	chk := chunk.NewChunkWithCapacity(tps, 1)
	err = rd.DecodeToChunk(newRow, kv.IntHandle(-1), chk)
	require.NoError(t, err)
	row := chk.GetRow(0)
	for i := 0; i < 3; i++ {
		require.Equal(t, int64(i)+1, row.GetInt64(i))
	}
}

func Test65535Bug(t *testing.T) {
	t.Parallel()

	colIds := []int64{1}
	tps := make([]*types.FieldType, 1)
	tps[0] = types.NewFieldType(mysql.TypeString)
	sc := new(stmtctx.StatementContext)
	text65535 := strings.Repeat("a", 65535)
	encode := rowcodec.Encoder{}
	bd, err := encode.Encode(sc, colIds, []types.Datum{types.NewStringDatum(text65535)}, nil)
	require.NoError(t, err)

	cols := make([]rowcodec.ColInfo, 1)
	cols[0] = rowcodec.ColInfo{
		ID: 1,
		Ft: tps[0],
	}
	dc := rowcodec.NewDatumMapDecoder(cols, nil)
	result, err := dc.DecodeToDatumMap(bd, nil)
	require.NoError(t, err)

	rs := result[1]
	require.Equal(t, text65535, rs.GetString())
}

var (
	withUnsigned = func(ft *types.FieldType) *types.FieldType {
		ft.Flag = ft.Flag | mysql.UnsignedFlag
		return ft
	}
	withEnumElems = func(elem ...string) func(ft *types.FieldType) *types.FieldType {
		return func(ft *types.FieldType) *types.FieldType {
			ft.Elems = elem
			return ft
		}
	}
	withFsp = func(fsp int) func(ft *types.FieldType) *types.FieldType {
		return func(ft *types.FieldType) *types.FieldType {
			ft.Decimal = fsp
			return ft
		}
	}
	withFlen = func(flen int) func(ft *types.FieldType) *types.FieldType {
		return func(ft *types.FieldType) *types.FieldType {
			ft.Flen = flen
			return ft
		}
	}
	getDuration = func(value string) types.Duration {
		dur, _ := types.ParseDuration(nil, value, 0)
		return dur
	}
	getOldDatumByte = func(d types.Datum) []byte {
		b, err := tablecodec.EncodeValue(nil, nil, d)
		if err != nil {
			panic(err)
		}
		return b
	}
	getDatumPoint = func(d types.Datum) *types.Datum {
		return &d
	}
	withFrac = func(f int) func(d types.Datum) types.Datum {
		return func(d types.Datum) types.Datum {
			d.SetFrac(f)
			return d
		}
	}
	withLen = func(len int) func(d types.Datum) types.Datum {
		return func(d types.Datum) types.Datum {
			d.SetLength(len)
			return d
		}
	}
)
