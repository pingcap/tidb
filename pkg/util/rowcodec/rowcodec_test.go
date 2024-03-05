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
	"encoding/binary"
	"hash/crc32"
	"math"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/sessionctx/stmtctx"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/codec"
	"github.com/pingcap/tidb/pkg/util/collate"
	"github.com/pingcap/tidb/pkg/util/rowcodec"
	"github.com/stretchr/testify/require"
)

type testData struct {
	id     int64
	ft     *types.FieldType
	input  types.Datum
	output types.Datum
	def    *types.Datum
	handle bool
}

func TestEncodeLargeSmallReuseBug(t *testing.T) {
	// reuse one rowcodec.Encoder.
	var encoder rowcodec.Encoder
	colFt := types.NewFieldType(mysql.TypeString)

	largeColID := int64(300)
	b, err := encoder.Encode(nil, []int64{largeColID}, []types.Datum{types.NewBytesDatum([]byte(""))}, nil)
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
	b, err = encoder.Encode(nil, []int64{smallColID}, []types.Datum{types.NewIntDatum(2)}, nil)
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
	handleID := int64(-1)
	handleValue := int64(10000)

	tests := []struct {
		name     string
		testData []testData
	}{
		{
			"signed int",
			[]testData{
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
			},
		},
		{
			"unsigned int",
			[]testData{
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
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			td := test.testData

			// transform test data into input.
			colIDs := make([]int64, 0, len(td))
			dts := make([]types.Datum, 0, len(td))
			fts := make([]*types.FieldType, 0, len(td))
			cols := make([]rowcodec.ColInfo, 0, len(td))
			handleColFtMap := make(map[int64]*types.FieldType)
			for _, d := range td {
				if d.handle {
					handleColFtMap[handleID] = d.ft
				} else {
					colIDs = append(colIDs, d.id)
					dts = append(dts, d.input)
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
			newRow, err := encoder.Encode(time.UTC, colIDs, dts, nil)
			require.NoError(t, err)

			// decode to datum map.
			mDecoder := rowcodec.NewDatumMapDecoder(cols, time.UTC)
			dm, err := mDecoder.DecodeToDatumMap(newRow, nil)
			require.NoError(t, err)

			dm, err = tablecodec.DecodeHandleToDatumMap(kv.IntHandle(handleValue), []int64{handleID}, handleColFtMap, time.UTC, dm)
			require.NoError(t, err)

			for _, d := range td {
				dat, exists := dm[d.id]
				require.True(t, exists)
				require.Equal(t, d.input, dat)
			}

			// decode to chunk.
			cDecoder := rowcodec.NewChunkDecoder(cols, []int64{-1}, nil, time.UTC)
			chk := chunk.New(fts, 1, 1)
			err = cDecoder.DecodeToChunk(newRow, kv.IntHandle(handleValue), chk)
			require.NoError(t, err)

			chkRow := chk.GetRow(0)
			cdt := chkRow.GetDatumRow(fts)
			for i, d := range td {
				dat := cdt[i]
				if dat.Kind() == types.KindMysqlDecimal {
					require.Equal(t, d.output.GetMysqlDecimal(), dat.GetMysqlDecimal())
				} else {
					require.Equal(t, d.output, dat)
				}
			}

			// decode to old row bytes.
			colOffset := make(map[int64]int)
			for i, t := range td {
				colOffset[t.id] = i
			}
			bDecoder := rowcodec.NewByteDecoder(cols, []int64{-1}, nil, nil)
			oldRow, err := bDecoder.DecodeToBytes(colOffset, kv.IntHandle(handleValue), newRow, nil)
			require.NoError(t, err)

			for i, d := range td {
				remain, dat, err := codec.DecodeOne(oldRow[i])
				require.NoError(t, err)
				require.Len(t, remain, 0)
				if dat.Kind() == types.KindMysqlDecimal {
					require.Equal(t, d.output.GetMysqlDecimal(), dat.GetMysqlDecimal())
				} else {
					require.Equal(t, d.output, dat)
				}
			}
		})
	}
}

func TestEncodeKindNullDatum(t *testing.T) {
	var encoder rowcodec.Encoder
	colIDs := []int64{1, 2}

	var nilDt types.Datum
	nilDt.SetNull()
	dts := []types.Datum{nilDt, types.NewIntDatum(2)}
	ft := types.NewFieldType(mysql.TypeLonglong)
	fts := []*types.FieldType{ft, ft}
	newRow, err := encoder.Encode(time.UTC, colIDs, dts, nil)
	require.NoError(t, err)

	cols := []rowcodec.ColInfo{{ID: 1, Ft: ft}, {ID: 2, Ft: ft}}
	cDecoder := rowcodec.NewChunkDecoder(cols, []int64{-1}, nil, time.UTC)
	chk := chunk.New(fts, 1, 1)
	err = cDecoder.DecodeToChunk(newRow, kv.IntHandle(-1), chk)
	require.NoError(t, err)

	chkRow := chk.GetRow(0)
	cdt := chkRow.GetDatumRow(fts)
	require.True(t, cdt[0].IsNull())
	require.Equal(t, int64(2), cdt[1].GetInt64())
}

func TestDecodeDecimalFspNotMatch(t *testing.T) {
	var encoder rowcodec.Encoder
	colIDs := []int64{
		1,
	}
	dec := withFrac(4)(withLen(6)(types.NewDecimalDatum(types.NewDecFromStringForTest("11.9900"))))
	dts := []types.Datum{dec}
	ft := types.NewFieldType(mysql.TypeNewDecimal)
	ft.SetDecimal(4)
	fts := []*types.FieldType{ft}
	newRow, err := encoder.Encode(time.UTC, colIDs, dts, nil)
	require.NoError(t, err)

	// decode to chunk.
	ft = types.NewFieldType(mysql.TypeNewDecimal)
	ft.SetDecimal(3)
	cols := make([]rowcodec.ColInfo, 0)
	cols = append(cols, rowcodec.ColInfo{
		ID: 1,
		Ft: ft,
	})
	cDecoder := rowcodec.NewChunkDecoder(cols, []int64{-1}, nil, time.UTC)
	chk := chunk.New(fts, 1, 1)
	err = cDecoder.DecodeToChunk(newRow, kv.IntHandle(-1), chk)
	require.NoError(t, err)

	chkRow := chk.GetRow(0)
	cdt := chkRow.GetDatumRow(fts)
	dec = withFrac(3)(withLen(6)(types.NewDecimalDatum(types.NewDecFromStringForTest("11.990"))))
	require.Equal(t, dec.GetMysqlDecimal().String(), cdt[0].GetMysqlDecimal().String())
}

func TestTypesNewRowCodec(t *testing.T) {
	getJSONDatum := func(value string) types.Datum {
		j, err := types.ParseBinaryJSONFromString(value)
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
		d, err := types.ParseTime(types.DefaultStmtNoWarningContext, value, mysql.TypeTimestamp, 6)
		require.NoError(t, err)
		return d
	}

	blobTp := types.NewFieldType(mysql.TypeBlob)
	blobTp.SetCollate(mysql.DefaultCollationName)
	blobTp.SetFlen(types.UnspecifiedLength)

	strTp := types.NewFieldType(mysql.TypeString)
	strTp.SetCollate(mysql.DefaultCollationName)

	enumTp := types.NewFieldType(mysql.TypeEnum)
	enumTp.SetCollate(mysql.DefaultCollationName)
	enumTp.SetFlen(collate.DefaultLen)

	setTp := types.NewFieldType(mysql.TypeSet)
	setTp.SetCollate(mysql.DefaultCollationName)
	setTp.SetFlen(collate.DefaultLen)

	varStrTp := types.NewFieldType(mysql.TypeVarString)
	varStrTp.SetCollate(mysql.DefaultCollationName)

	smallTestDataList := []testData{
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
			blobTp,
			types.NewStringDatum("abc"),
			types.NewStringDatum("abc"),
			nil,
			false,
		},
		{
			25,
			strTp,
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
			withEnumElems("y", "n")(enumTp),
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
			withEnumElems("n1", "n2")(setTp),
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
			varStrTp,
			types.NewStringDatum(""),
			types.NewBytesDatum([]byte("")),
			nil,
			false,
		},
	}

	largeColIDTestDataList := make([]testData, len(smallTestDataList))
	copy(largeColIDTestDataList, smallTestDataList)
	largeColIDTestDataList[0].id = 300

	largeTestDataList := make([]testData, len(smallTestDataList))
	copy(largeTestDataList, smallTestDataList)
	largeTestDataList[3].input = types.NewStringDatum(strings.Repeat("a", math.MaxUint16+1))
	largeTestDataList[3].output = types.NewStringDatum(strings.Repeat("a", math.MaxUint16+1))

	var encoder rowcodec.Encoder

	tests := []struct {
		name     string
		testData []testData
	}{
		{
			"small",
			smallTestDataList,
		},
		{
			"largeColID",
			largeColIDTestDataList,
		},
		{
			"largeData",
			largeTestDataList,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			td := test.testData

			// transform test data into input.
			colIDs := make([]int64, 0, len(td))
			dts := make([]types.Datum, 0, len(td))
			fts := make([]*types.FieldType, 0, len(td))
			cols := make([]rowcodec.ColInfo, 0, len(td))
			for _, d := range td {
				colIDs = append(colIDs, d.id)
				dts = append(dts, d.input)
				fts = append(fts, d.ft)
				cols = append(cols, rowcodec.ColInfo{
					ID:         d.id,
					IsPKHandle: d.handle,
					Ft:         d.ft,
				})
			}

			// test encode input.
			newRow, err := encoder.Encode(time.UTC, colIDs, dts, nil)
			require.NoError(t, err)

			// decode to datum map.
			mDecoder := rowcodec.NewDatumMapDecoder(cols, time.UTC)
			dm, err := mDecoder.DecodeToDatumMap(newRow, nil)
			require.NoError(t, err)

			for _, d := range td {
				dat, exists := dm[d.id]
				require.True(t, exists)
				require.Equal(t, d.input, dat)
			}

			// decode to chunk.
			cDecoder := rowcodec.NewChunkDecoder(cols, []int64{-1}, nil, time.UTC)
			chk := chunk.New(fts, 1, 1)
			err = cDecoder.DecodeToChunk(newRow, kv.IntHandle(-1), chk)
			require.NoError(t, err)

			chkRow := chk.GetRow(0)
			cdt := chkRow.GetDatumRow(fts)
			for i, d := range td {
				dat := cdt[i]
				if dat.Kind() == types.KindMysqlDecimal {
					require.Equal(t, d.output.GetMysqlDecimal(), dat.GetMysqlDecimal())
				} else {
					require.Equal(t, d.input, dat)
				}
			}

			// decode to old row bytes.
			colOffset := make(map[int64]int)
			for i, t := range td {
				colOffset[t.id] = i
			}
			bDecoder := rowcodec.NewByteDecoder(cols, []int64{-1}, nil, nil)
			oldRow, err := bDecoder.DecodeToBytes(colOffset, kv.IntHandle(-1), newRow, nil)
			require.NoError(t, err)

			for i, d := range td {
				remain, dat, err := codec.DecodeOne(oldRow[i])
				require.NoError(t, err)
				require.Len(t, remain, 0)
				if dat.Kind() == types.KindMysqlDecimal {
					require.Equal(t, d.output.GetMysqlDecimal(), dat.GetMysqlDecimal())
				} else if dat.Kind() == types.KindBytes {
					require.Equal(t, d.output.GetBytes(), dat.GetBytes())
				} else {
					require.Equal(t, d.output, dat)
				}
			}
		})
	}
}

func TestNilAndDefault(t *testing.T) {
	td := []testData{
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

	// transform test data into input.
	colIDs := make([]int64, 0, len(td))
	dts := make([]types.Datum, 0, len(td))
	cols := make([]rowcodec.ColInfo, 0, len(td))
	fts := make([]*types.FieldType, 0, len(td))
	for i := range td {
		d := td[i]
		if d.def == nil {
			colIDs = append(colIDs, d.id)
			dts = append(dts, d.input)
		}
		fts = append(fts, d.ft)
		cols = append(cols, rowcodec.ColInfo{
			ID:         d.id,
			IsPKHandle: d.handle,
			Ft:         d.ft,
		})
	}
	ddf := func(i int, chk *chunk.Chunk) error {
		d := td[i]
		if d.def == nil {
			chk.AppendNull(i)
			return nil
		}
		chk.AppendDatum(i, d.def)
		return nil
	}
	bdf := func(i int) ([]byte, error) {
		d := td[i]
		if d.def == nil {
			return nil, nil
		}
		return getOldDatumByte(*d.def), nil
	}

	// test encode input.
	var encoder rowcodec.Encoder
	newRow, err := encoder.Encode(time.UTC, colIDs, dts, nil)
	require.NoError(t, err)

	// decode to datum map.
	mDecoder := rowcodec.NewDatumMapDecoder(cols, time.UTC)
	dm, err := mDecoder.DecodeToDatumMap(newRow, nil)
	require.NoError(t, err)

	for _, d := range td {
		dat, exists := dm[d.id]
		if d.def != nil {
			// for datum should not fill default value.
			require.False(t, exists)
		} else {
			require.True(t, exists)
			require.Equal(t, d.output, dat)
		}
	}

	// decode to chunk.
	chk := chunk.New(fts, 1, 1)
	cDecoder := rowcodec.NewChunkDecoder(cols, []int64{-1}, ddf, time.UTC)
	err = cDecoder.DecodeToChunk(newRow, kv.IntHandle(-1), chk)
	require.NoError(t, err)

	chkRow := chk.GetRow(0)
	cdt := chkRow.GetDatumRow(fts)
	for i, d := range td {
		dat := cdt[i]
		if dat.Kind() == types.KindMysqlDecimal {
			require.Equal(t, d.output.GetMysqlDecimal(), dat.GetMysqlDecimal())
		} else {
			require.Equal(t, d.output, dat)
		}
	}

	chk = chunk.New(fts, 1, 1)
	cDecoder = rowcodec.NewChunkDecoder(cols, []int64{-1}, nil, time.UTC)
	err = cDecoder.DecodeToChunk(newRow, kv.IntHandle(-1), chk)
	require.NoError(t, err)

	chkRow = chk.GetRow(0)
	cdt = chkRow.GetDatumRow(fts)
	for i := range td {
		if i == 0 {
			continue
		}
		require.True(t, cdt[i].IsNull())
	}

	// decode to old row bytes.
	colOffset := make(map[int64]int)
	for i, t := range td {
		colOffset[t.id] = i
	}
	bDecoder := rowcodec.NewByteDecoder(cols, []int64{-1}, bdf, time.UTC)
	oldRow, err := bDecoder.DecodeToBytes(colOffset, kv.IntHandle(-1), newRow, nil)
	require.NoError(t, err)

	for i, d := range td {
		remain, dat, err := codec.DecodeOne(oldRow[i])
		require.NoError(t, err)
		require.Len(t, remain, 0)

		if dat.Kind() == types.KindMysqlDecimal {
			require.Equal(t, d.output.GetMysqlDecimal(), dat.GetMysqlDecimal())
		} else {
			require.Equal(t, d.output, dat)
		}
	}
}

func TestVarintCompatibility(t *testing.T) {
	td := []testData{
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

	// transform test data into input.
	colIDs := make([]int64, 0, len(td))
	dts := make([]types.Datum, 0, len(td))
	cols := make([]rowcodec.ColInfo, 0, len(td))
	for _, d := range td {
		colIDs = append(colIDs, d.id)
		dts = append(dts, d.input)
		cols = append(cols, rowcodec.ColInfo{
			ID:         d.id,
			IsPKHandle: d.handle,
			Ft:         d.ft,
		})
	}

	// test encode input.
	var encoder rowcodec.Encoder
	newRow, err := encoder.Encode(time.UTC, colIDs, dts, nil)
	require.NoError(t, err)

	decoder := rowcodec.NewByteDecoder(cols, []int64{-1}, nil, time.UTC)
	// decode to old row bytes.
	colOffset := make(map[int64]int)
	for i, t := range td {
		colOffset[t.id] = i
	}
	oldRow, err := decoder.DecodeToBytes(colOffset, kv.IntHandle(1), newRow, nil)
	require.NoError(t, err)

	for i, d := range td {
		oldVarint, err := tablecodec.EncodeValue(nil, nil, d.output) // tablecodec will encode as varint/varuint
		require.NoError(t, err)
		require.Equal(t, oldRow[i], oldVarint)
	}
}

func TestCodecUtil(t *testing.T) {
	colIDs := []int64{1, 2, 3, 4}
	tps := make([]*types.FieldType, 4)
	for i := 0; i < 3; i++ {
		tps[i] = types.NewFieldType(mysql.TypeLonglong)
	}
	tps[3] = types.NewFieldType(mysql.TypeNull)
	sc := stmtctx.NewStmtCtx()
	oldRow, err := tablecodec.EncodeOldRow(sc.TimeZone(), types.MakeDatums(1, 2, 3, nil), colIDs, nil, nil)
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
	isNull, err := d.ColumnIsNull(newRow, 4, nil)
	require.NoError(t, err)
	require.True(t, isNull)
	isNull, err = d.ColumnIsNull(newRow, 1, nil)
	require.NoError(t, err)
	require.False(t, isNull)
	isNull, err = d.ColumnIsNull(newRow, 5, nil)
	require.NoError(t, err)
	require.True(t, isNull)
	isNull, err = d.ColumnIsNull(newRow, 5, []byte{1})
	require.NoError(t, err)
	require.False(t, isNull)

	// test isRowKey
	require.False(t, rowcodec.IsRowKey([]byte{'b', 't'}))
	require.False(t, rowcodec.IsRowKey([]byte{'t', 'r'}))
}

func TestOldRowCodec(t *testing.T) {
	colIDs := []int64{1, 2, 3, 4}
	tps := make([]*types.FieldType, 4)
	for i := 0; i < 3; i++ {
		tps[i] = types.NewFieldType(mysql.TypeLonglong)
	}
	tps[3] = types.NewFieldType(mysql.TypeNull)
	sc := stmtctx.NewStmtCtx()
	oldRow, err := tablecodec.EncodeOldRow(sc.TimeZone(), types.MakeDatums(1, 2, 3, nil), colIDs, nil, nil)
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
		require.Equal(t, int64(i+1), row.GetInt64(i))
	}
}

func Test65535Bug(t *testing.T) {
	colIds := []int64{1}
	tps := make([]*types.FieldType, 1)
	tps[0] = types.NewFieldType(mysql.TypeString)
	text65535 := strings.Repeat("a", 65535)
	encode := rowcodec.Encoder{}
	bd, err := encode.Encode(time.UTC, colIds, []types.Datum{types.NewStringDatum(text65535)}, nil)
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

func TestColumnEncode(t *testing.T) {
	encodeUint64 := func(v uint64) []byte {
		return binary.LittleEndian.AppendUint64(nil, v)
	}
	encodeBytes := func(v []byte) []byte {
		return append(binary.LittleEndian.AppendUint32(nil, uint32(len(v))), v...)
	}
	convertTZ := func(ts types.Time) types.Time {
		require.NoError(t, ts.ConvertTimeZone(time.Local, time.UTC))
		return ts
	}
	var (
		buf     = make([]byte, 0, 128)
		intZero = 0
		intPos  = 42
		intNeg  = -2
		i8Min   = math.MinInt8
		i16Min  = math.MinInt16
		i32Min  = math.MinInt32
		i64Min  = math.MinInt64
		i24Min  = -1 << 23
		ct      = types.FromDate(2023, 1, 2, 3, 4, 5, 678)
		dur     = types.Duration{Duration: 123456*time.Microsecond + 7*time.Minute + 8*time.Hour, Fsp: 6}
		decZero = types.NewDecFromStringForTest("0.000")
		decPos  = types.NewDecFromStringForTest("3.14")
		decNeg  = types.NewDecFromStringForTest("-1.2")
		decMin  = types.NewMaxOrMinDec(true, 12, 6)
		decMax  = types.NewMaxOrMinDec(false, 12, 6)
		json1   = types.CreateBinaryJSON(nil)
		json2   = types.CreateBinaryJSON(int64(42))
		json3   = types.CreateBinaryJSON(map[string]any{"foo": "bar", "a": int64(42)})
	)

	for _, tt := range []struct {
		name string
		typ  *types.FieldType
		dat  types.Datum
		raw  []byte
		ok   bool
	}{
		{"unspecified", types.NewFieldType(mysql.TypeUnspecified), types.NewDatum(1), nil, false},
		{"wrong", types.NewFieldType(42), types.NewDatum(1), nil, false},
		{"mismatch/timestamp", types.NewFieldType(mysql.TypeTimestamp), types.NewDatum(1), nil, false},
		{"mismatch/datetime", types.NewFieldType(mysql.TypeDatetime), types.NewDatum(1), nil, false},
		{"mismatch/date", types.NewFieldType(mysql.TypeDate), types.NewDatum(1), nil, false},
		{"mismatch/newdate", types.NewFieldType(mysql.TypeNewDate), types.NewDatum(1), nil, false},
		{"mismatch/decimal", types.NewFieldType(mysql.TypeNewDecimal), types.NewDatum(1), nil, false},

		{"null", types.NewFieldType(mysql.TypeNull), types.NewDatum(1), nil, true},
		{"geometry", types.NewFieldType(mysql.TypeGeometry), types.NewDatum(1), nil, true},

		{"tinyint/zero", types.NewFieldType(mysql.TypeTiny), types.NewDatum(intZero), encodeUint64(uint64(intZero)), true},
		{"tinyint/pos", types.NewFieldType(mysql.TypeTiny), types.NewDatum(intPos), encodeUint64(uint64(intPos)), true},
		{"tinyint/neg", types.NewFieldType(mysql.TypeTiny), types.NewDatum(intNeg), encodeUint64(uint64(intNeg)), true},
		{"tinyint/min/signed", types.NewFieldType(mysql.TypeTiny), types.NewDatum(i8Min), encodeUint64(uint64(i8Min)), true},
		{"tinyint/max/signed", types.NewFieldType(mysql.TypeTiny), types.NewDatum(math.MaxInt8), encodeUint64(math.MaxInt8), true},
		{"tinyint/max/unsigned", types.NewFieldType(mysql.TypeTiny), types.NewDatum(math.MaxUint8), encodeUint64(math.MaxUint8), true},

		{"smallint/zero", types.NewFieldType(mysql.TypeShort), types.NewDatum(intZero), encodeUint64(uint64(intZero)), true},
		{"smallint/pos", types.NewFieldType(mysql.TypeShort), types.NewDatum(intPos), encodeUint64(uint64(intPos)), true},
		{"smallint/neg", types.NewFieldType(mysql.TypeShort), types.NewDatum(intNeg), encodeUint64(uint64(intNeg)), true},
		{"smallint/min/signed", types.NewFieldType(mysql.TypeShort), types.NewDatum(i16Min), encodeUint64(uint64(i16Min)), true},
		{"smallint/max/signed", types.NewFieldType(mysql.TypeShort), types.NewDatum(math.MaxInt16), encodeUint64(math.MaxInt16), true},
		{"smallint/max/unsigned", types.NewFieldType(mysql.TypeShort), types.NewDatum(math.MaxUint16), encodeUint64(math.MaxUint16), true},

		{"int/zero", types.NewFieldType(mysql.TypeLong), types.NewDatum(intZero), encodeUint64(uint64(intZero)), true},
		{"int/pos", types.NewFieldType(mysql.TypeLong), types.NewDatum(intPos), encodeUint64(uint64(intPos)), true},
		{"int/neg", types.NewFieldType(mysql.TypeLong), types.NewDatum(intNeg), encodeUint64(uint64(intNeg)), true},
		{"int/min/signed", types.NewFieldType(mysql.TypeLong), types.NewDatum(i32Min), encodeUint64(uint64(i32Min)), true},
		{"int/max/signed", types.NewFieldType(mysql.TypeLong), types.NewDatum(math.MaxInt32), encodeUint64(math.MaxInt32), true},
		{"int/max/unsigned", types.NewFieldType(mysql.TypeLong), types.NewDatum(math.MaxUint32), encodeUint64(math.MaxUint32), true},

		{"bigint/zero", types.NewFieldType(mysql.TypeLonglong), types.NewDatum(intZero), encodeUint64(uint64(intZero)), true},
		{"bigint/pos", types.NewFieldType(mysql.TypeLonglong), types.NewDatum(intPos), encodeUint64(uint64(intPos)), true},
		{"bigint/neg", types.NewFieldType(mysql.TypeLonglong), types.NewDatum(intNeg), encodeUint64(uint64(intNeg)), true},
		{"bigint/min/signed", types.NewFieldType(mysql.TypeLonglong), types.NewDatum(i64Min), encodeUint64(uint64(i64Min)), true},
		{"bigint/max/signed", types.NewFieldType(mysql.TypeLonglong), types.NewDatum(math.MaxInt64), encodeUint64(math.MaxInt64), true},
		{"bigint/max/unsigned", types.NewFieldType(mysql.TypeLonglong), types.NewDatum(uint64(math.MaxUint64)), encodeUint64(math.MaxUint64), true},

		{"mediumint/zero", types.NewFieldType(mysql.TypeInt24), types.NewDatum(intZero), encodeUint64(uint64(intZero)), true},
		{"mediumint/pos", types.NewFieldType(mysql.TypeInt24), types.NewDatum(intPos), encodeUint64(uint64(intPos)), true},
		{"mediumint/neg", types.NewFieldType(mysql.TypeInt24), types.NewDatum(intNeg), encodeUint64(uint64(intNeg)), true},
		{"mediumint/min/signed", types.NewFieldType(mysql.TypeInt24), types.NewDatum(i24Min), encodeUint64(uint64(i24Min)), true},
		{"mediumint/max/signed", types.NewFieldType(mysql.TypeInt24), types.NewDatum(1<<23 - 1), encodeUint64(1<<23 - 1), true},
		{"mediumint/max/unsigned", types.NewFieldType(mysql.TypeInt24), types.NewDatum(1<<24 - 1), encodeUint64(1<<24 - 1), true},

		{"year", types.NewFieldType(mysql.TypeYear), types.NewDatum(2023), encodeUint64(2023), true},

		{"varchar", types.NewFieldType(mysql.TypeVarchar), types.NewDatum("foo"), encodeBytes([]byte("foo")), true},
		{"varchar/empty", types.NewFieldType(mysql.TypeVarchar), types.NewDatum(""), encodeBytes([]byte{}), true},
		{"varbinary", types.NewFieldType(mysql.TypeVarString), types.NewDatum([]byte("foo")), encodeBytes([]byte("foo")), true},
		{"varbinary/empty", types.NewFieldType(mysql.TypeVarString), types.NewDatum([]byte("")), encodeBytes([]byte{}), true},
		{"char", types.NewFieldType(mysql.TypeString), types.NewDatum("foo"), encodeBytes([]byte("foo")), true},
		{"char/empty", types.NewFieldType(mysql.TypeString), types.NewDatum(""), encodeBytes([]byte{}), true},
		{"binary", types.NewFieldType(mysql.TypeString), types.NewDatum([]byte("foo")), encodeBytes([]byte("foo")), true},
		{"binary/empty", types.NewFieldType(mysql.TypeString), types.NewDatum([]byte("")), encodeBytes([]byte{}), true},
		{"text", types.NewFieldType(mysql.TypeBlob), types.NewDatum("foo"), encodeBytes([]byte("foo")), true},
		{"text/empty", types.NewFieldType(mysql.TypeBlob), types.NewDatum(""), encodeBytes([]byte{}), true},
		{"blob", types.NewFieldType(mysql.TypeBlob), types.NewDatum([]byte("foo")), encodeBytes([]byte("foo")), true},
		{"blob/empty", types.NewFieldType(mysql.TypeBlob), types.NewDatum([]byte("")), encodeBytes([]byte{}), true},
		{"longtext", types.NewFieldType(mysql.TypeLongBlob), types.NewDatum("foo"), encodeBytes([]byte("foo")), true},
		{"longtext/empty", types.NewFieldType(mysql.TypeLongBlob), types.NewDatum(""), encodeBytes([]byte{}), true},
		{"longblob", types.NewFieldType(mysql.TypeLongBlob), types.NewDatum([]byte("foo")), encodeBytes([]byte("foo")), true},
		{"longblob/empty", types.NewFieldType(mysql.TypeLongBlob), types.NewDatum([]byte("")), encodeBytes([]byte{}), true},
		{"mediumtext", types.NewFieldType(mysql.TypeMediumBlob), types.NewDatum("foo"), encodeBytes([]byte("foo")), true},
		{"mediumtext/empty", types.NewFieldType(mysql.TypeMediumBlob), types.NewDatum(""), encodeBytes([]byte{}), true},
		{"mediumblob", types.NewFieldType(mysql.TypeMediumBlob), types.NewDatum([]byte("foo")), encodeBytes([]byte("foo")), true},
		{"mediumblob/empty", types.NewFieldType(mysql.TypeMediumBlob), types.NewDatum([]byte("")), encodeBytes([]byte{}), true},
		{"tinytext", types.NewFieldType(mysql.TypeTinyBlob), types.NewDatum("foo"), encodeBytes([]byte("foo")), true},
		{"tinytext/empty", types.NewFieldType(mysql.TypeTinyBlob), types.NewDatum(""), encodeBytes([]byte{}), true},
		{"tinyblob", types.NewFieldType(mysql.TypeTinyBlob), types.NewDatum([]byte("foo")), encodeBytes([]byte("foo")), true},
		{"tinyblob/empty", types.NewFieldType(mysql.TypeTinyBlob), types.NewDatum([]byte("")), encodeBytes([]byte{}), true},

		{"float", types.NewFieldType(mysql.TypeFloat), types.NewDatum(float32(3.14)), encodeUint64(math.Float64bits(float64(float32(3.14)))), true},
		{"float/nan", types.NewFieldType(mysql.TypeFloat), types.NewDatum(float32(math.NaN())), encodeUint64(math.Float64bits(0)), true},
		{"float/+inf", types.NewFieldType(mysql.TypeFloat), types.NewDatum(float32(math.Inf(1))), encodeUint64(math.Float64bits(0)), true},
		{"float/-inf", types.NewFieldType(mysql.TypeFloat), types.NewDatum(float32(math.Inf(-1))), encodeUint64(math.Float64bits(0)), true},
		{"double", types.NewFieldType(mysql.TypeDouble), types.NewDatum(float64(3.14)), encodeUint64(math.Float64bits(3.14)), true},
		{"double/nan", types.NewFieldType(mysql.TypeDouble), types.NewDatum(math.NaN()), encodeUint64(math.Float64bits(0)), true},
		{"double/+inf", types.NewFieldType(mysql.TypeDouble), types.NewDatum(math.Inf(1)), encodeUint64(math.Float64bits(0)), true},
		{"double/-inf", types.NewFieldType(mysql.TypeDouble), types.NewDatum(math.Inf(-1)), encodeUint64(math.Float64bits(0)), true},

		{"enum", types.NewFieldType(mysql.TypeEnum), types.NewDatum(0b010), encodeUint64(0b010), true},
		{"set", types.NewFieldType(mysql.TypeSet), types.NewDatum(0b101), encodeUint64(0b101), true},
		{"bit", types.NewFieldType(mysql.TypeBit), types.NewBinaryLiteralDatum([]byte{0x12, 0x34}), encodeUint64(0x1234), true},
		{"bit/truncate", types.NewFieldType(mysql.TypeBit), types.NewBinaryLiteralDatum([]byte{0x12, 0x34, 0x12, 0x34, 0x12, 0x34, 0x12, 0x34, 0xff}), encodeUint64(math.MaxUint64), true},

		{
			"timestamp", types.NewFieldType(mysql.TypeTimestamp),
			types.NewTimeDatum(types.NewTime(ct, mysql.TypeTimestamp, 3)),
			encodeBytes([]byte(convertTZ(types.NewTime(ct, mysql.TypeTimestamp, 3)).String())),
			true,
		},
		{
			"timestamp/zero", types.NewFieldType(mysql.TypeTimestamp),
			types.NewTimeDatum(types.ZeroTimestamp),
			encodeBytes([]byte(convertTZ(types.ZeroTimestamp).String())),
			true,
		},
		{
			"timestamp/min", types.NewFieldType(mysql.TypeTimestamp),
			types.NewTimeDatum(types.MinTimestamp),
			encodeBytes([]byte(convertTZ(types.MinTimestamp).String())),
			true,
		},
		{
			"timestamp/max", types.NewFieldType(mysql.TypeTimestamp),
			types.NewTimeDatum(types.MaxTimestamp),
			encodeBytes([]byte(convertTZ(types.MaxTimestamp).String())),
			true,
		},
		{
			"datetime", types.NewFieldType(mysql.TypeDatetime),
			types.NewTimeDatum(types.NewTime(ct, mysql.TypeDatetime, 3)),
			encodeBytes([]byte(types.NewTime(ct, mysql.TypeDatetime, 3).String())),
			true,
		},
		{
			"datetime/zero", types.NewFieldType(mysql.TypeDatetime),
			types.NewTimeDatum(types.ZeroDatetime),
			encodeBytes([]byte(types.ZeroTimestamp.String())),
			true,
		},
		{
			"datetime/min", types.NewFieldType(mysql.TypeDatetime),
			types.NewTimeDatum(types.NewTime(types.MinDatetime, mysql.TypeDatetime, 6)),
			encodeBytes([]byte(types.NewTime(types.MinDatetime, mysql.TypeDatetime, 6).String())),
			true,
		},
		{
			"datetime/max", types.NewFieldType(mysql.TypeDatetime),
			types.NewTimeDatum(types.NewTime(types.MaxDatetime, mysql.TypeDatetime, 6)),
			encodeBytes([]byte(types.NewTime(types.MaxDatetime, mysql.TypeDatetime, 6).String())),
			true,
		},
		{
			"date", types.NewFieldType(mysql.TypeDate),
			types.NewTimeDatum(types.NewTime(ct, mysql.TypeDate, 3)),
			encodeBytes([]byte(types.NewTime(ct, mysql.TypeDate, 3).String())),
			true,
		},
		{
			"date/zero", types.NewFieldType(mysql.TypeDate),
			types.NewTimeDatum(types.ZeroDate),
			encodeBytes([]byte(types.ZeroDate.String())),
			true,
		},
		{
			"date/min",
			types.NewFieldType(mysql.TypeDate),
			types.NewTimeDatum(types.NewTime(types.MinDatetime, mysql.TypeDate, 6)),
			encodeBytes([]byte(types.NewTime(types.MinDatetime, mysql.TypeDate, 6).String())),
			true,
		},
		{
			"date/max",
			types.NewFieldType(mysql.TypeDate),
			types.NewTimeDatum(types.NewTime(types.MaxDatetime, mysql.TypeDate, 6)),
			encodeBytes([]byte(types.NewTime(types.MaxDatetime, mysql.TypeDate, 6).String())),
			true,
		},
		{
			"newdate", types.NewFieldType(mysql.TypeNewDate),
			types.NewTimeDatum(types.NewTime(ct, mysql.TypeNewDate, 3)),
			encodeBytes([]byte(types.NewTime(ct, mysql.TypeNewDate, 3).String())),
			true,
		},
		{
			"newdate/zero", types.NewFieldType(mysql.TypeNewDate),
			types.NewTimeDatum(types.ZeroDate),
			encodeBytes([]byte(types.ZeroDate.String())),
			true,
		},
		{
			"newdate/min",
			types.NewFieldType(mysql.TypeNewDate),
			types.NewTimeDatum(types.NewTime(types.MinDatetime, mysql.TypeNewDate, 6)),
			encodeBytes([]byte(types.NewTime(types.MinDatetime, mysql.TypeNewDate, 6).String())),
			true,
		},
		{
			"newdate/max",
			types.NewFieldType(mysql.TypeNewDate),
			types.NewTimeDatum(types.NewTime(types.MaxDatetime, mysql.TypeNewDate, 6)),
			encodeBytes([]byte(types.NewTime(types.MaxDatetime, mysql.TypeNewDate, 6).String())),
			true,
		},

		{"time", types.NewFieldType(mysql.TypeDuration), types.NewDurationDatum(dur), encodeBytes([]byte(dur.String())), true},
		{"time/zero", types.NewFieldType(mysql.TypeDuration), types.NewDurationDatum(types.ZeroDuration), encodeBytes([]byte(types.ZeroDuration.String())), true},
		{"time/max", types.NewFieldType(mysql.TypeDuration), types.NewDurationDatum(types.MaxMySQLDuration(3)), encodeBytes([]byte(types.MaxMySQLDuration(3).String())), true},

		{"decimal/zero", types.NewFieldType(mysql.TypeNewDecimal), types.NewDecimalDatum(decZero), encodeBytes([]byte(decZero.String())), true},
		{"decimal/pos", types.NewFieldType(mysql.TypeNewDecimal), types.NewDecimalDatum(decPos), encodeBytes([]byte(decPos.String())), true},
		{"decimal/neg", types.NewFieldType(mysql.TypeNewDecimal), types.NewDecimalDatum(decNeg), encodeBytes([]byte(decNeg.String())), true},
		{"decimal/min", types.NewFieldType(mysql.TypeNewDecimal), types.NewDecimalDatum(decMin), encodeBytes([]byte(decMin.String())), true},
		{"decimal/max", types.NewFieldType(mysql.TypeNewDecimal), types.NewDecimalDatum(decMax), encodeBytes([]byte(decMax.String())), true},

		{"json/1", types.NewFieldType(mysql.TypeJSON), types.NewJSONDatum(json1), encodeBytes([]byte(json1.String())), true},
		{"json/2", types.NewFieldType(mysql.TypeJSON), types.NewJSONDatum(json2), encodeBytes([]byte(json2.String())), true},
		{"json/3", types.NewFieldType(mysql.TypeJSON), types.NewJSONDatum(json3), encodeBytes([]byte(json3.String())), true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			col := rowcodec.ColData{&model.ColumnInfo{FieldType: *tt.typ}, &tt.dat}
			raw, err := col.Encode(time.Local, buf[:0])
			if tt.ok {
				require.NoError(t, err)
				if len(tt.raw) == 0 {
					require.Len(t, raw, 0)
				} else {
					require.Equal(t, tt.raw, raw)
				}
			} else {
				require.Error(t, err)
			}
		})
	}

	t.Run("nulldatum", func(t *testing.T) {
		for _, typ := range []byte{
			mysql.TypeUnspecified,
			mysql.TypeTiny,
			mysql.TypeShort,
			mysql.TypeLong,
			mysql.TypeFloat,
			mysql.TypeDouble,
			mysql.TypeNull,
			mysql.TypeTimestamp,
			mysql.TypeLonglong,
			mysql.TypeInt24,
			mysql.TypeDate,
			mysql.TypeDuration,
			mysql.TypeDatetime,
			mysql.TypeYear,
			mysql.TypeNewDate,
			mysql.TypeVarchar,
			mysql.TypeBit,
			mysql.TypeJSON,
			mysql.TypeNewDecimal,
			mysql.TypeEnum,
			mysql.TypeSet,
			mysql.TypeTinyBlob,
			mysql.TypeMediumBlob,
			mysql.TypeLongBlob,
			mysql.TypeBlob,
			mysql.TypeVarString,
			mysql.TypeString,
			mysql.TypeGeometry,
			42, // wrong type
		} {
			ft := types.NewFieldType(typ)
			dat := types.NewDatum(nil)
			col := rowcodec.ColData{&model.ColumnInfo{FieldType: *ft}, &dat}
			raw, err := col.Encode(time.Local, nil)
			require.NoError(t, err)
			require.Len(t, raw, 0)
		}
	})
}

func TestRowChecksum(t *testing.T) {
	typ1 := types.NewFieldType(mysql.TypeNull)
	dat1 := types.NewDatum(nil)
	col1 := rowcodec.ColData{&model.ColumnInfo{ID: 1, FieldType: *typ1}, &dat1}
	typ2 := types.NewFieldType(mysql.TypeLong)
	dat2 := types.NewDatum(42)
	col2 := rowcodec.ColData{&model.ColumnInfo{ID: 2, FieldType: *typ2}, &dat2}
	typ3 := types.NewFieldType(mysql.TypeVarchar)
	dat3 := types.NewDatum("foobar")
	col3 := rowcodec.ColData{&model.ColumnInfo{ID: 3, FieldType: *typ3}, &dat3}
	typ4 := types.NewFieldType(mysql.TypeTimestamp)
	dat4 := types.NewTimeDatum(types.NewTime(types.FromGoTime(time.Now()), mysql.TypeTimestamp, 6))
	col4 := rowcodec.ColData{&model.ColumnInfo{ID: 4, FieldType: *typ4}, &dat4}
	buf := make([]byte, 0, 64)
	for _, tt := range []struct {
		name string
		cols []rowcodec.ColData
	}{
		{"nil", nil},
		{"empty", []rowcodec.ColData{}},
		{"nullonly", []rowcodec.ColData{col1}},
		{"ordered", []rowcodec.ColData{col1, col2, col3, col4}},
		{"unordered", []rowcodec.ColData{col3, col1, col4, col2}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			row := rowcodec.RowData{tt.cols, buf}
			if !sort.IsSorted(row) {
				sort.Sort(row)
			}
			checksum, err := row.Checksum(time.Local)
			require.NoError(t, err)
			raw, err := row.Encode(time.Local)
			require.NoError(t, err)
			require.Equal(t, crc32.ChecksumIEEE(raw), checksum)
		})
	}
}

func TestEncodeDecodeRowWithChecksum(t *testing.T) {
	enc := rowcodec.Encoder{}

	for _, tt := range []struct {
		name      string
		checksums []uint32
	}{
		{"NoChecksum", nil},
		{"OneChecksum", []uint32{1}},
		{"TwoChecksum", []uint32{1, 2}},
		{"ThreeChecksum", []uint32{1, 2, 3}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			raw, err := enc.Encode(time.UTC, nil, nil, nil, tt.checksums...)
			require.NoError(t, err)
			dec := rowcodec.NewDatumMapDecoder([]rowcodec.ColInfo{}, time.UTC)
			_, err = dec.DecodeToDatumMap(raw, nil)
			require.NoError(t, err)
			v1, ok1 := enc.GetChecksum()
			v2, ok2 := enc.GetExtraChecksum()
			v3, ok3 := dec.GetChecksum()
			v4, ok4 := dec.GetExtraChecksum()
			if len(tt.checksums) == 0 {
				require.False(t, ok1)
				require.False(t, ok2)
				require.False(t, ok3)
				require.False(t, ok4)
			} else if len(tt.checksums) == 1 {
				require.True(t, ok1)
				require.False(t, ok2)
				require.True(t, ok3)
				require.False(t, ok4)
				require.Equal(t, tt.checksums[0], v1)
				require.Equal(t, tt.checksums[0], v3)
				require.Zero(t, v2)
				require.Zero(t, v4)
			} else {
				require.True(t, ok1)
				require.True(t, ok2)
				require.True(t, ok3)
				require.True(t, ok4)
				require.Equal(t, tt.checksums[0], v1)
				require.Equal(t, tt.checksums[1], v2)
				require.Equal(t, tt.checksums[0], v3)
				require.Equal(t, tt.checksums[1], v4)
			}
		})
	}

	t.Run("ReuseDecoder", func(t *testing.T) {
		dec := rowcodec.NewDatumMapDecoder([]rowcodec.ColInfo{}, time.UTC)

		raw1, err := enc.Encode(time.UTC, nil, nil, nil)
		require.NoError(t, err)
		_, err = dec.DecodeToDatumMap(raw1, nil)
		require.NoError(t, err)
		v1, ok1 := dec.GetChecksum()
		v2, ok2 := dec.GetExtraChecksum()
		require.False(t, ok1)
		require.False(t, ok2)
		require.Zero(t, v1)
		require.Zero(t, v2)

		raw2, err := enc.Encode(time.UTC, nil, nil, nil, 1, 2)
		require.NoError(t, err)
		_, err = dec.DecodeToDatumMap(raw2, nil)
		require.NoError(t, err)
		v1, ok1 = dec.GetChecksum()
		v2, ok2 = dec.GetExtraChecksum()
		require.True(t, ok1)
		require.True(t, ok2)
		require.Equal(t, uint32(1), v1)
		require.Equal(t, uint32(2), v2)

		raw3, err := enc.Encode(time.UTC, nil, nil, nil, 1)
		require.NoError(t, err)
		_, err = dec.DecodeToDatumMap(raw3, nil)
		require.NoError(t, err)
		v1, ok1 = dec.GetChecksum()
		v2, ok2 = dec.GetExtraChecksum()
		require.True(t, ok1)
		require.False(t, ok2)
		require.Equal(t, uint32(1), v1)
		require.Zero(t, v2)
	})
}

var (
	withUnsigned = func(ft *types.FieldType) *types.FieldType {
		ft.AddFlag(mysql.UnsignedFlag)
		return ft
	}
	withEnumElems = func(elem ...string) func(ft *types.FieldType) *types.FieldType {
		return func(ft *types.FieldType) *types.FieldType {
			ft.SetElems(elem)
			return ft
		}
	}
	withFsp = func(fsp int) func(ft *types.FieldType) *types.FieldType {
		return func(ft *types.FieldType) *types.FieldType {
			ft.SetDecimal(fsp)
			return ft
		}
	}
	withFlen = func(flen int) func(ft *types.FieldType) *types.FieldType {
		return func(ft *types.FieldType) *types.FieldType {
			ft.SetFlen(flen)
			return ft
		}
	}
	getDuration = func(value string) types.Duration {
		dur, _, _ := types.ParseDuration(types.DefaultStmtNoWarningContext, value, 0)
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
	withLen = func(l int) func(d types.Datum) types.Datum {
		return func(d types.Datum) types.Datum {
			d.SetLength(l)
			return d
		}
	}
)
