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

	. "github.com/pingcap/check"
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
)

func TestT(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testSuite{})

type testSuite struct{}

type testData struct {
	id     int64
	ft     *types.FieldType
	dt     types.Datum
	bt     types.Datum
	def    *types.Datum
	handle bool
}

func (s *testSuite) TestEncodeLargeSmallReuseBug(c *C) {
	// reuse one rowcodec.Encoder.
	var encoder rowcodec.Encoder
	colFt := types.NewFieldType(mysql.TypeString)

	largeColID := int64(300)
	b, err := encoder.Encode(&stmtctx.StatementContext{}, []int64{largeColID}, []types.Datum{types.NewBytesDatum([]byte(""))}, nil)
	c.Assert(err, IsNil)

	bDecoder := rowcodec.NewDatumMapDecoder([]rowcodec.ColInfo{
		{
			ID:         largeColID,
			Tp:         int32(colFt.Tp),
			Flag:       int32(colFt.Flag),
			IsPKHandle: false,
			Flen:       colFt.Flen,
			Decimal:    colFt.Decimal,
			Elems:      colFt.Elems,
		},
	}, []int64{-1}, nil)
	m, err := bDecoder.DecodeToDatumMap(b, kv.IntHandle(-1), nil)
	c.Assert(err, IsNil)
	v := m[largeColID]

	colFt = types.NewFieldType(mysql.TypeLonglong)
	smallColID := int64(1)
	b, err = encoder.Encode(&stmtctx.StatementContext{}, []int64{smallColID}, []types.Datum{types.NewIntDatum(2)}, nil)
	c.Assert(err, IsNil)

	bDecoder = rowcodec.NewDatumMapDecoder([]rowcodec.ColInfo{
		{
			ID:         smallColID,
			Tp:         int32(colFt.Tp),
			Flag:       int32(colFt.Flag),
			IsPKHandle: false,
			Flen:       colFt.Flen,
			Decimal:    colFt.Decimal,
			Elems:      colFt.Elems,
		},
	}, []int64{-1}, nil)
	m, err = bDecoder.DecodeToDatumMap(b, kv.IntHandle(-1), nil)
	c.Assert(err, IsNil)
	v = m[smallColID]
	c.Assert(v.GetInt64(), Equals, int64(2))
}

func (s *testSuite) TestDecodeRowWithHandle(c *C) {
	handleID := int64(-1)
	handleValue := int64(10000)

	encodeAndDecodeHandle := func(c *C, testData []testData) {
		// transform test data into input.
		colIDs := make([]int64, 0, len(testData))
		dts := make([]types.Datum, 0, len(testData))
		fts := make([]*types.FieldType, 0, len(testData))
		cols := make([]rowcodec.ColInfo, 0, len(testData))
		for i := range testData {
			t := testData[i]
			if !t.handle {
				colIDs = append(colIDs, t.id)
				dts = append(dts, t.dt)
			}
			fts = append(fts, t.ft)
			cols = append(cols, rowcodec.ColInfo{
				ID:         t.id,
				Tp:         int32(t.ft.Tp),
				Flag:       int32(t.ft.Flag),
				IsPKHandle: t.handle,
				Flen:       t.ft.Flen,
				Decimal:    t.ft.Decimal,
				Elems:      t.ft.Elems,
				Collate:    t.ft.Collate,
			})
		}

		// test encode input.
		var encoder rowcodec.Encoder
		sc := new(stmtctx.StatementContext)
		sc.TimeZone = time.UTC
		newRow, err := encoder.Encode(sc, colIDs, dts, nil)
		c.Assert(err, IsNil)

		// decode to datum map.
		mDecoder := rowcodec.NewDatumMapDecoder(cols, []int64{-1}, sc.TimeZone)
		dm, err := mDecoder.DecodeToDatumMap(newRow, kv.IntHandle(handleValue), nil)
		c.Assert(err, IsNil)
		for _, t := range testData {
			d, exists := dm[t.id]
			c.Assert(exists, IsTrue)
			c.Assert(d, DeepEquals, t.dt)
		}

		// decode to chunk.
		cDecoder := rowcodec.NewChunkDecoder(cols, []int64{-1}, nil, sc.TimeZone)
		chk := chunk.New(fts, 1, 1)
		err = cDecoder.DecodeToChunk(newRow, kv.IntHandle(handleValue), chk)
		c.Assert(err, IsNil)
		chkRow := chk.GetRow(0)
		cdt := chkRow.GetDatumRow(fts)
		for i, t := range testData {
			d := cdt[i]
			if d.Kind() == types.KindMysqlDecimal {
				c.Assert(d.GetMysqlDecimal(), DeepEquals, t.bt.GetMysqlDecimal())
			} else {
				c.Assert(d, DeepEquals, t.bt)
			}
		}

		// decode to old row bytes.
		colOffset := make(map[int64]int)
		for i, t := range testData {
			colOffset[t.id] = i
		}
		bDecoder := rowcodec.NewByteDecoder(cols, []int64{-1}, nil, nil)
		oldRow, err := bDecoder.DecodeToBytes(colOffset, kv.IntHandle(handleValue), newRow, nil)
		c.Assert(err, IsNil)
		for i, t := range testData {
			remain, d, err := codec.DecodeOne(oldRow[i])
			c.Assert(err, IsNil)
			c.Assert(len(remain), Equals, 0)
			if d.Kind() == types.KindMysqlDecimal {
				c.Assert(d.GetMysqlDecimal(), DeepEquals, t.bt.GetMysqlDecimal())
			} else {
				c.Assert(d, DeepEquals, t.bt)
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
	encodeAndDecodeHandle(c, testDataSigned)

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
	encodeAndDecodeHandle(c, testDataUnsigned)
}

func (s *testSuite) TestEncodeKindNullDatum(c *C) {
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
	c.Assert(err, IsNil)

	cols := []rowcodec.ColInfo{{
		ID:      1,
		Tp:      int32(ft.Tp),
		Flag:    int32(ft.Flag),
		Flen:    ft.Flen,
		Decimal: ft.Decimal,
		Elems:   ft.Elems,
		Collate: ft.Collate,
	},
		{
			ID:      2,
			Tp:      int32(ft.Tp),
			Flag:    int32(ft.Flag),
			Flen:    ft.Flen,
			Decimal: ft.Decimal,
			Elems:   ft.Elems,
			Collate: ft.Collate,
		}}
	cDecoder := rowcodec.NewChunkDecoder(cols, []int64{-1}, nil, sc.TimeZone)
	chk := chunk.New(fts, 1, 1)
	err = cDecoder.DecodeToChunk(newRow, kv.IntHandle(-1), chk)
	c.Assert(err, IsNil)
	chkRow := chk.GetRow(0)
	cdt := chkRow.GetDatumRow(fts)
	c.Assert(cdt[0].IsNull(), Equals, true)
	c.Assert(cdt[1].GetInt64(), Equals, int64(2))
}

func (s *testSuite) TestDecodeDecimalFspNotMatch(c *C) {
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
	c.Assert(err, IsNil)

	// decode to chunk.
	ft = types.NewFieldType(mysql.TypeNewDecimal)
	ft.Decimal = 3
	cols := make([]rowcodec.ColInfo, 0)
	cols = append(cols, rowcodec.ColInfo{
		ID:      1,
		Tp:      int32(ft.Tp),
		Flag:    int32(ft.Flag),
		Flen:    ft.Flen,
		Decimal: ft.Decimal,
		Elems:   ft.Elems,
		Collate: ft.Collate,
	})
	cDecoder := rowcodec.NewChunkDecoder(cols, []int64{-1}, nil, sc.TimeZone)
	chk := chunk.New(fts, 1, 1)
	err = cDecoder.DecodeToChunk(newRow, kv.IntHandle(-1), chk)
	c.Assert(err, IsNil)
	chkRow := chk.GetRow(0)
	cdt := chkRow.GetDatumRow(fts)
	dec = withFrac(3)(withLen(6)(types.NewDecimalDatum(types.NewDecFromStringForTest("11.990"))))
	c.Assert(cdt[0].GetMysqlDecimal().String(), DeepEquals, dec.GetMysqlDecimal().String())
}

func (s *testSuite) TestTypesNewRowCodec(c *C) {
	getJSONDatum := func(value string) types.Datum {
		j, err := json.ParseBinaryFromString(value)
		c.Assert(err, IsNil)
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
		t, err := types.ParseTime(&stmtctx.StatementContext{TimeZone: time.UTC}, value, mysql.TypeTimestamp, 6)
		c.Assert(err, IsNil)
		return t
	}

	var encoder rowcodec.Encoder
	encodeAndDecode := func(c *C, testData []testData) {
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
				Tp:         int32(t.ft.Tp),
				Flag:       int32(t.ft.Flag),
				IsPKHandle: t.handle,
				Flen:       t.ft.Flen,
				Decimal:    t.ft.Decimal,
				Elems:      t.ft.Elems,
				Collate:    t.ft.Collate,
			})
		}

		// test encode input.
		sc := new(stmtctx.StatementContext)
		sc.TimeZone = time.UTC
		newRow, err := encoder.Encode(sc, colIDs, dts, nil)
		c.Assert(err, IsNil)

		// decode to datum map.
		mDecoder := rowcodec.NewDatumMapDecoder(cols, []int64{-1}, sc.TimeZone)
		dm, err := mDecoder.DecodeToDatumMap(newRow, kv.IntHandle(-1), nil)
		c.Assert(err, IsNil)
		for _, t := range testData {
			d, exists := dm[t.id]
			c.Assert(exists, IsTrue)
			c.Assert(d, DeepEquals, t.dt)
		}

		// decode to chunk.
		cDecoder := rowcodec.NewChunkDecoder(cols, []int64{-1}, nil, sc.TimeZone)
		chk := chunk.New(fts, 1, 1)
		err = cDecoder.DecodeToChunk(newRow, kv.IntHandle(-1), chk)
		c.Assert(err, IsNil)
		chkRow := chk.GetRow(0)
		cdt := chkRow.GetDatumRow(fts)
		for i, t := range testData {
			d := cdt[i]
			if d.Kind() == types.KindMysqlDecimal {
				c.Assert(d.GetMysqlDecimal(), DeepEquals, t.bt.GetMysqlDecimal())
			} else {
				c.Assert(d, DeepEquals, t.dt)
			}
		}

		// decode to old row bytes.
		colOffset := make(map[int64]int)
		for i, t := range testData {
			colOffset[t.id] = i
		}
		bDecoder := rowcodec.NewByteDecoder(cols, []int64{-1}, nil, nil)
		oldRow, err := bDecoder.DecodeToBytes(colOffset, kv.IntHandle(-1), newRow, nil)
		c.Assert(err, IsNil)
		for i, t := range testData {
			remain, d, err := codec.DecodeOne(oldRow[i])
			c.Assert(err, IsNil)
			c.Assert(len(remain), Equals, 0)
			if d.Kind() == types.KindMysqlDecimal {
				c.Assert(d.GetMysqlDecimal(), DeepEquals, t.bt.GetMysqlDecimal())
			} else {
				c.Assert(d, DeepEquals, t.bt)
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
			types.NewFieldType(mysql.TypeBlob),
			types.NewBytesDatum([]byte("abc")),
			types.NewBytesDatum([]byte("abc")),
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
	encodeAndDecode(c, testData)

	// test large colID
	testData[0].id = 300
	encodeAndDecode(c, testData)
	testData[0].id = 1

	// test large data
	testData[3].dt = types.NewBytesDatum([]byte(strings.Repeat("a", math.MaxUint16+1)))
	testData[3].bt = types.NewBytesDatum([]byte(strings.Repeat("a", math.MaxUint16+1)))
	encodeAndDecode(c, testData)
}

func (s *testSuite) TestNilAndDefault(c *C) {
	encodeAndDecode := func(c *C, testData []testData) {
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
				Tp:         int32(t.ft.Tp),
				Flag:       int32(t.ft.Flag),
				IsPKHandle: t.handle,
				Flen:       t.ft.Flen,
				Decimal:    t.ft.Decimal,
				Elems:      t.ft.Elems,
				Collate:    t.ft.Collate,
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
		c.Assert(err, IsNil)

		// decode to datum map.
		mDecoder := rowcodec.NewDatumMapDecoder(cols, []int64{-1}, sc.TimeZone)
		dm, err := mDecoder.DecodeToDatumMap(newRow, kv.IntHandle(-1), nil)
		c.Assert(err, IsNil)
		for _, t := range testData {
			d, exists := dm[t.id]
			if t.def != nil {
				// for datum should not fill default value.
				c.Assert(exists, IsFalse)
			} else {
				c.Assert(exists, IsTrue)
				c.Assert(d, DeepEquals, t.bt)
			}
		}

		//decode to chunk.
		chk := chunk.New(fts, 1, 1)
		cDecoder := rowcodec.NewChunkDecoder(cols, []int64{-1}, ddf, sc.TimeZone)
		err = cDecoder.DecodeToChunk(newRow, kv.IntHandle(-1), chk)
		c.Assert(err, IsNil)
		chkRow := chk.GetRow(0)
		cdt := chkRow.GetDatumRow(fts)
		for i, t := range testData {
			d := cdt[i]
			if d.Kind() == types.KindMysqlDecimal {
				c.Assert(d.GetMysqlDecimal(), DeepEquals, t.bt.GetMysqlDecimal())
			} else {
				c.Assert(d, DeepEquals, t.bt)
			}
		}

		chk = chunk.New(fts, 1, 1)
		cDecoder = rowcodec.NewChunkDecoder(cols, []int64{-1}, nil, sc.TimeZone)
		err = cDecoder.DecodeToChunk(newRow, kv.IntHandle(-1), chk)
		c.Assert(err, IsNil)
		chkRow = chk.GetRow(0)
		cdt = chkRow.GetDatumRow(fts)
		for i := range testData {
			if i == 0 {
				continue
			}
			d := cdt[i]
			c.Assert(d.IsNull(), Equals, true)
		}

		// decode to old row bytes.
		colOffset := make(map[int64]int)
		for i, t := range testData {
			colOffset[t.id] = i
		}
		bDecoder := rowcodec.NewByteDecoder(cols, []int64{-1}, bdf, sc.TimeZone)
		oldRow, err := bDecoder.DecodeToBytes(colOffset, kv.IntHandle(-1), newRow, nil)
		c.Assert(err, IsNil)
		for i, t := range testData {
			remain, d, err := codec.DecodeOne(oldRow[i])
			c.Assert(err, IsNil)
			c.Assert(len(remain), Equals, 0)
			if d.Kind() == types.KindMysqlDecimal {
				c.Assert(d.GetMysqlDecimal(), DeepEquals, t.bt.GetMysqlDecimal())
			} else {
				c.Assert(d, DeepEquals, t.bt)
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
	encodeAndDecode(c, dtNilData)
}

func (s *testSuite) TestVarintCompatibility(c *C) {
	encodeAndDecodeByte := func(c *C, testData []testData) {
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
				Tp:         int32(t.ft.Tp),
				Flag:       int32(t.ft.Flag),
				IsPKHandle: t.handle,
				Flen:       t.ft.Flen,
				Decimal:    t.ft.Decimal,
				Elems:      t.ft.Elems,
				Collate:    t.ft.Collate,
			})
		}

		// test encode input.
		var encoder rowcodec.Encoder
		sc := new(stmtctx.StatementContext)
		sc.TimeZone = time.UTC
		newRow, err := encoder.Encode(sc, colIDs, dts, nil)
		c.Assert(err, IsNil)
		decoder := rowcodec.NewByteDecoder(cols, []int64{-1}, nil, sc.TimeZone)
		// decode to old row bytes.
		colOffset := make(map[int64]int)
		for i, t := range testData {
			colOffset[t.id] = i
		}
		oldRow, err := decoder.DecodeToBytes(colOffset, kv.IntHandle(1), newRow, nil)
		c.Assert(err, IsNil)
		for i, t := range testData {
			oldVarint, err := tablecodec.EncodeValue(nil, nil, t.bt) // tablecodec will encode as varint/varuint
			c.Assert(err, IsNil)
			c.Assert(oldVarint, DeepEquals, oldRow[i])
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
	encodeAndDecodeByte(c, testDataValue)
}

func (s *testSuite) TestCodecUtil(c *C) {
	colIDs := []int64{1, 2, 3, 4}
	tps := make([]*types.FieldType, 4)
	for i := 0; i < 3; i++ {
		tps[i] = types.NewFieldType(mysql.TypeLonglong)
	}
	tps[3] = types.NewFieldType(mysql.TypeNull)
	sc := new(stmtctx.StatementContext)
	oldRow, err := tablecodec.EncodeOldRow(sc, types.MakeDatums(1, 2, 3, nil), colIDs, nil, nil)
	c.Check(err, IsNil)
	var (
		rb     rowcodec.Encoder
		newRow []byte
	)
	newRow, err = rowcodec.EncodeFromOldRow(&rb, nil, oldRow, nil)
	c.Assert(err, IsNil)
	c.Assert(rowcodec.IsNewFormat(newRow), IsTrue)
	c.Assert(rowcodec.IsNewFormat(oldRow), IsFalse)

	// test stringer for decoder.
	var cols []rowcodec.ColInfo
	for i, ft := range tps {
		cols = append(cols, rowcodec.ColInfo{
			ID:         colIDs[i],
			Tp:         int32(ft.Tp),
			Flag:       int32(ft.Flag),
			IsPKHandle: false,
			Flen:       ft.Flen,
			Decimal:    ft.Decimal,
			Elems:      ft.Elems,
			Collate:    ft.Collate,
		})
	}
	d := rowcodec.NewDecoder(cols, []int64{-1}, nil)

	// test ColumnIsNull
	isNil, err := d.ColumnIsNull(newRow, 4, nil)
	c.Assert(err, IsNil)
	c.Assert(isNil, IsTrue)
	isNil, err = d.ColumnIsNull(newRow, 1, nil)
	c.Assert(err, IsNil)
	c.Assert(isNil, IsFalse)
	isNil, err = d.ColumnIsNull(newRow, 5, nil)
	c.Assert(err, IsNil)
	c.Assert(isNil, IsTrue)
	isNil, err = d.ColumnIsNull(newRow, 5, []byte{1})
	c.Assert(err, IsNil)
	c.Assert(isNil, IsFalse)

	// test isRowKey
	c.Assert(rowcodec.IsRowKey([]byte{'b', 't'}), IsFalse)
	c.Assert(rowcodec.IsRowKey([]byte{'t', 'r'}), IsFalse)
}

func (s *testSuite) TestOldRowCodec(c *C) {
	colIDs := []int64{1, 2, 3, 4}
	tps := make([]*types.FieldType, 4)
	for i := 0; i < 3; i++ {
		tps[i] = types.NewFieldType(mysql.TypeLonglong)
	}
	tps[3] = types.NewFieldType(mysql.TypeNull)
	sc := new(stmtctx.StatementContext)
	oldRow, err := tablecodec.EncodeOldRow(sc, types.MakeDatums(1, 2, 3, nil), colIDs, nil, nil)
	c.Check(err, IsNil)

	var (
		rb     rowcodec.Encoder
		newRow []byte
	)
	newRow, err = rowcodec.EncodeFromOldRow(&rb, nil, oldRow, nil)
	c.Check(err, IsNil)
	cols := make([]rowcodec.ColInfo, len(tps))
	for i, tp := range tps {
		cols[i] = rowcodec.ColInfo{
			ID:      colIDs[i],
			Tp:      int32(tp.Tp),
			Flag:    int32(tp.Flag),
			Flen:    tp.Flen,
			Decimal: tp.Decimal,
			Elems:   tp.Elems,
			Collate: tp.Collate,
		}
	}
	rd := rowcodec.NewChunkDecoder(cols, []int64{-1}, nil, time.Local)
	chk := chunk.NewChunkWithCapacity(tps, 1)
	err = rd.DecodeToChunk(newRow, kv.IntHandle(-1), chk)
	c.Assert(err, IsNil)
	row := chk.GetRow(0)
	for i := 0; i < 3; i++ {
		c.Assert(row.GetInt64(i), Equals, int64(i)+1)
	}
}

func (s *testSuite) Test65535Bug(c *C) {
	colIds := []int64{1}
	tps := make([]*types.FieldType, 1)
	tps[0] = types.NewFieldType(mysql.TypeString)
	sc := new(stmtctx.StatementContext)
	text65535 := strings.Repeat("a", 65535)
	encode := rowcodec.Encoder{}
	bd, err := encode.Encode(sc, colIds, []types.Datum{types.NewStringDatum(text65535)}, nil)
	c.Check(err, IsNil)

	cols := make([]rowcodec.ColInfo, 1)
	cols[0] = rowcodec.ColInfo{
		ID:   1,
		Tp:   int32(tps[0].Tp),
		Flag: int32(tps[0].Flag),
	}
	dc := rowcodec.NewDatumMapDecoder(cols, []int64{-1}, nil)
	result, err := dc.DecodeToDatumMap(bd, kv.IntHandle(-1), nil)
	c.Check(err, IsNil)
	rs := result[1]
	c.Check(rs.GetString(), Equals, text65535)
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
