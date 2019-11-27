// Copyright 2019 PingCAP, Inc.	package rowcodec_test
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
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/types/json"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/codec"
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
	def    []byte
	handle bool
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
				DefaultValue: func() ([]byte, error) {
					return t.def, nil
				},
				Flen:    t.ft.Flen,
				Decimal: t.ft.Decimal,
				Elems:   t.ft.Elems,
			})
		}

		// test encode input.
		var encoder rowcodec.Encoder
		sc := new(stmtctx.StatementContext)
		sc.TimeZone = time.UTC
		newRow, err := encoder.Encode(sc, colIDs, dts, nil)
		c.Assert(err, IsNil)
		decoder := rowcodec.NewDecoder(cols, handleID, sc.TimeZone)
		// decode to datum map.
		dm, err := decoder.DecodeToDatumMap(newRow, handleValue, nil)
		c.Assert(err, IsNil)
		for _, t := range testData {
			d, exists := dm[t.id]
			c.Assert(exists, IsTrue)
			c.Assert(d, DeepEquals, t.dt)
		}

		// decode to chunk.
		chk := chunk.New(fts, 1, 1)
		err = decoder.DecodeToChunk(newRow, handleValue, chk)
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
		oldRow, err := decoder.DecodeToBytes(colOffset, handleValue, newRow, nil)
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
			types.NewIntDatum(handleValue),          // decode as chunk & map, always encode it as int
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
		d.SetMysqlSet(types.Set{Name: name, Value: value})
		return d
	}
	getTime := func(value string) types.Time {
		t, err := types.ParseTime(&stmtctx.StatementContext{TimeZone: time.UTC}, value, mysql.TypeTimestamp, 6)
		c.Assert(err, IsNil)
		return t
	}

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
				DefaultValue: func() ([]byte, error) {
					return t.def, nil
				},
				Flen:    t.ft.Flen,
				Decimal: t.ft.Decimal,
				Elems:   t.ft.Elems,
			})
		}

		// test encode input.
		var encoder rowcodec.Encoder
		sc := new(stmtctx.StatementContext)
		sc.TimeZone = time.UTC
		newRow, err := encoder.Encode(sc, colIDs, dts, nil)
		c.Assert(err, IsNil)
		decoder := rowcodec.NewDecoder(cols, -1, sc.TimeZone)

		// decode to datum map.
		dm, err := decoder.DecodeToDatumMap(newRow, -1, nil)
		c.Assert(err, IsNil)
		for _, t := range testData {
			d, exists := dm[t.id]
			c.Assert(exists, IsTrue)
			c.Assert(d, DeepEquals, t.dt)
		}

		// decode to chunk.
		chk := chunk.New(fts, 1, 1)
		err = decoder.DecodeToChunk(newRow, -1, chk)
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
		oldRow, err := decoder.DecodeToBytes(colOffset, -1, newRow, nil)
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
			types.NewFieldType(mysql.TypeString),
			types.NewBytesDatum([]byte("abc")),
			types.NewBytesDatum([]byte("abc")),
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
			types.NewDecimalDatum(types.NewDecFromStringForTest("1.99")),
			types.NewDecimalDatum(types.NewDecFromStringForTest("1.99")),
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
			withEnumElems("y", "n")(types.NewFieldType(mysql.TypeEnum)),
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
			withEnumElems("n1", "n2")(types.NewFieldType(mysql.TypeSet)),
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
				DefaultValue: func() ([]byte, error) {
					return t.def, nil
				},
				Flen:    t.ft.Flen,
				Decimal: t.ft.Decimal,
				Elems:   t.ft.Elems,
			})
		}

		// test encode input.
		var encoder rowcodec.Encoder
		sc := new(stmtctx.StatementContext)
		sc.TimeZone = time.UTC
		newRow, err := encoder.Encode(sc, colIDs, dts, nil)
		c.Assert(err, IsNil)
		decoder := rowcodec.NewDecoder(cols, -1, sc.TimeZone)

		// decode to datum map.
		dm, err := decoder.DecodeToDatumMap(newRow, -1, nil)
		c.Assert(err, IsNil)
		for _, t := range testData {
			d, exists := dm[t.id]
			c.Assert(exists, IsTrue)
			c.Assert(d, DeepEquals, t.bt)
		}

		// decode to chunk.
		chk := chunk.New(fts, 1, 1)
		err = decoder.DecodeToChunk(newRow, -1, chk)
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
		oldRow, err := decoder.DecodeToBytes(colOffset, -1, newRow, nil)
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
	getDatumByte := func(d types.Datum) []byte {
		b, err := rowcodec.EncodeValueDatum(nil, d, nil)
		c.Assert(err, IsNil)
		return b
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
			getDatumByte(types.NewUintDatum(9)),
			false,
		},
	}
	encodeAndDecode(c, dtNilData)

	dtNilCol := []testData{
		{
			1,
			types.NewFieldType(mysql.TypeLonglong),
			types.NewDatum(nil),
			types.NewDatum(nil),
			[]byte{},
			false,
		},
	}
	colIDs := make([]int64, 0, len(dtNilCol))
	dts := make([]types.Datum, 0, len(dtNilCol))
	cols := make([]rowcodec.ColInfo, 0, len(dtNilCol))
	fts := make([]*types.FieldType, 0, len(dtNilCol))
	for i := range dtNilCol {
		t := dtNilCol[i]
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
			DefaultValue: func() ([]byte, error) {
				return t.def, nil
			},
			Flen:    t.ft.Flen,
			Decimal: t.ft.Decimal,
			Elems:   t.ft.Elems,
		})
	}
	var encoder rowcodec.Encoder
	sc := new(stmtctx.StatementContext)
	sc.TimeZone = time.UTC
	newRow, err := encoder.Encode(sc, colIDs, dts, nil)
	c.Assert(err, IsNil)
	tp := types.NewFieldType(mysql.TypeLonglong)
	tp.Flag |= mysql.NotNullFlag
	notExistButNotNilCol := rowcodec.ColInfo{
		ID:           2,
		Tp:           int32(tp.Tp),
		Flag:         int32(tp.Flag),
		IsPKHandle:   false,
		DefaultValue: nil,
	}
	rd := rowcodec.NewDecoder(append(cols, notExistButNotNilCol), -1, nil)
	colOffset := make(map[int64]int)
	colOffset[1] = 0
	colOffset[2] = 1
	_, err = rd.DecodeToBytes(colOffset, -1, newRow, nil)
	c.Assert(err.Error(), Equals, "Miss column 2")
}

func (s *testSuite) TestForceVarint(c *C) {
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
				DefaultValue: func() ([]byte, error) {
					return t.def, nil
				},
				Flen:    t.ft.Flen,
				Decimal: t.ft.Decimal,
				Elems:   t.ft.Elems,
			})
		}

		// test encode input.
		var encoder rowcodec.Encoder
		sc := new(stmtctx.StatementContext)
		sc.TimeZone = time.UTC
		newRow, err := encoder.Encode(sc, colIDs, dts, nil)
		c.Assert(err, IsNil)
		decoder := rowcodec.NewDecoder(cols, -1, sc.TimeZone, rowcodec.WithForceVarint)
		// decode to old row bytes.
		colOffset := make(map[int64]int)
		for i, t := range testData {
			colOffset[t.id] = i
		}
		oldRow, err := decoder.DecodeToBytes(colOffset, 1, newRow, nil)
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

	testSigPK := []testData{
		{
			-1,
			types.NewFieldType(mysql.TypeLonglong),
			types.NewIntDatum(1),
			types.NewIntDatum(1),
			nil,
			true,
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
	encodeAndDecodeByte(c, testSigPK)

	testUnsigPK := []testData{
		{
			-1,
			withUnsigned(types.NewFieldType(mysql.TypeLonglong)),
			types.NewUintDatum(1),
			types.NewUintDatum(1),
			nil,
			true,
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
	encodeAndDecodeByte(c, testUnsigPK)
}

func (s *testSuite) TestCodecUtil(c *C) {
	colIDs := []int64{1, 2, 3, 4}
	tps := make([]*types.FieldType, 4)
	for i := 0; i < 3; i++ {
		tps[i] = types.NewFieldType(mysql.TypeLonglong)
	}
	tps[3] = types.NewFieldType(mysql.TypeNull)
	sc := new(stmtctx.StatementContext)
	oldRow, err := tablecodec.EncodeRow(sc, types.MakeDatums(1, 2, 3, nil), colIDs, nil, nil)
	c.Check(err, IsNil)
	var (
		rb     rowcodec.Encoder
		newRow []byte
	)
	newRow, err = rowcodec.EncodeFromOldRowForTest(&rb, nil, oldRow, nil)
	c.Assert(err, IsNil)
	c.Assert(rowcodec.IsNewFormat(newRow), IsTrue)
	c.Assert(rowcodec.IsNewFormat(oldRow), IsFalse)

	// test stringer for decoder.
	var cols []rowcodec.ColInfo
	for i, ft := range tps {
		cols = append(cols, rowcodec.ColInfo{
			ID:           colIDs[i],
			Tp:           int32(ft.Tp),
			Flag:         int32(ft.Flag),
			IsPKHandle:   false,
			DefaultValue: nil,
			Flen:         ft.Flen,
			Decimal:      ft.Decimal,
			Elems:        ft.Elems,
		})
	}
	d := rowcodec.NewDecoder(cols, -1, nil)
	_, err = d.DecodeToDatumMap(newRow, -1, nil)
	c.Assert(err, IsNil)
	c.Assert(d.String(), Equals, "(1:[1]),(2:[2]),(3:[3])")

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
	oldRow, err := tablecodec.EncodeRow(sc, types.MakeDatums(1, 2, 3, nil), colIDs, nil, nil)
	c.Check(err, IsNil)

	var (
		rb     rowcodec.Encoder
		newRow []byte
	)
	newRow, err = rowcodec.EncodeFromOldRowForTest(&rb, nil, oldRow, nil)
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
		}
	}
	rd := rowcodec.NewDecoder(cols, 0, time.Local)
	chk := chunk.NewChunkWithCapacity(tps, 1)
	err = rd.DecodeToChunk(newRow, -1, chk)
	c.Assert(err, IsNil)
	row := chk.GetRow(0)
	for i := 0; i < 3; i++ {
		c.Assert(row.GetInt64(i), Equals, int64(i)+1)
	}
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
)
