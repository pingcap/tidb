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
// See the License for the specific language governing permissions and
// limitations under the License.

package tablecodec

import (
	"fmt"
	"math"
	"testing"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/rowcodec"
	"github.com/pingcap/tidb/util/testleak"
)

func TestT(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testTableCodecSuite{})

type testTableCodecSuite struct{}

// TestTableCodec  tests some functions in package tablecodec
// TODO: add more tests.
func (s *testTableCodecSuite) TestTableCodec(c *C) {
	defer testleak.AfterTest(c)()
	key := EncodeRowKey(1, codec.EncodeInt(nil, 2))
	h, err := DecodeRowKey(key)
	c.Assert(err, IsNil)
	c.Assert(h.IntValue(), Equals, int64(2))

	key = EncodeRowKeyWithHandle(1, kv.IntHandle(2))
	h, err = DecodeRowKey(key)
	c.Assert(err, IsNil)
	c.Assert(h.IntValue(), Equals, int64(2))
}

// column is a structure used for test
type column struct {
	id int64
	tp *types.FieldType
}

func (s *testTableCodecSuite) TestRowCodec(c *C) {
	defer testleak.AfterTest(c)()

	c1 := &column{id: 1, tp: types.NewFieldType(mysql.TypeLonglong)}
	c2 := &column{id: 2, tp: types.NewFieldType(mysql.TypeVarchar)}
	c3 := &column{id: 3, tp: types.NewFieldType(mysql.TypeNewDecimal)}
	c4 := &column{id: 4, tp: &types.FieldType{Tp: mysql.TypeEnum, Elems: []string{"a"}}}
	c5 := &column{id: 5, tp: &types.FieldType{Tp: mysql.TypeSet, Elems: []string{"a"}}}
	c6 := &column{id: 6, tp: &types.FieldType{Tp: mysql.TypeBit, Flen: 8}}
	cols := []*column{c1, c2, c3, c4, c5, c6}

	row := make([]types.Datum, 6)
	row[0] = types.NewIntDatum(100)
	row[1] = types.NewBytesDatum([]byte("abc"))
	row[2] = types.NewDecimalDatum(types.NewDecFromInt(1))
	row[3] = types.NewMysqlEnumDatum(types.Enum{Name: "a", Value: 1})
	row[4] = types.NewDatum(types.Set{Name: "a", Value: 1})
	row[5] = types.NewDatum(types.BinaryLiteral{100})
	// Encode
	colIDs := make([]int64, 0, len(row))
	for _, col := range cols {
		colIDs = append(colIDs, col.id)
	}
	rd := rowcodec.Encoder{Enable: true}
	sc := &stmtctx.StatementContext{TimeZone: time.Local}
	bs, err := EncodeRow(sc, row, colIDs, nil, nil, &rd)
	c.Assert(err, IsNil)
	c.Assert(bs, NotNil)

	// Decode
	colMap := make(map[int64]*types.FieldType, len(row))
	for _, col := range cols {
		colMap[col.id] = col.tp
	}
	r, err := DecodeRowToDatumMap(bs, colMap, time.UTC)
	c.Assert(err, IsNil)
	c.Assert(r, NotNil)
	c.Assert(r, HasLen, len(row))
	// Compare decoded row and original row
	for i, col := range cols {
		v, ok := r[col.id]
		c.Assert(ok, IsTrue)
		equal, err1 := v.CompareDatum(sc, &row[i])
		c.Assert(err1, IsNil)
		c.Assert(equal, Equals, 0, Commentf("expect: %v, got %v", row[i], v))
	}

	// colMap may contains more columns than encoded row.
	//colMap[4] = types.NewFieldType(mysql.TypeFloat)
	r, err = DecodeRowToDatumMap(bs, colMap, time.UTC)
	c.Assert(err, IsNil)
	c.Assert(r, NotNil)
	c.Assert(r, HasLen, len(row))
	for i, col := range cols {
		v, ok := r[col.id]
		c.Assert(ok, IsTrue)
		equal, err1 := v.CompareDatum(sc, &row[i])
		c.Assert(err1, IsNil)
		c.Assert(equal, Equals, 0)
	}

	// colMap may contains less columns than encoded row.
	delete(colMap, 3)
	delete(colMap, 4)
	r, err = DecodeRowToDatumMap(bs, colMap, time.UTC)
	c.Assert(err, IsNil)
	c.Assert(r, NotNil)
	c.Assert(r, HasLen, len(row)-2)
	for i, col := range cols {
		if i > 1 {
			break
		}
		v, ok := r[col.id]
		c.Assert(ok, IsTrue)
		equal, err1 := v.CompareDatum(sc, &row[i])
		c.Assert(err1, IsNil)
		c.Assert(equal, Equals, 0)
	}

	// Make sure empty row return not nil value.
	bs, err = EncodeOldRow(sc, []types.Datum{}, []int64{}, nil, nil)
	c.Assert(err, IsNil)
	c.Assert(bs, HasLen, 1)

	r, err = DecodeRowToDatumMap(bs, colMap, time.UTC)
	c.Assert(err, IsNil)
	c.Assert(len(r), Equals, 0)
}

func (s *testTableCodecSuite) TestDecodeColumnValue(c *C) {
	sc := &stmtctx.StatementContext{TimeZone: time.Local}

	// test timestamp
	d := types.NewTimeDatum(types.NewTime(types.FromGoTime(time.Now()), mysql.TypeTimestamp, types.DefaultFsp))
	bs, err := EncodeOldRow(sc, []types.Datum{d}, []int64{1}, nil, nil)
	c.Assert(err, IsNil)
	c.Assert(bs, NotNil)
	_, bs, err = codec.CutOne(bs) // ignore colID
	c.Assert(err, IsNil)
	tp := types.NewFieldType(mysql.TypeTimestamp)
	d1, err := DecodeColumnValue(bs, tp, sc.TimeZone)
	c.Assert(err, IsNil)
	cmp, err := d1.CompareDatum(sc, &d)
	c.Assert(err, IsNil)
	c.Assert(cmp, Equals, 0)

	// test set
	elems := []string{"a", "b", "c", "d", "e"}
	e, _ := types.ParseSetValue(elems, uint64(1))
	d = types.NewMysqlSetDatum(e, "")
	bs, err = EncodeOldRow(sc, []types.Datum{d}, []int64{1}, nil, nil)
	c.Assert(err, IsNil)
	c.Assert(bs, NotNil)
	_, bs, err = codec.CutOne(bs) // ignore colID
	c.Assert(err, IsNil)
	tp = types.NewFieldType(mysql.TypeSet)
	tp.Elems = elems
	d1, err = DecodeColumnValue(bs, tp, sc.TimeZone)
	c.Assert(err, IsNil)
	cmp, err = d1.CompareDatum(sc, &d)
	c.Assert(err, IsNil)
	c.Assert(cmp, Equals, 0)

	// test bit
	d = types.NewMysqlBitDatum(types.NewBinaryLiteralFromUint(3223600, 3))
	bs, err = EncodeOldRow(sc, []types.Datum{d}, []int64{1}, nil, nil)
	c.Assert(err, IsNil)
	c.Assert(bs, NotNil)
	_, bs, err = codec.CutOne(bs) // ignore colID
	c.Assert(err, IsNil)
	tp = types.NewFieldType(mysql.TypeBit)
	tp.Flen = 24
	d1, err = DecodeColumnValue(bs, tp, sc.TimeZone)
	c.Assert(err, IsNil)
	cmp, err = d1.CompareDatum(sc, &d)
	c.Assert(err, IsNil)
	c.Assert(cmp, Equals, 0)

	// test empty enum
	d = types.NewMysqlEnumDatum(types.Enum{})
	bs, err = EncodeOldRow(sc, []types.Datum{d}, []int64{1}, nil, nil)
	c.Assert(err, IsNil)
	c.Assert(bs, NotNil)
	_, bs, err = codec.CutOne(bs) // ignore colID
	c.Assert(err, IsNil)
	tp = types.NewFieldType(mysql.TypeEnum)
	d1, err = DecodeColumnValue(bs, tp, sc.TimeZone)
	c.Assert(err, IsNil)
	cmp, err = d1.CompareDatum(sc, &d)
	c.Assert(err, IsNil)
	c.Assert(cmp, Equals, 0)
}

func (s *testTableCodecSuite) TestUnflattenDatums(c *C) {
	sc := &stmtctx.StatementContext{TimeZone: time.UTC}
	input := types.MakeDatums(int64(1))
	tps := []*types.FieldType{types.NewFieldType(mysql.TypeLonglong)}
	output, err := UnflattenDatums(input, tps, sc.TimeZone)
	c.Assert(err, IsNil)
	cmp, err := input[0].CompareDatum(sc, &output[0])
	c.Assert(err, IsNil)
	c.Assert(cmp, Equals, 0)
}

func (s *testTableCodecSuite) TestTimeCodec(c *C) {
	defer testleak.AfterTest(c)()

	c1 := &column{id: 1, tp: types.NewFieldType(mysql.TypeLonglong)}
	c2 := &column{id: 2, tp: types.NewFieldType(mysql.TypeVarchar)}
	c3 := &column{id: 3, tp: types.NewFieldType(mysql.TypeTimestamp)}
	c4 := &column{id: 4, tp: types.NewFieldType(mysql.TypeDuration)}
	cols := []*column{c1, c2, c3, c4}
	colLen := len(cols)

	row := make([]types.Datum, colLen)
	row[0] = types.NewIntDatum(100)
	row[1] = types.NewBytesDatum([]byte("abc"))
	ts, err := types.ParseTimestamp(&stmtctx.StatementContext{TimeZone: time.UTC},
		"2016-06-23 11:30:45")
	c.Assert(err, IsNil)
	row[2] = types.NewDatum(ts)
	du, err := types.ParseDuration(nil, "12:59:59.999999", 6)
	c.Assert(err, IsNil)
	row[3] = types.NewDatum(du)

	// Encode
	colIDs := make([]int64, 0, colLen)
	for _, col := range cols {
		colIDs = append(colIDs, col.id)
	}
	rd := rowcodec.Encoder{Enable: true}
	sc := &stmtctx.StatementContext{TimeZone: time.UTC}
	bs, err := EncodeRow(sc, row, colIDs, nil, nil, &rd)
	c.Assert(err, IsNil)
	c.Assert(bs, NotNil)

	// Decode
	colMap := make(map[int64]*types.FieldType, colLen)
	for _, col := range cols {
		colMap[col.id] = col.tp
	}
	r, err := DecodeRowToDatumMap(bs, colMap, time.UTC)
	c.Assert(err, IsNil)
	c.Assert(r, NotNil)
	c.Assert(r, HasLen, colLen)
	// Compare decoded row and original row
	for i, col := range cols {
		v, ok := r[col.id]
		c.Assert(ok, IsTrue)
		equal, err1 := v.CompareDatum(sc, &row[i])
		c.Assert(err1, IsNil)
		c.Assert(equal, Equals, 0)
	}
}

func (s *testTableCodecSuite) TestCutRow(c *C) {
	defer testleak.AfterTest(c)()

	var err error
	c1 := &column{id: 1, tp: types.NewFieldType(mysql.TypeLonglong)}
	c2 := &column{id: 2, tp: types.NewFieldType(mysql.TypeVarchar)}
	c3 := &column{id: 3, tp: types.NewFieldType(mysql.TypeNewDecimal)}
	cols := []*column{c1, c2, c3}

	row := make([]types.Datum, 3)
	row[0] = types.NewIntDatum(100)
	row[1] = types.NewBytesDatum([]byte("abc"))
	row[2] = types.NewDecimalDatum(types.NewDecFromInt(1))

	sc := &stmtctx.StatementContext{TimeZone: time.UTC}
	data := make([][]byte, 3)
	data[0], err = EncodeValue(sc, nil, row[0])
	c.Assert(err, IsNil)
	data[1], err = EncodeValue(sc, nil, row[1])
	c.Assert(err, IsNil)
	data[2], err = EncodeValue(sc, nil, row[2])
	c.Assert(err, IsNil)
	// Encode
	colIDs := make([]int64, 0, 3)
	for _, col := range cols {
		colIDs = append(colIDs, col.id)
	}
	bs, err := EncodeOldRow(sc, row, colIDs, nil, nil)
	c.Assert(err, IsNil)
	c.Assert(bs, NotNil)

	// Decode
	colMap := make(map[int64]int, 3)
	for i, col := range cols {
		colMap[col.id] = i
	}
	r, err := CutRowNew(bs, colMap)
	c.Assert(err, IsNil)
	c.Assert(r, NotNil)
	c.Assert(r, HasLen, 3)
	// Compare cut row and original row
	for i := range colIDs {
		c.Assert(r[i], DeepEquals, data[i])
	}
	bs = []byte{codec.NilFlag}
	r, err = CutRowNew(bs, colMap)
	c.Assert(err, IsNil)
	c.Assert(r, IsNil)
	bs = nil
	r, err = CutRowNew(bs, colMap)
	c.Assert(err, IsNil)
	c.Assert(r, IsNil)
}

func (s *testTableCodecSuite) TestCutKeyNew(c *C) {
	values := []types.Datum{types.NewIntDatum(1), types.NewBytesDatum([]byte("abc")), types.NewFloat64Datum(5.5)}
	handle := types.NewIntDatum(100)
	values = append(values, handle)
	sc := &stmtctx.StatementContext{TimeZone: time.UTC}
	encodedValue, err := codec.EncodeKey(sc, nil, values...)
	c.Assert(err, IsNil)
	tableID := int64(4)
	indexID := int64(5)
	indexKey := EncodeIndexSeekKey(tableID, indexID, encodedValue)
	valuesBytes, handleBytes, err := CutIndexKeyNew(indexKey, 3)
	c.Assert(err, IsNil)
	for i := 0; i < 3; i++ {
		valueBytes := valuesBytes[i]
		var val types.Datum
		_, val, _ = codec.DecodeOne(valueBytes)
		c.Assert(val, DeepEquals, values[i])
	}
	_, handleVal, _ := codec.DecodeOne(handleBytes)
	c.Assert(handleVal, DeepEquals, types.NewIntDatum(100))
}

func (s *testTableCodecSuite) TestCutKey(c *C) {
	colIDs := []int64{1, 2, 3}
	values := []types.Datum{types.NewIntDatum(1), types.NewBytesDatum([]byte("abc")), types.NewFloat64Datum(5.5)}
	handle := types.NewIntDatum(100)
	values = append(values, handle)
	sc := &stmtctx.StatementContext{TimeZone: time.UTC}
	encodedValue, err := codec.EncodeKey(sc, nil, values...)
	c.Assert(err, IsNil)
	tableID := int64(4)
	indexID := int64(5)
	indexKey := EncodeIndexSeekKey(tableID, indexID, encodedValue)
	valuesMap, handleBytes, err := CutIndexKey(indexKey, colIDs)
	c.Assert(err, IsNil)
	for i, colID := range colIDs {
		valueBytes := valuesMap[colID]
		var val types.Datum
		_, val, _ = codec.DecodeOne(valueBytes)
		c.Assert(val, DeepEquals, values[i])
	}
	_, handleVal, _ := codec.DecodeOne(handleBytes)
	c.Assert(handleVal, DeepEquals, types.NewIntDatum(100))
}

func (s *testTableCodecSuite) TestDecodeBadDecical(c *C) {
	c.Assert(failpoint.Enable("github.com/pingcap/tidb/util/codec/errorInDecodeDecimal", `return(true)`), IsNil)
	defer func() {
		c.Assert(failpoint.Disable("github.com/pingcap/tidb/util/codec/errorInDecodeDecimal"), IsNil)
	}()
	dec := types.NewDecFromStringForTest("0.111")
	b, err := codec.EncodeDecimal(nil, dec, 0, 0)
	c.Assert(err, IsNil)
	// Expect no panic.
	_, _, err = codec.DecodeOne(b)
	c.Assert(err, NotNil)
}

func (s *testTableCodecSuite) TestIndexKey(c *C) {
	tableID := int64(4)
	indexID := int64(5)
	indexKey := EncodeIndexSeekKey(tableID, indexID, []byte{})
	tTableID, tIndexID, isRecordKey, err := DecodeKeyHead(indexKey)
	c.Assert(err, IsNil)
	c.Assert(tTableID, Equals, tableID)
	c.Assert(tIndexID, Equals, indexID)
	c.Assert(isRecordKey, IsFalse)
}

func (s *testTableCodecSuite) TestRecordKey(c *C) {
	tableID := int64(55)
	tableKey := EncodeRowKeyWithHandle(tableID, kv.IntHandle(math.MaxUint32))
	tTableID, _, isRecordKey, err := DecodeKeyHead(tableKey)
	c.Assert(err, IsNil)
	c.Assert(tTableID, Equals, tableID)
	c.Assert(isRecordKey, IsTrue)

	encodedHandle := codec.EncodeInt(nil, math.MaxUint32)
	rowKey := EncodeRowKey(tableID, encodedHandle)
	c.Assert([]byte(tableKey), BytesEquals, []byte(rowKey))
	tTableID, handle, err := DecodeRecordKey(rowKey)
	c.Assert(err, IsNil)
	c.Assert(tTableID, Equals, tableID)
	c.Assert(handle.IntValue(), Equals, int64(math.MaxUint32))

	recordPrefix := GenTableRecordPrefix(tableID)
	rowKey = EncodeRecordKey(recordPrefix, kv.IntHandle(math.MaxUint32))
	c.Assert([]byte(tableKey), BytesEquals, []byte(rowKey))

	_, _, err = DecodeRecordKey(nil)
	c.Assert(err, NotNil)
	_, _, err = DecodeRecordKey([]byte("abcdefghijklmnopqrstuvwxyz"))
	c.Assert(err, NotNil)
	c.Assert(DecodeTableID(nil), Equals, int64(0))
}

func (s *testTableCodecSuite) TestPrefix(c *C) {
	const tableID int64 = 66
	key := EncodeTablePrefix(tableID)
	tTableID := DecodeTableID(key)
	c.Assert(tTableID, Equals, tableID)

	c.Assert(TablePrefix(), BytesEquals, tablePrefix)

	tablePrefix1 := GenTablePrefix(tableID)
	c.Assert([]byte(tablePrefix1), BytesEquals, []byte(key))

	indexPrefix := EncodeTableIndexPrefix(tableID, math.MaxUint32)
	tTableID, indexID, isRecordKey, err := DecodeKeyHead(indexPrefix)
	c.Assert(err, IsNil)
	c.Assert(tTableID, Equals, tableID)
	c.Assert(indexID, Equals, int64(math.MaxUint32))
	c.Assert(isRecordKey, IsFalse)

	prefixKey := GenTableIndexPrefix(tableID)
	c.Assert(DecodeTableID(prefixKey), Equals, tableID)

	c.Assert(TruncateToRowKeyLen(append(indexPrefix, "xyz"...)), HasLen, RecordRowKeyLen)
	c.Assert(TruncateToRowKeyLen(key), HasLen, len(key))
}

func (s *testTableCodecSuite) TestDecodeIndexKey(c *C) {
	tableID := int64(4)
	indexID := int64(5)
	values := []types.Datum{
		types.NewIntDatum(1),
		types.NewBytesDatum([]byte("abc")),
		types.NewFloat64Datum(123.45),
		// MysqlTime is not supported.
		// types.NewTimeDatum(types.Time{
		// 	Time: types.FromGoTime(time.Now()),
		// 	Fsp:  6,
		// 	Type: mysql.TypeTimestamp,
		// }),
	}
	valueStrs := make([]string, 0, len(values))
	for _, v := range values {
		str, err := v.ToString()
		if err != nil {
			str = fmt.Sprintf("%d-%v", v.Kind(), v.GetValue())
		}
		valueStrs = append(valueStrs, str)
	}
	sc := &stmtctx.StatementContext{TimeZone: time.UTC}
	encodedValue, err := codec.EncodeKey(sc, nil, values...)
	c.Assert(err, IsNil)
	indexKey := EncodeIndexSeekKey(tableID, indexID, encodedValue)

	decodeTableID, decodeIndexID, decodeValues, err := DecodeIndexKey(indexKey)
	c.Assert(err, IsNil)
	c.Assert(decodeTableID, Equals, tableID)
	c.Assert(decodeIndexID, Equals, indexID)
	c.Assert(decodeValues, DeepEquals, valueStrs)
}

func (s *testTableCodecSuite) TestCutPrefix(c *C) {
	key := EncodeTableIndexPrefix(42, 666)
	res := CutRowKeyPrefix(key)
	c.Assert(res, BytesEquals, []byte{0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x2, 0x9a})
	res = CutIndexPrefix(key)
	c.Assert(res, BytesEquals, []byte{})
}

func (s *testTableCodecSuite) TestRange(c *C) {
	s1, e1 := GetTableHandleKeyRange(22)
	s2, e2 := GetTableHandleKeyRange(23)
	c.Assert(s1, Less, e1)
	c.Assert(e1, Less, s2)
	c.Assert(s2, Less, e2)

	s1, e1 = GetTableIndexKeyRange(42, 666)
	s2, e2 = GetTableIndexKeyRange(42, 667)
	c.Assert(s1, Less, e1)
	c.Assert(e1, Less, s2)
	c.Assert(s2, Less, e2)
}

func (s *testTableCodecSuite) TestDecodeAutoIDMeta(c *C) {
	keyBytes := []byte{0x6d, 0x44, 0x42, 0x3a, 0x35, 0x36, 0x0, 0x0, 0x0, 0xfc, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x68, 0x54, 0x49, 0x44, 0x3a, 0x31, 0x30, 0x38, 0x0, 0xfe}
	key, field, err := DecodeMetaKey(keyBytes)
	c.Assert(err, IsNil)
	c.Assert(string(key), Equals, "DB:56")
	c.Assert(string(field), Equals, "TID:108")
}

func BenchmarkHasTablePrefix(b *testing.B) {
	k := kv.Key("foobar")
	for i := 0; i < b.N; i++ {
		hasTablePrefix(k)
	}
}

func BenchmarkHasTablePrefixBuiltin(b *testing.B) {
	k := kv.Key("foobar")
	for i := 0; i < b.N; i++ {
		k.HasPrefix(tablePrefix)
	}
}

// Bench result:
// BenchmarkEncodeValue      5000000           368 ns/op
func BenchmarkEncodeValue(b *testing.B) {
	row := make([]types.Datum, 7)
	row[0] = types.NewIntDatum(100)
	row[1] = types.NewBytesDatum([]byte("abc"))
	row[2] = types.NewDecimalDatum(types.NewDecFromInt(1))
	row[3] = types.NewMysqlEnumDatum(types.Enum{Name: "a", Value: 0})
	row[4] = types.NewDatum(types.Set{Name: "a", Value: 0})
	row[5] = types.NewDatum(types.BinaryLiteral{100})
	row[6] = types.NewFloat32Datum(1.5)
	b.ResetTimer()
	encodedCol := make([]byte, 0, 16)
	for i := 0; i < b.N; i++ {
		for _, d := range row {
			encodedCol = encodedCol[:0]
			EncodeValue(nil, encodedCol, d)
		}
	}
}

func (s *testTableCodecSuite) TestError(c *C) {
	kvErrs := []*terror.Error{
		errInvalidKey,
		errInvalidRecordKey,
		errInvalidIndexKey,
	}
	for _, err := range kvErrs {
		code := err.ToSQLError().Code
		c.Assert(code != mysql.ErrUnknown && code == uint16(err.Code()), IsTrue, Commentf("err: %v", err))
	}
}
