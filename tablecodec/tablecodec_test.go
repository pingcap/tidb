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
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/parser/terror"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/collate"
	"github.com/pingcap/tidb/util/rowcodec"
	"github.com/stretchr/testify/require"
)

// TestTableCodec  tests some functions in package tablecodec
// TODO: add more tests.
func TestTableCodec(t *testing.T) {
	key := EncodeRowKey(1, codec.EncodeInt(nil, 2))
	h, err := DecodeRowKey(key)
	require.NoError(t, err)
	require.Equal(t, int64(2), h.IntValue())

	key = EncodeRowKeyWithHandle(1, kv.IntHandle(2))
	h, err = DecodeRowKey(key)
	require.NoError(t, err)
	require.Equal(t, int64(2), h.IntValue())
}

// https://github.com/pingcap/tidb/issues/27687.
func TestTableCodecInvalid(t *testing.T) {
	tableID := int64(100)
	buf := make([]byte, 0, 11)
	buf = append(buf, 't')
	buf = codec.EncodeInt(buf, tableID)
	buf = append(buf, '_', 'r')
	buf = codec.EncodeInt(buf, -9078412423848787968)
	buf = append(buf, '0')
	_, err := DecodeRowKey(buf)
	require.NotNil(t, err)
	require.Equal(t, "invalid encoded key", err.Error())
}

// column is a structure used for test
type column struct {
	id int64
	tp *types.FieldType
}

func TestRowCodec(t *testing.T) {
	c1 := &column{id: 1, tp: types.NewFieldType(mysql.TypeLonglong)}
	c2 := &column{id: 2, tp: types.NewFieldType(mysql.TypeVarchar)}
	c3 := &column{id: 3, tp: types.NewFieldType(mysql.TypeNewDecimal)}
	c4tp := &types.FieldType{}
	c4tp.SetType(mysql.TypeEnum)
	c4tp.SetElems([]string{"a"})
	c4 := &column{id: 4, tp: c4tp}
	c5tp := &types.FieldType{}
	c5tp.SetType(mysql.TypeSet)
	c5tp.SetElems([]string{"a"})
	c5 := &column{id: 5, tp: c5tp}
	c6tp := &types.FieldType{}
	c6tp.SetType(mysql.TypeBit)
	c6tp.SetFlen(8)
	c6 := &column{id: 6, tp: c6tp}
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
	require.NoError(t, err)
	require.NotNil(t, bs)

	// Decode
	colMap := make(map[int64]*types.FieldType, len(row))
	for _, col := range cols {
		colMap[col.id] = col.tp
	}
	r, err := DecodeRowToDatumMap(bs, colMap, time.UTC)
	require.NoError(t, err)
	require.NotNil(t, r)
	require.Len(t, r, len(row))
	// Compare decoded row and original row
	for i, col := range cols {
		v, ok := r[col.id]
		require.True(t, ok)
		equal, err1 := v.Compare(sc, &row[i], collate.GetBinaryCollator())
		require.NoError(t, err1)
		require.Equalf(t, 0, equal, "expect: %v, got %v", row[i], v)
	}

	// colMap may contains more columns than encoded row.
	// colMap[4] = types.NewFieldType(mysql.TypeFloat)
	r, err = DecodeRowToDatumMap(bs, colMap, time.UTC)
	require.NoError(t, err)
	require.NotNil(t, r)
	require.Len(t, r, len(row))
	for i, col := range cols {
		v, ok := r[col.id]
		require.True(t, ok)
		equal, err1 := v.Compare(sc, &row[i], collate.GetBinaryCollator())
		require.NoError(t, err1)
		require.Equal(t, 0, equal)
	}

	// colMap may contains less columns than encoded row.
	delete(colMap, 3)
	delete(colMap, 4)
	r, err = DecodeRowToDatumMap(bs, colMap, time.UTC)
	require.NoError(t, err)
	require.NotNil(t, r)
	require.Len(t, r, len(row)-2)
	for i, col := range cols {
		if i > 1 {
			break
		}
		v, ok := r[col.id]
		require.True(t, ok)
		equal, err1 := v.Compare(sc, &row[i], collate.GetBinaryCollator())
		require.NoError(t, err1)
		require.Equal(t, 0, equal)
	}

	// Make sure empty row return not nil value.
	bs, err = EncodeOldRow(sc, []types.Datum{}, []int64{}, nil, nil)
	require.NoError(t, err)
	require.Len(t, bs, 1)

	r, err = DecodeRowToDatumMap(bs, colMap, time.UTC)
	require.NoError(t, err)
	require.Len(t, r, 0)
}

func TestDecodeColumnValue(t *testing.T) {
	sc := &stmtctx.StatementContext{TimeZone: time.Local}

	// test timestamp
	d := types.NewTimeDatum(types.NewTime(types.FromGoTime(time.Now()), mysql.TypeTimestamp, types.DefaultFsp))
	bs, err := EncodeOldRow(sc, []types.Datum{d}, []int64{1}, nil, nil)
	require.NoError(t, err)
	require.NotNil(t, bs)
	_, bs, err = codec.CutOne(bs) // ignore colID
	require.NoError(t, err)
	tp := types.NewFieldType(mysql.TypeTimestamp)
	d1, err := DecodeColumnValue(bs, tp, sc.TimeZone)
	require.NoError(t, err)
	cmp, err := d1.Compare(sc, &d, collate.GetBinaryCollator())
	require.NoError(t, err)
	require.Equal(t, 0, cmp)

	// test set
	elems := []string{"a", "b", "c", "d", "e"}
	e, _ := types.ParseSetValue(elems, uint64(1))
	d = types.NewMysqlSetDatum(e, "")
	bs, err = EncodeOldRow(sc, []types.Datum{d}, []int64{1}, nil, nil)
	require.NoError(t, err)
	require.NotNil(t, bs)
	_, bs, err = codec.CutOne(bs) // ignore colID
	require.NoError(t, err)
	tp = types.NewFieldType(mysql.TypeSet)
	tp.SetElems(elems)
	d1, err = DecodeColumnValue(bs, tp, sc.TimeZone)
	require.NoError(t, err)
	cmp, err = d1.Compare(sc, &d, collate.GetCollator(tp.GetCollate()))
	require.NoError(t, err)
	require.Equal(t, 0, cmp)

	// test bit
	d = types.NewMysqlBitDatum(types.NewBinaryLiteralFromUint(3223600, 3))
	bs, err = EncodeOldRow(sc, []types.Datum{d}, []int64{1}, nil, nil)
	require.NoError(t, err)
	require.NotNil(t, bs)
	_, bs, err = codec.CutOne(bs) // ignore colID
	require.NoError(t, err)
	tp = types.NewFieldType(mysql.TypeBit)
	tp.SetFlen(24)
	d1, err = DecodeColumnValue(bs, tp, sc.TimeZone)
	require.NoError(t, err)
	cmp, err = d1.Compare(sc, &d, collate.GetBinaryCollator())
	require.NoError(t, err)
	require.Equal(t, 0, cmp)

	// test empty enum
	d = types.NewMysqlEnumDatum(types.Enum{})
	bs, err = EncodeOldRow(sc, []types.Datum{d}, []int64{1}, nil, nil)
	require.NoError(t, err)
	require.NotNil(t, bs)
	_, bs, err = codec.CutOne(bs) // ignore colID
	require.NoError(t, err)
	tp = types.NewFieldType(mysql.TypeEnum)
	d1, err = DecodeColumnValue(bs, tp, sc.TimeZone)
	require.NoError(t, err)
	cmp, err = d1.Compare(sc, &d, collate.GetCollator(tp.GetCollate()))
	require.NoError(t, err)
	require.Equal(t, 0, cmp)
}

func TestUnflattenDatums(t *testing.T) {
	sc := &stmtctx.StatementContext{TimeZone: time.UTC}
	input := types.MakeDatums(int64(1))
	tps := []*types.FieldType{types.NewFieldType(mysql.TypeLonglong)}
	output, err := UnflattenDatums(input, tps, sc.TimeZone)
	require.NoError(t, err)
	cmp, err := input[0].Compare(sc, &output[0], collate.GetBinaryCollator())
	require.NoError(t, err)
	require.Equal(t, 0, cmp)

	input = []types.Datum{types.NewCollationStringDatum("aaa", "utf8mb4_unicode_ci")}
	tps = []*types.FieldType{types.NewFieldType(mysql.TypeBlob)}
	tps[0].SetCollate("utf8mb4_unicode_ci")
	output, err = UnflattenDatums(input, tps, sc.TimeZone)
	require.NoError(t, err)
	cmp, err = input[0].Compare(sc, &output[0], collate.GetBinaryCollator())
	require.NoError(t, err)
	require.Equal(t, 0, cmp)
	require.Equal(t, "utf8mb4_unicode_ci", output[0].Collation())
}

func TestTimeCodec(t *testing.T) {
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
	require.NoError(t, err)
	row[2] = types.NewDatum(ts)
	du, err := types.ParseDuration(nil, "12:59:59.999999", 6)
	require.NoError(t, err)
	row[3] = types.NewDatum(du)

	// Encode
	colIDs := make([]int64, 0, colLen)
	for _, col := range cols {
		colIDs = append(colIDs, col.id)
	}
	rd := rowcodec.Encoder{Enable: true}
	sc := &stmtctx.StatementContext{TimeZone: time.UTC}
	bs, err := EncodeRow(sc, row, colIDs, nil, nil, &rd)
	require.NoError(t, err)
	require.NotNil(t, bs)

	// Decode
	colMap := make(map[int64]*types.FieldType, colLen)
	for _, col := range cols {
		colMap[col.id] = col.tp
	}
	r, err := DecodeRowToDatumMap(bs, colMap, time.UTC)
	require.NoError(t, err)
	require.NotNil(t, r)
	require.Len(t, r, colLen)
	// Compare decoded row and original row
	for i, col := range cols {
		v, ok := r[col.id]
		require.True(t, ok)
		equal, err1 := v.Compare(sc, &row[i], collate.GetBinaryCollator())
		require.Nil(t, err1)
		require.Equal(t, 0, equal)
	}
}

func TestCutRow(t *testing.T) {
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
	require.NoError(t, err)
	data[1], err = EncodeValue(sc, nil, row[1])
	require.NoError(t, err)
	data[2], err = EncodeValue(sc, nil, row[2])
	require.NoError(t, err)
	// Encode
	colIDs := make([]int64, 0, 3)
	for _, col := range cols {
		colIDs = append(colIDs, col.id)
	}
	bs, err := EncodeOldRow(sc, row, colIDs, nil, nil)
	require.NoError(t, err)
	require.NotNil(t, bs)

	// Decode
	colMap := make(map[int64]int, 3)
	for i, col := range cols {
		colMap[col.id] = i
	}
	r, err := CutRowNew(bs, colMap)
	require.NoError(t, err)
	require.NotNil(t, r)
	require.Len(t, r, 3)
	// Compare cut row and original row
	for i := range colIDs {
		require.Equal(t, data[i], r[i])
	}
	bs = []byte{codec.NilFlag}
	r, err = CutRowNew(bs, colMap)
	require.NoError(t, err)
	require.Nil(t, r)
	bs = nil
	r, err = CutRowNew(bs, colMap)
	require.NoError(t, err)
	require.Nil(t, r)
}

func TestCutKeyNew(t *testing.T) {
	values := []types.Datum{types.NewIntDatum(1), types.NewBytesDatum([]byte("abc")), types.NewFloat64Datum(5.5)}
	handle := types.NewIntDatum(100)
	values = append(values, handle)
	sc := &stmtctx.StatementContext{TimeZone: time.UTC}
	encodedValue, err := codec.EncodeKey(sc, nil, values...)
	require.NoError(t, err)
	tableID := int64(4)
	indexID := int64(5)
	indexKey := EncodeIndexSeekKey(tableID, indexID, encodedValue)
	valuesBytes, handleBytes, err := CutIndexKeyNew(indexKey, 3)
	require.NoError(t, err)
	for i := 0; i < 3; i++ {
		valueBytes := valuesBytes[i]
		var val types.Datum
		_, val, _ = codec.DecodeOne(valueBytes)
		require.Equal(t, values[i], val)
	}
	_, handleVal, _ := codec.DecodeOne(handleBytes)
	require.Equal(t, types.NewIntDatum(100), handleVal)
}

func TestCutKey(t *testing.T) {
	colIDs := []int64{1, 2, 3}
	values := []types.Datum{types.NewIntDatum(1), types.NewBytesDatum([]byte("abc")), types.NewFloat64Datum(5.5)}
	handle := types.NewIntDatum(100)
	values = append(values, handle)
	sc := &stmtctx.StatementContext{TimeZone: time.UTC}
	encodedValue, err := codec.EncodeKey(sc, nil, values...)
	require.NoError(t, err)
	tableID := int64(4)
	indexID := int64(5)
	indexKey := EncodeIndexSeekKey(tableID, indexID, encodedValue)
	valuesMap, handleBytes, err := CutIndexKey(indexKey, colIDs)
	require.NoError(t, err)
	for i, colID := range colIDs {
		valueBytes := valuesMap[colID]
		var val types.Datum
		_, val, _ = codec.DecodeOne(valueBytes)
		require.Equal(t, values[i], val)
	}
	_, handleVal, _ := codec.DecodeOne(handleBytes)
	require.Equal(t, types.NewIntDatum(100), handleVal)
}

func TestDecodeBadDecical(t *testing.T) {
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/util/codec/errorInDecodeDecimal", `return(true)`))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/util/codec/errorInDecodeDecimal"))
	}()
	dec := types.NewDecFromStringForTest("0.111")
	b, err := codec.EncodeDecimal(nil, dec, 0, 0)
	require.NoError(t, err)
	// Expect no panic.
	_, _, err = codec.DecodeOne(b)
	require.Error(t, err)
}

func TestIndexKey(t *testing.T) {
	tableID := int64(4)
	indexID := int64(5)
	indexKey := EncodeIndexSeekKey(tableID, indexID, []byte{})
	tTableID, tIndexID, isRecordKey, err := DecodeKeyHead(indexKey)
	require.NoError(t, err)
	require.Equal(t, tableID, tTableID)
	require.Equal(t, indexID, tIndexID)
	require.False(t, isRecordKey)
}

func TestRecordKey(t *testing.T) {
	tableID := int64(55)
	tableKey := EncodeRowKeyWithHandle(tableID, kv.IntHandle(math.MaxUint32))
	tTableID, _, isRecordKey, err := DecodeKeyHead(tableKey)
	require.NoError(t, err)
	require.Equal(t, tableID, tTableID)
	require.True(t, isRecordKey)

	encodedHandle := codec.EncodeInt(nil, math.MaxUint32)
	rowKey := EncodeRowKey(tableID, encodedHandle)
	require.Equal(t, []byte(rowKey), []byte(tableKey))
	tTableID, handle, err := DecodeRecordKey(rowKey)
	require.NoError(t, err)
	require.Equal(t, tableID, tTableID)
	require.Equal(t, int64(math.MaxUint32), handle.IntValue())

	recordPrefix := GenTableRecordPrefix(tableID)
	rowKey = EncodeRecordKey(recordPrefix, kv.IntHandle(math.MaxUint32))
	require.Equal(t, []byte(rowKey), []byte(tableKey))

	_, _, err = DecodeRecordKey(nil)
	require.Error(t, err)
	_, _, err = DecodeRecordKey([]byte("abcdefghijklmnopqrstuvwxyz"))
	require.Error(t, err)
	require.Equal(t, int64(0), DecodeTableID(nil))
}

func TestPrefix(t *testing.T) {
	const tableID int64 = 66
	key := EncodeTablePrefix(tableID)
	tTableID := DecodeTableID(key)
	require.Equal(t, tableID, tTableID)

	require.Equal(t, tablePrefix, TablePrefix())

	tablePrefix1 := GenTablePrefix(tableID)
	require.Equal(t, []byte(key), []byte(tablePrefix1))

	indexPrefix := EncodeTableIndexPrefix(tableID, math.MaxUint32)
	tTableID, indexID, isRecordKey, err := DecodeKeyHead(indexPrefix)
	require.NoError(t, err)
	require.Equal(t, tableID, tTableID)
	require.Equal(t, int64(math.MaxUint32), indexID)
	require.False(t, isRecordKey)

	prefixKey := GenTableIndexPrefix(tableID)
	require.Equal(t, tableID, DecodeTableID(prefixKey))

	require.Len(t, TruncateToRowKeyLen(append(indexPrefix, "xyz"...)), RecordRowKeyLen)
	require.Len(t, TruncateToRowKeyLen(key), len(key))
}

func TestDecodeIndexKey(t *testing.T) {
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
	require.NoError(t, err)
	indexKey := EncodeIndexSeekKey(tableID, indexID, encodedValue)

	decodeTableID, decodeIndexID, decodeValues, err := DecodeIndexKey(indexKey)
	require.NoError(t, err)
	require.Equal(t, tableID, decodeTableID)
	require.Equal(t, indexID, decodeIndexID)
	require.Equal(t, valueStrs, decodeValues)
}

func TestCutPrefix(t *testing.T) {
	key := EncodeTableIndexPrefix(42, 666)
	res := CutRowKeyPrefix(key)
	require.Equal(t, []byte{0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x2, 0x9a}, res)
	res = CutIndexPrefix(key)
	require.Equal(t, []byte{}, res)
}

func TestRange(t *testing.T) {
	s1, e1 := GetTableHandleKeyRange(22)
	s2, e2 := GetTableHandleKeyRange(23)
	require.Less(t, string(s1), string(e1))
	require.Less(t, string(e1), string(s2))
	require.Less(t, string(s2), string(e2))

	s1, e1 = GetTableIndexKeyRange(42, 666)
	s2, e2 = GetTableIndexKeyRange(42, 667)
	require.Less(t, string(s1), string(e1))
	require.Less(t, string(e1), string(s2))
	require.Less(t, string(s2), string(e2))
}

func TestDecodeAutoIDMeta(t *testing.T) {
	keyBytes := []byte{0x6d, 0x44, 0x42, 0x3a, 0x35, 0x36, 0x0, 0x0, 0x0, 0xfc, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x68, 0x54, 0x49, 0x44, 0x3a, 0x31, 0x30, 0x38, 0x0, 0xfe}
	key, field, err := DecodeMetaKey(keyBytes)
	require.NoError(t, err)
	require.Equal(t, "DB:56", string(key))
	require.Equal(t, "TID:108", string(field))
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
			_, err := EncodeValue(nil, encodedCol, d)
			if err != nil {
				b.Fatal(err)
			}
		}
	}
}

func TestError(t *testing.T) {
	kvErrs := []*terror.Error{
		errInvalidKey,
		errInvalidRecordKey,
		errInvalidIndexKey,
	}
	for _, err := range kvErrs {
		code := terror.ToSQLError(err).Code
		require.NotEqual(t, code, mysql.ErrUnknown)
		require.Equal(t, code, uint16(err.Code()))
	}
}

func TestUntouchedIndexKValue(t *testing.T) {
	untouchedIndexKey := []byte("t00000001_i000000001")
	untouchedIndexValue := []byte{0, 0, 0, 0, 0, 0, 0, 1, 49}
	require.True(t, IsUntouchedIndexKValue(untouchedIndexKey, untouchedIndexValue))
}
