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
	"math"
	"testing"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/codec"
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
	c.Assert(h, Equals, int64(2))

	key = EncodeRowKeyWithHandle(1, 2)
	h, err = DecodeRowKey(key)
	c.Assert(err, IsNil)
	c.Assert(h, Equals, int64(2))
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
	cols := []*column{c1, c2, c3}

	row := make([]types.Datum, 3)
	row[0] = types.NewIntDatum(100)
	row[1] = types.NewBytesDatum([]byte("abc"))
	row[2] = types.NewDecimalDatum(types.NewDecFromInt(1))
	// Encode
	colIDs := make([]int64, 0, 3)
	for _, col := range cols {
		colIDs = append(colIDs, col.id)
	}
	sc := &stmtctx.StatementContext{TimeZone: time.Local}
	bs, err := EncodeRow(sc, row, colIDs, nil, nil)
	c.Assert(err, IsNil)
	c.Assert(bs, NotNil)

	// Decode
	colMap := make(map[int64]*types.FieldType, 3)
	for _, col := range cols {
		colMap[col.id] = col.tp
	}
	r, err := DecodeRow(bs, colMap, time.Local)
	c.Assert(err, IsNil)
	c.Assert(r, NotNil)
	c.Assert(r, HasLen, 3)
	// Compare decoded row and original row
	for i, col := range cols {
		v, ok := r[col.id]
		c.Assert(ok, IsTrue)
		equal, err1 := v.CompareDatum(sc, &row[i])
		c.Assert(err1, IsNil)
		c.Assert(equal, Equals, 0)
	}

	// colMap may contains more columns than encoded row.
	colMap[4] = types.NewFieldType(mysql.TypeFloat)
	r, err = DecodeRow(bs, colMap, time.Local)
	c.Assert(err, IsNil)
	c.Assert(r, NotNil)
	c.Assert(r, HasLen, 3)
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
	r, err = DecodeRow(bs, colMap, time.Local)
	c.Assert(err, IsNil)
	c.Assert(r, NotNil)
	c.Assert(r, HasLen, 2)
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
	bs, err = EncodeRow(sc, []types.Datum{}, []int64{}, nil, nil)
	c.Assert(err, IsNil)
	c.Assert(bs, HasLen, 1)

	r, err = DecodeRow(bs, colMap, time.Local)
	c.Assert(err, IsNil)
	c.Assert(r, IsNil)
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
	ts, err := types.ParseTimestamp(nil, "2016-06-23 11:30:45")
	c.Assert(err, IsNil)
	row[2] = types.NewDatum(ts)
	du, err := types.ParseDuration("12:59:59.999999", 6)
	c.Assert(err, IsNil)
	row[3] = types.NewDatum(du)

	// Encode
	colIDs := make([]int64, 0, colLen)
	for _, col := range cols {
		colIDs = append(colIDs, col.id)
	}
	sc := &stmtctx.StatementContext{TimeZone: time.Local}
	bs, err := EncodeRow(sc, row, colIDs, nil, nil)
	c.Assert(err, IsNil)
	c.Assert(bs, NotNil)

	// Decode
	colMap := make(map[int64]*types.FieldType, colLen)
	for _, col := range cols {
		colMap[col.id] = col.tp
	}
	r, err := DecodeRow(bs, colMap, time.Local)
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

	sc := &stmtctx.StatementContext{TimeZone: time.Local}
	data := make([][]byte, 3)
	data[0], err = EncodeValue(sc, row[0])
	c.Assert(err, IsNil)
	data[1], err = EncodeValue(sc, row[1])
	c.Assert(err, IsNil)
	data[2], err = EncodeValue(sc, row[2])
	c.Assert(err, IsNil)
	// Encode
	colIDs := make([]int64, 0, 3)
	for _, col := range cols {
		colIDs = append(colIDs, col.id)
	}
	bs, err := EncodeRow(sc, row, colIDs, nil, nil)
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
}

func (s *testTableCodecSuite) TestCutKeyNew(c *C) {
	values := []types.Datum{types.NewIntDatum(1), types.NewBytesDatum([]byte("abc")), types.NewFloat64Datum(5.5)}
	handle := types.NewIntDatum(100)
	values = append(values, handle)
	sc := &stmtctx.StatementContext{TimeZone: time.Local}
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
	sc := &stmtctx.StatementContext{TimeZone: time.Local}
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
	tableKey := EncodeRowKeyWithHandle(tableID, math.MaxUint32)
	tTableID, _, isRecordKey, err := DecodeKeyHead(tableKey)
	c.Assert(err, IsNil)
	c.Assert(tTableID, Equals, tableID)
	c.Assert(isRecordKey, IsTrue)
}

func (s *testTableCodecSuite) TestReplaceRecordKeyTableID(c *C) {
	tableID := int64(1)
	tableKey := EncodeRowKeyWithHandle(tableID, 1)
	tTableID, _, _, err := DecodeKeyHead(tableKey)
	c.Assert(err, IsNil)
	c.Assert(tTableID, Equals, tableID)

	tableID = 2
	tableKey = ReplaceRecordKeyTableID(tableKey, tableID)
	tTableID, _, _, err = DecodeKeyHead(tableKey)
	c.Assert(err, IsNil)
	c.Assert(tTableID, Equals, tableID)

	tableID = 3
	ReplaceRecordKeyTableID(tableKey, tableID)
	tableKey = ReplaceRecordKeyTableID(tableKey, tableID)
	tTableID, _, _, err = DecodeKeyHead(tableKey)
	c.Assert(err, IsNil)
	c.Assert(tTableID, Equals, tableID)

	tableID = -1
	tableKey = ReplaceRecordKeyTableID(tableKey, tableID)
	tTableID, _, _, err = DecodeKeyHead(tableKey)
	c.Assert(err, IsNil)
	c.Assert(tTableID, Equals, tableID)
}
