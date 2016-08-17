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
	"math/rand"
	"testing"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/testleak"
	"github.com/pingcap/tidb/util/types"
)

func TestT(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testTableCodecSuite{})

type testTableCodecSuite struct{}

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
	row[2] = types.NewDecimalDatum(mysql.NewDecimalFromInt(1, 1))
	// Encode
	colIDs := make([]int64, 0, 3)
	for _, col := range cols {
		colIDs = append(colIDs, col.id)
	}
	bs, err := EncodeRow(row, colIDs)
	c.Assert(err, IsNil)
	c.Assert(bs, NotNil)
	fmt.Println(len(bs))

	// Decode
	colMap := make(map[int64]*types.FieldType, 3)
	for _, col := range cols {
		colMap[col.id] = col.tp
	}
	r, err := DecodeRow(bs, colMap)
	c.Assert(err, IsNil)
	c.Assert(r, NotNil)
	c.Assert(r, HasLen, 3)
	// Compare decoded row and original row
	for i, col := range cols {
		v, ok := r[col.id]
		c.Assert(ok, IsTrue)
		equal, err1 := v.CompareDatum(row[i])
		c.Assert(err1, IsNil)
		c.Assert(equal, Equals, 0)
	}

	// colMap may contains more columns than encoded row.
	colMap[4] = types.NewFieldType(mysql.TypeFloat)
	r, err = DecodeRow(bs, colMap)
	c.Assert(err, IsNil)
	c.Assert(r, NotNil)
	c.Assert(r, HasLen, 3)
	for i, col := range cols {
		v, ok := r[col.id]
		c.Assert(ok, IsTrue)
		equal, err1 := v.CompareDatum(row[i])
		c.Assert(err1, IsNil)
		c.Assert(equal, Equals, 0)
	}

	// colMap may contains less columns than encoded row.
	delete(colMap, 3)
	delete(colMap, 4)
	r, err = DecodeRow(bs, colMap)
	c.Assert(err, IsNil)
	c.Assert(r, NotNil)
	c.Assert(r, HasLen, 2)
	for i, col := range cols {
		if i > 1 {
			break
		}
		v, ok := r[col.id]
		c.Assert(ok, IsTrue)
		equal, err1 := v.CompareDatum(row[i])
		c.Assert(err1, IsNil)
		c.Assert(equal, Equals, 0)
	}

	// Make sure empty row return not nil value.
	bs, err = EncodeRow([]types.Datum{}, []int64{})
	c.Assert(err, IsNil)
	c.Assert(bs, HasLen, 1)

	r, err = DecodeRow(bs, colMap)
	c.Assert(err, IsNil)
	c.Assert(r, IsNil)
}

func (s *testTableCodecSuite) TestTimeCodec(c *C) {
	defer testleak.AfterTest(c)()

	c1 := &column{id: 1, tp: types.NewFieldType(mysql.TypeLonglong)}
	c2 := &column{id: 2, tp: types.NewFieldType(mysql.TypeVarchar)}
	c3 := &column{id: 3, tp: types.NewFieldType(mysql.TypeTimestamp)}
	cols := []*column{c1, c2, c3}

	row := make([]types.Datum, 3)
	row[0] = types.NewIntDatum(100)
	row[1] = types.NewBytesDatum([]byte("abc"))
	ts, err := mysql.ParseTimestamp("2016-06-23 11:30:45")
	c.Assert(err, IsNil)
	row[2] = types.NewDatum(ts)
	// Encode
	colIDs := make([]int64, 0, 3)
	for _, col := range cols {
		colIDs = append(colIDs, col.id)
	}
	bs, err := EncodeRow(row, colIDs)
	c.Assert(err, IsNil)
	c.Assert(bs, NotNil)

	// Decode
	colMap := make(map[int64]*types.FieldType, 3)
	for _, col := range cols {
		colMap[col.id] = col.tp
	}
	r, err := DecodeRow(bs, colMap)
	c.Assert(err, IsNil)
	c.Assert(r, NotNil)
	c.Assert(r, HasLen, 3)
	// Compare decoded row and original row
	for i, col := range cols {
		v, ok := r[col.id]
		c.Assert(ok, IsTrue)
		equal, err1 := v.CompareDatum(row[i])
		c.Assert(err1, IsNil)
		c.Assert(equal, Equals, 0)
	}
}

func (s *testTableCodecSuite) TestCutKey(c *C) {
	colIDs := []int64{1, 2, 3}
	values := []types.Datum{types.NewIntDatum(1), types.NewBytesDatum([]byte("abc")), types.NewFloat64Datum(5.5)}
	handle := types.NewIntDatum(100)
	values = append(values, handle)
	encodedValue, err := codec.EncodeKey(nil, values...)
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

func benchmarkRowCodec(b *testing.B,
	encode func([]types.Datum, []int64) ([]byte, error),
	decode func([]byte, map[int64]*types.FieldType) (map[int64]types.Datum, error),
	fullRow bool) {
	c1 := &column{id: 1, tp: types.NewFieldType(mysql.TypeLonglong)}
	c2 := &column{id: 2, tp: types.NewFieldType(mysql.TypeVarchar)}
	c3 := &column{id: 3, tp: types.NewFieldType(mysql.TypeTimestamp)}
	randcols := []*column{c1, c2, c3}

	randrow := make([]types.Datum, 3)
	randrow[0] = types.NewIntDatum(100)
	randrow[1] = types.NewBytesDatum([]byte("abc"))
	ts, _ := mysql.ParseTimestamp("2016-06-23 11:30:45")
	randrow[2] = types.NewDatum(ts)

	cols := [30]column{}
	row := [30]types.Datum{}
	for i := 0; i < 30; i++ {
		r := rand.Intn(3)
		cols[i].id = int64(i) + 1
		cols[i].tp = randcols[r].tp
		row[i] = randrow[r]
	}

	// Encode
	colIDs := make([]int64, 0, 30)
	for _, col := range cols {
		colIDs = append(colIDs, col.id)
	}
	xx, _ := encode(row[:], colIDs)

	var colMap map[int64]*types.FieldType
	if fullRow {
		colMap = make(map[int64]*types.FieldType, 30)
		for i := 0; i < 30; i++ {
			colMap[cols[i].id] = cols[i].tp
		}
	} else {
		colMap = make(map[int64]*types.FieldType, 3)
		colMap[4] = cols[3].tp
		colMap[7] = cols[6].tp
		colMap[23] = cols[22].tp
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := decode(xx, colMap)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func Benchmark0(b *testing.B) {
	benchmarkRowCodec(b, EncodeRow, DecodeRow, false)
}

func Benchmark1(b *testing.B) {
	benchmarkRowCodec(b, EncodeRow1, DecodeRow1, false)
}

func Benchmark2(b *testing.B) {
	benchmarkRowCodec(b, EncodeRow2, DecodeRow2, false)
}

func Benchmark3(b *testing.B) {
	benchmarkRowCodec(b, EncodeRow3, DecodeRow3, false)
}

func Benchmark00(b *testing.B) {
	benchmarkRowCodec(b, EncodeRow, DecodeRow, true)
}

func Benchmark11(b *testing.B) {
	benchmarkRowCodec(b, EncodeRow1, DecodeRow1, true)
}

func Benchmark22(b *testing.B) {
	benchmarkRowCodec(b, EncodeRow2, DecodeRow2, true)
}

func Benchmark33(b *testing.B) {
	benchmarkRowCodec(b, EncodeRow3, DecodeRow3, true)
}
