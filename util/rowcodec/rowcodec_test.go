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

package rowcodec

import (
	"math"
	"testing"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/types/json"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/codec"
)

func TestT(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testSuite{})

type testSuite struct{}

func (s *testSuite) TestRowCodec(c *C) {
	colIDs := []int64{1, 2, 3, 4, 5, 10}
	rb := NewEncoder(colIDs, new(stmtctx.StatementContext))
	dt, err := types.ParseDatetime(rb.sc, "2018-01-19 03:14:07.999999")
	c.Assert(err, IsNil)
	datums := types.MakeDatums(
		dt,
		"abc",
		nil,
		1,
		types.NewDecFromInt(1),
		"abc",
	)
	newRow, err := rb.Encode(datums, nil)
	c.Check(err, IsNil)
	s.checkDecode(c, rb.sc, newRow)

	// Test large column ID
	rb.tempColIDs = []int64{1, 2, 3, 4, 5, 512}
	newRow, err = rb.Encode(datums, nil)
	c.Check(err, IsNil)
	s.checkDecode(c, rb.sc, newRow)

	// Test large column value
	rb.tempColIDs = []int64{1, 2, 3, 4, 5, 10}
	datums[5] = types.NewBytesDatum(make([]byte, 65536))
	newRow, err = rb.Encode(datums, nil)
	c.Check(err, IsNil)
	s.checkDecode(c, rb.sc, newRow)
}

func (s *testSuite) TestIntCodec(c *C) {
	uints := []uint64{255, math.MaxUint16, math.MaxUint32, math.MaxUint32 + 1}
	sizes := []int{1, 2, 4, 8}
	for i, v := range uints {
		data := encodeUint(nil, v)
		c.Assert(len(data), Equals, sizes[i])
		c.Assert(decodeUint(data), Equals, v)
	}

	ints := []int64{127, math.MaxInt16, math.MaxInt32, math.MaxInt32 + 1}
	for i, v := range ints {
		data := encodeInt(nil, v)
		c.Assert(len(data), Equals, sizes[i])
		c.Assert(decodeInt(data), Equals, v)
	}
}

func (s *testSuite) TestMoreTypes(c *C) {
	colIDs := []int64{1, 2, 3, 4, 5, 6, 7, 8}
	sc := new(stmtctx.StatementContext)
	sc.TimeZone = time.Local
	rb := NewEncoder(colIDs, sc)
	ts, err := types.ParseTimestampFromNum(rb.sc, 20181111090909)
	c.Assert(err, IsNil)
	datums := types.MakeDatums(
		float32(1.0),
		float64(1.0),
		ts,
		types.Duration{Duration: time.Minute},
		types.Enum{Name: "a", Value: 1},
		types.Set{Name: "a", Value: 1},
		json.CreateBinary("abc"),
		types.BitLiteral([]byte{101}),
	)
	newRow, err := rb.Encode(datums, nil)
	c.Check(err, IsNil)
	fieldTypes := []*types.FieldType{
		types.NewFieldType(mysql.TypeFloat),
		types.NewFieldType(mysql.TypeDouble),
		{Tp: mysql.TypeTimestamp, Decimal: 0},
		types.NewFieldType(mysql.TypeDuration),
		{Tp: mysql.TypeEnum, Elems: []string{"a"}},
		{Tp: mysql.TypeSet, Elems: []string{"a"}},
		types.NewFieldType(mysql.TypeJSON),
		{Tp: mysql.TypeBit, Flen: 8},
	}
	rd, err := NewDecoder(colIDs, 0, fieldTypes, make([][]byte, 8), rb.sc)
	c.Assert(err, IsNil)
	chk := chunk.NewChunkWithCapacity(fieldTypes, 1)
	err = rd.Decode(newRow, 0, chk)
	c.Assert(err, IsNil)
	row := chk.GetRow(0)
	c.Assert(row.GetFloat32(0), Equals, float32(1.0))
	c.Assert(row.GetFloat64(1), Equals, float64(1.0))
	c.Assert(row.GetTime(2).String(), Equals, ts.String())
	c.Assert(row.GetDuration(3, 0), Equals, datums[3].GetMysqlDuration())
	c.Assert(row.GetEnum(4), Equals, datums[4].GetMysqlEnum())
	c.Assert(row.GetSet(5), Equals, datums[5].GetMysqlSet())
	c.Assert(row.GetJSON(6), DeepEquals, datums[6].GetMysqlJSON())
	c.Assert(row.GetBytes(7), DeepEquals, []byte(datums[7].GetMysqlBit()))
}

func (s *testSuite) checkDecode(c *C, sc *stmtctx.StatementContext, newRow []byte) {
	readRowTypes := []*types.FieldType{
		types.NewFieldType(mysql.TypeVarString),
		types.NewFieldType(mysql.TypeLonglong),
		types.NewFieldType(mysql.TypeLonglong),
		types.NewFieldType(mysql.TypeNewDecimal),
		types.NewFieldType(mysql.TypeLonglong),
		types.NewFieldType(mysql.TypeLonglong),
	}
	readColIDS := []int64{2, 3, 4, 5, 6, 7}
	defaultColVal, err := codec.EncodeValue(sc, nil, types.NewIntDatum(5))
	c.Assert(err, IsNil)
	defaults := [][]byte{nil, defaultColVal, defaultColVal, nil, defaultColVal, nil}

	rd, err := NewDecoder(readColIDS, 7, readRowTypes, defaults, sc)
	c.Assert(err, IsNil)
	chk := chunk.NewChunkWithCapacity(readRowTypes, 1)
	err = rd.Decode(newRow, 1000, chk)
	c.Assert(err, IsNil)
	row := chk.GetRow(0)
	c.Assert(row.GetString(0), Equals, "abc")
	c.Assert(row.IsNull(1), IsTrue)
	c.Assert(row.GetInt64(2), Equals, int64(1))
	c.Assert(row.GetMyDecimal(3).String(), Equals, "1")
	c.Assert(row.GetInt64(4), Equals, int64(5))
	c.Assert(row.GetInt64(5), Equals, int64(1000))
}

func BenchmarkEncode(b *testing.B) {
	b.ReportAllocs()
	oldRow := types.MakeDatums(1, "abc", 1.1)
	xb := NewEncoder([]int64{1, 2, 3}, new(stmtctx.StatementContext))
	var buf []byte
	var err error
	for i := 0; i < b.N; i++ {
		buf = buf[:0]
		buf, err = xb.Encode(oldRow, buf)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDecode(b *testing.B) {
	b.ReportAllocs()
	oldRow := types.MakeDatums(1, "abc", 1.1)
	colIDs := []int64{-1, 2, 3}
	tps := []*types.FieldType{
		types.NewFieldType(mysql.TypeLonglong),
		types.NewFieldType(mysql.TypeString),
		types.NewFieldType(mysql.TypeDouble),
	}
	xb := NewEncoder(colIDs, new(stmtctx.StatementContext))
	xRowData, err := xb.Encode(oldRow, nil)
	if err != nil {
		b.Fatal(err)
	}
	decoder, err := NewDecoder(colIDs, -1, tps, make([][]byte, 3), xb.sc)
	if err != nil {
		b.Fatal(err)
	}
	chk := chunk.NewChunkWithCapacity(tps, 1)
	for i := 0; i < b.N; i++ {
		chk.Reset()
		err = decoder.Decode(xRowData, 1, chk)
		if err != nil {
			b.Fatal(err)
		}
	}
}
