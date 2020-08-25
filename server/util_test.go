// Copyright 2015 PingCAP, Inc.
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

package server

import (
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/errors"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/store/mockstore"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/types/json"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/mock"
	"github.com/pingcap/tidb/util/testleak"
)

var _ = Suite(&testUtilSuite{})

func newStoreWithBootstrap() (kv.Storage, *domain.Domain, error) {
	store, err := mockstore.NewMockStore()
	if err != nil {
		return nil, nil, errors.Trace(err)
	}
	session.SetSchemaLease(0)
	dom, err := session.BootstrapSession(store)
	return store, dom, errors.Trace(err)
}

type testUtilSuite struct {
	store kv.Storage
	dom   *domain.Domain
}

func (s *testUtilSuite) SetUpSuite(c *C) {
	testleak.BeforeTest()

	var err error
	s.store, s.dom, err = newStoreWithBootstrap()
	c.Assert(err, IsNil)
}

func (s *testUtilSuite) TearDownSuite(c *C) {
	s.dom.Close()
	s.store.Close()

	testleak.AfterTest(c)()
}

func (s *testUtilSuite) TestDumpBinaryTime(c *C) {
	t, err := types.ParseTimestamp(nil, "0000-00-00 00:00:00.000000")
	c.Assert(err, IsNil)
	d := dumpBinaryDateTime(nil, t)
	c.Assert(d, DeepEquals, []byte{0})

	t, err = types.ParseTimestamp(&stmtctx.StatementContext{TimeZone: time.Local}, "1991-05-01 01:01:01.100001")
	c.Assert(err, IsNil)
	d = dumpBinaryDateTime(nil, t)
	// 199 & 7 composed to uint16 1991 (litter-endian)
	// 160 & 134 & 1 & 0 composed to uint32 1000001 (litter-endian)
	c.Assert(d, DeepEquals, []byte{11, 199, 7, 5, 1, 1, 1, 1, 161, 134, 1, 0})

	t, err = types.ParseDatetime(nil, "0000-00-00 00:00:00.000000")
	c.Assert(err, IsNil)
	d = dumpBinaryDateTime(nil, t)
	c.Assert(d, DeepEquals, []byte{0})
	t, err = types.ParseDatetime(nil, "1993-07-13 01:01:01.000000")
	c.Assert(err, IsNil)
	d = dumpBinaryDateTime(nil, t)
	// 201 & 7 composed to uint16 1993 (litter-endian)
	c.Assert(d, DeepEquals, []byte{11, 201, 7, 7, 13, 1, 1, 1, 0, 0, 0, 0})

	t, err = types.ParseDate(nil, "0000-00-00")
	c.Assert(err, IsNil)
	d = dumpBinaryDateTime(nil, t)
	c.Assert(d, DeepEquals, []byte{0})
	t, err = types.ParseDate(nil, "1992-06-01")
	c.Assert(err, IsNil)
	d = dumpBinaryDateTime(nil, t)
	// 200 & 7 composed to uint16 1992 (litter-endian)
	c.Assert(d, DeepEquals, []byte{4, 200, 7, 6, 1})

	t, err = types.ParseDate(nil, "0000-00-00")
	c.Assert(err, IsNil)
	d = dumpBinaryDateTime(nil, t)
	c.Assert(d, DeepEquals, []byte{0})

	myDuration, err := types.ParseDuration(nil, "0000-00-00 00:00:00.000000", 6)
	c.Assert(err, IsNil)
	d = dumpBinaryTime(myDuration.Duration)
	c.Assert(d, DeepEquals, []byte{0})

	d = dumpBinaryTime(0)
	c.Assert(d, DeepEquals, []byte{0})

	d = dumpBinaryTime(-1)
	c.Assert(d, DeepEquals, []byte{12, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0})

	d = dumpBinaryTime(time.Nanosecond + 86400*1000*time.Microsecond)
	c.Assert(d, DeepEquals, []byte{12, 0, 0, 0, 0, 0, 0, 1, 26, 128, 26, 6, 0})
}

func (s *testUtilSuite) TestDumpTextValue(c *C) {
	columns := []*ColumnInfo{{
		Type:    mysql.TypeLonglong,
		Decimal: mysql.NotFixedDec,
	}}

	null := types.NewIntDatum(0)
	null.SetNull()
	bs, err := dumpTextRow(nil, columns, chunk.MutRowFromDatums([]types.Datum{null}).ToRow())
	c.Assert(err, IsNil)
	_, isNull, _, err := parseLengthEncodedBytes(bs)
	c.Assert(err, IsNil)
	c.Assert(isNull, IsTrue)

	bs, err = dumpTextRow(nil, columns, chunk.MutRowFromDatums([]types.Datum{types.NewIntDatum(10)}).ToRow())
	c.Assert(err, IsNil)
	c.Assert(mustDecodeStr(c, bs), Equals, "10")

	bs, err = dumpTextRow(nil, columns, chunk.MutRowFromDatums([]types.Datum{types.NewUintDatum(11)}).ToRow())
	c.Assert(err, IsNil)
	c.Assert(mustDecodeStr(c, bs), Equals, "11")

	columns[0].Flag = columns[0].Flag | uint16(mysql.UnsignedFlag)
	bs, err = dumpTextRow(nil, columns, chunk.MutRowFromDatums([]types.Datum{types.NewUintDatum(11)}).ToRow())
	c.Assert(err, IsNil)
	c.Assert(mustDecodeStr(c, bs), Equals, "11")

	columns[0].Type = mysql.TypeFloat
	columns[0].Decimal = 1
	f32 := types.NewFloat32Datum(1.2)
	bs, err = dumpTextRow(nil, columns, chunk.MutRowFromDatums([]types.Datum{f32}).ToRow())
	c.Assert(err, IsNil)
	c.Assert(mustDecodeStr(c, bs), Equals, "1.2")

	columns[0].Decimal = 2
	bs, err = dumpTextRow(nil, columns, chunk.MutRowFromDatums([]types.Datum{f32}).ToRow())
	c.Assert(err, IsNil)
	c.Assert(mustDecodeStr(c, bs), Equals, "1.20")

	f64 := types.NewFloat64Datum(2.2)
	columns[0].Type = mysql.TypeDouble
	columns[0].Decimal = 1
	bs, err = dumpTextRow(nil, columns, chunk.MutRowFromDatums([]types.Datum{f64}).ToRow())
	c.Assert(err, IsNil)
	c.Assert(mustDecodeStr(c, bs), Equals, "2.2")

	columns[0].Decimal = 2
	bs, err = dumpTextRow(nil, columns, chunk.MutRowFromDatums([]types.Datum{f64}).ToRow())
	c.Assert(err, IsNil)
	c.Assert(mustDecodeStr(c, bs), Equals, "2.20")

	columns[0].Type = mysql.TypeBlob
	bs, err = dumpTextRow(nil, columns, chunk.MutRowFromDatums([]types.Datum{types.NewBytesDatum([]byte("foo"))}).ToRow())
	c.Assert(err, IsNil)
	c.Assert(mustDecodeStr(c, bs), Equals, "foo")

	columns[0].Type = mysql.TypeVarchar
	bs, err = dumpTextRow(nil, columns, chunk.MutRowFromDatums([]types.Datum{types.NewStringDatum("bar")}).ToRow())
	c.Assert(err, IsNil)
	c.Assert(mustDecodeStr(c, bs), Equals, "bar")

	var d types.Datum

	sc := mock.NewContext().GetSessionVars().StmtCtx
	sc.IgnoreZeroInDate = true
	losAngelesTz, err := time.LoadLocation("America/Los_Angeles")
	c.Assert(err, IsNil)
	sc.TimeZone = losAngelesTz

	time, err := types.ParseTime(sc, "2017-01-05 23:59:59.575601", mysql.TypeDatetime, 0)
	c.Assert(err, IsNil)
	d.SetMysqlTime(time)
	columns[0].Type = mysql.TypeDatetime
	bs, err = dumpTextRow(nil, columns, chunk.MutRowFromDatums([]types.Datum{d}).ToRow())
	c.Assert(err, IsNil)
	c.Assert(mustDecodeStr(c, bs), Equals, "2017-01-06 00:00:00")

	duration, err := types.ParseDuration(sc, "11:30:45", 0)
	c.Assert(err, IsNil)
	d.SetMysqlDuration(duration)
	columns[0].Type = mysql.TypeDuration
	columns[0].Decimal = 0
	bs, err = dumpTextRow(nil, columns, chunk.MutRowFromDatums([]types.Datum{d}).ToRow())
	c.Assert(err, IsNil)
	c.Assert(mustDecodeStr(c, bs), Equals, "11:30:45")

	d.SetMysqlDecimal(types.NewDecFromStringForTest("1.23"))
	columns[0].Type = mysql.TypeNewDecimal
	bs, err = dumpTextRow(nil, columns, chunk.MutRowFromDatums([]types.Datum{d}).ToRow())
	c.Assert(err, IsNil)
	c.Assert(mustDecodeStr(c, bs), Equals, "1.23")

	year := types.NewIntDatum(0)
	columns[0].Type = mysql.TypeYear
	bs, err = dumpTextRow(nil, columns, chunk.MutRowFromDatums([]types.Datum{year}).ToRow())
	c.Assert(err, IsNil)
	c.Assert(mustDecodeStr(c, bs), Equals, "0000")

	year.SetInt64(1984)
	columns[0].Type = mysql.TypeYear
	bs, err = dumpTextRow(nil, columns, chunk.MutRowFromDatums([]types.Datum{year}).ToRow())
	c.Assert(err, IsNil)
	c.Assert(mustDecodeStr(c, bs), Equals, "1984")

	enum := types.NewMysqlEnumDatum(types.Enum{Name: "ename", Value: 0})
	columns[0].Type = mysql.TypeEnum
	bs, err = dumpTextRow(nil, columns, chunk.MutRowFromDatums([]types.Datum{enum}).ToRow())
	c.Assert(err, IsNil)
	c.Assert(mustDecodeStr(c, bs), Equals, "ename")

	set := types.Datum{}
	set.SetMysqlSet(types.Set{Name: "sname", Value: 0}, mysql.DefaultCollationName)
	columns[0].Type = mysql.TypeSet
	bs, err = dumpTextRow(nil, columns, chunk.MutRowFromDatums([]types.Datum{set}).ToRow())
	c.Assert(err, IsNil)
	c.Assert(mustDecodeStr(c, bs), Equals, "sname")

	js := types.Datum{}
	binaryJSON, err := json.ParseBinaryFromString(`{"a": 1, "b": 2}`)
	c.Assert(err, IsNil)
	js.SetMysqlJSON(binaryJSON)
	columns[0].Type = mysql.TypeJSON
	bs, err = dumpTextRow(nil, columns, chunk.MutRowFromDatums([]types.Datum{js}).ToRow())
	c.Assert(err, IsNil)
	c.Assert(mustDecodeStr(c, bs), Equals, `{"a": 1, "b": 2}`)
}

func mustDecodeStr(c *C, b []byte) string {
	str, _, _, err := parseLengthEncodedBytes(b)
	c.Assert(err, IsNil)
	return string(str)
}

func (s *testUtilSuite) TestAppendFormatFloat(c *C) {
	tests := []struct {
		fVal    float64
		out     string
		prec    int
		bitSize int
	}{
		{
			99999999999999999999,
			"1e20",
			-1,
			64,
		},
		{
			1e15,
			"1e15",
			-1,
			64,
		},
		{
			9e14,
			"900000000000000",
			-1,
			64,
		},
		{
			-9999999999999999,
			"-1e16",
			-1,
			64,
		},
		{
			999999999999999,
			"999999999999999",
			-1,
			64,
		},
		{
			0.000000000000001,
			"0.000000000000001",
			-1,
			64,
		},
		{
			0.0000000000000009,
			"9e-16",
			-1,
			64,
		},
		{
			-0.0000000000000009,
			"-9e-16",
			-1,
			64,
		},
		{
			0.11111,
			"0.111",
			3,
			64,
		},
		{
			0.11111,
			"0.111",
			3,
			64,
		},
		{
			0.1111111111111111111,
			"0.11111111",
			-1,
			32,
		},
		{
			0.1111111111111111111,
			"0.1111111111111111",
			-1,
			64,
		},
		{
			0.0000000000000009,
			"0.000",
			3,
			64,
		},
		{
			0,
			"0",
			-1,
			64,
		},
	}
	for _, t := range tests {
		c.Assert(string(appendFormatFloat(nil, t.fVal, t.prec, t.bitSize)), Equals, t.out)
	}
}

func (s *testUtilSuite) TestDumpLengthEncodedInt(c *C) {
	testCases := []struct {
		num    uint64
		buffer []byte
	}{
		{
			uint64(0),
			[]byte{0x00},
		},
		{
			uint64(513),
			[]byte{'\xfc', '\x01', '\x02'},
		},
		{
			uint64(197121),
			[]byte{'\xfd', '\x01', '\x02', '\x03'},
		},
		{
			uint64(578437695752307201),
			[]byte{'\xfe', '\x01', '\x02', '\x03', '\x04', '\x05', '\x06', '\x07', '\x08'},
		},
	}
	for _, tc := range testCases {
		b := dumpLengthEncodedInt(nil, tc.num)
		c.Assert(b, DeepEquals, tc.buffer)
	}
}

func (s *testUtilSuite) TestParseLengthEncodedInt(c *C) {
	testCases := []struct {
		buffer []byte
		num    uint64
		isNull bool
		n      int
	}{
		{
			[]byte{'\xfb'},
			uint64(0),
			true,
			1,
		},
		{
			[]byte{'\x00'},
			uint64(0),
			false,
			1,
		},
		{
			[]byte{'\xfc', '\x01', '\x02'},
			uint64(513),
			false,
			3,
		},
		{
			[]byte{'\xfd', '\x01', '\x02', '\x03'},
			uint64(197121),
			false,
			4,
		},
		{
			[]byte{'\xfe', '\x01', '\x02', '\x03', '\x04', '\x05', '\x06', '\x07', '\x08'},
			uint64(578437695752307201),
			false,
			9,
		},
	}

	for _, tc := range testCases {
		num, isNull, n := parseLengthEncodedInt(tc.buffer)
		c.Assert(num, Equals, tc.num)
		c.Assert(isNull, Equals, tc.isNull)
		c.Assert(n, Equals, tc.n)

		c.Assert(lengthEncodedIntSize(tc.num), Equals, tc.n)
	}
}

func (s *testUtilSuite) TestDumpUint(c *C) {
	testCases := []uint64{
		0,
		1,
		1<<64 - 1,
	}
	parseUint64 := func(b []byte) uint64 {
		return uint64(b[0]) | uint64(b[1])<<8 | uint64(b[2])<<16 |
			uint64(b[3])<<24 | uint64(b[4])<<32 | uint64(b[5])<<40 |
			uint64(b[6])<<48 | uint64(b[7])<<56
	}
	for _, tc := range testCases {
		b := dumpUint64(nil, tc)
		c.Assert(len(b), Equals, 8)
		c.Assert(parseUint64(b), Equals, tc)
	}
}

func (s *testUtilSuite) TestParseLengthEncodedBytes(c *C) {
	buffer := []byte{'\xfb'}
	b, isNull, n, err := parseLengthEncodedBytes(buffer)
	c.Assert(b, IsNil)
	c.Assert(isNull, IsTrue)
	c.Assert(n, Equals, 1)
	c.Assert(err, IsNil)

	buffer = []byte{0}
	b, isNull, n, err = parseLengthEncodedBytes(buffer)
	c.Assert(b, IsNil)
	c.Assert(isNull, IsFalse)
	c.Assert(n, Equals, 1)
	c.Assert(err, IsNil)

	buffer = []byte{'\x01'}
	b, isNull, n, err = parseLengthEncodedBytes(buffer)
	c.Assert(b, IsNil)
	c.Assert(isNull, IsFalse)
	c.Assert(n, Equals, 2)
	c.Assert(err.Error(), Equals, "EOF")
}

func (s *testUtilSuite) TestParseNullTermString(c *C) {
	for _, t := range []struct {
		input  string
		str    string
		remain string
	}{
		{
			"abc\x00def",
			"abc",
			"def",
		},
		{
			"\x00def",
			"",
			"def",
		},
		{
			"def\x00hig\x00k",
			"def",
			"hig\x00k",
		},
		{
			"abcdef",
			"",
			"abcdef",
		},
	} {
		str, remain := parseNullTermString([]byte(t.input))
		c.Assert(string(str), Equals, t.str)
		c.Assert(string(remain), Equals, t.remain)
	}
}

func newTestConfig() *config.Config {
	cfg := config.NewConfig()
	cfg.Host = "127.0.0.1"
	cfg.Status.StatusHost = "127.0.0.1"
	return cfg
}
