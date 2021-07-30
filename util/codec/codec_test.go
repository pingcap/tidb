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

package codec

import (
	"bytes"
	"math"
	"testing"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/types/json"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/testleak"
)

func TestT(t *testing.T) {
	CustomVerboseFlag = true
	TestingT(t)
}

var _ = Suite(&testCodecSuite{})

type testCodecSuite struct {
}

func (s *testCodecSuite) TestCodecKey(c *C) {
	defer testleak.AfterTest(c)()
	table := []struct {
		Input  []types.Datum
		Expect []types.Datum
	}{
		{
			types.MakeDatums(int64(1)),
			types.MakeDatums(int64(1)),
		},

		{
			types.MakeDatums(float32(1), float64(3.15), []byte("123"), "123"),
			types.MakeDatums(float64(1), float64(3.15), []byte("123"), []byte("123")),
		},
		{
			types.MakeDatums(uint64(1), float64(3.15), []byte("123"), int64(-1)),
			types.MakeDatums(uint64(1), float64(3.15), []byte("123"), int64(-1)),
		},

		{
			types.MakeDatums(true, false),
			types.MakeDatums(int64(1), int64(0)),
		},

		{
			types.MakeDatums(nil),
			types.MakeDatums(nil),
		},

		{
			types.MakeDatums(types.NewBinaryLiteralFromUint(100, -1), types.NewBinaryLiteralFromUint(100, 4)),
			types.MakeDatums(uint64(100), uint64(100)),
		},

		{
			types.MakeDatums(types.Enum{Name: "a", Value: 1}, types.Set{Name: "a", Value: 1}),
			types.MakeDatums(uint64(1), uint64(1)),
		},
	}
	sc := &stmtctx.StatementContext{TimeZone: time.Local}
	for i, t := range table {
		comment := Commentf("%d %v", i, t)
		b, err := EncodeKey(sc, nil, t.Input...)
		c.Assert(err, IsNil, comment)
		args, err := Decode(b, 1)
		c.Assert(err, IsNil)
		c.Assert(args, DeepEquals, t.Expect)

		b, err = EncodeValue(sc, nil, t.Input...)
		c.Assert(err, IsNil)
		args, err = Decode(b, 1)
		c.Assert(err, IsNil)
		c.Assert(args, DeepEquals, t.Expect)
	}
}

func (s *testCodecSuite) TestCodecKeyCompare(c *C) {
	defer testleak.AfterTest(c)()
	table := []struct {
		Left   []types.Datum
		Right  []types.Datum
		Expect int
	}{
		{
			types.MakeDatums(1),
			types.MakeDatums(1),
			0,
		},
		{
			types.MakeDatums(-1),
			types.MakeDatums(1),
			-1,
		},
		{
			types.MakeDatums(3.15),
			types.MakeDatums(3.12),
			1,
		},
		{
			types.MakeDatums("abc"),
			types.MakeDatums("abcd"),
			-1,
		},
		{
			types.MakeDatums("abcdefgh"),
			types.MakeDatums("abcdefghi"),
			-1,
		},
		{
			types.MakeDatums(1, "abc"),
			types.MakeDatums(1, "abcd"),
			-1,
		},
		{
			types.MakeDatums(1, "abc", "def"),
			types.MakeDatums(1, "abcd", "af"),
			-1,
		},
		{
			types.MakeDatums(3.12, "ebc", "def"),
			types.MakeDatums(2.12, "abcd", "af"),
			1,
		},
		{
			types.MakeDatums([]byte{0x01, 0x00}, []byte{0xFF}),
			types.MakeDatums([]byte{0x01, 0x00, 0xFF}),
			-1,
		},
		{
			types.MakeDatums([]byte{0x01}, uint64(0xFFFFFFFFFFFFFFF)),
			types.MakeDatums([]byte{0x01, 0x10}, 0),
			-1,
		},
		{
			types.MakeDatums(0),
			types.MakeDatums(nil),
			1,
		},
		{
			types.MakeDatums([]byte{0x00}),
			types.MakeDatums(nil),
			1,
		},
		{
			types.MakeDatums(math.SmallestNonzeroFloat64),
			types.MakeDatums(nil),
			1,
		},
		{
			types.MakeDatums(int64(math.MinInt64)),
			types.MakeDatums(nil),
			1,
		},
		{
			types.MakeDatums(1, int64(math.MinInt64), nil),
			types.MakeDatums(1, nil, uint64(math.MaxUint64)),
			1,
		},
		{
			types.MakeDatums(1, []byte{}, nil),
			types.MakeDatums(1, nil, 123),
			1,
		},
		{
			types.MakeDatums(parseTime(c, "2011-11-11 00:00:00"), 1),
			types.MakeDatums(parseTime(c, "2011-11-11 00:00:00"), 0),
			1,
		},
		{
			types.MakeDatums(parseDuration(c, "00:00:00"), 1),
			types.MakeDatums(parseDuration(c, "00:00:01"), 0),
			-1,
		},
		{
			[]types.Datum{types.MinNotNullDatum()},
			[]types.Datum{types.MaxValueDatum()},
			-1,
		},
	}
	sc := &stmtctx.StatementContext{TimeZone: time.Local}
	for _, t := range table {
		b1, err := EncodeKey(sc, nil, t.Left...)
		c.Assert(err, IsNil)

		b2, err := EncodeKey(sc, nil, t.Right...)
		c.Assert(err, IsNil)

		c.Assert(bytes.Compare(b1, b2), Equals, t.Expect, Commentf("%v - %v - %v - %v - %v", t.Left, t.Right, b1, b2, t.Expect))
	}
}

func (s *testCodecSuite) TestNumberCodec(c *C) {
	defer testleak.AfterTest(c)()
	tblInt64 := []int64{
		math.MinInt64,
		math.MinInt32,
		math.MinInt16,
		math.MinInt8,
		0,
		math.MaxInt8,
		math.MaxInt16,
		math.MaxInt32,
		math.MaxInt64,
		1<<47 - 1,
		-1 << 47,
		1<<23 - 1,
		-1 << 23,
		1<<55 - 1,
		-1 << 55,
		1,
		-1,
	}

	for _, t := range tblInt64 {
		b := EncodeInt(nil, t)
		_, v, err := DecodeInt(b)
		c.Assert(err, IsNil)
		c.Assert(v, Equals, t)

		b = EncodeIntDesc(nil, t)
		_, v, err = DecodeIntDesc(b)
		c.Assert(err, IsNil)
		c.Assert(v, Equals, t)

		b = EncodeVarint(nil, t)
		_, v, err = DecodeVarint(b)
		c.Assert(err, IsNil)
		c.Assert(v, Equals, t)

		b = EncodeComparableVarint(nil, t)
		_, v, err = DecodeComparableVarint(b)
		c.Assert(err, IsNil)
		c.Assert(v, Equals, t)
	}

	tblUint64 := []uint64{
		0,
		math.MaxUint8,
		math.MaxUint16,
		math.MaxUint32,
		math.MaxUint64,
		1<<24 - 1,
		1<<48 - 1,
		1<<56 - 1,
		1,
		math.MaxInt16,
		math.MaxInt8,
		math.MaxInt32,
		math.MaxInt64,
	}

	for _, t := range tblUint64 {
		b := EncodeUint(nil, t)
		_, v, err := DecodeUint(b)
		c.Assert(err, IsNil)
		c.Assert(v, Equals, t)

		b = EncodeUintDesc(nil, t)
		_, v, err = DecodeUintDesc(b)
		c.Assert(err, IsNil)
		c.Assert(v, Equals, t)

		b = EncodeUvarint(nil, t)
		_, v, err = DecodeUvarint(b)
		c.Assert(err, IsNil)
		c.Assert(v, Equals, t)

		b = EncodeComparableUvarint(nil, t)
		_, v, err = DecodeComparableUvarint(b)
		c.Assert(err, IsNil)
		c.Assert(v, Equals, t)
	}
	var b []byte
	b = EncodeComparableVarint(b, -1)
	b = EncodeComparableUvarint(b, 1)
	b = EncodeComparableVarint(b, 2)
	b, i, err := DecodeComparableVarint(b)
	c.Assert(err, IsNil)
	c.Assert(i, Equals, int64(-1))
	b, u, err := DecodeComparableUvarint(b)
	c.Assert(err, IsNil)
	c.Assert(u, Equals, uint64(1))
	_, i, err = DecodeComparableVarint(b)
	c.Assert(err, IsNil)
	c.Assert(i, Equals, int64(2))
}

func (s *testCodecSuite) TestNumberOrder(c *C) {
	defer testleak.AfterTest(c)()
	tblInt64 := []struct {
		Arg1 int64
		Arg2 int64
		Ret  int
	}{
		{-1, 1, -1},
		{math.MaxInt64, math.MinInt64, 1},
		{math.MaxInt64, math.MaxInt32, 1},
		{math.MinInt32, math.MaxInt16, -1},
		{math.MinInt64, math.MaxInt8, -1},
		{0, math.MaxInt8, -1},
		{math.MinInt8, 0, -1},
		{math.MinInt16, math.MaxInt16, -1},
		{1, -1, 1},
		{1, 0, 1},
		{-1, 0, -1},
		{0, 0, 0},
		{math.MaxInt16, math.MaxInt16, 0},
	}

	for _, t := range tblInt64 {
		b1 := EncodeInt(nil, t.Arg1)
		b2 := EncodeInt(nil, t.Arg2)

		ret := bytes.Compare(b1, b2)
		c.Assert(ret, Equals, t.Ret)

		b1 = EncodeIntDesc(nil, t.Arg1)
		b2 = EncodeIntDesc(nil, t.Arg2)

		ret = bytes.Compare(b1, b2)
		c.Assert(ret, Equals, -t.Ret)

		b1 = EncodeComparableVarint(nil, t.Arg1)
		b2 = EncodeComparableVarint(nil, t.Arg2)
		ret = bytes.Compare(b1, b2)
		c.Assert(ret, Equals, t.Ret)
	}

	tblUint64 := []struct {
		Arg1 uint64
		Arg2 uint64
		Ret  int
	}{
		{0, 0, 0},
		{1, 0, 1},
		{0, 1, -1},
		{math.MaxInt8, math.MaxInt16, -1},
		{math.MaxUint32, math.MaxInt32, 1},
		{math.MaxUint8, math.MaxInt8, 1},
		{math.MaxUint16, math.MaxInt32, -1},
		{math.MaxUint64, math.MaxInt64, 1},
		{math.MaxInt64, math.MaxUint32, 1},
		{math.MaxUint64, 0, 1},
		{0, math.MaxUint64, -1},
	}

	for _, t := range tblUint64 {
		b1 := EncodeUint(nil, t.Arg1)
		b2 := EncodeUint(nil, t.Arg2)

		ret := bytes.Compare(b1, b2)
		c.Assert(ret, Equals, t.Ret)

		b1 = EncodeUintDesc(nil, t.Arg1)
		b2 = EncodeUintDesc(nil, t.Arg2)

		ret = bytes.Compare(b1, b2)
		c.Assert(ret, Equals, -t.Ret)

		b1 = EncodeComparableUvarint(nil, t.Arg1)
		b2 = EncodeComparableUvarint(nil, t.Arg2)
		ret = bytes.Compare(b1, b2)
		c.Assert(ret, Equals, t.Ret)
	}
}

func (s *testCodecSuite) TestFloatCodec(c *C) {
	defer testleak.AfterTest(c)()
	tblFloat := []float64{
		-1,
		0,
		1,
		math.MaxFloat64,
		math.MaxFloat32,
		math.SmallestNonzeroFloat32,
		math.SmallestNonzeroFloat64,
		math.Inf(-1),
		math.Inf(1),
	}

	for _, t := range tblFloat {
		b := EncodeFloat(nil, t)
		_, v, err := DecodeFloat(b)
		c.Assert(err, IsNil)
		c.Assert(v, Equals, t)

		b = EncodeFloatDesc(nil, t)
		_, v, err = DecodeFloatDesc(b)
		c.Assert(err, IsNil)
		c.Assert(v, Equals, t)
	}

	tblCmp := []struct {
		Arg1 float64
		Arg2 float64
		Ret  int
	}{
		{1, -1, 1},
		{1, 0, 1},
		{0, -1, 1},
		{0, 0, 0},
		{math.MaxFloat64, 1, 1},
		{math.MaxFloat32, math.MaxFloat64, -1},
		{math.MaxFloat64, 0, 1},
		{math.MaxFloat64, math.SmallestNonzeroFloat64, 1},
		{math.Inf(-1), 0, -1},
		{math.Inf(1), 0, 1},
		{math.Inf(-1), math.Inf(1), -1},
	}

	for _, t := range tblCmp {
		b1 := EncodeFloat(nil, t.Arg1)
		b2 := EncodeFloat(nil, t.Arg2)

		ret := bytes.Compare(b1, b2)
		c.Assert(ret, Equals, t.Ret)

		b1 = EncodeFloatDesc(nil, t.Arg1)
		b2 = EncodeFloatDesc(nil, t.Arg2)

		ret = bytes.Compare(b1, b2)
		c.Assert(ret, Equals, -t.Ret)
	}
}

func (s *testCodecSuite) TestBytes(c *C) {
	defer testleak.AfterTest(c)()
	tblBytes := [][]byte{
		{},
		{0x00, 0x01},
		{0xff, 0xff},
		{0x01, 0x00},
		[]byte("abc"),
		[]byte("hello world"),
	}

	for _, t := range tblBytes {
		b := EncodeBytes(nil, t)
		_, v, err := DecodeBytes(b, nil)
		c.Assert(err, IsNil)
		c.Assert(t, DeepEquals, v, Commentf("%v - %v - %v", t, b, v))

		b = EncodeBytesDesc(nil, t)
		_, v, err = DecodeBytesDesc(b, nil)
		c.Assert(err, IsNil)
		c.Assert(t, DeepEquals, v, Commentf("%v - %v - %v", t, b, v))

		b = EncodeCompactBytes(nil, t)
		_, v, err = DecodeCompactBytes(b)
		c.Assert(err, IsNil)
		c.Assert(t, DeepEquals, v, Commentf("%v - %v - %v", t, b, v))
	}

	tblCmp := []struct {
		Arg1 []byte
		Arg2 []byte
		Ret  int
	}{
		{[]byte{}, []byte{0x00}, -1},
		{[]byte{0x00}, []byte{0x00}, 0},
		{[]byte{0xFF}, []byte{0x00}, 1},
		{[]byte{0xFF}, []byte{0xFF, 0x00}, -1},
		{[]byte("a"), []byte("b"), -1},
		{[]byte("a"), []byte{0x00}, 1},
		{[]byte{0x00}, []byte{0x01}, -1},
		{[]byte{0x00, 0x01}, []byte{0x00, 0x00}, 1},
		{[]byte{0x00, 0x00, 0x00}, []byte{0x00, 0x00}, 1},
		{[]byte{0x00, 0x00, 0x00}, []byte{0x00, 0x00}, 1},
		{[]byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}, []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}, -1},
		{[]byte{0x01, 0x02, 0x03, 0x00}, []byte{0x01, 0x02, 0x03}, 1},
		{[]byte{0x01, 0x03, 0x03, 0x04}, []byte{0x01, 0x03, 0x03, 0x05}, -1},
		{[]byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07}, []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08}, -1},
		{[]byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09}, []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08}, 1},
		{[]byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x00}, []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08}, 1},
	}

	for _, t := range tblCmp {
		b1 := EncodeBytes(nil, t.Arg1)
		b2 := EncodeBytes(nil, t.Arg2)

		ret := bytes.Compare(b1, b2)
		c.Assert(ret, Equals, t.Ret)

		b1 = EncodeBytesDesc(nil, t.Arg1)
		b2 = EncodeBytesDesc(nil, t.Arg2)

		ret = bytes.Compare(b1, b2)
		c.Assert(ret, Equals, -t.Ret)
	}
}

func parseTime(c *C, s string) types.Time {
	m, err := types.ParseTime(nil, s, mysql.TypeDatetime, types.DefaultFsp)
	c.Assert(err, IsNil)
	return m
}

func parseDuration(c *C, s string) types.Duration {
	m, err := types.ParseDuration(nil, s, types.DefaultFsp)
	c.Assert(err, IsNil)
	return m
}

func (s *testCodecSuite) TestTime(c *C) {
	defer testleak.AfterTest(c)()
	tbl := []string{
		"2011-01-01 00:00:00",
		"2011-01-01 00:00:00",
		"0001-01-01 00:00:00",
	}
	sc := &stmtctx.StatementContext{TimeZone: time.Local}
	for _, t := range tbl {
		m := types.NewDatum(parseTime(c, t))

		b, err := EncodeKey(sc, nil, m)
		c.Assert(err, IsNil)
		v, err := Decode(b, 1)
		c.Assert(err, IsNil)
		var t types.Time
		t.Type = mysql.TypeDatetime
		t.FromPackedUint(v[0].GetUint64())
		c.Assert(types.NewDatum(t), DeepEquals, m)
	}

	tblCmp := []struct {
		Arg1 string
		Arg2 string
		Ret  int
	}{
		{"2011-10-10 00:00:00", "2000-12-12 11:11:11", 1},
		{"2000-10-10 00:00:00", "2001-10-10 00:00:00", -1},
		{"2000-10-10 00:00:00", "2000-10-10 00:00:00", 0},
	}

	for _, t := range tblCmp {
		m1 := types.NewDatum(parseTime(c, t.Arg1))
		m2 := types.NewDatum(parseTime(c, t.Arg2))

		b1, err := EncodeKey(sc, nil, m1)
		c.Assert(err, IsNil)
		b2, err := EncodeKey(sc, nil, m2)
		c.Assert(err, IsNil)

		ret := bytes.Compare(b1, b2)
		c.Assert(ret, Equals, t.Ret)
	}
}

func (s *testCodecSuite) TestDuration(c *C) {
	defer testleak.AfterTest(c)()
	tbl := []string{
		"11:11:11",
		"00:00:00",
		"1 11:11:11",
	}
	sc := &stmtctx.StatementContext{TimeZone: time.Local}
	for _, t := range tbl {
		m := parseDuration(c, t)

		b, err := EncodeKey(sc, nil, types.NewDatum(m))
		c.Assert(err, IsNil)
		v, err := Decode(b, 1)
		c.Assert(err, IsNil)
		m.Fsp = types.MaxFsp
		c.Assert(v, DeepEquals, types.MakeDatums(m))
	}

	tblCmp := []struct {
		Arg1 string
		Arg2 string
		Ret  int
	}{
		{"20:00:00", "11:11:11", 1},
		{"00:00:00", "00:00:01", -1},
		{"00:00:00", "00:00:00", 0},
	}

	for _, t := range tblCmp {
		m1 := parseDuration(c, t.Arg1)
		m2 := parseDuration(c, t.Arg2)

		b1, err := EncodeKey(sc, nil, types.NewDatum(m1))
		c.Assert(err, IsNil)
		b2, err := EncodeKey(sc, nil, types.NewDatum(m2))
		c.Assert(err, IsNil)

		ret := bytes.Compare(b1, b2)
		c.Assert(ret, Equals, t.Ret)
	}
}

func (s *testCodecSuite) TestDecimal(c *C) {
	defer testleak.AfterTest(c)()
	tbl := []string{
		"1234.00",
		"1234",
		"12.34",
		"12.340",
		"0.1234",
		"0.0",
		"0",
		"-0.0",
		"-0.0000",
		"-1234.00",
		"-1234",
		"-12.34",
		"-12.340",
		"-0.1234",
	}
	sc := &stmtctx.StatementContext{TimeZone: time.Local}
	for _, t := range tbl {
		dec := new(types.MyDecimal)
		err := dec.FromString([]byte(t))
		c.Assert(err, IsNil)
		b, err := EncodeKey(sc, nil, types.NewDatum(dec))
		c.Assert(err, IsNil)
		v, err := Decode(b, 1)
		c.Assert(err, IsNil)
		c.Assert(v, HasLen, 1)
		vv := v[0].GetMysqlDecimal()
		c.Assert(vv.Compare(dec), Equals, 0)
	}

	tblCmp := []struct {
		Arg1 interface{}
		Arg2 interface{}
		Ret  int
	}{
		// Test for float type decimal.
		{"1234", "123400", -1},
		{"12340", "123400", -1},
		{"1234", "1234.5", -1},
		{"1234", "1234.0000", 0},
		{"1234", "12.34", 1},
		{"12.34", "12.35", -1},
		{"0.12", "0.1234", -1},
		{"0.1234", "12.3400", -1},
		{"0.1234", "0.1235", -1},
		{"0.123400", "12.34", -1},
		{"12.34000", "12.34", 0},
		{"0.01234", "0.01235", -1},
		{"0.1234", "0", 1},
		{"0.0000", "0", 0},
		{"0.0001", "0", 1},
		{"0.0001", "0.0000", 1},
		{"0", "-0.0000", 0},
		{"-0.0001", "0", -1},
		{"-0.1234", "0", -1},
		{"-0.1234", "-0.12", -1},
		{"-0.12", "-0.1234", 1},
		{"-0.12", "-0.1200", 0},
		{"-0.1234", "0.1234", -1},
		{"-1.234", "-12.34", 1},
		{"-0.1234", "-12.34", 1},
		{"-12.34", "1234", -1},
		{"-12.34", "-12.35", 1},
		{"-0.01234", "-0.01235", 1},
		{"-1234", "-123400", 1},
		{"-12340", "-123400", 1},

		// Test for int type decimal.
		{int64(-1), int64(1), -1},
		{int64(math.MaxInt64), int64(math.MinInt64), 1},
		{int64(math.MaxInt64), int64(math.MaxInt32), 1},
		{int64(math.MinInt32), int64(math.MaxInt16), -1},
		{int64(math.MinInt64), int64(math.MaxInt8), -1},
		{int64(0), int64(math.MaxInt8), -1},
		{int64(math.MinInt8), int64(0), -1},
		{int64(math.MinInt16), int64(math.MaxInt16), -1},
		{int64(1), int64(-1), 1},
		{int64(1), int64(0), 1},
		{int64(-1), int64(0), -1},
		{int64(0), int64(0), 0},
		{int64(math.MaxInt16), int64(math.MaxInt16), 0},

		// Test for uint type decimal.
		{uint64(0), uint64(0), 0},
		{uint64(1), uint64(0), 1},
		{uint64(0), uint64(1), -1},
		{uint64(math.MaxInt8), uint64(math.MaxInt16), -1},
		{uint64(math.MaxUint32), uint64(math.MaxInt32), 1},
		{uint64(math.MaxUint8), uint64(math.MaxInt8), 1},
		{uint64(math.MaxUint16), uint64(math.MaxInt32), -1},
		{uint64(math.MaxUint64), uint64(math.MaxInt64), 1},
		{uint64(math.MaxInt64), uint64(math.MaxUint32), 1},
		{uint64(math.MaxUint64), uint64(0), 1},
		{uint64(0), uint64(math.MaxUint64), -1},
	}
	for _, t := range tblCmp {
		d1 := types.NewDatum(t.Arg1)
		dec1, err := d1.ToDecimal(sc)
		c.Assert(err, IsNil)
		d1.SetMysqlDecimal(dec1)
		d2 := types.NewDatum(t.Arg2)
		dec2, err := d2.ToDecimal(sc)
		c.Assert(err, IsNil)
		d2.SetMysqlDecimal(dec2)

		d1.SetLength(30)
		d1.SetFrac(6)
		d2.SetLength(30)
		d2.SetFrac(6)
		b1, err := EncodeKey(sc, nil, d1)
		c.Assert(err, IsNil)
		b2, err := EncodeKey(sc, nil, d2)
		c.Assert(err, IsNil)

		ret := bytes.Compare(b1, b2)
		c.Assert(ret, Equals, t.Ret, Commentf("%v %x %x", t, b1, b2))
	}

	floats := []float64{-123.45, -123.40, -23.45, -1.43, -0.93, -0.4333, -0.068,
		-0.0099, 0, 0.001, 0.0012, 0.12, 1.2, 1.23, 123.3, 2424.242424}
	decs := make([][]byte, 0, len(floats))
	for i := range floats {
		dec := types.NewDecFromFloatForTest(floats[i])
		var d types.Datum
		d.SetLength(20)
		d.SetFrac(6)
		d.SetMysqlDecimal(dec)
		b, err := EncodeDecimal(nil, d.GetMysqlDecimal(), d.Length(), d.Frac())
		c.Assert(err, IsNil)
		decs = append(decs, b)
	}
	for i := 0; i < len(decs)-1; i++ {
		cmp := bytes.Compare(decs[i], decs[i+1])
		c.Assert(cmp, LessEqual, 0)
	}

	d := types.NewDecFromStringForTest("-123.123456789")
	_, err := EncodeDecimal(nil, d, 20, 5)
	c.Assert(terror.ErrorEqual(err, types.ErrTruncated), IsTrue, Commentf("err %v", err))
	_, err = EncodeDecimal(nil, d, 12, 10)
	c.Assert(terror.ErrorEqual(err, types.ErrOverflow), IsTrue, Commentf("err %v", err))

	sc.IgnoreTruncate = true
	decimalDatum := types.NewDatum(d)
	decimalDatum.SetLength(20)
	decimalDatum.SetFrac(5)
	_, err = EncodeValue(sc, nil, decimalDatum)
	c.Assert(err, IsNil)

	sc.OverflowAsWarning = true
	decimalDatum.SetLength(12)
	decimalDatum.SetFrac(10)
	_, err = EncodeValue(sc, nil, decimalDatum)
	c.Assert(err, IsNil)
}

func (s *testCodecSuite) TestJSON(c *C) {
	defer testleak.AfterTest(c)()
	tbl := []string{
		"1234.00",
		`{"a": "b"}`,
	}

	datums := make([]types.Datum, 0, len(tbl))
	for _, t := range tbl {
		var d types.Datum
		j, err := json.ParseBinaryFromString(t)
		c.Assert(err, IsNil)
		d.SetMysqlJSON(j)
		datums = append(datums, d)
	}

	buf := make([]byte, 0, 4096)
	buf, err := encode(nil, buf, datums, false, false)
	c.Assert(err, IsNil)

	datums1, err := Decode(buf, 2)
	c.Assert(err, IsNil)

	for i := range datums1 {
		lhs := datums[i].GetMysqlJSON().String()
		rhs := datums1[i].GetMysqlJSON().String()
		c.Assert(lhs, Equals, rhs)
	}
}

func (s *testCodecSuite) TestCut(c *C) {
	defer testleak.AfterTest(c)()
	table := []struct {
		Input  []types.Datum
		Expect []types.Datum
	}{
		{
			types.MakeDatums(int64(1)),
			types.MakeDatums(int64(1)),
		},

		{
			types.MakeDatums(float32(1), float64(3.15), []byte("123"), "123"),
			types.MakeDatums(float64(1), float64(3.15), []byte("123"), []byte("123")),
		},
		{
			types.MakeDatums(uint64(1), float64(3.15), []byte("123"), int64(-1)),
			types.MakeDatums(uint64(1), float64(3.15), []byte("123"), int64(-1)),
		},

		{
			types.MakeDatums(true, false),
			types.MakeDatums(int64(1), int64(0)),
		},

		{
			types.MakeDatums(nil),
			types.MakeDatums(nil),
		},

		{
			types.MakeDatums(types.NewBinaryLiteralFromUint(100, -1), types.NewBinaryLiteralFromUint(100, 4)),
			types.MakeDatums(uint64(100), uint64(100)),
		},

		{
			types.MakeDatums(types.Enum{Name: "a", Value: 1}, types.Set{Name: "a", Value: 1}),
			types.MakeDatums(uint64(1), uint64(1)),
		},
		{
			types.MakeDatums(float32(1), float64(3.15), []byte("123456789012345")),
			types.MakeDatums(float64(1), float64(3.15), []byte("123456789012345")),
		},
		{
			types.MakeDatums(types.NewDecFromInt(0), types.NewDecFromFloatForTest(-1.3)),
			types.MakeDatums(types.NewDecFromInt(0), types.NewDecFromFloatForTest(-1.3)),
		},
	}
	sc := &stmtctx.StatementContext{TimeZone: time.Local}
	for i, t := range table {
		comment := Commentf("%d %v", i, t)
		b, err := EncodeKey(sc, nil, t.Input...)
		c.Assert(err, IsNil, comment)
		var d []byte
		for j, e := range t.Expect {
			d, b, err = CutOne(b)
			c.Assert(err, IsNil)
			c.Assert(d, NotNil)
			ed, err1 := EncodeKey(sc, nil, e)
			c.Assert(err1, IsNil)
			c.Assert(d, DeepEquals, ed, Commentf("%d:%d %#v", i, j, e))
		}
		c.Assert(b, HasLen, 0)
	}
	for i, t := range table {
		comment := Commentf("%d %v", i, t)
		b, err := EncodeValue(sc, nil, t.Input...)
		c.Assert(err, IsNil, comment)
		var d []byte
		for j, e := range t.Expect {
			d, b, err = CutOne(b)
			c.Assert(err, IsNil)
			c.Assert(d, NotNil)
			ed, err1 := EncodeValue(sc, nil, e)
			c.Assert(err1, IsNil)
			c.Assert(d, DeepEquals, ed, Commentf("%d:%d %#v", i, j, e))
		}
		c.Assert(b, HasLen, 0)
	}

	b, err := EncodeValue(sc, nil, types.NewDatum(42))
	c.Assert(err, IsNil)
	rem, n, err := CutColumnID(b)
	c.Assert(err, IsNil)
	c.Assert(rem, HasLen, 0)
	c.Assert(n, Equals, int64(42))
}

func (s *testCodecSuite) TestSetRawValues(c *C) {
	sc := &stmtctx.StatementContext{TimeZone: time.Local}
	datums := types.MakeDatums(1, "abc", 1.1, []byte("def"))
	rowData, err := EncodeValue(sc, nil, datums...)
	c.Assert(err, IsNil)
	values := make([]types.Datum, 4)
	err = SetRawValues(rowData, values)
	c.Assert(err, IsNil)
	for i, rawVal := range values {
		c.Assert(rawVal.Kind(), Equals, types.KindRaw)
		encoded, err1 := EncodeValue(sc, nil, datums[i])
		c.Assert(err1, IsNil)
		c.Assert(encoded, BytesEquals, rawVal.GetBytes())
	}
}

func (s *testCodecSuite) TestDecodeOneToChunk(c *C) {
	defer testleak.AfterTest(c)()
	sc := &stmtctx.StatementContext{TimeZone: time.Local}
	datums, tps := datumsForTest(sc)
	rowCount := 3
	chk := chunkForTest(c, sc, datums, tps, rowCount)
	for colIdx, tp := range tps {
		for rowIdx := 0; rowIdx < rowCount; rowIdx++ {
			got := chk.GetRow(rowIdx).GetDatum(colIdx, tp)
			expect := datums[colIdx]
			if got.IsNull() {
				c.Assert(expect.IsNull(), IsTrue)
			} else {
				cmp, err := got.CompareDatum(sc, &expect)
				c.Assert(err, IsNil)
				c.Assert(cmp, Equals, 0)
			}
		}
	}
}

func datumsForTest(sc *stmtctx.StatementContext) ([]types.Datum, []*types.FieldType) {
	table := []struct {
		value interface{}
		tp    *types.FieldType
	}{
		{nil, types.NewFieldType(mysql.TypeLonglong)},
		{int64(1), types.NewFieldType(mysql.TypeTiny)},
		{int64(1), types.NewFieldType(mysql.TypeShort)},
		{int64(1), types.NewFieldType(mysql.TypeInt24)},
		{int64(1), types.NewFieldType(mysql.TypeLong)},
		{int64(-1), types.NewFieldType(mysql.TypeLong)},
		{int64(1), types.NewFieldType(mysql.TypeLonglong)},
		{uint64(1), types.NewFieldType(mysql.TypeLonglong)},
		{float32(1), types.NewFieldType(mysql.TypeFloat)},
		{float64(1), types.NewFieldType(mysql.TypeDouble)},
		{types.NewDecFromInt(1), types.NewFieldType(mysql.TypeNewDecimal)},
		{"abc", types.NewFieldType(mysql.TypeString)},
		{"def", types.NewFieldType(mysql.TypeVarchar)},
		{"ghi", types.NewFieldType(mysql.TypeVarString)},
		{[]byte("abc"), types.NewFieldType(mysql.TypeBlob)},
		{[]byte("abc"), types.NewFieldType(mysql.TypeTinyBlob)},
		{[]byte("abc"), types.NewFieldType(mysql.TypeMediumBlob)},
		{[]byte("abc"), types.NewFieldType(mysql.TypeLongBlob)},
		{types.CurrentTime(mysql.TypeDatetime), types.NewFieldType(mysql.TypeDatetime)},
		{types.CurrentTime(mysql.TypeDate), types.NewFieldType(mysql.TypeDate)},
		{types.Time{
			Time: types.FromGoTime(time.Now()),
			Type: mysql.TypeTimestamp,
		}, types.NewFieldType(mysql.TypeTimestamp)},
		{types.Duration{Duration: time.Second, Fsp: 1}, types.NewFieldType(mysql.TypeDuration)},
		{types.Enum{Name: "a", Value: 1}, &types.FieldType{Tp: mysql.TypeEnum, Elems: []string{"a"}}},
		{types.Set{Name: "a", Value: 1}, &types.FieldType{Tp: mysql.TypeSet, Elems: []string{"a"}}},
		{types.BinaryLiteral{100}, &types.FieldType{Tp: mysql.TypeBit, Flen: 8}},
		{json.CreateBinary("abc"), types.NewFieldType(mysql.TypeJSON)},
		{int64(1), types.NewFieldType(mysql.TypeYear)},
	}

	datums := make([]types.Datum, 0, len(table)+2)
	tps := make([]*types.FieldType, 0, len(table)+2)
	for _, t := range table {
		tps = append(tps, t.tp)
		datums = append(datums, types.NewDatum(t.value))
	}
	return datums, tps
}

func chunkForTest(c *C, sc *stmtctx.StatementContext, datums []types.Datum, tps []*types.FieldType, rowCount int) *chunk.Chunk {
	decoder := NewDecoder(chunk.New(tps, 32, 32), sc.TimeZone)
	for rowIdx := 0; rowIdx < rowCount; rowIdx++ {
		encoded, err := EncodeValue(sc, nil, datums...)
		c.Assert(err, IsNil)
		decoder.buf = make([]byte, 0, len(encoded))
		for colIdx, tp := range tps {
			encoded, err = decoder.DecodeOne(encoded, colIdx, tp)
			c.Assert(err, IsNil)
		}
	}
	return decoder.chk
}

func (s *testCodecSuite) TestDecodeRange(c *C) {
	_, _, err := DecodeRange(nil, 0)
	c.Assert(err, NotNil)

	datums := types.MakeDatums(1, "abc", 1.1, []byte("def"))
	rowData, err := EncodeValue(nil, nil, datums...)
	c.Assert(err, IsNil)

	datums1, _, err := DecodeRange(rowData, len(datums))
	c.Assert(err, IsNil)
	for i, datum := range datums1 {
		cmp, err := datum.CompareDatum(nil, &datums[i])
		c.Assert(err, IsNil)
		c.Assert(cmp, Equals, 0)
	}

	for _, b := range []byte{NilFlag, bytesFlag, maxFlag, maxFlag + 1} {
		newData := append(rowData, b)
		_, _, err := DecodeRange(newData, len(datums)+1)
		c.Assert(err, IsNil)
	}
}

func (s *testCodecSuite) TestHashChunkRow(c *C) {
	sc := &stmtctx.StatementContext{TimeZone: time.Local}
	datums, tps := datumsForTest(sc)
	chk := chunkForTest(c, sc, datums, tps, 1)

	colIdx := make([]int, len(tps))
	for i := 0; i < len(tps); i++ {
		colIdx[i] = i
	}
	_, err1 := HashChunkRow(sc, nil, chk.GetRow(0), tps, colIdx)

	c.Assert(err1, IsNil)
}
