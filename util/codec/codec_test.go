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

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/util/testleak"
	"github.com/pingcap/tidb/util/types"
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
			types.MakeDatums(types.Hex{Value: 100}, types.Bit{Value: 100, Width: 8}),
			types.MakeDatums(int64(100), uint64(100)),
		},

		{
			types.MakeDatums(types.Enum{Name: "a", Value: 1}, types.Set{Name: "a", Value: 1}),
			types.MakeDatums(uint64(1), uint64(1)),
		},
	}

	for i, t := range table {
		comment := Commentf("%d %v", i, t)
		b, err := EncodeKey(nil, t.Input...)
		c.Assert(err, IsNil, comment)
		args, err := Decode(b, 1)
		c.Assert(err, IsNil)
		c.Assert(args, DeepEquals, t.Expect)

		b, err = EncodeValue(nil, t.Input...)
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
	}

	for _, t := range table {
		b1, err := EncodeKey(nil, t.Left...)
		c.Assert(err, IsNil)

		b2, err := EncodeKey(nil, t.Right...)
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
	b, i, err = DecodeComparableVarint(b)
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
		_, v, err := DecodeBytes(b)
		c.Assert(err, IsNil)
		c.Assert(t, DeepEquals, v, Commentf("%v - %v - %v", t, b, v))

		b = EncodeBytesDesc(nil, t)
		_, v, err = DecodeBytesDesc(b)
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
	m, err := types.ParseTime(s, mysql.TypeDatetime, types.DefaultFsp)
	c.Assert(err, IsNil)
	return m
}

func parseDuration(c *C, s string) types.Duration {
	m, err := types.ParseDuration(s, types.DefaultFsp)
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

	for _, t := range tbl {
		m := types.NewDatum(parseTime(c, t))

		b, err := EncodeKey(nil, m)
		c.Assert(err, IsNil)
		v, err := Decode(b, 1)
		c.Assert(err, IsNil)
		var t types.Time
		t.Type = mysql.TypeDatetime
		t.FromPackedUint(v[0].GetUint64())
		t.TimeZone = nil
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

		b1, err := EncodeKey(nil, m1)
		c.Assert(err, IsNil)
		b2, err := EncodeKey(nil, m2)
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

	for _, t := range tbl {
		m := parseDuration(c, t)

		b, err := EncodeKey(nil, types.NewDatum(m))
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

		b1, err := EncodeKey(nil, types.NewDatum(m1))
		c.Assert(err, IsNil)
		b2, err := EncodeKey(nil, types.NewDatum(m2))
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

	for _, t := range tbl {
		dec := new(types.MyDecimal)
		err := dec.FromString([]byte(t))
		c.Assert(err, IsNil)
		b, err := EncodeKey(nil, types.NewDatum(dec))
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
	sc := new(variable.StatementContext)
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
		b1, err := EncodeKey(nil, d1)
		c.Assert(err, IsNil)
		b2, err := EncodeKey(nil, d2)
		c.Assert(err, IsNil)

		ret := bytes.Compare(b1, b2)
		c.Assert(ret, Equals, t.Ret, Commentf("%v %x %x", t, b1, b2))
	}

	floats := []float64{-123.45, -123.40, -23.45, -1.43, -0.93, -0.4333, -0.068,
		-0.0099, 0, 0.001, 0.0012, 0.12, 1.2, 1.23, 123.3, 2424.242424}
	var decs [][]byte
	for i := range floats {
		dec := types.NewDecFromFloatForTest(floats[i])
		var d types.Datum
		d.SetLength(20)
		d.SetFrac(6)
		d.SetMysqlDecimal(dec)
		decs = append(decs, EncodeDecimal(nil, d))
	}
	for i := 0; i < len(decs)-1; i++ {
		cmp := bytes.Compare(decs[i], decs[i+1])
		c.Assert(cmp, LessEqual, 0)
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
			types.MakeDatums(types.Hex{Value: 100}, types.Bit{Value: 100, Width: 8}),
			types.MakeDatums(int64(100), uint64(100)),
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
	for i, t := range table {
		comment := Commentf("%d %v", i, t)
		b, err := EncodeKey(nil, t.Input...)
		c.Assert(err, IsNil, comment)
		var d []byte
		for j, e := range t.Expect {
			d, b, err = CutOne(b)
			c.Assert(err, IsNil)
			c.Assert(d, NotNil)
			ed, err1 := EncodeKey(nil, e)
			c.Assert(err1, IsNil)
			c.Assert(d, DeepEquals, ed, Commentf("%d:%d %#v", i, j, e))
		}
		c.Assert(b, HasLen, 0)
	}
	for i, t := range table {
		comment := Commentf("%d %v", i, t)
		b, err := EncodeValue(nil, t.Input...)
		c.Assert(err, IsNil, comment)
		var d []byte
		for j, e := range t.Expect {
			d, b, err = CutOne(b)
			c.Assert(err, IsNil)
			c.Assert(d, NotNil)
			ed, err1 := EncodeValue(nil, e)
			c.Assert(err1, IsNil)
			c.Assert(d, DeepEquals, ed, Commentf("%d:%d %#v", i, j, e))
		}
		c.Assert(b, HasLen, 0)
	}
}

func (s *testCodecSuite) TestSetRawValues(c *C) {
	datums := types.MakeDatums(1, "abc", 1.1, []byte("def"))
	rowData, err := EncodeValue(nil, datums...)
	c.Assert(err, IsNil)
	values := make([]types.Datum, 4)
	err = SetRawValues(rowData, values)
	c.Assert(err, IsNil)
	for i, rawVal := range values {
		c.Assert(rawVal.Kind(), Equals, types.KindRaw)
		encoded, err1 := EncodeValue(nil, datums[i])
		c.Assert(err1, IsNil)
		c.Assert(encoded, BytesEquals, rawVal.GetBytes())
	}
}
