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
)

func TestT(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testCodecSuite{})

type testCodecSuite struct {
}

func (s *testCodecSuite) TestCodecKey(c *C) {
	table := []struct {
		Input  []interface{}
		Expect []interface{}
	}{
		{
			[]interface{}{int64(1)},
			[]interface{}{int64(1)},
		},

		{
			[]interface{}{float32(1), float64(3.15), []byte("123"), "123"},
			[]interface{}{float64(1), float64(3.15), []byte("123"), "123"},
		},

		{
			[]interface{}{uint64(1), float64(3.15), []byte("123"), int64(-1)},
			[]interface{}{uint64(1), float64(3.15), []byte("123"), int64(-1)},
		},

		{
			[]interface{}{true, false},
			[]interface{}{int64(1), int64(0)},
		},

		{
			[]interface{}{int(1), int8(1), int16(1), int32(1), int64(1)},
			[]interface{}{int64(1), int64(1), int64(1), int64(1), int64(1)},
		},

		{
			[]interface{}{uint(1), uint8(1), uint16(1), uint32(1), uint64(1)},
			[]interface{}{uint64(1), uint64(1), uint64(1), uint64(1), uint64(1)},
		},

		{
			[]interface{}{nil},
			[]interface{}{nil},
		},

		{
			[]interface{}{mysql.Hex{Value: 100}, mysql.Bit{Value: 100, Width: 8}},
			[]interface{}{int64(100), uint64(100)},
		},

		{
			[]interface{}{mysql.Enum{Name: "a", Value: 1}, mysql.Set{Name: "a", Value: 1}},
			[]interface{}{uint64(1), uint64(1)},
		},
	}

	for _, t := range table {
		b, err := EncodeKey(t.Input...)
		c.Assert(err, IsNil)
		args, err := DecodeKey(b)
		c.Assert(err, IsNil)

		c.Assert(args, DeepEquals, t.Expect)
	}
}

func (s *testCodecSuite) TestCodecKeyCompare(c *C) {
	table := []struct {
		Left   []interface{}
		Right  []interface{}
		Expect int
	}{
		{
			[]interface{}{1},
			[]interface{}{1},
			0,
		},
		{
			[]interface{}{-1},
			[]interface{}{1},
			-1,
		},
		{
			[]interface{}{3.15},
			[]interface{}{3.12},
			1,
		},
		{
			[]interface{}{"abc"},
			[]interface{}{"abcd"},
			-1,
		},
		{
			[]interface{}{1, "abc"},
			[]interface{}{1, "abcd"},
			-1,
		},
		{
			[]interface{}{1, "abc", "def"},
			[]interface{}{1, "abcd", "af"},
			-1,
		},
		{
			[]interface{}{3.12, "ebc", "def"},
			[]interface{}{2.12, "abcd", "af"},
			1,
		},
		{
			[]interface{}{[]byte{0x01, 0x00}, []byte{0xFF}},
			[]interface{}{[]byte{0x01, 0x00, 0xFF}},
			-1,
		},

		{
			[]interface{}{[]byte{0x01}, uint64(0xFFFFFFFFFFFFFFF)},
			[]interface{}{[]byte{0x01, 0x10}, 0},
			-1,
		},
		{
			[]interface{}{0},
			[]interface{}{nil},
			1,
		},
		{
			[]interface{}{[]byte{0x00}},
			[]interface{}{nil},
			1,
		},
		{
			[]interface{}{math.SmallestNonzeroFloat64},
			[]interface{}{nil},
			1,
		},
		{
			[]interface{}{int64(math.MinInt64)},
			[]interface{}{nil},
			1,
		},
		{
			[]interface{}{1, int64(math.MinInt64), nil},
			[]interface{}{1, nil, uint64(math.MaxUint64)},
			1,
		},
		{
			[]interface{}{1, []byte{}, nil},
			[]interface{}{1, nil, 123},
			1,
		},
		{
			[]interface{}{parseTime(c, "2011-11-11 00:00:00"), 1},
			[]interface{}{parseTime(c, "2011-11-11 00:00:00"), 0},
			1,
		},
		{
			[]interface{}{parseDuration(c, "00:00:00"), 1},
			[]interface{}{parseDuration(c, "00:00:01"), 0},
			-1,
		},
	}

	for _, t := range table {
		b1, err := EncodeKey(t.Left...)
		c.Assert(err, IsNil)

		b2, err := EncodeKey(t.Right...)
		c.Assert(err, IsNil)

		c.Assert(bytes.Compare(b1, b2), Equals, t.Expect)
	}
}

func (s *testCodecSuite) TestNumberCodec(c *C) {
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
	}
}

func (s *testCodecSuite) TestNumberOrder(c *C) {
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
	}
}

func (s *testCodecSuite) TestFloatCodec(c *C) {
	tblFloat := []float64{
		-1,
		0,
		1,
		math.MaxFloat64,
		math.MaxFloat32,
		math.SmallestNonzeroFloat32,
		math.SmallestNonzeroFloat64}

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
		c.Assert(t, DeepEquals, v)

		b = EncodeBytesDesc(nil, t)
		_, v, err = DecodeBytesDesc(b)
		c.Assert(err, IsNil)
		c.Assert(t, DeepEquals, v)
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

func parseTime(c *C, s string) mysql.Time {
	m, err := mysql.ParseTime(s, mysql.TypeDatetime, mysql.DefaultFsp)
	c.Assert(err, IsNil)
	return m
}

func parseDuration(c *C, s string) mysql.Duration {
	m, err := mysql.ParseDuration(s, mysql.DefaultFsp)
	c.Assert(err, IsNil)
	return m
}

func (s *testCodecSuite) TestTime(c *C) {
	tbl := []string{
		"2011-01-01 00:00:00",
		"2011-01-01 00:00:00",
		"0001-01-01 00:00:00",
	}

	for _, t := range tbl {
		m := parseTime(c, t)

		b, err := EncodeKey(m)
		c.Assert(err, IsNil)
		v, err := DecodeKey(b)
		c.Assert(err, IsNil)
		c.Assert(v, DeepEquals, []interface{}{t})
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
		m1 := parseTime(c, t.Arg1)
		m2 := parseTime(c, t.Arg2)

		b1, err := EncodeKey(m1)
		c.Assert(err, IsNil)
		b2, err := EncodeKey(m2)
		c.Assert(err, IsNil)

		ret := bytes.Compare(b1, b2)
		c.Assert(ret, Equals, t.Ret)
	}
}

func (s *testCodecSuite) TestDuration(c *C) {
	tbl := []string{
		"11:11:11",
		"00:00:00",
		"1 11:11:11",
	}

	for _, t := range tbl {
		m := parseDuration(c, t)

		b, err := EncodeKey(m)
		c.Assert(err, IsNil)
		v, err := DecodeKey(b)
		c.Assert(err, IsNil)
		m.Fsp = mysql.MaxFsp
		c.Assert(v, DeepEquals, []interface{}{m})
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

		b1, err := EncodeKey(m1)
		c.Assert(err, IsNil)
		b2, err := EncodeKey(m2)
		c.Assert(err, IsNil)

		ret := bytes.Compare(b1, b2)
		c.Assert(ret, Equals, t.Ret)
	}
}

func (s *testCodecSuite) TestDecimal(c *C) {
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
		"-0.1234"}

	for _, t := range tbl {
		m, err := mysql.ParseDecimal(t)
		c.Assert(err, IsNil)
		b, err := EncodeKey(m)
		c.Assert(err, IsNil)
		v, err := DecodeKey(b)
		c.Assert(err, IsNil)
		c.Assert(v, HasLen, 1)
		vv, ok := v[0].(mysql.Decimal)
		c.Assert(ok, IsTrue)
		c.Assert(vv.Equals(m), IsTrue)
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
		m1, err := mysql.ConvertToDecimal(t.Arg1)
		c.Assert(err, IsNil)
		m2, err := mysql.ConvertToDecimal(t.Arg2)
		c.Assert(err, IsNil)

		b1, err := EncodeKey(m1)
		c.Assert(err, IsNil)
		b2, err := EncodeKey(m2)
		c.Assert(err, IsNil)

		ret := bytes.Compare(b1, b2)
		c.Assert(ret, Equals, t.Ret)
	}
}
