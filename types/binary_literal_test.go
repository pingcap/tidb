// Copyright 2017 PingCAP, Inc.
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

package types

import (
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/v4/sessionctx/stmtctx"
	"github.com/pingcap/tidb/v4/util/testleak"
)

var _ = Suite(&testBinaryLiteralSuite{})

type testBinaryLiteralSuite struct {
}

func (s *testBinaryLiteralSuite) TestTrimLeadingZeroBytes(c *C) {
	defer testleak.AfterTest(c)()
	tbl := []struct {
		Input    []byte
		Expected []byte
	}{
		{[]byte{}, []byte{}},
		{[]byte{0x0}, []byte{0x0}},
		{[]byte{0x1}, []byte{0x1}},
		{[]byte{0x1, 0x0}, []byte{0x1, 0x0}},
		{[]byte{0x0, 0x1}, []byte{0x1}},
		{[]byte{0x0, 0x0, 0x0}, []byte{0x0}},
		{[]byte{0x1, 0x0, 0x0}, []byte{0x1, 0x0, 0x0}},
		{[]byte{0x0, 0x1, 0x0, 0x0, 0x1, 0x0, 0x0}, []byte{0x1, 0x0, 0x0, 0x1, 0x0, 0x0}},
		{[]byte{0x0, 0x0, 0x0, 0x0, 0x0, 0x1, 0x0, 0x0, 0x1, 0x0, 0x0}, []byte{0x1, 0x0, 0x0, 0x1, 0x0, 0x0}},
	}
	for _, t := range tbl {
		b := trimLeadingZeroBytes(t.Input)
		c.Assert(b, DeepEquals, t.Expected, Commentf("%#v", t))
	}
}

func (s *testBinaryLiteralSuite) TestParseBitStr(c *C) {
	defer testleak.AfterTest(c)()
	tbl := []struct {
		Input    string
		Expected []byte
		IsError  bool
	}{
		{"b''", []byte{}, false},
		{"B''", []byte{}, false},
		{"0b''", nil, true},
		{"0b0", []byte{0x0}, false},
		{"b'0'", []byte{0x0}, false},
		{"B'0'", []byte{0x0}, false},
		{"0B0", nil, true},
		{"0b123", nil, true},
		{"b'123'", nil, true},
		{"0b'1010'", nil, true},
		{"0b0000000", []byte{0x0}, false},
		{"b'0000000'", []byte{0x0}, false},
		{"B'0000000'", []byte{0x0}, false},
		{"0b00000000", []byte{0x0}, false},
		{"b'00000000'", []byte{0x0}, false},
		{"B'00000000'", []byte{0x0}, false},
		{"0b000000000", []byte{0x0, 0x0}, false},
		{"b'000000000'", []byte{0x0, 0x0}, false},
		{"B'000000000'", []byte{0x0, 0x0}, false},
		{"0b1", []byte{0x1}, false},
		{"b'1'", []byte{0x1}, false},
		{"B'1'", []byte{0x1}, false},
		{"0b00000001", []byte{0x1}, false},
		{"b'00000001'", []byte{0x1}, false},
		{"B'00000001'", []byte{0x1}, false},
		{"0b000000010", []byte{0x0, 0x2}, false},
		{"b'000000010'", []byte{0x0, 0x2}, false},
		{"B'000000010'", []byte{0x0, 0x2}, false},
		{"0b000000001", []byte{0x0, 0x1}, false},
		{"b'000000001'", []byte{0x0, 0x1}, false},
		{"B'000000001'", []byte{0x0, 0x1}, false},
		{"0b11111111", []byte{0xFF}, false},
		{"b'11111111'", []byte{0xFF}, false},
		{"B'11111111'", []byte{0xFF}, false},
		{"0b111111111", []byte{0x1, 0xFF}, false},
		{"b'111111111'", []byte{0x1, 0xFF}, false},
		{"B'111111111'", []byte{0x1, 0xFF}, false},
		{"0b1101000011001010110110001101100011011110010000001110111011011110111001001101100011001000010000001100110011011110110111100100000011000100110000101110010", []byte("hello world foo bar"), false},
		{"b'1101000011001010110110001101100011011110010000001110111011011110111001001101100011001000010000001100110011011110110111100100000011000100110000101110010'", []byte("hello world foo bar"), false},
		{"B'1101000011001010110110001101100011011110010000001110111011011110111001001101100011001000010000001100110011011110110111100100000011000100110000101110010'", []byte("hello world foo bar"), false},
		{"0b01101000011001010110110001101100011011110010000001110111011011110111001001101100011001000010000001100110011011110110111100100000011000100110000101110010", []byte("hello world foo bar"), false},
		{"b'01101000011001010110110001101100011011110010000001110111011011110111001001101100011001000010000001100110011011110110111100100000011000100110000101110010'", []byte("hello world foo bar"), false},
		{"B'01101000011001010110110001101100011011110010000001110111011011110111001001101100011001000010000001100110011011110110111100100000011000100110000101110010'", []byte("hello world foo bar"), false},
	}
	for _, t := range tbl {
		b, err := ParseBitStr(t.Input)
		if t.IsError {
			c.Assert(err, NotNil, Commentf("%#v", t))
		} else {
			c.Assert(err, IsNil, Commentf("%#v", t))
			c.Assert([]byte(b), DeepEquals, t.Expected, Commentf("%#v", t))
		}
	}
	b, err := ParseBitStr("")
	c.Assert(b, IsNil)
	c.Assert(err, ErrorMatches, "invalid empty .*")
}

func (s *testBinaryLiteralSuite) TestParseHexStr(c *C) {
	defer testleak.AfterTest(c)()
	tbl := []struct {
		Input    string
		Expected []byte
		IsError  bool
	}{
		{"x'1'", nil, true},
		{"x'01'", []byte{0x1}, false},
		{"X'01'", []byte{0x1}, false},
		{"0x1", []byte{0x1}, false},
		{"0x-1", nil, true},
		{"0X11", nil, true},
		{"x'01+'", nil, true},
		{"0x123", []byte{0x01, 0x23}, false},
		{"0x10", []byte{0x10}, false},
		{"0x4D7953514C", []byte("MySQL"), false},
		{"0x4920616D2061206C6F6E672068657820737472696E67", []byte("I am a long hex string"), false},
		{"x'4920616D2061206C6F6E672068657820737472696E67'", []byte("I am a long hex string"), false},
		{"X'4920616D2061206C6F6E672068657820737472696E67'", []byte("I am a long hex string"), false},
		{"x''", []byte{}, false},
	}
	for _, t := range tbl {
		hex, err := ParseHexStr(t.Input)
		if t.IsError {
			c.Assert(err, NotNil, Commentf("%#v", t))
		} else {
			c.Assert(err, IsNil, Commentf("%#v", t))
			c.Assert([]byte(hex), DeepEquals, t.Expected, Commentf("%#v", t))
		}
	}
	hex, err := ParseHexStr("")
	c.Assert(hex, IsNil)
	c.Assert(err, ErrorMatches, "invalid empty .*")
}

func (s *testBinaryLiteralSuite) TestString(c *C) {
	defer testleak.AfterTest(c)()
	tbl := []struct {
		Input    BinaryLiteral
		Expected string
	}{
		{BinaryLiteral{}, ""}, // Expected
		{BinaryLiteral{0x0}, "0x00"},
		{BinaryLiteral{0x1}, "0x01"},
		{BinaryLiteral{0xff, 0x01}, "0xff01"},
	}
	for _, t := range tbl {
		str := t.Input.String()
		c.Assert(str, Equals, t.Expected)
	}
}

func (s *testBinaryLiteralSuite) TestToBitLiteralString(c *C) {
	defer testleak.AfterTest(c)()
	tbl := []struct {
		Input           BinaryLiteral
		TrimLeadingZero bool
		Expected        string
	}{
		{BinaryLiteral{}, true, "b''"},
		{BinaryLiteral{}, false, "b''"},
		{BinaryLiteral{0x0}, true, "b'0'"},
		{BinaryLiteral{0x0}, false, "b'00000000'"},
		{BinaryLiteral{0x0, 0x0}, true, "b'0'"},
		{BinaryLiteral{0x0, 0x0}, false, "b'0000000000000000'"},
		{BinaryLiteral{0x1}, true, "b'1'"},
		{BinaryLiteral{0x1}, false, "b'00000001'"},
		{BinaryLiteral{0xff, 0x01}, true, "b'1111111100000001'"},
		{BinaryLiteral{0xff, 0x01}, false, "b'1111111100000001'"},
		{BinaryLiteral{0x0, 0xff, 0x01}, true, "b'1111111100000001'"},
		{BinaryLiteral{0x0, 0xff, 0x01}, false, "b'000000001111111100000001'"},
	}
	for _, t := range tbl {
		str := t.Input.ToBitLiteralString(t.TrimLeadingZero)
		c.Assert(str, Equals, t.Expected)
	}
}

func (s *testBinaryLiteralSuite) TestToInt(c *C) {
	defer testleak.AfterTest(c)()
	tbl := []struct {
		Input    string
		Expected uint64
		HasError bool
	}{
		{"x''", 0, false},
		{"0x00", 0x0, false},
		{"0xff", 0xff, false},
		{"0x10ff", 0x10ff, false},
		{"0x1010ffff", 0x1010ffff, false},
		{"0x1010ffff8080", 0x1010ffff8080, false},
		{"0x1010ffff8080ff12", 0x1010ffff8080ff12, false},
		{"0x1010ffff8080ff12ff", 0xffffffffffffffff, true},
	}
	sc := new(stmtctx.StatementContext)
	for _, t := range tbl {
		hex, err := ParseHexStr(t.Input)
		c.Assert(err, IsNil)
		intValue, err := hex.ToInt(sc)
		if t.HasError {
			c.Assert(err, NotNil)
		} else {
			c.Assert(err, IsNil)
		}
		c.Assert(intValue, Equals, t.Expected)
	}
}

func (s *testBinaryLiteralSuite) TestNewBinaryLiteralFromUint(c *C) {
	defer testleak.AfterTest(c)()
	tbl := []struct {
		Input    uint64
		ByteSize int
		Expected []byte
	}{
		{0x0, -1, []byte{0x0}},
		{0x0, 1, []byte{0x0}},
		{0x0, 2, []byte{0x0, 0x0}},
		{0x1, -1, []byte{0x1}},
		{0x1, 1, []byte{0x1}},
		{0x1, 2, []byte{0x0, 0x1}},
		{0x1, 3, []byte{0x0, 0x0, 0x1}},
		{0x10, -1, []byte{0x10}},
		{0x123, -1, []byte{0x1, 0x23}},
		{0x123, 2, []byte{0x1, 0x23}},
		{0x123, 1, []byte{0x23}},
		{0x123, 5, []byte{0x0, 0x0, 0x0, 0x1, 0x23}},
		{0x4D7953514C, -1, []byte{0x4D, 0x79, 0x53, 0x51, 0x4C}},
		{0x4D7953514C, 8, []byte{0x0, 0x0, 0x0, 0x4D, 0x79, 0x53, 0x51, 0x4C}},
		{0x4920616D2061206C, -1, []byte{0x49, 0x20, 0x61, 0x6D, 0x20, 0x61, 0x20, 0x6C}},
		{0x4920616D2061206C, 8, []byte{0x49, 0x20, 0x61, 0x6D, 0x20, 0x61, 0x20, 0x6C}},
		{0x4920616D2061206C, 5, []byte{0x6D, 0x20, 0x61, 0x20, 0x6C}},
	}
	for _, t := range tbl {
		hex := NewBinaryLiteralFromUint(t.Input, t.ByteSize)
		c.Assert([]byte(hex), DeepEquals, t.Expected, Commentf("%#v", t))
	}

	defer func() {
		r := recover()
		c.Assert(r, NotNil)
	}()
	NewBinaryLiteralFromUint(0x123, -2)
}

func (s *testBinaryLiteralSuite) TestCompare(c *C) {
	tbl := []struct {
		a   BinaryLiteral
		b   BinaryLiteral
		cmp int
	}{
		{BinaryLiteral{0, 0, 1}, BinaryLiteral{2}, -1},
		{BinaryLiteral{0, 1}, BinaryLiteral{0, 0, 2}, -1},
		{BinaryLiteral{0, 1}, BinaryLiteral{1}, 0},
		{BinaryLiteral{0, 2, 1}, BinaryLiteral{1, 2}, 1},
	}
	for _, t := range tbl {
		c.Assert(t.a.Compare(t.b), Equals, t.cmp)
	}
}

func (s *testBinaryLiteralSuite) TestToString(c *C) {
	h, _ := NewHexLiteral("x'3A3B'")
	str := h.ToString()
	c.Assert(str, Equals, ":;")

	b, _ := NewBitLiteral("b'00101011'")
	str = b.ToString()
	c.Assert(str, Equals, "+")
}
