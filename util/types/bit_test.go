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

package types

import (
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/util/testleak"
)

var _ = Suite(&testBitSuite{})

type testBitSuite struct {
}

func (s *testBitSuite) TestParseBitStr(c *C) {
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
			c.Assert(b.Value, DeepEquals, t.Expected, Commentf("%#v", t))
		}
	}
}

func (s *testBitSuite) TestString(c *C) {
	defer testleak.AfterTest(c)()
	tbl := []struct {
		Input    Bit
		Expected string
	}{
		{Bit{Hex{[]byte{}}}, ""}, // Expected
		{Bit{Hex{[]byte{0x0}}}, "0b00000000"},
		{Bit{Hex{[]byte{0x1}}}, "0b00000001"},
		{Bit{Hex{[]byte{0xff, 0x01}}}, "0b1111111100000001"},
	}
	for _, t := range tbl {
		str := t.Input.String()
		c.Assert(str, Equals, t.Expected)
	}
}
