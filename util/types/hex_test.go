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

var _ = Suite(&testHexSuite{})

type testHexSuite struct {
}

func (s *testHexSuite) TestParseHexStr(c *C) {
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
			c.Assert(hex.Value, DeepEquals, t.Expected, Commentf("%#v", t))
		}
	}
}

func (s *testHexSuite) TestString(c *C) {
	defer testleak.AfterTest(c)()
	tbl := []struct {
		Input    Hex
		Expected string
	}{
		{Hex{[]byte{}}, ""}, // Expected
		{Hex{[]byte{0x0}}, "0x00"},
		{Hex{[]byte{0x1}}, "0x01"},
		{Hex{[]byte{0xff, 0x01}}, "0xff01"},
	}
	for _, t := range tbl {
		str := t.Input.String()
		c.Assert(str, Equals, t.Expected)
	}
}

func (s *testHexSuite) TestToInt(c *C) {
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
	for _, t := range tbl {
		hex, err := ParseHexStr(t.Input)
		c.Assert(err, IsNil)
		intValue, err := hex.ToInt()
		if t.HasError {
			c.Assert(err, NotNil)
		} else {
			c.Assert(err, IsNil)
		}
		c.Assert(intValue, Equals, t.Expected)
	}
}

func (s *testHexSuite) TestNewHexFromUint(c *C) {
	defer testleak.AfterTest(c)()
	tbl := []struct {
		Input    uint64
		Expected []byte
	}{
		{0x0, []byte{0x0}},
		{0x1, []byte{0x1}},
		{0x10, []byte{0x10}},
		{0x123, []byte{0x1, 0x23}},
		{0x4D7953514C, []byte{0x4D, 0x79, 0x53, 0x51, 0x4C}},
		{0x4920616D2061206C, []byte{0x49, 0x20, 0x61, 0x6D, 0x20, 0x61, 0x20, 0x6C}},
	}
	for _, t := range tbl {
		hex := NewHexFromUint(t.Input)
		c.Assert(hex.Value, DeepEquals, t.Expected, Commentf("%#v", t))
	}
}
