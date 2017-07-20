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

func (s *testBitSuite) TestBit(c *C) {
	defer testleak.AfterTest(c)()
	tbl := []struct {
		Input     string
		Width     int
		Number    int64
		String    string
		BitString string
	}{
		{"0b01", 8, 1, "0b00000001", "\x01"},
		{"0b111111111", 16, 511, "0b0000000111111111", "\x01\xff"},
		{"0b01", -1, 1, "0b00000001", "\x01"},
	}

	for _, t := range tbl {
		b, err := ParseBit(t.Input, t.Width)
		c.Assert(err, IsNil)
		c.Assert(b.ToNumber(), Equals, float64(t.Number))
		c.Assert(b.String(), Equals, t.String)
		c.Assert(b.ToString(), Equals, t.BitString)

		n, err := ParseStringToBitValue(t.BitString, t.Width)
		c.Assert(err, IsNil)
		c.Assert(n, Equals, uint64(t.Number))
	}

	tblErr := []struct {
		Input string
		Width int
	}{
		{"0b11", 1},
		{"0B11", 2},
	}

	for _, t := range tblErr {
		_, err := ParseBit(t.Input, t.Width)
		c.Assert(err, NotNil)
	}
}
