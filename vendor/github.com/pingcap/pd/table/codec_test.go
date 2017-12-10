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

package table

import (
	"testing"

	. "github.com/pingcap/check"
)

func TestTable(t *testing.T) {
	TestingT(t)
}

var pads = make([]byte, encGroupSize)

var _ = Suite(&testCodecSuite{})

type testCodecSuite struct{}

func encodeBytes(data []byte) Key {
	// Allocate more space to avoid unnecessary slice growing.
	// Assume that the byte slice size is about `(len(data) / encGroupSize + 1) * (encGroupSize + 1)` bytes,
	// that is `(len(data) / 8 + 1) * 9` in our implement.
	dLen := len(data)
	result := make([]byte, 0, (dLen/encGroupSize+1)*(encGroupSize+1))
	for idx := 0; idx <= dLen; idx += encGroupSize {
		remain := dLen - idx
		padCount := 0
		if remain >= encGroupSize {
			result = append(result, data[idx:idx+encGroupSize]...)
		} else {
			padCount = encGroupSize - remain
			result = append(result, data[idx:]...)
			result = append(result, pads[:padCount]...)
		}

		marker := encMarker - byte(padCount)
		result = append(result, marker)
	}
	return result
}

func (s *testCodecSuite) TestDecodeBytes(c *C) {
	key := "abcdefghijklmnopqrstuvwxyz"
	for i := 0; i < len(key); i++ {
		_, k, err := decodeBytes(encodeBytes([]byte(key[:i])))
		c.Assert(err, IsNil)
		c.Assert(string(k), Equals, key[:i])
	}
}

func (s *testCodecSuite) TestTableID(c *C) {
	key := encodeBytes([]byte("t\x80\x00\x00\x00\x00\x00\x00\xff"))
	c.Assert(Key(key).TableID(), Equals, int64(0xff))

	key = encodeBytes([]byte("t\x80\x00\x00\x00\x00\x00\x00\xff_i\x01\x02"))
	c.Assert(Key(key).TableID(), Equals, int64(0xff))

	key = []byte("t\x80\x00\x00\x00\x00\x00\x00\xff")
	c.Assert(Key(key).TableID(), Equals, int64(0))

	key = encodeBytes([]byte("T\x00\x00\x00\x00\x00\x00\x00\xff"))
	c.Assert(Key(key).TableID(), Equals, int64(0))

	key = encodeBytes([]byte("t\x80\x00\x00\x00\x00\x00\xff"))
	c.Assert(Key(key).TableID(), Equals, int64(0))
}
