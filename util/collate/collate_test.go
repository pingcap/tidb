// Copyright 2020 PingCAP, Inc.
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

package collate

import (
	"testing"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/util/testleak"
)

func TestT(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testBinCollatorSuite{})
var _ = Suite(&testGeneralCICollatorSuite{})
var _ = Suite(&testGetCollatorSuite{})

type testBinCollatorSuite struct {
}

type testGeneralCICollatorSuite struct {
}

type testGetCollatorSuite struct {
}

type compareTable struct {
	Left   string
	Right  string
	Expect int
}

type keyTable struct {
	Str    string
	Expect []byte
}

func testCompareTable(table []compareTable, collate string, c *C) {
	for i, t := range table {
		comment := Commentf("%d %v %v", i, t.Left, t.Right)
		c.Assert(GetCollator(collate).Compare(t.Left, t.Right, CollatorOption{}), Equals, t.Expect, comment)
	}
}

func testKeyTable(table []keyTable, collate string, c *C) {
	for i, t := range table {
		comment := Commentf("%d %s", i, t.Str)
		c.Assert(GetCollator(collate).Key(t.Str, CollatorOption{}), DeepEquals, t.Expect, comment)
	}
}

func (s *testBinCollatorSuite) TestBinCollator(c *C) {
	defer testleak.AfterTest(c)()
	compareTable := []compareTable{
		{"a", "b", -1},
		{"a", "A", 1},
		{"abc", "abc", 0},
		{"abc", "ab", 1},
	}
	keyTable := []keyTable{
		{"a", []byte{0x61}},
		{"A", []byte{0x41}},
		{"Foo © bar 𝌆 baz ☃ qux", []byte{0x46, 0x6f, 0x6f, 0x20, 0xc2, 0xa9, 0x20, 0x62, 0x61, 0x72, 0x20, 0xf0,
			0x9d, 0x8c, 0x86, 0x20, 0x62, 0x61, 0x7a, 0x20, 0xe2, 0x98, 0x83, 0x20, 0x71, 0x75, 0x78}},
	}
	testCompareTable(compareTable, "binary", c)
	testKeyTable(keyTable, "binary", c)
}

func (s *testBinCollatorSuite) TestGeneralCICollator(c *C) {
	defer testleak.AfterTest(c)()
	compareTable := []compareTable{
		{"a", "b", -1},
		{"a", "A", 0},
		{"À", "A", 0},
		{"abc", "abc", 0},
		{"abc", "ab", 1},
		{"😜", "😃", 0},
	}
	keyTable := []keyTable{
		{"a", []byte{0x0, 0x41}},
		{"A", []byte{0x0, 0x41}},
		{"😃", []byte{0xff, 0xfd}},
		{"Foo © bar 𝌆 baz ☃ qux", []byte{0x0, 0x46, 0x0, 0x4f, 0x0, 0x4f, 0x0, 0x20, 0x0, 0xa9, 0x0, 0x20, 0x0,
			0x42, 0x0, 0x41, 0x0, 0x52, 0x0, 0x20, 0xff, 0xfd, 0x0, 0x20, 0x0, 0x42, 0x0, 0x41, 0x0, 0x5a, 0x0, 0x20, 0x26,
			0x3, 0x0, 0x20, 0x0, 0x51, 0x0, 0x55, 0x0, 0x58}},
		{string([]byte{0x88, 0xe6}), []byte{0xff, 0xfd, 0xff, 0xfd}},
	}
	testCompareTable(compareTable, "utf8mb4_general_ci", c)
	testKeyTable(keyTable, "utf8mb4_general_ci", c)
}

func (s *testGetCollatorSuite) TestGetCollator(c *C) {
	c.Assert(GetCollator("binary"), FitsTypeOf, &binCollator{})
	c.Assert(GetCollator("utf8mb4_bin"), FitsTypeOf, &binCollator{})
	c.Assert(GetCollator("utf8_bin"), FitsTypeOf, &binCollator{})
	c.Assert(GetCollator("utf8mb4_general_ci"), FitsTypeOf, &generalCICollator{})
	c.Assert(GetCollator("utf8_general_ci"), FitsTypeOf, &generalCICollator{})

	c.Assert(GetCollatorByID(63), FitsTypeOf, &binCollator{})
	c.Assert(GetCollatorByID(46), FitsTypeOf, &binCollator{})
	c.Assert(GetCollatorByID(83), FitsTypeOf, &binCollator{})
	c.Assert(GetCollatorByID(45), FitsTypeOf, &generalCICollator{})
	c.Assert(GetCollatorByID(33), FitsTypeOf, &generalCICollator{})
}
