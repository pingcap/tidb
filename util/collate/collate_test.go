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
	"bytes"
	"testing"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/util/testleak"
)

func TestT(t *testing.T) {
	TestingT(t)
}

var (
	_ = SerialSuites(&testCollateSuite{})
)

type testCollateSuite struct {
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
		c.Assert(GetCollator(collate).Compare(t.Left, t.Right), Equals, t.Expect, comment)
	}
}

func testKeyTable(table []keyTable, collate string, c *C) {
	for i, t := range table {
		comment := Commentf("%d %s", i, t.Str)
		c.Assert(GetCollator(collate).Key(t.Str), DeepEquals, t.Expect, comment)
	}
}

func (s *testCollateSuite) TestBinCollator(c *C) {
	defer testleak.AfterTest(c)()
	SetNewCollationEnabledForTest(false)
	compareTable := []compareTable{
		{"a", "b", -1},
		{"a", "A", 1},
		{"abc", "abc", 0},
		{"abc", "ab", 1},
		{"a", "a ", -1},
		{"a ", "a  ", -1},
		{"a\t", "a", 1},
	}
	keyTable := []keyTable{
		{"a", []byte{0x61}},
		{"A", []byte{0x41}},
		{"Foo © bar 𝌆 baz ☃ qux", []byte{0x46, 0x6f, 0x6f, 0x20, 0xc2, 0xa9, 0x20, 0x62, 0x61, 0x72, 0x20, 0xf0,
			0x9d, 0x8c, 0x86, 0x20, 0x62, 0x61, 0x7a, 0x20, 0xe2, 0x98, 0x83, 0x20, 0x71, 0x75, 0x78}},
		{"a ", []byte{0x61, 0x20}},
		{"a", []byte{0x61}},
	}
	testCompareTable(compareTable, "utf8mb4_bin", c)
	testKeyTable(keyTable, "utf8mb4_bin", c)
}

func (s *testCollateSuite) TestBinPaddingCollator(c *C) {
	defer testleak.AfterTest(c)()
	SetNewCollationEnabledForTest(true)
	defer SetNewCollationEnabledForTest(false)
	compareTable := []compareTable{
		{"a", "b", -1},
		{"a", "A", 1},
		{"abc", "abc", 0},
		{"abc", "ab", 1},
		{"a", "a ", 0},
		{"a ", "a  ", 0},
		{"a\t", "a", 1},
	}
	keyTable := []keyTable{
		{"a", []byte{0x61}},
		{"A", []byte{0x41}},
		{"Foo © bar 𝌆 baz ☃ qux", []byte{0x46, 0x6f, 0x6f, 0x20, 0xc2, 0xa9, 0x20, 0x62, 0x61,
			0x72, 0x20, 0xf0, 0x9d, 0x8c, 0x86, 0x20, 0x62, 0x61, 0x7a, 0x20, 0xe2, 0x98, 0x83, 0x20, 0x71, 0x75, 0x78}},
		{"a ", []byte{0x61}},
		{"a", []byte{0x61}},
	}
	testCompareTable(compareTable, "utf8mb4_bin", c)
	testKeyTable(keyTable, "utf8mb4_bin", c)
}

func (s *testCollateSuite) TestGeneralCICollator(c *C) {
	defer testleak.AfterTest(c)()
	SetNewCollationEnabledForTest(true)
	defer SetNewCollationEnabledForTest(false)
	compareTable := []compareTable{
		{"a", "b", -1},
		{"a", "A", 0},
		{"À", "A", 0},
		{"abc", "abc", 0},
		{"abc", "ab", 1},
		{"😜", "😃", 0},
		{"a ", "a  ", 0},
		{"a\t", "a", 1},
	}
	keyTable := []keyTable{
		{"a", []byte{0x0, 0x41}},
		{"A", []byte{0x0, 0x41}},
		{"😃", []byte{0xff, 0xfd}},
		{"Foo © bar 𝌆 baz ☃ qux", []byte{0x0, 0x46, 0x0, 0x4f, 0x0, 0x4f, 0x0, 0x20, 0x0, 0xa9, 0x0, 0x20, 0x0,
			0x42, 0x0, 0x41, 0x0, 0x52, 0x0, 0x20, 0xff, 0xfd, 0x0, 0x20, 0x0, 0x42, 0x0, 0x41, 0x0, 0x5a, 0x0, 0x20, 0x26,
			0x3, 0x0, 0x20, 0x0, 0x51, 0x0, 0x55, 0x0, 0x58}},
		{string([]byte{0x88, 0xe6}), []byte{0xff, 0xfd, 0xff, 0xfd}},
		{"a ", []byte{0x0, 0x41}},
		{"a", []byte{0x0, 0x41}},
	}
	testCompareTable(compareTable, "utf8mb4_general_ci", c)
	testKeyTable(keyTable, "utf8mb4_general_ci", c)
}

func (s *testCollateSuite) TestUnicodeCICollator(c *C) {
	defer testleak.AfterTest(c)()
	SetNewCollationEnabledForTest(true)
	defer SetNewCollationEnabledForTest(false)

	compareTable := []compareTable{
		{"a", "b", -1},
		{"a", "A", 0},
		{"abc", "abc", 0},
		{"abc", "ab", 1},
		{"a", "a ", 0},
		{"a ", "a  ", 0},
		{"😜", "😃", 0},
		{"a\t", "a", 1},
	}
	keyTable := []keyTable{
		{"a", []byte{0x0E, 0x33}},
		{"A", []byte{0x0E, 0x33}},
		{"ß", []byte{0x0F, 0xEA, 0x0F, 0xEA}},
		{"Foo © bar 𝌆 baz ☃ qux", []byte{0x0E, 0xB9, 0x0F, 0x82, 0x0F, 0x82, 0x02, 0x09, 0x02,
			0xC5, 0x02, 0x09, 0x0E, 0x4A, 0x0E, 0x33, 0x0F, 0xC0, 0x02, 0x09, 0xFF, 0xFD, 0x02,
			0x09, 0x0E, 0x4A, 0x0E, 0x33, 0x10, 0x6A, 0x02, 0x09, 0x06, 0xFF, 0x02, 0x09, 0x0F,
			0xB4, 0x10, 0x1F, 0x10, 0x5A}},
		{"a ", []byte{0x0E, 0x33}},
	}

	testCompareTable(compareTable, "utf8mb4_unicode_ci", c)
	testKeyTable(keyTable, "utf8mb4_unicode_ci", c)
}

func BenchmarkUnicodeCICompareSpecial(b *testing.B) {
	SetNewCollationEnabledForTest(true)
	defer SetNewCollationEnabledForTest(false)

	var (
		s1 = `ßßßaaaAAAAßsssssßßßßaAaaaAAAssßßssßßsßAAaa😃☃😃😃`
		s2 = `ssßßAAAAAAAßsßssßssssßaAaAAAAAßßßssßßsßAAaa😜☃😃😜`
	)

	collator := GetCollator("utf8mb4_unicode_ci")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		match := collator.Compare(s1, s2)
		if match != 0 {
			b.Fatal("equal expected.")
		}
	}
}

func BenchmarkUnicodeCIKeySpecial(b *testing.B) {
	SetNewCollationEnabledForTest(true)
	defer SetNewCollationEnabledForTest(false)

	keyTable := []keyTable{
		{"a", []byte{0x0E, 0x33}},
		{"A", []byte{0x0E, 0x33}},
		{"ß", []byte{0x0F, 0xEA, 0x0F, 0xEA}},
		{"Foo © bar 𝌆 baz ☃ qux", []byte{0x0E, 0xB9, 0x0F, 0x82, 0x0F, 0x82, 0x02, 0x09, 0x02,
			0xC5, 0x02, 0x09, 0x0E, 0x4A, 0x0E, 0x33, 0x0F, 0xC0, 0x02, 0x09, 0xFF, 0xFD, 0x02,
			0x09, 0x0E, 0x4A, 0x0E, 0x33, 0x10, 0x6A, 0x02, 0x09, 0x06, 0xFF, 0x02, 0x09, 0x0F,
			0xB4, 0x10, 0x1F, 0x10, 0x5A}},
		{"a ", []byte{0x0E, 0x33}},
	}
	collator := GetCollator("utf8mb4_unicode_ci")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for _, t := range keyTable {
			if !bytes.Equal(collator.Key(t.Str), t.Expect) {
				b.Fatal("equal expected.")
			}
		}
	}
}

func BenchmarkUnicodeCIMatchSpecial(b *testing.B) {
	SetNewCollationEnabledForTest(true)
	defer SetNewCollationEnabledForTest(false)

	var (
		pattern = `㍿%㍿%㍿%㍿%㍿%㍿%㍿%㍿%b`
		target  = `㍿㍿㍿㍿㍿㍿㍿㍿㍿㍿㍿㍿㍿㍿㍿㍿㍿㍿㍿㍿㍿㍿㍿㍿㍿㍿㍿㍿㍿㍿㍿㍿`
		escape  = byte('\\')
	)

	p := GetCollator("utf8mb4_unicode_ci").Pattern()
	p.Compile(pattern, escape)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		match := p.DoMatch(target)
		if match {
			b.Fatal("Unmatch expected.")
		}
	}
}

func (s *testCollateSuite) TestSetNewCollateEnabled(c *C) {
	defer SetNewCollationEnabledForTest(false)

	SetNewCollationEnabledForTest(true)
	c.Assert(NewCollationEnabled(), Equals, true)
}

func (s *testCollateSuite) TestRewriteAndRestoreCollationID(c *C) {
	SetNewCollationEnabledForTest(true)
	c.Assert(RewriteNewCollationIDIfNeeded(5), Equals, int32(-5))
	c.Assert(RewriteNewCollationIDIfNeeded(-5), Equals, int32(-5))
	c.Assert(RestoreCollationIDIfNeeded(-5), Equals, int32(5))
	c.Assert(RestoreCollationIDIfNeeded(5), Equals, int32(5))

	SetNewCollationEnabledForTest(false)
	c.Assert(RewriteNewCollationIDIfNeeded(5), Equals, int32(5))
	c.Assert(RewriteNewCollationIDIfNeeded(-5), Equals, int32(-5))
	c.Assert(RestoreCollationIDIfNeeded(5), Equals, int32(5))
	c.Assert(RestoreCollationIDIfNeeded(-5), Equals, int32(-5))
}

func (s *testCollateSuite) TestGetCollator(c *C) {
	defer testleak.AfterTest(c)()
	SetNewCollationEnabledForTest(true)
	defer SetNewCollationEnabledForTest(false)
	c.Assert(GetCollator("binary"), FitsTypeOf, &binCollator{})
	c.Assert(GetCollator("utf8mb4_bin"), FitsTypeOf, &binPaddingCollator{})
	c.Assert(GetCollator("utf8_bin"), FitsTypeOf, &binPaddingCollator{})
	c.Assert(GetCollator("utf8mb4_general_ci"), FitsTypeOf, &generalCICollator{})
	c.Assert(GetCollator("utf8_general_ci"), FitsTypeOf, &generalCICollator{})
	c.Assert(GetCollator("utf8mb4_unicode_ci"), FitsTypeOf, &unicodeCICollator{})
	c.Assert(GetCollator("utf8_unicode_ci"), FitsTypeOf, &unicodeCICollator{})
	c.Assert(GetCollator("default_test"), FitsTypeOf, &binPaddingCollator{})
	c.Assert(GetCollatorByID(63), FitsTypeOf, &binCollator{})
	c.Assert(GetCollatorByID(46), FitsTypeOf, &binPaddingCollator{})
	c.Assert(GetCollatorByID(83), FitsTypeOf, &binPaddingCollator{})
	c.Assert(GetCollatorByID(45), FitsTypeOf, &generalCICollator{})
	c.Assert(GetCollatorByID(33), FitsTypeOf, &generalCICollator{})
	c.Assert(GetCollatorByID(224), FitsTypeOf, &unicodeCICollator{})
	c.Assert(GetCollatorByID(192), FitsTypeOf, &unicodeCICollator{})
	c.Assert(GetCollatorByID(9999), FitsTypeOf, &binPaddingCollator{})

	SetNewCollationEnabledForTest(false)
	c.Assert(GetCollator("binary"), FitsTypeOf, &binCollator{})
	c.Assert(GetCollator("utf8mb4_bin"), FitsTypeOf, &binCollator{})
	c.Assert(GetCollator("utf8_bin"), FitsTypeOf, &binCollator{})
	c.Assert(GetCollator("utf8mb4_general_ci"), FitsTypeOf, &binCollator{})
	c.Assert(GetCollator("utf8_general_ci"), FitsTypeOf, &binCollator{})
	c.Assert(GetCollator("utf8mb4_unicode_ci"), FitsTypeOf, &binCollator{})
	c.Assert(GetCollator("utf8_unicode_ci"), FitsTypeOf, &binCollator{})
	c.Assert(GetCollator("default_test"), FitsTypeOf, &binCollator{})
	c.Assert(GetCollatorByID(63), FitsTypeOf, &binCollator{})
	c.Assert(GetCollatorByID(46), FitsTypeOf, &binCollator{})
	c.Assert(GetCollatorByID(83), FitsTypeOf, &binCollator{})
	c.Assert(GetCollatorByID(45), FitsTypeOf, &binCollator{})
	c.Assert(GetCollatorByID(33), FitsTypeOf, &binCollator{})
	c.Assert(GetCollatorByID(224), FitsTypeOf, &binCollator{})
	c.Assert(GetCollatorByID(192), FitsTypeOf, &binCollator{})
	c.Assert(GetCollatorByID(9999), FitsTypeOf, &binCollator{})
}
