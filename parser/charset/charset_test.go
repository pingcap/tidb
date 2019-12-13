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

package charset

import (
	"math/rand"
	"testing"

	. "github.com/pingcap/check"
)

func TestT(t *testing.T) {
	CustomVerboseFlag = true
	TestingT(t)
}

var _ = Suite(&testCharsetSuite{})

type testCharsetSuite struct {
}

func testValidCharset(c *C, charset string, collation string, expect bool) {
	b := ValidCharsetAndCollation(charset, collation)
	c.Assert(b, Equals, expect)
}

func (s *testCharsetSuite) TestValidCharset(c *C) {
	tests := []struct {
		cs   string
		co   string
		succ bool
	}{
		{"utf8", "utf8_general_ci", true},
		{"", "utf8_general_ci", true},
		{"utf8mb4", "utf8mb4_bin", true},
		{"latin1", "latin1_bin", true},
		{"utf8", "utf8_invalid_ci", false},
		{"utf16", "utf16_bin", false},
		{"gb2312", "gb2312_chinese_ci", false},
		{"UTF8", "UTF8_BIN", true},
		{"UTF8", "utf8_bin", true},
		{"UTF8MB4", "utf8mb4_bin", true},
		{"UTF8MB4", "UTF8MB4_bin", true},
		{"UTF8MB4", "UTF8MB4_general_ci", true},
		{"Utf8", "uTf8_bIN", true},
	}
	for _, tt := range tests {
		testValidCharset(c, tt.cs, tt.co, tt.succ)
	}
}

func (s *testCharsetSuite) TestGetSupportedCharsets(c *C) {
	charset := &Charset{"test", "test_bin", nil, "Test", 5}
	charsetInfos = append(charsetInfos, charset)
	descs := GetSupportedCharsets()
	c.Assert(len(descs), Equals, len(charsetInfos)-1)
}

func testGetDefaultCollation(c *C, charset string, expectCollation string, succ bool) {
	b, err := GetDefaultCollation(charset)
	if !succ {
		c.Assert(err, NotNil)
		return
	}
	c.Assert(b, Equals, expectCollation)
}

func (s *testCharsetSuite) TestGetDefaultCollation(c *C) {
	tests := []struct {
		cs   string
		co   string
		succ bool
	}{
		{"utf8", "utf8_bin", true},
		{"UTF8", "utf8_bin", true},
		{"utf8mb4", "utf8mb4_bin", true},
		{"ascii", "ascii_bin", true},
		{"binary", "binary", true},
		{"latin1", "latin1_bin", true},
		{"invalid_cs", "", false},
		{"", "utf8_bin", false},
	}
	for _, tt := range tests {
		testGetDefaultCollation(c, tt.cs, tt.co, tt.succ)
	}

	// Test the consistency of collations table and charset desc table
	charset_num := 0
	for _, collate := range collations {
		if collate.IsDefault {
			if desc, ok := charsets[collate.CharsetName]; ok {
				c.Assert(collate.Name, Equals, desc.DefaultCollation)
				charset_num += 1
			}
		}
	}
	c.Assert(charset_num, Equals, len(charsets))
}

func (s *testCharsetSuite) TestSupportedCollations(c *C) {
	// All supportedCollation are defined from their names
	c.Assert(len(supportedCollationNames), Equals, len(supportedCollationNames))

	// The default collations of supported charsets is the subset of supported collations
	errMsg := "Charset [%v] is supported but its default collation [%v] is not."
	for _, desc := range GetSupportedCharsets() {
		found := false
		for _, c := range GetSupportedCollations() {
			if desc.DefaultCollation == c.Name {
				found = true
				break
			}
		}
		c.Assert(found, IsTrue, Commentf(errMsg, desc.Name, desc.DefaultCollation))
	}
}

func (s *testCharsetSuite) TestGetCharsetDesc(c *C) {
	tests := []struct {
		cs     string
		result string
		succ   bool
	}{
		{"utf8", "utf8", true},
		{"UTF8", "utf8", true},
		{"utf8mb4", "utf8mb4", true},
		{"ascii", "ascii", true},
		{"binary", "binary", true},
		{"latin1", "latin1", true},
		{"invalid_cs", "", false},
		{"", "utf8_bin", false},
	}
	for _, tt := range tests {
		desc, err := GetCharsetDesc(tt.cs)
		if !tt.succ {
			c.Assert(err, NotNil)
		} else {
			c.Assert(desc.Name, Equals, tt.result)
		}
	}
}

func (s *testCharsetSuite) TestGetCollationByName(c *C) {

	for _, collation := range collations {
		coll, err := GetCollationByName(collation.Name)
		c.Assert(err, IsNil)
		c.Assert(coll, Equals, collation)
	}

	_, err := GetCollationByName("non_exist")
	c.Assert(err, ErrorMatches, "\\[ddl:1273\\]Unknown collation: 'non_exist'")
}

func BenchmarkGetCharsetDesc(b *testing.B) {
	b.ResetTimer()
	charsets := []string{CharsetUTF8, CharsetUTF8MB4, CharsetASCII, CharsetLatin1, CharsetBin}
	index := rand.Intn(len(charsets))
	cs := charsets[index]

	for i := 0; i < b.N; i++ {
		GetCharsetDesc(cs)
	}
}
