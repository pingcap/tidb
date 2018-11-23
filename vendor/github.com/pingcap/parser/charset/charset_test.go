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
	"testing"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/util/testleak"
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
	defer testleak.AfterTest(c)()
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
	}
	for _, tt := range tests {
		testValidCharset(c, tt.cs, tt.co, tt.succ)
	}
}

func (s *testCharsetSuite) TestGetAllCharsets(c *C) {
	defer testleak.AfterTest(c)()
	charset := &Charset{"test", "test_bin", nil, "Test", 5}
	charsetInfos = append(charsetInfos, charset)
	descs := GetAllCharsets()
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
	defer testleak.AfterTest(c)()
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
}
