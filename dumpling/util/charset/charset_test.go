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
)

func TestT(t *testing.T) {
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
	testValidCharset(c, "utf8", "utf8_general_ci", true)
	testValidCharset(c, "", "utf8_general_ci", true)
	testValidCharset(c, "latin1", "", true)
	testValidCharset(c, "utf8", "utf8_invalid_ci", false)
	testValidCharset(c, "gb2312", "gb2312_chinese_ci", false)
}

func (s *testCharsetSuite) TestGetAllCharsets(c *C) {
	charset := &Charset{"test", nil, nil, "Test", 5}
	charsetInfos = append(charsetInfos, charset)
	descs := GetAllCharsets()
	c.Assert(len(descs), Equals, len(charsetInfos)-1)
}
