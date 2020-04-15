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

package encrypt

import (
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/v4/util/testleak"
)

func (s *testEncryptSuite) TestSQLDecode(c *C) {
	defer testleak.AfterTest(c)()
	tests := []struct {
		str     string
		passwd  string
		expect  string
		isError bool
	}{
		{"", "", "", false},
		{"pingcap", "1234567890123456", "2C35B5A4ADF391", false},
		{"pingcap", "asdfjasfwefjfjkj", "351CC412605905", false},
		{"pingcap123", "123456789012345678901234", "7698723DC6DFE7724221", false},
		{"pingcap#%$%^", "*^%YTu1234567", "8634B9C55FF55E5B6328F449", false},
		{"pingcap", "", "4A77B524BD2C5C", false},
		{"分布式データベース", "pass1234@#$%%^^&", "80CADC8D328B3026D04FB285F36FED04BBCA0CC685BF78B1E687CE", false},
		{"分布式データベース", "分布式7782734adgwy1242", "0E24CFEF272EE32B6E0BFBDB89F29FB43B4B30DAA95C3F914444BC", false},
		{"pingcap", "密匙", "CE5C02A5010010", false},
		{"pingcap数据库", "数据库passwd12345667", "36D5F90D3834E30E396BE3226E3B4ED3", false},
	}

	for _, t := range tests {
		crypted, err := SQLDecode(t.str, t.passwd)
		if t.isError {
			c.Assert(err, NotNil, Commentf("%v", t))
			continue
		}
		c.Assert(err, IsNil, Commentf("%v", t))
		result := toHex([]byte(crypted))
		c.Assert(result, Equals, t.expect, Commentf("%v", t))
	}
}

func (s *testEncryptSuite) TestSQLEncode(c *C) {
	defer testleak.AfterTest(c)()
	tests := []struct {
		str     string
		passwd  string
		expect  string
		isError bool
	}{
		{"", "", "", false},
		{"pingcap", "1234567890123456", "pingcap", false},
		{"pingcap", "asdfjasfwefjfjkj", "pingcap", false},
		{"pingcap123", "123456789012345678901234", "pingcap123", false},
		{"pingcap#%$%^", "*^%YTu1234567", "pingcap#%$%^", false},
		{"pingcap", "", "pingcap", false},
		{"分布式データベース", "pass1234@#$%%^^&", "分布式データベース", false},
		{"分布式データベース", "分布式7782734adgwy1242", "分布式データベース", false},
		{"pingcap", "密匙", "pingcap", false},
		{"pingcap数据库", "数据库passwd12345667", "pingcap数据库", false},
	}

	for _, t := range tests {
		crypted, err := SQLDecode(t.str, t.passwd)
		c.Assert(err, IsNil)
		uncrypte, err := SQLEncode(crypted, t.passwd)
		if t.isError {
			c.Assert(err, NotNil, Commentf("%v", t))
			continue
		}
		c.Assert(err, IsNil, Commentf("%v", t))
		c.Assert(uncrypte, Equals, t.expect, Commentf("%v", t))
	}
}
