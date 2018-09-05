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
	"crypto/aes"
	"encoding/hex"
	"strings"
	"testing"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/util/testleak"
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
	}

	for _, t := range tests {
		crypted, err := SQLDecode(t.str, t.passwd)
		if t.isError {
			c.Assert(err, NotNil, Commentf("%v", t))
			continue
		}
		c.Assert(err, IsNil, Commentf("%v", t))
		result := toHex(crypted)
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
        }

        for _, t := range tests {
                crypted, err := SQLDecode(t.str, t.passwd)
		uncrypte, err := SQLEncode(crypte,  t.passwd)

                if t.isError {
                        c.Assert(err, NotNil, Commentf("%v", t))
                        continue
                }
                c.Assert(err, IsNil, Commentf("%v", t))
                c.Assert(ncrypte, Equals, t.expect, Commentf("%v", t))
        }
}
