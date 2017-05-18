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
	"encoding/hex"
	"strings"
	"testing"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/util/testleak"
)

var _ = Suite(&testEncryptSuite{})

func TestT(t *testing.T) {
	CustomVerboseFlag = true
	TestingT(t)
}

type testEncryptSuite struct {
}

func (s *testEncryptSuite) TestAESEncryptWith(c *C) {
	defer testleak.AfterTest(c)()
	tests := []struct {
		str     string
		key     string
		expect  string
		isError bool
	}{
		// 128 bits key
		{"pingcap", "1234567890123456", "697BFE9B3F8C2F289DD82C88C7BC95C4", false},
		{"pingcap123", "1234567890123456", "CEC348F4EF5F84D3AA6C4FA184C65766", false},
		// 192 bits key
		{"pingcap", "123456789012345678901234", "E435438AC6798B4718533096436EC342", false}, // 192 bit
		// negtive cases: invalid key length
		{"pingcap", "12345678901234567", "", true},
		{"pingcap", "123456789012345", "", true},
	}

	for _, t := range tests {
		str := []byte(t.str)
		key := []byte(t.key)

		crypted, err := AESEncryptWithECB(str, key)
		if t.isError {
			c.Assert(err, NotNil)
			continue
		}
		c.Assert(err, IsNil)
		result := strings.ToUpper(hex.EncodeToString(crypted))
		c.Assert(result, Equals, t.expect)
	}
}

func (s *testEncryptSuite) TestAESDecryptWith(c *C) {
	defer testleak.AfterTest(c)()
	tests := []struct {
		expect      string
		key         string
		hexCryptStr string
		isError     bool
	}{
		// 128 bits key
		{"pingcap", "1234567890123456", "697BFE9B3F8C2F289DD82C88C7BC95C4", false},
		{"pingcap123", "1234567890123456", "CEC348F4EF5F84D3AA6C4FA184C65766", false},
		// 192 bits key
		{"pingcap", "123456789012345678901234", "E435438AC6798B4718533096436EC342", false}, // 192 bit
		// negtive cases: invalid key length
		{"pingcap", "12345678901234567", "", true},
		{"pingcap", "123456789012345", "", true},
	}

	for _, t := range tests {
		cryptStr, _ := hex.DecodeString(t.hexCryptStr)
		key := []byte(t.key)

		result, err := AESDecryptWithECB(cryptStr, key)
		if t.isError {
			c.Assert(err, NotNil)
			continue
		}
		c.Assert(err, IsNil)
		c.Assert(string(result), Equals, t.expect)
	}
}
