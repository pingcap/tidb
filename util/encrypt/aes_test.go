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
	"github.com/pingcap/tidb/v4/util/testleak"
)

var _ = Suite(&testEncryptSuite{})

func TestT(t *testing.T) {
	CustomVerboseFlag = true
	TestingT(t)
}

type testEncryptSuite struct {
}

func toHex(buf []byte) string {
	return strings.ToUpper(hex.EncodeToString(buf))
}

func (s *testEncryptSuite) TestPad(c *C) {
	defer testleak.AfterTest(c)()

	p := []byte{0x0A, 0x0B, 0x0C, 0x0D}
	p, err := PKCS7Pad(p, 8)
	c.Assert(err, IsNil)
	c.Assert(toHex(p), Equals, "0A0B0C0D04040404")

	p = []byte{0x0A, 0x0B, 0x0C, 0x0D, 0x0A, 0x0B, 0x0C, 0x0D}
	p, err = PKCS7Pad(p, 8)
	c.Assert(err, IsNil)
	c.Assert(toHex(p), Equals, "0A0B0C0D0A0B0C0D0808080808080808")

	p = []byte{0x0A, 0x0B, 0x0C, 0x0D}
	p, err = PKCS7Pad(p, 16)
	c.Assert(err, IsNil)
	c.Assert(toHex(p), Equals, "0A0B0C0D0C0C0C0C0C0C0C0C0C0C0C0C")
}

func (s *testEncryptSuite) TestUnpad(c *C) {
	defer testleak.AfterTest(c)()

	// Valid paddings.
	p := []byte{0x0A, 0x0B, 0x0C, 0x0D, 0x04, 0x04, 0x04, 0x04}
	p, err := PKCS7Unpad(p, 8)
	c.Assert(err, IsNil)
	c.Assert(toHex(p), Equals, "0A0B0C0D")

	p = []byte{0x0A, 0x0B, 0x0C, 0x0D, 0x0A, 0x0B, 0x0C, 0x0D, 0x08, 0x08, 0x08, 0x08, 0x08, 0x08, 0x08, 0x08}
	p, err = PKCS7Unpad(p, 8)
	c.Assert(err, IsNil)
	c.Assert(toHex(p), Equals, "0A0B0C0D0A0B0C0D")

	p = []byte{0x0A, 0x0B, 0x0C, 0x0D, 0x0C, 0x0C, 0x0C, 0x0C, 0x0C, 0x0C, 0x0C, 0x0C, 0x0C, 0x0C, 0x0C, 0x0C}
	p, err = PKCS7Unpad(p, 16)
	c.Assert(err, IsNil)
	c.Assert(toHex(p), Equals, "0A0B0C0D")

	p = []byte{0x08, 0x08, 0x08, 0x08, 0x08, 0x08, 0x08, 0x08}
	p, err = PKCS7Unpad(p, 8)
	c.Assert(err, IsNil)
	c.Assert(toHex(p), Equals, "")

	// Invalid padding: incorrect block size
	p = []byte{0x0A, 0x0B, 0x0C, 0x04, 0x04, 0x04, 0x04}
	_, err = PKCS7Unpad(p, 8)
	c.Assert(err, NotNil)

	p = []byte{0x0A, 0x0B, 0x0C, 0x02, 0x03, 0x04, 0x04, 0x04, 0x04}
	_, err = PKCS7Unpad(p, 8)
	c.Assert(err, NotNil)

	p = []byte{}
	_, err = PKCS7Unpad(p, 8)
	c.Assert(err, NotNil)

	// Invalid padding: padding length > block length
	p = []byte{0x0C, 0x0C, 0x0C, 0x0C, 0x0C, 0x0C, 0x0C, 0x09, 0x09, 0x09, 0x09, 0x09, 0x09, 0x09, 0x09, 0x09}
	_, err = PKCS7Unpad(p, 8)
	c.Assert(err, NotNil)

	// Invalid padding: padding length == 0
	p = []byte{0x0C, 0x0C, 0x0C, 0x0C, 0x0C, 0x0C, 0x0C, 0x00}
	//                                                   ^^^^
	_, err = PKCS7Unpad(p, 8)
	c.Assert(err, NotNil)

	// Invalid padding: padding content invalid
	p = []byte{0x0A, 0x0B, 0x0C, 0x0D, 0x0A, 0x0B, 0x0C, 0x0D, 0x04, 0x08, 0x08, 0x08, 0x08, 0x08, 0x08, 0x08}
	//                                                         ^^^^
	_, err = PKCS7Unpad(p, 8)
	c.Assert(err, NotNil)

	// Invalid padding: padding content invalid
	p = []byte{0x03, 0x08, 0x08, 0x08, 0x08, 0x08, 0x08, 0x08}
	//         ^^^^
	_, err = PKCS7Unpad(p, 8)
	c.Assert(err, NotNil)

	// Invalid padding: padding content invalid
	p = []byte{0x0A, 0x0B, 0x0C, 0x0D, 0x04, 0x04, 0x03, 0x04}
	//                                             ^^^^
	_, err = PKCS7Unpad(p, 8)
	c.Assert(err, NotNil)
}

func (s *testEncryptSuite) TestAESECB(c *C) {
	defer testleak.AfterTest(c)()
	var commonInput = []byte{
		0x6b, 0xc1, 0xbe, 0xe2, 0x2e, 0x40, 0x9f, 0x96, 0xe9, 0x3d, 0x7e, 0x11, 0x73, 0x93, 0x17, 0x2a,
		0xae, 0x2d, 0x8a, 0x57, 0x1e, 0x03, 0xac, 0x9c, 0x9e, 0xb7, 0x6f, 0xac, 0x45, 0xaf, 0x8e, 0x51,
		0x30, 0xc8, 0x1c, 0x46, 0xa3, 0x5c, 0xe4, 0x11, 0xe5, 0xfb, 0xc1, 0x19, 0x1a, 0x0a, 0x52, 0xef,
		0xf6, 0x9f, 0x24, 0x45, 0xdf, 0x4f, 0x9b, 0x17, 0xad, 0x2b, 0x41, 0x7b, 0xe6, 0x6c, 0x37, 0x10,
	}
	var commonKey128 = []byte{0x2b, 0x7e, 0x15, 0x16, 0x28, 0xae, 0xd2, 0xa6, 0xab, 0xf7, 0x15, 0x88, 0x09, 0xcf, 0x4f, 0x3c}
	var commonKey192 = []byte{
		0x8e, 0x73, 0xb0, 0xf7, 0xda, 0x0e, 0x64, 0x52, 0xc8, 0x10, 0xf3, 0x2b, 0x80, 0x90, 0x79, 0xe5,
		0x62, 0xf8, 0xea, 0xd2, 0x52, 0x2c, 0x6b, 0x7b,
	}
	var commonKey256 = []byte{
		0x60, 0x3d, 0xeb, 0x10, 0x15, 0xca, 0x71, 0xbe, 0x2b, 0x73, 0xae, 0xf0, 0x85, 0x7d, 0x77, 0x81,
		0x1f, 0x35, 0x2c, 0x07, 0x3b, 0x61, 0x08, 0xd7, 0x2d, 0x98, 0x10, 0xa3, 0x09, 0x14, 0xdf, 0xf4,
	}
	var ecbAESTests = []struct {
		name string
		key  []byte
		in   []byte
		out  []byte
	}{
		// NIST SP 800-38A pp 24-27
		{
			"ECB-AES128",
			commonKey128,
			commonInput,
			[]byte{
				0x3a, 0xd7, 0x7b, 0xb4, 0x0d, 0x7a, 0x36, 0x60, 0xa8, 0x9e, 0xca, 0xf3, 0x24, 0x66, 0xef, 0x97,
				0xf5, 0xd3, 0xd5, 0x85, 0x03, 0xb9, 0x69, 0x9d, 0xe7, 0x85, 0x89, 0x5a, 0x96, 0xfd, 0xba, 0xaf,
				0x43, 0xb1, 0xcd, 0x7f, 0x59, 0x8e, 0xce, 0x23, 0x88, 0x1b, 0x00, 0xe3, 0xed, 0x03, 0x06, 0x88,
				0x7b, 0x0c, 0x78, 0x5e, 0x27, 0xe8, 0xad, 0x3f, 0x82, 0x23, 0x20, 0x71, 0x04, 0x72, 0x5d, 0xd4,
			},
		},
		{
			"ECB-AES192",
			commonKey192,
			commonInput,
			[]byte{
				0xbd, 0x33, 0x4f, 0x1d, 0x6e, 0x45, 0xf2, 0x5f, 0xf7, 0x12, 0xa2, 0x14, 0x57, 0x1f, 0xa5, 0xcc,
				0x97, 0x41, 0x04, 0x84, 0x6d, 0x0a, 0xd3, 0xad, 0x77, 0x34, 0xec, 0xb3, 0xec, 0xee, 0x4e, 0xef,
				0xef, 0x7a, 0xfd, 0x22, 0x70, 0xe2, 0xe6, 0x0a, 0xdc, 0xe0, 0xba, 0x2f, 0xac, 0xe6, 0x44, 0x4e,
				0x9a, 0x4b, 0x41, 0xba, 0x73, 0x8d, 0x6c, 0x72, 0xfb, 0x16, 0x69, 0x16, 0x03, 0xc1, 0x8e, 0x0e,
			},
		},
		{
			"ECB-AES256",
			commonKey256,
			commonInput,
			[]byte{
				0xf3, 0xee, 0xd1, 0xbd, 0xb5, 0xd2, 0xa0, 0x3c, 0x06, 0x4b, 0x5a, 0x7e, 0x3d, 0xb1, 0x81, 0xf8,
				0x59, 0x1c, 0xcb, 0x10, 0xd4, 0x10, 0xed, 0x26, 0xdc, 0x5b, 0xa7, 0x4a, 0x31, 0x36, 0x28, 0x70,
				0xb6, 0xed, 0x21, 0xb9, 0x9c, 0xa6, 0xf4, 0xf9, 0xf1, 0x53, 0xe7, 0xb1, 0xbe, 0xaf, 0xed, 0x1d,
				0x23, 0x30, 0x4b, 0x7a, 0x39, 0xf9, 0xf3, 0xff, 0x06, 0x7d, 0x8d, 0x8f, 0x9e, 0x24, 0xec, 0xc7,
			},
		},
	}

	for _, tt := range ecbAESTests {
		test := tt.name

		cipher, err := aes.NewCipher(tt.key)
		c.Assert(err, IsNil, Commentf("%s: NewCipher(%d bytes) = %s", test, len(tt.key), err))

		encrypter := newECBEncrypter(cipher)
		d := make([]byte, len(tt.in))
		encrypter.CryptBlocks(d, tt.in)
		c.Assert(toHex(tt.out), Equals, toHex(d), Commentf("%s: ECBEncrypter\nhave %x\nwant %x", test, d, tt.out))

		decrypter := newECBDecrypter(cipher)
		p := make([]byte, len(d))
		decrypter.CryptBlocks(p, d)
		c.Assert(toHex(tt.in), Equals, toHex(p), Commentf("%s: ECBDecrypter\nhave %x\nwant %x", test, d, tt.in))
	}
}

func (s *testEncryptSuite) TestAESEncryptWithECB(c *C) {
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
			c.Assert(err, NotNil, Commentf("%v", t))
			continue
		}
		c.Assert(err, IsNil, Commentf("%v", t))
		result := toHex(crypted)
		c.Assert(result, Equals, t.expect, Commentf("%v", t))
	}
}

func (s *testEncryptSuite) TestAESDecryptWithECB(c *C) {
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
		// negtive cases: invalid padding / padding size
		{"", "1234567890123456", "11223344556677112233", true},
		{"", "1234567890123456", "11223344556677112233112233445566", true},
		{"", "1234567890123456", "1122334455667711223311223344556611", true},
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

func (s *testEncryptSuite) TestAESEncryptWithCBC(c *C) {
	defer testleak.AfterTest(c)()
	tests := []struct {
		str     string
		key     string
		iv      string
		expect  string
		isError bool
	}{
		// 128 bits key
		{"pingcap", "1234567890123456", "1234567890123456", "2ECA0077C5EA5768A0485AA522774792", false},
		{"pingcap123", "1234567890123456", "1234567890123456", "042962D340F2F95BCC07B56EAC378D3A", false},
		// 192 bits key
		{"pingcap", "123456789012345678901234", "1234567890123456", "EDECE05D9FE662E381130F7F19BA67F7", false}, // 192 bit
		// negtive cases: invalid key length
		{"pingcap", "12345678901234567", "1234567890123456", "", true},
		{"pingcap", "123456789012345", "1234567890123456", "", true},
	}

	for _, t := range tests {
		str := []byte(t.str)
		key := []byte(t.key)
		iv := []byte(t.iv)

		crypted, err := AESEncryptWithCBC(str, key, iv)
		if t.isError {
			c.Assert(err, NotNil, Commentf("%v", t))
			continue
		}
		c.Assert(err, IsNil, Commentf("%v", t))
		result := toHex(crypted)
		c.Assert(result, Equals, t.expect, Commentf("%v", t))
	}
}

func (s *testEncryptSuite) TestAESEncryptWithOFB(c *C) {
	defer testleak.AfterTest(c)()
	tests := []struct {
		str     string
		key     string
		iv      string
		expect  string
		isError bool
	}{
		// 128 bits key
		{"pingcap", "1234567890123456", "1234567890123456", "0515A36BBF3DE0", false},
		{"pingcap123", "1234567890123456", "1234567890123456", "0515A36BBF3DE0DBE9DD", false},
		// 192 bits key
		{"pingcap", "123456789012345678901234", "1234567890123456", "45A57592449893", false}, // 192 bit
		// negtive cases: invalid key length
		{"pingcap", "12345678901234567", "1234567890123456", "", true},
		{"pingcap", "123456789012345", "1234567890123456", "", true},
	}

	for _, t := range tests {
		str := []byte(t.str)
		key := []byte(t.key)
		iv := []byte(t.iv)

		crypted, err := AESEncryptWithOFB(str, key, iv)
		if t.isError {
			c.Assert(err, NotNil, Commentf("%v", t))
			continue
		}
		c.Assert(err, IsNil, Commentf("%v", t))
		result := toHex(crypted)
		c.Assert(result, Equals, t.expect, Commentf("%v", t))
	}
}

func (s *testEncryptSuite) TestAESDecryptWithOFB(c *C) {
	defer testleak.AfterTest(c)()
	tests := []struct {
		str     string
		key     string
		iv      string
		expect  string
		isError bool
	}{
		// 128 bits key
		{"0515A36BBF3DE0", "1234567890123456", "1234567890123456", "pingcap", false},
		{"0515A36BBF3DE0DBE9DD", "1234567890123456", "1234567890123456", "pingcap123", false},
		// 192 bits key
		{"45A57592449893", "123456789012345678901234", "1234567890123456", "pingcap", false}, // 192 bit
		// negtive cases: invalid key length
		{"pingcap", "12345678901234567", "1234567890123456", "", true},
		{"pingcap", "123456789012345", "1234567890123456", "", true},
	}

	for _, t := range tests {
		str, _ := hex.DecodeString(t.str)
		key := []byte(t.key)
		iv := []byte(t.iv)

		plainText, err := AESDecryptWithOFB(str, key, iv)
		if t.isError {
			c.Assert(err, NotNil, Commentf("%v", t))
			continue
		}
		c.Assert(err, IsNil, Commentf("%v", t))
		c.Assert(string(plainText), Equals, t.expect, Commentf("%v", t))
	}
}

func (s *testEncryptSuite) TestAESDecryptWithCBC(c *C) {
	defer testleak.AfterTest(c)()
	tests := []struct {
		expect      string
		key         string
		iv          string
		hexCryptStr string
		isError     bool
	}{
		// 128 bits key
		{"pingcap", "1234567890123456", "1234567890123456", "2ECA0077C5EA5768A0485AA522774792", false},
		{"pingcap123", "1234567890123456", "1234567890123456", "042962D340F2F95BCC07B56EAC378D3A", false},
		// 192 bits key
		{"pingcap", "123456789012345678901234", "1234567890123456", "EDECE05D9FE662E381130F7F19BA67F7", false}, // 192 bit
		// negtive cases: invalid key length
		{"pingcap", "12345678901234567", "1234567890123456", "", true},
		{"pingcap", "123456789012345", "1234567890123456", "", true},
		// negtive cases: invalid padding / padding size
		{"", "1234567890123456", "1234567890123456", "11223344556677112233", true},
		{"", "1234567890123456", "1234567890123456", "11223344556677112233112233445566", true},
		{"", "1234567890123456", "1234567890123456", "1122334455667711223311223344556611", true},
	}

	for _, t := range tests {
		cryptStr, _ := hex.DecodeString(t.hexCryptStr)
		key := []byte(t.key)
		iv := []byte(t.iv)

		result, err := AESDecryptWithCBC(cryptStr, key, iv)
		if t.isError {
			c.Assert(err, NotNil)
			continue
		}
		c.Assert(err, IsNil)
		c.Assert(string(result), Equals, t.expect)
	}
}

func (s *testEncryptSuite) TestAESEncryptWithCFB(c *C) {
	defer testleak.AfterTest(c)()
	tests := []struct {
		str     string
		key     string
		iv      string
		expect  string
		isError bool
	}{
		// 128 bits key
		{"pingcap", "1234567890123456", "1234567890123456", "0515A36BBF3DE0", false},
		{"pingcap123", "1234567890123456", "1234567890123456", "0515A36BBF3DE0DBE9DD", false},
		// 192 bits key
		{"pingcap", "123456789012345678901234", "1234567890123456", "45A57592449893", false}, // 192 bit
		// negtive cases: invalid key length
		{"pingcap", "12345678901234567", "1234567890123456", "", true},
		{"pingcap", "123456789012345", "1234567890123456", "", true},
	}

	for _, t := range tests {
		str := []byte(t.str)
		key := []byte(t.key)
		iv := []byte(t.iv)

		crypted, err := AESEncryptWithCFB(str, key, iv)
		if t.isError {
			c.Assert(err, NotNil, Commentf("%v", t))
			continue
		}
		c.Assert(err, IsNil, Commentf("%v", t))
		result := toHex(crypted)
		c.Assert(result, Equals, t.expect, Commentf("%v", t))
	}
}

func (s *testEncryptSuite) TestAESDecryptWithCFB(c *C) {
	defer testleak.AfterTest(c)()
	tests := []struct {
		str     string
		key     string
		iv      string
		expect  string
		isError bool
	}{
		// 128 bits key
		{"0515A36BBF3DE0", "1234567890123456", "1234567890123456", "pingcap", false},
		{"0515A36BBF3DE0DBE9DD", "1234567890123456", "1234567890123456", "pingcap123", false},
		// 192 bits key
		{"45A57592449893", "123456789012345678901234", "1234567890123456", "pingcap", false}, // 192 bit
		// negtive cases: invalid key length
		{"pingcap", "12345678901234567", "1234567890123456", "", true},
		{"pingcap", "123456789012345", "1234567890123456", "", true},
	}

	for _, t := range tests {
		str, _ := hex.DecodeString(t.str)
		key := []byte(t.key)
		iv := []byte(t.iv)

		plainText, err := AESDecryptWithCFB(str, key, iv)
		if t.isError {
			c.Assert(err, NotNil, Commentf("%v", t))
			continue
		}
		c.Assert(err, IsNil, Commentf("%v", t))
		c.Assert(string(plainText), Equals, t.expect, Commentf("%v", t))
	}
}

func (s *testEncryptSuite) TestDeriveKeyMySQL(c *C) {
	defer testleak.AfterTest(c)()

	p := []byte("MySQL=insecure! MySQL=insecure! ")
	p = DeriveKeyMySQL(p, 16)
	c.Assert(toHex(p), Equals, "00000000000000000000000000000000")

	// Short password.
	p = []byte{0xC0, 0x10, 0x44, 0xCC, 0x10, 0xD9}
	p = DeriveKeyMySQL(p, 16)
	c.Assert(toHex(p), Equals, "C01044CC10D900000000000000000000")

	// Long password.
	p = []byte("MySecretVeryLooooongPassword")
	p = DeriveKeyMySQL(p, 16)
	c.Assert(toHex(p), Equals, "22163D0233131607210A001D4C6F6F6F")
}
