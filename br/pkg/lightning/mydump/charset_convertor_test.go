// Copyright 2021 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package mydump

import (
	"io"
	"os"

	. "github.com/pingcap/check"
)

var _ = Suite(&testCharsetConvertorSuite{})

type testCharsetConvertorSuite struct{}

func (s *testCharsetConvertorSuite) SetUpSuite(c *C)    {}
func (s *testCharsetConvertorSuite) TearDownSuite(c *C) {}

const (
	testUTF8DataFile = "./csv/utf8_test_file.csv"
	testGBKDataFile  = "./csv/gb18030_test_file.csv"
	testTempDataFile = "./csv/temp_test_file.csv"
)

var (
	normalCharUTF8MB4 = []byte{0xE4, 0xBD, 0xA0, 0xE5, 0xA5, 0xBD, 0xEF, 0xBC, 0x8C, 0xE4, 0xB8, 0x96, 0xE7, 0x95, 0x8C, 0xEF, 0xBC, 0x81} // “你好，世界！” in utf8mb4
	normalCharGB18030 = []byte{0xC4, 0xE3, 0xBA, 0xC3, 0xA3, 0xAC, 0xCA, 0xC0, 0xBD, 0xE7, 0xA3, 0xA1}                                     // “你好，世界！” in gb18030
	invalidChar       = []byte{0xff}                                                                                                       // Invalid gb18030 char
)

func (s testCharsetConvertorSuite) TestCharsetConvertor(c *C) {
	utf8Reader, err := os.Open(testUTF8DataFile)
	c.Assert(err, IsNil)
	utf8Data, err := io.ReadAll(utf8Reader)
	c.Assert(err, IsNil)
	gbkReader, err := os.Open(testGBKDataFile)
	c.Assert(err, IsNil)
	gbkData, err := io.ReadAll(gbkReader)
	c.Assert(err, IsNil)

	cc, err := NewCharsetConvertor("gb18030", "\ufffd")
	c.Assert(err, IsNil)
	gbkToUTF8Data, err := cc.Decode(string(gbkData))
	c.Assert(err, IsNil)
	c.Assert(gbkToUTF8Data, DeepEquals, string(utf8Data))

	utf8ToGBKData, err := cc.Encode(string(normalCharUTF8MB4))
	c.Assert(err, IsNil)
	c.Assert(utf8ToGBKData, DeepEquals, string(normalCharGB18030))
}

func (s testCharsetConvertorSuite) TestInvalidCharReplace(c *C) {
	dataInvalidCharReplace := "😅😅😅"
	// Input: 你好invalid char你好
	inputData := append(normalCharGB18030, invalidChar...)
	inputData = append(inputData, normalCharGB18030...)
	// Expect: 你好😅😅😅你好
	expectedData := append(normalCharUTF8MB4, []byte(dataInvalidCharReplace)...)
	expectedData = append(expectedData, normalCharUTF8MB4...)

	// Prepare the file data.
	c.Assert(os.WriteFile(testTempDataFile, inputData, 0666), IsNil)
	defer func() { c.Assert(os.Remove(testTempDataFile), IsNil) }()

	gbkReader, err := os.Open(testTempDataFile)
	c.Assert(err, IsNil)
	gbkData, err := io.ReadAll(gbkReader)
	c.Assert(err, IsNil)
	cc, err := NewCharsetConvertor("gb18030", dataInvalidCharReplace)
	c.Assert(err, IsNil)
	gbkToUTF8Data, err := cc.Decode(string(gbkData))
	c.Assert(err, IsNil)
	c.Assert(gbkToUTF8Data, DeepEquals, string(expectedData))
}
