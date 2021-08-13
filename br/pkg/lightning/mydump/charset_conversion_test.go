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
// See the License for the specific language governing permissions and
// limitations under the License.

package mydump_test

import (
	"io"
	"os"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/br/pkg/lightning/config"
	"github.com/pingcap/tidb/br/pkg/lightning/mydump"
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

func (s testCharsetConvertorSuite) TestCharsetConvertor(c *C) {
	utf8Reader, err := os.Open(testUTF8DataFile)
	c.Assert(err, IsNil)
	originalUTF8Data, err := io.ReadAll(utf8Reader)
	c.Assert(err, IsNil)

	cfg := &config.MydumperRuntime{DataCharacterSet: "gb18030", DataInvalidCharReplace: "\ufffd"}
	gbkReader, err := os.Open(testGBKDataFile)
	c.Assert(err, IsNil)
	cc, err := mydump.NewCharsetConvertor(cfg, gbkReader)
	c.Assert(err, IsNil)
	gbkToUTF8Data := make([]byte, len(originalUTF8Data))
	_, err = cc.Read(gbkToUTF8Data)
	c.Assert(err, IsNil)
	c.Assert(gbkToUTF8Data, DeepEquals, originalUTF8Data)
}

func (s testCharsetConvertorSuite) TestInvalidCharReplace(c *C) {
	dataInvalidCharReplace := "😅😅😅"
	// 你好 in gb18030
	normalCharGB18030 := []byte{0xC4, 0xE3, 0xBA, 0xC3}
	// 你好 in utf8mb4
	normalCharUTF8MB4 := []byte{0xE4, 0xBD, 0xA0, 0xE5, 0xA5, 0xBD}
	// invalid char
	invalidChar := []byte{0xff}
	// Input: 你好invalid char你好
	inputData := append(normalCharGB18030, invalidChar...)
	inputData = append(inputData, normalCharGB18030...)
	// Expect: 你好😅😅😅你好
	expectedData := append(normalCharUTF8MB4, []byte(dataInvalidCharReplace)...)
	expectedData = append(expectedData, normalCharUTF8MB4...)

	// Prepare the file data.
	c.Assert(os.WriteFile(testTempDataFile, inputData, 0666), IsNil)
	defer func() { c.Assert(os.Remove(testTempDataFile), IsNil) }()

	cfg := &config.MydumperRuntime{DataCharacterSet: "gb18030", DataInvalidCharReplace: dataInvalidCharReplace}
	gbkReader, err := os.Open(testTempDataFile)
	c.Assert(err, IsNil)
	cc, err := mydump.NewCharsetConvertor(cfg, gbkReader)
	c.Assert(err, IsNil)
	gbkToUTF8Data := make([]byte, len(expectedData))
	_, err = cc.Read(gbkToUTF8Data)
	c.Assert(err, IsNil)
	c.Assert(gbkToUTF8Data, DeepEquals, expectedData)
}
