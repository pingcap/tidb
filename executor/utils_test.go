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

package executor_test

import (
	"math/rand"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/executor"
)

type testUtilsSuite struct{}

func (s *testUtilsSuite) TestResourceGroupTagEncoding(c *C) {
	sqlDigest := ""
	tag := executor.EncodeResourceGroupTag(sqlDigest)
	c.Assert(len(tag), Equals, 0)
	decodedSqlDigest, err := executor.DecodeResourceGroupTag(tag)
	c.Assert(err, IsNil)
	c.Assert(len(decodedSqlDigest), Equals, 0)

	sqlDigest = "aa"
	tag = executor.EncodeResourceGroupTag(sqlDigest)
	// version(1) + prefix(1) + length(1) + content(2hex -> 1byte)
	c.Assert(len(tag), Equals, 4)
	decodedSqlDigest, err = executor.DecodeResourceGroupTag(tag)
	c.Assert(err, IsNil)
	c.Assert(decodedSqlDigest, Equals, sqlDigest)

	sqlDigest = genRandHex(64)
	tag = executor.EncodeResourceGroupTag(sqlDigest)
	decodedSqlDigest, err = executor.DecodeResourceGroupTag(tag)
	c.Assert(err, IsNil)
	c.Assert(decodedSqlDigest, Equals, sqlDigest)

	sqlDigest = genRandHex(510)
	tag = executor.EncodeResourceGroupTag(sqlDigest)
	decodedSqlDigest, err = executor.DecodeResourceGroupTag(tag)
	c.Assert(err, IsNil)
	c.Assert(decodedSqlDigest, Equals, sqlDigest)

	// The max supported length is 255 bytes (510 hex digits).
	sqlDigest = genRandHex(512)
	tag = executor.EncodeResourceGroupTag(sqlDigest)
	c.Assert(len(tag), Equals, 0)

	// A hex string can't have odd length.
	sqlDigest = genRandHex(15)
	tag = executor.EncodeResourceGroupTag(sqlDigest)
	c.Assert(len(tag), Equals, 0)

	// Non-hexadecimal character is invalid
	sqlDigest = "aabbccddgg"
	tag = executor.EncodeResourceGroupTag(sqlDigest)
	c.Assert(len(tag), Equals, 0)

	// A tag should start with a supported version
	tag = []byte("\x00")
	_, err = executor.DecodeResourceGroupTag(tag)
	c.Assert(err, NotNil)

	// The fields should have format like `[prefix, length, content...]`, otherwise decoding it should returns error.
	tag = []byte("\x01\x01")
	_, err = executor.DecodeResourceGroupTag(tag)
	c.Assert(err, NotNil)

	tag = []byte("\x01\x01\x02")
	_, err = executor.DecodeResourceGroupTag(tag)
	c.Assert(err, NotNil)

	tag = []byte("\x01\x01\x02AB")
	decodedSqlDigest, err = executor.DecodeResourceGroupTag(tag)
	c.Assert(err, IsNil)
	c.Assert(decodedSqlDigest, Equals, "4142")

	tag = []byte("\x01\x01\x00")
	decodedSqlDigest, err = executor.DecodeResourceGroupTag(tag)
	c.Assert(err, IsNil)
	c.Assert(len(decodedSqlDigest), Equals, 0)

	// Unsupported field
	tag = []byte("\x01\x99")
	decodedSqlDigest, err = executor.DecodeResourceGroupTag(tag)
	c.Assert(err, NotNil)
}

func genRandHex(length int) string {
	const chars = "0123456789abcdef"
	res := make([]byte, length)
	for i := 0; i < length; i++ {
		res[i] = chars[rand.Intn(len(chars))]
	}
	return string(res)
}
