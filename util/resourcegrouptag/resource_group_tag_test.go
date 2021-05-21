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

package resourcegrouptag

import (
	"crypto/sha256"
	"errors"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tipb/go-tipb"
	"math/rand"
	"testing"

	. "github.com/pingcap/check"
)

type testUtilsSuite struct{}

var _ = Suite(&testUtilsSuite{})

func TestT(t *testing.T) {
	TestingT(t)
}

func (s *testUtilsSuite) TestResourceGroupTagEncoding(c *C) {
	sqlDigest := ""
	tag := EncodeResourceGroupTag(sqlDigest)
	c.Assert(len(tag), Equals, 0)
	decodedSQLDigest, err := DecodeResourceGroupTag(tag)
	c.Assert(err, IsNil)
	c.Assert(len(decodedSQLDigest), Equals, 0)

	sqlDigest = "aa"
	tag = EncodeResourceGroupTag(sqlDigest)
	// version(1) + prefix(1) + length(1) + content(2hex -> 1byte)
	c.Assert(len(tag), Equals, 4)
	decodedSQLDigest, err = DecodeResourceGroupTag(tag)
	c.Assert(err, IsNil)
	c.Assert(decodedSQLDigest, Equals, sqlDigest)

	sqlDigest = genRandHex(64)
	tag = EncodeResourceGroupTag(sqlDigest)
	decodedSQLDigest, err = DecodeResourceGroupTag(tag)
	c.Assert(err, IsNil)
	c.Assert(decodedSQLDigest, Equals, sqlDigest)

	sqlDigest = genRandHex(510)
	tag = EncodeResourceGroupTag(sqlDigest)
	decodedSQLDigest, err = DecodeResourceGroupTag(tag)
	c.Assert(err, IsNil)
	c.Assert(decodedSQLDigest, Equals, sqlDigest)

	// The max supported length is 255 bytes (510 hex digits).
	sqlDigest = genRandHex(512)
	tag = EncodeResourceGroupTag(sqlDigest)
	c.Assert(len(tag), Equals, 0)

	// A hex string can't have odd length.
	sqlDigest = genRandHex(15)
	tag = EncodeResourceGroupTag(sqlDigest)
	c.Assert(len(tag), Equals, 0)

	// Non-hexadecimal character is invalid
	sqlDigest = "aabbccddgg"
	tag = EncodeResourceGroupTag(sqlDigest)
	c.Assert(len(tag), Equals, 0)

	// A tag should start with a supported version
	tag = []byte("\x00")
	_, err = DecodeResourceGroupTag(tag)
	c.Assert(err, NotNil)

	// The fields should have format like `[prefix, length, content...]`, otherwise decoding it should returns error.
	tag = []byte("\x01\x01")
	_, err = DecodeResourceGroupTag(tag)
	c.Assert(err, NotNil)

	tag = []byte("\x01\x01\x02")
	_, err = DecodeResourceGroupTag(tag)
	c.Assert(err, NotNil)

	tag = []byte("\x01\x01\x02AB")
	decodedSQLDigest, err = DecodeResourceGroupTag(tag)
	c.Assert(err, IsNil)
	c.Assert(decodedSQLDigest, Equals, "4142")

	tag = []byte("\x01\x01\x00")
	decodedSQLDigest, err = DecodeResourceGroupTag(tag)
	c.Assert(err, IsNil)
	c.Assert(len(decodedSQLDigest), Equals, 0)

	// Unsupported field
	tag = []byte("\x01\x99")
	_, err = DecodeResourceGroupTag(tag)
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

func genRandDigest(str string) []byte {
	hasher := sha256.New()
	hasher.Write([]byte(str))
	return hasher.Sum(nil)
}

func (s *testUtilsSuite) TestResourceGroupTagEncodingPB(c *C) {
	digest1 := genRandDigest("abc")
	digest2 := genRandDigest("abcdefg")
	// Test for manualEncode
	data := manualEncodeResourceGroupTag(digest1, digest2)
	c.Assert(len(data), Equals, 69)
	sqlDigest, planDigest, err := manualDecodeResourceGroupTag(data)
	c.Assert(err, IsNil)
	c.Assert(sqlDigest, DeepEquals, digest1)
	c.Assert(planDigest, DeepEquals, digest2)

	// Test for protobuf
	resourceTag := &tipb.ResourceGroupTag{
		SqlDigest:  digest1,
		PlanDigest: digest2,
	}
	buf, err := resourceTag.Marshal()
	c.Assert(err, IsNil)
	tag := &tipb.ResourceGroupTag{}
	err = tag.Unmarshal(buf)
	c.Assert(err, IsNil)
	c.Assert(tag.SqlDigest, DeepEquals, digest1)
	c.Assert(tag.PlanDigest, DeepEquals, digest2)
}

func manualEncodeResourceGroupTag(sqlDigest []byte, planDigest []byte) []byte {
	buf := make([]byte, 1, len(sqlDigest)+len(planDigest)+8)
	buf[0] = 1 // version
	if len(sqlDigest) > 0 {
		buf = append(buf, 1) // sql digest flag
		buf = codec.EncodeVarint(buf, int64(len(sqlDigest)))
		buf = append(buf, sqlDigest...)
	}
	buf = append(buf, 2) // plan digest flag
	buf = codec.EncodeVarint(buf, int64(len(planDigest)))
	buf = append(buf, planDigest...)
	return buf
}

func manualDecodeResourceGroupTag(buf []byte) (sqlDigest []byte, planDigest []byte, err error) {
	if len(buf) == 0 {
		return nil, nil, errors.New("invalid")
	}
	if buf[0] != 1 {
		return nil, nil, errors.New("invalid")
	}
	buf = buf[1:]
	var l int64
	for len(buf) > 0 {
		flag := buf[0]
		buf, l, err = codec.DecodeVarint(buf[1:])
		if err != nil {
			return nil, nil, errors.New("invalid")
		}
		if len(buf) < int(l) {
			return nil, nil, errors.New("invalid")
		}
		data := make([]byte, l)
		copy(data, buf[:l])
		buf = buf[l:]
		switch flag {
		case 1: // sql_digest
			sqlDigest = data
		case 2: // plan digest
			planDigest = data
		default:
			return nil, nil, errors.New("invalid")
		}
	}
	return
}

func BenchmarkResourceGroupManualEncode(b *testing.B) {
	digest1 := genRandDigest("abc")
	digest2 := genRandDigest("abcdefg")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		manualEncodeResourceGroupTag(digest1, digest2)
	}
}

func BenchmarkResourceGroupTagPBEncode(b *testing.B) {
	digest1 := genRandDigest("abc")
	digest2 := genRandDigest("abcdefg")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		resourceTag := &tipb.ResourceGroupTag{
			SqlDigest:  digest1,
			PlanDigest: digest2,
		}
		resourceTag.Marshal()
	}
}

func BenchmarkResourceGroupTagManualDecode(b *testing.B) {
	digest1 := genRandDigest("abc")
	digest2 := genRandDigest("abcdefg")
	data := manualEncodeResourceGroupTag(digest1, digest2)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		manualDecodeResourceGroupTag(data)
	}
}

func BenchmarkResourceGroupTagPBDecode(b *testing.B) {
	digest1 := genRandDigest("abc")
	digest2 := genRandDigest("abcdefg")
	resourceTag := &tipb.ResourceGroupTag{
		SqlDigest:  digest1,
		PlanDigest: digest2,
	}
	data, _ := resourceTag.Marshal()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tag := &tipb.ResourceGroupTag{}
		tag.Unmarshal(data)
	}
}
