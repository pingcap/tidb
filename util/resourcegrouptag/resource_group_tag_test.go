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
	"math/rand"
	"testing"

	. "github.com/pingcap/check"
	"github.com/pingcap/parser"
	"github.com/pingcap/tidb/util/hack"
	"github.com/pingcap/tipb/go-tipb"
)

type testUtilsSuite struct{}

var _ = Suite(&testUtilsSuite{})

func TestT(t *testing.T) {
	TestingT(t)
}

func (s *testUtilsSuite) TestResourceGroupTagEncoding(c *C) {
	sqlDigest := parser.NewDigest(nil)
	tag := EncodeResourceGroupTag(sqlDigest, nil)
	c.Assert(len(tag), Equals, 0)
	decodedSQLDigest, err := DecodeResourceGroupTag(tag)
	c.Assert(err, IsNil)
	c.Assert(len(decodedSQLDigest), Equals, 0)

	sqlDigest = parser.NewDigest([]byte{'a', 'a'})
	tag = EncodeResourceGroupTag(sqlDigest, nil)
	// version(1) + prefix(1) + length(1) + content(2hex -> 1byte)
	c.Assert(len(tag), Equals, 4)
	decodedSQLDigest, err = DecodeResourceGroupTag(tag)
	c.Assert(err, IsNil)
	c.Assert(decodedSQLDigest, DeepEquals, sqlDigest.Bytes())

	sqlDigest = parser.NewDigest(genRandHex(64))
	tag = EncodeResourceGroupTag(sqlDigest, nil)
	decodedSQLDigest, err = DecodeResourceGroupTag(tag)
	c.Assert(err, IsNil)
	c.Assert(decodedSQLDigest, DeepEquals, sqlDigest.Bytes())

	sqlDigest = parser.NewDigest(genRandHex(510))
	tag = EncodeResourceGroupTag(sqlDigest, nil)
	decodedSQLDigest, err = DecodeResourceGroupTag(tag)
	c.Assert(err, IsNil)
	c.Assert(decodedSQLDigest, DeepEquals, sqlDigest.Bytes())
}

func genRandHex(length int) []byte {
	const chars = "0123456789abcdef"
	res := make([]byte, length)
	for i := 0; i < length; i++ {
		res[i] = chars[rand.Intn(len(chars))]
	}
	return res
}

func genDigest(str string) []byte {
	hasher := sha256.New()
	hasher.Write(hack.Slice(str))
	return hasher.Sum(nil)
}

func (s *testUtilsSuite) TestResourceGroupTagEncodingPB(c *C) {
	digest1 := genDigest("abc")
	digest2 := genDigest("abcdefg")
	// Test for protobuf
	resourceTag := &tipb.ResourceGroupTag{
		SqlDigest:  digest1,
		PlanDigest: digest2,
	}
	buf, err := resourceTag.Marshal()
	c.Assert(err, IsNil)
	c.Assert(len(buf), Equals, 68)
	tag := &tipb.ResourceGroupTag{}
	err = tag.Unmarshal(buf)
	c.Assert(err, IsNil)
	c.Assert(tag.SqlDigest, DeepEquals, digest1)
	c.Assert(tag.PlanDigest, DeepEquals, digest2)

	// Test for protobuf sql_digest only
	resourceTag = &tipb.ResourceGroupTag{
		SqlDigest: digest1,
	}
	buf, err = resourceTag.Marshal()
	c.Assert(err, IsNil)
	c.Assert(len(buf), Equals, 34)
	tag = &tipb.ResourceGroupTag{}
	err = tag.Unmarshal(buf)
	c.Assert(err, IsNil)
	c.Assert(tag.SqlDigest, DeepEquals, digest1)
	c.Assert(tag.PlanDigest, IsNil)
}
