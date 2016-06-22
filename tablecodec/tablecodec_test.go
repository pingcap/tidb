// Copyright 2016 PingCAP, Inc.
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

package tablecodec

import (
	"testing"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/testleak"
)

func TestT(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testTableCodecSuite{})

type testTableCodecSuite struct{}

// TODO: add more tests.
func (s *testTableCodecSuite) TestTableCodec(c *C) {
	defer testleak.AfterTest(c)()
	key := EncodeRowKey(1, codec.EncodeInt(nil, 2))
	h, err := DecodeRowKey(key)
	c.Assert(err, IsNil)
	c.Assert(h, Equals, int64(2))

	key = EncodeColumnKey(1, 2, 3)
	h, err = DecodeRowKey(key)
	c.Assert(err, IsNil)
	c.Assert(h, Equals, int64(2))
}
