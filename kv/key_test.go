// Copyright 2015 PingCAP, Inc.
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

package kv

import (
	"bytes"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/testleak"
	"github.com/pingcap/tidb/util/types"
)

var _ = Suite(&testKeySuite{})

type testKeySuite struct {
}

func (s *testKeySuite) TestPartialNext(c *C) {
	defer testleak.AfterTest(c)()
	// keyA represents a multi column index.
	keyA, err := codec.EncodeValue(nil, types.NewDatum("abc"), types.NewDatum("def"))
	c.Check(err, IsNil)
	keyB, err := codec.EncodeValue(nil, types.NewDatum("abca"), types.NewDatum("def"))

	// We only use first column value to seek.
	seekKey, err := codec.EncodeValue(nil, types.NewDatum("abc"))
	c.Check(err, IsNil)

	nextKey := Key(seekKey).Next()
	cmp := bytes.Compare(nextKey, keyA)
	c.Assert(cmp, Equals, -1)

	// Use next partial key, we can skip all index keys with first column value equal to "abc".
	nextPartialKey := Key(seekKey).PrefixNext()
	cmp = bytes.Compare(nextPartialKey, keyA)
	c.Assert(cmp, Equals, 1)

	cmp = bytes.Compare(nextPartialKey, keyB)
	c.Assert(cmp, Equals, -1)
}
