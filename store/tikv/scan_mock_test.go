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

package tikv

import (
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/kv"
)

type testScanMockSuite struct {
}

var _ = Suite(&testScanMockSuite{})

func (s *testScanMockSuite) TestScanMultipleRegions(c *C) {
	store := NewMockTikvStore().(*tikvStore)
	txn, err := store.Begin()
	c.Assert(err, IsNil)
	for ch := byte('a'); ch <= byte('z'); ch++ {
		err = txn.Set([]byte{ch}, []byte{ch})
		c.Assert(err, IsNil)
	}
	err = txn.Commit()
	c.Assert(err, IsNil)

	txn, err = store.Begin()
	c.Assert(err, IsNil)
	snapshot := newTiKVSnapshot(store, kv.Version{Ver: txn.StartTS()})
	scanner, err := newScanner(snapshot, []byte("a"), 10)
	c.Assert(err, IsNil)
	for ch := byte('a'); ch <= byte('z'); ch++ {
		c.Assert([]byte{ch}, BytesEquals, []byte(scanner.Key()))
		if ch < byte('z') {
			c.Assert(scanner.Next(), IsNil)
		}
	}
	c.Assert(scanner.Next(), NotNil)
	c.Assert(scanner.Valid(), IsFalse)
}
