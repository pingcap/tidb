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

package tikv_test

import (
	"context"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/store/tikv"
)

type testScanMockSuite struct {
	OneByOneSuite
}

var _ = Suite(&testScanMockSuite{})

func (s *testScanMockSuite) TestScanMultipleRegions(c *C) {
	store := tikv.StoreProbe{KVStore: NewTestStore(c)}
	defer store.Close()

	txn, err := store.Begin()
	c.Assert(err, IsNil)
	for ch := byte('a'); ch <= byte('z'); ch++ {
		err = txn.Set([]byte{ch}, []byte{ch})
		c.Assert(err, IsNil)
	}
	err = txn.Commit(context.Background())
	c.Assert(err, IsNil)

	txn, err = store.Begin()
	c.Assert(err, IsNil)
	scanner, err := txn.NewScanner([]byte("a"), nil, 10, false)
	c.Assert(err, IsNil)
	for ch := byte('a'); ch <= byte('z'); ch++ {
		c.Assert([]byte{ch}, BytesEquals, scanner.Key())
		c.Assert(scanner.Next(), IsNil)
	}
	c.Assert(scanner.Valid(), IsFalse)

	scanner, err = txn.NewScanner([]byte("a"), []byte("i"), 10, false)
	c.Assert(err, IsNil)
	for ch := byte('a'); ch <= byte('h'); ch++ {
		c.Assert([]byte{ch}, BytesEquals, scanner.Key())
		c.Assert(scanner.Next(), IsNil)
	}
	c.Assert(scanner.Valid(), IsFalse)
}

func (s *testScanMockSuite) TestReverseScan(c *C) {
	store := tikv.StoreProbe{KVStore: NewTestStore(c)}
	defer store.Close()

	txn, err := store.Begin()
	c.Assert(err, IsNil)
	for ch := byte('a'); ch <= byte('z'); ch++ {
		err = txn.Set([]byte{ch}, []byte{ch})
		c.Assert(err, IsNil)
	}
	err = txn.Commit(context.Background())
	c.Assert(err, IsNil)

	txn, err = store.Begin()
	c.Assert(err, IsNil)
	scanner, err := txn.NewScanner(nil, []byte("z"), 10, true)
	c.Assert(err, IsNil)
	for ch := byte('y'); ch >= byte('a'); ch-- {
		c.Assert(string([]byte{ch}), Equals, string(scanner.Key()))
		c.Assert(scanner.Next(), IsNil)
	}
	c.Assert(scanner.Valid(), IsFalse)

	scanner, err = txn.NewScanner([]byte("a"), []byte("i"), 10, true)
	c.Assert(err, IsNil)
	for ch := byte('h'); ch >= byte('a'); ch-- {
		c.Assert(string([]byte{ch}), Equals, string(scanner.Key()))
		c.Assert(scanner.Next(), IsNil)
	}
	c.Assert(scanner.Valid(), IsFalse)
}
