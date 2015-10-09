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

package meta_test

import (
	"testing"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/store/localstore"
	"github.com/pingcap/tidb/store/localstore/goleveldb"
)

func TestT(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testSuite{})

type testSuite struct {
}

func (*testSuite) TestT(c *C) {
	driver := localstore.Driver{Driver: goleveldb.MemoryDriver{}}
	store, err := driver.Open("memory")
	c.Assert(err, IsNil)
	defer store.Close()

	// For GenID
	txn, err := store.Begin()
	c.Assert(err, IsNil)
	key := []byte(meta.AutoIDKey(1))
	id, err := meta.GenID(txn, key, 1)
	c.Assert(id, Equals, int64(1))
	id, err = meta.GenID(txn, key, 2)
	c.Assert(id, Equals, int64(3))
	id, err = meta.GenID(txn, []byte{}, 1)
	c.Assert(err, NotNil)

	// For DBMetaKey
	mkey := []byte(meta.DBMetaKey(1))
	c.Assert(mkey, DeepEquals, meta.MakeMetaKey("mDB::1"))

	//For AutoIDKey
	mkey = []byte(meta.AutoIDKey(1))
	c.Assert(mkey, DeepEquals, meta.MakeMetaKey("mTable::1_autoID"))
	mkey = []byte(meta.AutoIDKey(0))
	c.Assert(mkey, DeepEquals, meta.MakeMetaKey("mTable::0_autoID"))

	// For GenGlobalID
	id, err = meta.GenGlobalID(store)
	c.Assert(err, IsNil)
	c.Assert(id, Equals, int64(1))
	id, err = meta.GenGlobalID(store)
	c.Assert(err, IsNil)
	c.Assert(id, Equals, int64(2))
}
