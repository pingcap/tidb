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
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/store/localstore"
	"github.com/pingcap/tidb/store/localstore/goleveldb"
)

func TestT(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testSuite{})

type testSuite struct {
	store kv.Storage
}

func (s *testSuite) TestT(c *C) {
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
	c.Assert(mkey, DeepEquals, meta.MakeMetaKey("mTable::1_auto_id"))
	mkey = []byte(meta.AutoIDKey(0))
	c.Assert(mkey, DeepEquals, meta.MakeMetaKey("mTable::0_auto_id"))

	// For GenGlobalID
	id, err = meta.GenGlobalID(store)
	c.Assert(err, IsNil)
	c.Assert(id, Equals, int64(1))
	id, err = meta.GenGlobalID(store)
	c.Assert(err, IsNil)
	c.Assert(id, Equals, int64(2))
}

func (s *testSuite) TestMeta(c *C) {
	driver := localstore.Driver{Driver: goleveldb.MemoryDriver{}}
	store, err := driver.Open("memory")
	c.Assert(err, IsNil)
	defer store.Close()

	m := meta.NewMeta(store)

	t, err := m.Begin()
	c.Assert(err, IsNil)

	defer t.Rollback()

	n, err := t.GenGlobalID()
	c.Assert(err, IsNil)
	c.Assert(n, Equals, int64(1))

	n, err = t.GenAutoTableID(1, 10)
	c.Assert(err, IsNil)
	c.Assert(n, Equals, int64(10))

	n, err = t.GetSchemaVersion()
	c.Assert(err, IsNil)
	c.Assert(n, Equals, int64(0))

	n, err = t.GenSchemaVersion()
	c.Assert(err, IsNil)
	c.Assert(n, Equals, int64(1))

	n, err = t.GetSchemaVersion()
	c.Assert(err, IsNil)
	c.Assert(n, Equals, int64(1))

	err = t.CreateDatabase(1, []byte("a"))
	c.Assert(err, IsNil)

	err = t.CreateDatabase(1, []byte("a"))
	c.Assert(err, NotNil)

	v, err := t.GetDatabase(1)
	c.Assert(err, IsNil)
	c.Assert(v, DeepEquals, []byte("a"))

	err = t.UpdateDatabase(1, []byte("aa"))
	c.Assert(err, IsNil)

	v, err = t.GetDatabase(1)
	c.Assert(err, IsNil)
	c.Assert(v, DeepEquals, []byte("aa"))

	ids, err := t.ListDatabaseIDs()
	c.Assert(err, IsNil)
	c.Assert(ids, DeepEquals, []int64{1})

	err = t.CreateTable(1, 1, []byte("b"))
	c.Assert(err, IsNil)

	err = t.CreateTable(1, 1, []byte("b"))
	c.Assert(err, NotNil)

	err = t.UpdateTable(1, 1, []byte("c"))
	c.Assert(err, IsNil)

	v, err = t.GetTable(1, 1)
	c.Assert(err, IsNil)
	c.Assert(v, DeepEquals, []byte("c"))

	v, err = t.GetTable(1, 2)
	c.Assert(err, IsNil)
	c.Assert(v, IsNil)

	err = t.CreateTable(1, 2, []byte("bb"))
	c.Assert(err, IsNil)

	ids, err = t.ListTableIDs(1)
	c.Assert(err, IsNil)
	c.Assert(ids, DeepEquals, []int64{1, 2})

	err = t.DropTable(1, 2)
	c.Assert(err, IsNil)

	ids, err = t.ListTableIDs(1)
	c.Assert(err, IsNil)
	c.Assert(ids, DeepEquals, []int64{1})

	err = t.DropDatabase(1)
	c.Assert(err, IsNil)

	ids, err = t.ListDatabaseIDs()
	c.Assert(err, IsNil)
	c.Assert(ids, HasLen, 0)

	err = t.Commit()
	c.Assert(err, IsNil)
}
