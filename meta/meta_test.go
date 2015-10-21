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

	n, err = t.GetGlobalID()
	c.Assert(err, IsNil)
	c.Assert(n, Equals, int64(1))

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

	dbs, err := t.ListDatabases()
	c.Assert(err, IsNil)
	c.Assert(dbs, DeepEquals, map[int64][]byte{
		1: []byte("aa"),
	})

	err = t.CreateTable(1, 1, []byte("b"))
	c.Assert(err, IsNil)

	n, err = t.GenAutoTableID(1, 1, 10)
	c.Assert(err, IsNil)
	c.Assert(n, Equals, int64(10))

	n, err = t.GetAutoTableID(1, 1)
	c.Assert(err, IsNil)
	c.Assert(n, Equals, int64(10))

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

	tables, err := t.ListTables(1)
	c.Assert(err, IsNil)
	c.Assert(tables, DeepEquals, map[int64][]byte{
		1: []byte("c"),
		2: []byte("bb"),
	})

	err = t.DropTable(1, 2)
	c.Assert(err, IsNil)

	tables, err = t.ListTables(1)
	c.Assert(err, IsNil)
	c.Assert(tables, DeepEquals, map[int64][]byte{
		1: []byte("c"),
	})

	err = t.DropDatabase(1)
	c.Assert(err, IsNil)

	dbs, err = t.ListDatabases()
	c.Assert(err, IsNil)
	c.Assert(dbs, HasLen, 0)

	err = t.Commit()
	c.Assert(err, IsNil)

	fn := func(txn *meta.TMeta) error {
		n, err = txn.GenSchemaVersion()
		c.Assert(err, IsNil)

		var n1 int64
		n1, err = txn.GetSchemaVersion()
		c.Assert(err, IsNil)
		c.Assert(n, Equals, n1)
		return nil
	}

	err = m.RunInNewTxn(false, fn)
	c.Assert(err, IsNil)

	n, err = m.GenGlobalID()
	c.Assert(err, IsNil)
	n1, err := m.GenGlobalID()
	c.Assert(err, IsNil)
	c.Assert(n1, Equals, n+1)
}
