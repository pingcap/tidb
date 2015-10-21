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

package autoid_test

import (
	"testing"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/meta/autoid"
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

	m := meta.NewMeta(store)
	err = m.RunInNewTxn(false, func(txn *meta.TMeta) error {
		err = txn.CreateDatabase(1, []byte("a"))
		c.Assert(err, IsNil)
		err = txn.CreateTable(1, 1, []byte("b"))
		c.Assert(err, IsNil)
		return nil
	})
	c.Assert(err, IsNil)

	alloc := autoid.NewAllocator(m, 1)
	c.Assert(alloc, NotNil)

	id, err := alloc.Alloc(1)
	c.Assert(err, IsNil)
	c.Assert(id, Equals, int64(1))
	id, err = alloc.Alloc(1)
	c.Assert(err, IsNil)
	c.Assert(id, Equals, int64(2))
	id, err = alloc.Alloc(0)
	c.Assert(err, NotNil)
}
