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

package domain

import (
	"testing"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/store/localstore"
	"github.com/pingcap/tidb/store/localstore/goleveldb"
	"github.com/pingcap/tidb/util/mock"
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

	ctx := mock.NewContext()

	dom, err := NewDomain(store, 0)
	c.Assert(err, IsNil)
	store = dom.Store()
	dd := dom.DDL()
	c.Assert(dd, NotNil)
	err = dd.CreateSchema(ctx, model.NewCIStr("aaa"))
	c.Assert(err, IsNil)
	is := dom.InfoSchema()
	c.Assert(is, NotNil)
	dom, err = NewDomain(store, 0)
	c.Assert(err, IsNil)

	dom.SetLease(10 * time.Second)

	m, err := dom.Stats()
	c.Assert(err, IsNil)
	c.Assert(m[ddlLastReloadSchemaTS], GreaterEqual, int64(0))

	dom.SetLease(50 * time.Millisecond)
	store.Close()
	time.Sleep(1 * time.Second)
}
