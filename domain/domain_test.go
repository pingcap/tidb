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

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/model"
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

	dom, err := NewDomain(store, 0)
	c.Assert(err, IsNil)
	store = dom.Store()
	dd := dom.DDL()
	c.Assert(dd, NotNil)
	err = dd.CreateSchema(nil, model.NewCIStr("aaa"))
	c.Assert(err, IsNil)
	is := dom.InfoSchema()
	c.Assert(is, NotNil)
	dom, err = NewDomain(store, 0)
	c.Assert(err, IsNil)
}
