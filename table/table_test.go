// Copyright 2017 PingCAP, Inc.
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

package table

import (
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/store/localstore"
	"github.com/pingcap/tidb/store/localstore/goleveldb"
	"github.com/pingcap/tidb/util/testleak"
)

var _ = Suite(&testTableSuite{})

type testTableSuite struct {
	store kv.Storage
	se    tidb.Session
}

func (ts *testTableSuite) SetUpSuite(c *C) {
	driver := localstore.Driver{Driver: goleveldb.MemoryDriver{}}
	store, err := driver.Open("memory")
	c.Check(err, IsNil)
	ts.store = store
	_, err = tidb.BootstrapSession(store)
	c.Assert(err, IsNil)
	ts.se, err = tidb.CreateSession(ts.store)
	c.Assert(err, IsNil)
}

func (ts *testTableSuite) TestSlice(c *C) {
	defer testleak.AfterTest(c)()

	_, err := ts.se.Execute("CREATE TABLE test.slice (a int primary key auto_increment, b varchar(255) unique)")
	c.Assert(err, IsNil)
	ctx := ts.se.(context.Context)
	c.Assert(ctx.NewTxn(), IsNil)
	dom := sessionctx.GetDomain(ctx)
	tb, err := dom.InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("slice"))

	_, err = ts.se.Execute("CREATE TABLE test.slice2 (a int primary key auto_increment, b varchar(255) unique)")
	c.Assert(err, IsNil)
	ctx2 := ts.se.(context.Context)
	c.Assert(ctx2.NewTxn(), IsNil)
	dom2 := sessionctx.GetDomain(ctx2)
	tb2, err := dom2.InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("slice2"))

	sl := Slice{tb, tb2}
	len := sl.Len()
	c.Assert(len, Equals, 2)
	less := sl.Less(0, 1)
	c.Assert(less, Equals, true)
	sl.Swap(0, 1)
	c.Assert(tb.Meta().ID, Equals, int64(25))
	c.Assert(tb2.Meta().ID, Equals, int64(27))
}
