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
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/store/localstore"
	"github.com/pingcap/tidb/store/localstore/goleveldb"
	"github.com/pingcap/tidb/util/mock"
	"github.com/pingcap/tidb/util/testleak"
)

func TestT(t *testing.T) {
	CustomVerboseFlag = true
	TestingT(t)
}

var _ = Suite(&testSuite{})

type testSuite struct {
}

func (*testSuite) TestT(c *C) {
	driver := localstore.Driver{Driver: goleveldb.MemoryDriver{}}
	store, err := driver.Open("memory")
	c.Assert(err, IsNil)
	defer testleak.AfterTest(c)()
	dom, err := NewDomain(store, 80*time.Millisecond)
	c.Assert(err, IsNil)
	store = dom.Store()
	ctx := mock.NewContext()
	ctx.Store = store
	dd := dom.DDL()
	c.Assert(dd, NotNil)
	c.Assert(dd.GetLease(), Equals, 80*time.Millisecond)
	cs := &ast.CharsetOpt{
		Chs: "utf8",
		Col: "utf8_bin",
	}
	err = dd.CreateSchema(ctx, model.NewCIStr("aaa"), cs)
	c.Assert(err, IsNil)
	is := dom.InfoSchema()
	c.Assert(is, NotNil)

	// for setting lease
	lease := 100 * time.Millisecond

	// for schemaValidator
	schemaVer := dom.SchemaValidator.Latest()
	ver, err := store.CurrentVersion()
	c.Assert(err, IsNil)
	ts := ver.Ver

	succ := dom.SchemaValidator.Check(ts, schemaVer)
	c.Assert(succ, IsTrue)
	dom.MockReloadFailed.SetValue(true)
	err = dom.Reload()
	c.Assert(err, NotNil)
	succ = dom.SchemaValidator.Check(ts, schemaVer)
	c.Assert(succ, IsTrue)
	time.Sleep(lease)

	ver, err = store.CurrentVersion()
	c.Assert(err, IsNil)
	ts = ver.Ver
	succ = dom.SchemaValidator.Check(ts, schemaVer)
	c.Assert(succ, IsFalse)
	dom.MockReloadFailed.SetValue(false)
	err = dom.Reload()
	c.Assert(err, IsNil)
	succ = dom.SchemaValidator.Check(ts, schemaVer)
	c.Assert(succ, IsTrue)
	ver, err = store.CurrentVersion()
	c.Assert(err, IsNil)
	succ = dom.SchemaValidator.Check(ver.Ver, schemaVer)
	c.Assert(succ, IsTrue)

	err = store.Close()
	c.Assert(err, IsNil)
}
