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
	"github.com/pingcap/tidb/sessionctx/variable"
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
	ctx := mock.NewContext()

	dom, err := NewDomain(store, 80*time.Millisecond)
	c.Assert(err, IsNil)
	store = dom.Store()
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

	m, err := dom.Stats()
	c.Assert(err, IsNil)
	c.Assert(m[ddlLastReloadSchemaTS], GreaterEqual, int64(0))
	c.Assert(dom.GetScope("dummy_status"), Equals, variable.DefaultScopeFlag)

	// for setting lease
	lease := 100 * time.Millisecond
	dom.SetLease(lease)
	c.Assert(dd.GetLease(), Equals, lease)
	dom.SetLease(lease)
	c.Assert(dd.GetLease(), Equals, lease)
	dom.SetLease(0 * time.Millisecond)
	c.Assert(dd.GetLease(), Equals, lease)
	dom1, err := NewDomain(store, 0)
	c.Assert(err, IsNil)
	dom1.SetLease(50 * time.Millisecond)
	c.Assert(dom1.DDL().GetLease(), Equals, 0*time.Second)

	// for schemaValidity
	schemaVer, err := dom.SchemaValidity.Check(0, 0)
	c.Assert(err, IsNil)
	dom.SchemaValidity.MockReloadFailed.SetValue(true)
	err = dom.Reload()
	c.Assert(err, NotNil)
	time.Sleep(lease)
	_, err = dom.SchemaValidity.Check(0, 0)
	c.Assert(err, NotNil)
	_, err = dom.SchemaValidity.Check(0, schemaVer)
	c.Assert(err, NotNil)
	dom.SchemaValidity.MockReloadFailed.SetValue(false)
	dom.SchemaValidity.SetExpireInfo(false, 0)
	_, err = dom.SchemaValidity.Check(1, 0)
	c.Assert(err, NotNil)
	schemaVer1, err := dom.SchemaValidity.Check(0, schemaVer)
	c.Assert(err, IsNil)
	err = dom.Reload()
	c.Assert(err, IsNil)
	time.Sleep(lease)
	schemaVer2, err := dom.SchemaValidity.Check(0, 0)
	c.Assert(err, IsNil)
	c.Assert(schemaVer1, Equals, schemaVer2)

	err = store.Close()
	c.Assert(err, IsNil)
}
