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

	"github.com/juju/errors"
	"github.com/ngaut/pools"
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/store/tikv"
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

func mockFactory() (pools.Resource, error) {
	return nil, errors.New("mock factory should not be called")
}

func sysMockFactory(dom *Domain) (pools.Resource, error) {
	return nil, nil
}

func (*testSuite) TestT(c *C) {
	defer testleak.AfterTest(c)()
	store, err := tikv.NewMockTikvStore()
	c.Assert(err, IsNil)
	ddlLease := 80 * time.Millisecond
	dom := NewDomain(store, ddlLease, 0, mockFactory)
	err = dom.Init(ddlLease, sysMockFactory)
	c.Assert(err, IsNil)
	defer func() {
		dom.Close()
	}()
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
	schemaVer := dom.SchemaValidator.(*schemaValidator).latestSchemaVer
	ver, err := store.CurrentVersion()
	c.Assert(err, IsNil)
	ts := ver.Ver

	succ := dom.SchemaValidator.Check(ts, schemaVer, nil)
	c.Assert(succ, Equals, ResultSucc)
	dom.MockReloadFailed.SetValue(true)
	err = dom.Reload()
	c.Assert(err, NotNil)
	succ = dom.SchemaValidator.Check(ts, schemaVer, nil)
	c.Assert(succ, Equals, ResultSucc)
	time.Sleep(lease)

	ver, err = store.CurrentVersion()
	c.Assert(err, IsNil)
	ts = ver.Ver
	succ = dom.SchemaValidator.Check(ts, schemaVer, nil)
	c.Assert(succ, Equals, ResultUnknown)
	dom.MockReloadFailed.SetValue(false)
	err = dom.Reload()
	c.Assert(err, IsNil)
	succ = dom.SchemaValidator.Check(ts, schemaVer, nil)
	c.Assert(succ, Equals, ResultSucc)

	err = store.Close()
	c.Assert(err, IsNil)
}
