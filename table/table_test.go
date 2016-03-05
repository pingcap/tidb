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

package table_test

import (
	"testing"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/sessionctx/db"
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
	var ident = ast.Ident{
		Name: model.NewCIStr("t"),
	}
	c.Assert(ident.String(), Not(Equals), "")
	driver := localstore.Driver{Driver: goleveldb.MemoryDriver{}}
	store, err := driver.Open("memory")
	c.Assert(err, IsNil)
	se, err := tidb.CreateSession(store)
	c.Assert(err, IsNil)
	ctx := se.(context.Context)
	db.BindCurrentSchema(ctx, "test")
	fullIdent := ident.Full(ctx)
	c.Assert(fullIdent.Schema.L, Equals, "test")
	c.Assert(fullIdent.Name.L, Equals, "t")
	c.Assert(fullIdent.String(), Not(Equals), "")
	fullIdent2 := fullIdent.Full(ctx)
	c.Assert(fullIdent2.Schema.L, Equals, fullIdent.Schema.L)
}
