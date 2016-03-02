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

package builtin

import (
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/sessionctx/db"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/util/mock"
)

var _ = Suite(&testBuiltinInfoSuite{})

type testBuiltinInfoSuite struct {
}

func (s *testBuiltinSuite) TestDatabase(c *C) {
	ctx := mock.NewContext()
	v, err := builtinDatabase(nil, ctx)
	c.Assert(err, IsNil)
	c.Assert(v, IsNil)

	db.BindCurrentSchema(ctx, "test")
	v, err = builtinDatabase(nil, ctx)
	c.Assert(err, IsNil)
	c.Assert(v, Equals, "test")
}

func (s *testBuiltinSuite) TestFoundRows(c *C) {
	ctx := mock.NewContext()
	v, err := builtinFoundRows(nil, ctx)
	c.Assert(err, NotNil)

	variable.BindSessionVars(ctx)

	v, err = builtinFoundRows(nil, ctx)
	c.Assert(err, IsNil)
	c.Assert(v, Equals, uint64(0))
}

func (s *testBuiltinSuite) TestUser(c *C) {
	ctx := mock.NewContext()
	variable.BindSessionVars(ctx)
	sessionVars := variable.GetSessionVars(ctx)
	sessionVars.User = "root@localhost"

	v, err := builtinUser(nil, ctx)
	c.Assert(err, IsNil)
	c.Assert(v, Equals, "root@localhost")
}

func (s *testBuiltinSuite) TestCurrentUser(c *C) {
	ctx := mock.NewContext()
	variable.BindSessionVars(ctx)
	sessionVars := variable.GetSessionVars(ctx)
	sessionVars.User = "root@localhost"

	v, err := builtinCurrentUser(nil, ctx)
	c.Assert(err, IsNil)
	c.Assert(v, Equals, "root@localhost")
}

func (s *testBuiltinSuite) TestConnectionID(c *C) {
	ctx := mock.NewContext()
	variable.BindSessionVars(ctx)
	sessionVars := variable.GetSessionVars(ctx)
	sessionVars.ConnectionID = uint64(1)

	v, err := builtinConnectionID(nil, ctx)
	c.Assert(err, IsNil)
	c.Assert(v, Equals, uint64(1))
}

func (s *testBuiltinSuite) TestVersion(c *C) {
	ctx := mock.NewContext()
	v, err := builtinVersion(nil, ctx)
	c.Assert(err, IsNil)
	c.Assert(v, Equals, mysql.ServerVersion)
}
