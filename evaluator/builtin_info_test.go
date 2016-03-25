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

package evaluator

import (
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/sessionctx/db"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/util/mock"
	"github.com/pingcap/tidb/util/types"
)

func (s *testEvaluatorSuite) TestDatabase(c *C) {
	ctx := mock.NewContext()
	d, err := builtinDatabase(types.MakeDatums(), ctx)
	c.Assert(err, IsNil)
	c.Assert(d.Kind(), Equals, types.KindNull)

	db.BindCurrentSchema(ctx, "test")
	d, err = builtinDatabase(types.MakeDatums(), ctx)
	c.Assert(err, IsNil)
	c.Assert(d.GetString(), Equals, "test")
}

func (s *testEvaluatorSuite) TestFoundRows(c *C) {
	ctx := mock.NewContext()
	d, err := builtinFoundRows(types.MakeDatums(), ctx)
	c.Assert(err, NotNil)

	variable.BindSessionVars(ctx)

	d, err = builtinFoundRows(types.MakeDatums(), ctx)
	c.Assert(err, IsNil)
	c.Assert(d.GetUint64(), Equals, uint64(0))
}

func (s *testEvaluatorSuite) TestUser(c *C) {
	ctx := mock.NewContext()
	variable.BindSessionVars(ctx)
	sessionVars := variable.GetSessionVars(ctx)
	sessionVars.User = "root@localhost"

	d, err := builtinUser(types.MakeDatums(), ctx)
	c.Assert(err, IsNil)
	c.Assert(d.GetString(), Equals, "root@localhost")
}

func (s *testEvaluatorSuite) TestCurrentUser(c *C) {
	ctx := mock.NewContext()
	variable.BindSessionVars(ctx)
	sessionVars := variable.GetSessionVars(ctx)
	sessionVars.User = "root@localhost"

	d, err := builtinCurrentUser(types.MakeDatums(), ctx)
	c.Assert(err, IsNil)
	c.Assert(d.GetString(), Equals, "root@localhost")
}

func (s *testEvaluatorSuite) TestConnectionID(c *C) {
	ctx := mock.NewContext()
	variable.BindSessionVars(ctx)
	sessionVars := variable.GetSessionVars(ctx)
	sessionVars.ConnectionID = uint64(1)

	d, err := builtinConnectionID(types.MakeDatums(), ctx)
	c.Assert(err, IsNil)
	c.Assert(d.GetUint64(), Equals, uint64(1))
}

func (s *testEvaluatorSuite) TestVersion(c *C) {
	ctx := mock.NewContext()
	v, err := builtinVersion(nil, ctx)
	c.Assert(err, IsNil)
	c.Assert(v.GetString(), Equals, mysql.ServerVersion)
}
