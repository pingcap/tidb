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

package rsets

import (
	. "github.com/pingcap/check"
)

var _ = Suite(&testTableRsetFuncSuite{})

type testTableRsetFuncSuite struct {
}

func (s *testTableRsetFuncSuite) TestNewTableRset(c *C) {
	schema := "rset_test"
	name := "rset_table"

	r, err := newTableRset(schema, name)
	c.Assert(err, IsNil)
	c.Assert(r.Schema, Equals, schema)
	c.Assert(r.Name, Equals, name)

	newSchema := schema + "xxx"
	newName := newSchema + "." + name

	r, err = newTableRset(schema, newName)
	c.Assert(err, IsNil)
	c.Assert(r.Schema, Equals, newSchema)
	c.Assert(r.Name, Equals, name)

	badName := newName + ".bad"
	r, err = newTableRset(schema, badName)
	c.Assert(err, NotNil)
}
