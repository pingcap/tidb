// Copyright 2021 PingCAP, Inc.
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

package infoschema_test

import (
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/infoschema"
)

var _ = Suite(&testInfoCacheSuite{})

type testInfoCacheSuite struct {
}

func (s *testInfoCacheSuite) TestNewCache(c *C) {
	ic := infoschema.NewCache(16)
	c.Assert(ic, NotNil)
}

func (s *testInfoCacheSuite) TestInsert(c *C) {
	ic := infoschema.NewCache(3)
	c.Assert(ic, NotNil)

	is2 := infoschema.MockInfoSchemaWithSchemaVer(nil, 2)
	ic.Insert(is2)
	c.Assert(ic.GetByVersion(2), NotNil)

	// newer
	is5 := infoschema.MockInfoSchemaWithSchemaVer(nil, 5)
	ic.Insert(is5)
	c.Assert(ic.GetByVersion(5), NotNil)
	c.Assert(ic.GetByVersion(2), NotNil)

	// older
	is0 := infoschema.MockInfoSchemaWithSchemaVer(nil, 0)
	ic.Insert(is0)
	c.Assert(ic.GetByVersion(5), NotNil)
	c.Assert(ic.GetByVersion(2), NotNil)
	c.Assert(ic.GetByVersion(0), NotNil)

	// replace 5, drop 0
	is6 := infoschema.MockInfoSchemaWithSchemaVer(nil, 6)
	ic.Insert(is6)
	c.Assert(ic.GetByVersion(6), NotNil)
	c.Assert(ic.GetByVersion(5), NotNil)
	c.Assert(ic.GetByVersion(2), NotNil)
	c.Assert(ic.GetByVersion(0), IsNil)

	// replace 2, drop 2
	is3 := infoschema.MockInfoSchemaWithSchemaVer(nil, 3)
	ic.Insert(is3)
	c.Assert(ic.GetByVersion(6), NotNil)
	c.Assert(ic.GetByVersion(5), NotNil)
	c.Assert(ic.GetByVersion(3), NotNil)
	c.Assert(ic.GetByVersion(2), IsNil)
	c.Assert(ic.GetByVersion(0), IsNil)

	// insert 2, but failed silently
	ic.Insert(is2)
	c.Assert(ic.GetByVersion(6), NotNil)
	c.Assert(ic.GetByVersion(5), NotNil)
	c.Assert(ic.GetByVersion(3), NotNil)
	c.Assert(ic.GetByVersion(2), IsNil)
	c.Assert(ic.GetByVersion(0), IsNil)

	// insert 5, but it is already in
	ic.Insert(is5)
	c.Assert(ic.GetByVersion(6), NotNil)
	c.Assert(ic.GetByVersion(5), NotNil)
	c.Assert(ic.GetByVersion(3), NotNil)
	c.Assert(ic.GetByVersion(2), IsNil)
	c.Assert(ic.GetByVersion(0), IsNil)
}

func (s *testInfoCacheSuite) TestGetByVersion(c *C) {
	ic := infoschema.NewCache(2)
	c.Assert(ic, NotNil)
	is1 := infoschema.MockInfoSchemaWithSchemaVer(nil, 1)
	ic.Insert(is1)
	is3 := infoschema.MockInfoSchemaWithSchemaVer(nil, 3)
	ic.Insert(is3)

	c.Assert(ic.GetByVersion(1), Equals, is1)
	c.Assert(ic.GetByVersion(3), Equals, is3)
	c.Assert(ic.GetByVersion(0), IsNil, Commentf("index == 0, but not found"))
	c.Assert(ic.GetByVersion(2), IsNil, Commentf("index in the middle, but not found"))
	c.Assert(ic.GetByVersion(4), IsNil, Commentf("index == length, but not found"))
}

func (s *testInfoCacheSuite) TestGetLatest(c *C) {
	ic := infoschema.NewCache(16)
	c.Assert(ic, NotNil)
	c.Assert(ic.GetLatest(), IsNil)

	is1 := infoschema.MockInfoSchemaWithSchemaVer(nil, 1)
	ic.Insert(is1)
	c.Assert(ic.GetLatest(), Equals, is1)

	// newer change the newest
	is2 := infoschema.MockInfoSchemaWithSchemaVer(nil, 2)
	ic.Insert(is2)
	c.Assert(ic.GetLatest(), Equals, is2)

	// older schema doesn't change the newest
	is0 := infoschema.MockInfoSchemaWithSchemaVer(nil, 0)
	ic.Insert(is0)
	c.Assert(ic.GetLatest(), Equals, is2)
}
