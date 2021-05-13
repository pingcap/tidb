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

package mysql

import (
	. "github.com/pingcap/check"
)

var _ = Suite(&testPrivsSuite{})

type testPrivsSuite struct{}

func (s *testPrivsSuite) TestPrivString(c *C) {
	for i := 0; ; i++ {
		p := PrivilegeType(1 << i)
		if p > AllPriv {
			break
		}
		c.Assert(p.String(), Not(Equals), "", Commentf("%d-th", i))
	}
}

func (s *testPrivsSuite) TestPrivColumn(c *C) {
	for _, p := range AllGlobalPrivs {
		c.Assert(p.ColumnString(), Not(Equals), "", Commentf("%s", p))
		np, ok := NewPrivFromColumn(p.ColumnString())
		c.Assert(ok, IsTrue, Commentf("%s", p))
		c.Assert(np, Equals, p)
	}
	for _, p := range StaticGlobalOnlyPrivs {
		c.Assert(p.ColumnString(), Not(Equals), "", Commentf("%s", p))
		np, ok := NewPrivFromColumn(p.ColumnString())
		c.Assert(ok, IsTrue, Commentf("%s", p))
		c.Assert(np, Equals, p)
	}
	for _, p := range AllDBPrivs {
		c.Assert(p.ColumnString(), Not(Equals), "", Commentf("%s", p))
		np, ok := NewPrivFromColumn(p.ColumnString())
		c.Assert(ok, IsTrue, Commentf("%s", p))
		c.Assert(np, Equals, p)
	}
}

func (s *testPrivsSuite) TestPrivSetString(c *C) {
	for _, p := range AllTablePrivs {
		c.Assert(p.SetString(), Not(Equals), "", Commentf("%s", p))
		np, ok := NewPrivFromSetEnum(p.SetString())
		c.Assert(ok, IsTrue, Commentf("%s", p))
		c.Assert(np, Equals, p)
	}
	for _, p := range AllColumnPrivs {
		c.Assert(p.SetString(), Not(Equals), "", Commentf("%s", p))
		np, ok := NewPrivFromSetEnum(p.SetString())
		c.Assert(ok, IsTrue, Commentf("%s", p))
		c.Assert(np, Equals, p)
	}
}

func (s *testPrivsSuite) TestPrivsHas(c *C) {
	// it is a simple helper, does not handle all&dynamic privs
	privs := Privileges{AllPriv}
	c.Assert(privs.Has(AllPriv), IsTrue)
	c.Assert(privs.Has(InsertPriv), IsFalse)

	// multiple privs
	privs = Privileges{InsertPriv, SelectPriv}
	c.Assert(privs.Has(SelectPriv), IsTrue)
	c.Assert(privs.Has(InsertPriv), IsTrue)
	c.Assert(privs.Has(DropPriv), IsFalse)
}

func (s *testPrivsSuite) TestPrivAllConsistency(c *C) {
	// AllPriv in mysql.user columns.
	for priv := PrivilegeType(CreatePriv); priv != AllPriv; priv = priv << 1 {
		_, ok := Priv2UserCol[priv]
		c.Assert(ok, IsTrue, Commentf("priv fail %d", priv))
	}

	c.Assert(len(Priv2UserCol), Equals, len(AllGlobalPrivs)+1)

	// USAGE privilege doesn't have a column in Priv2UserCol
	// ALL privilege doesn't have a column in Priv2UserCol
	// so it's +2
	c.Assert(len(Priv2Str), Equals, len(Priv2UserCol)+2)
}
