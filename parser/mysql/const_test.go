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

package mysql

import (
	"testing"

	. "github.com/pingcap/check"
)

var _ = Suite(&testConstSuite{})

type testConstSuite struct{}

func TestT(t *testing.T) {
	TestingT(t)
}

func (s *testConstSuite) TestPrivAllConsistency(c *C) {
	// AllPriv in mysql.user columns.
	for priv := PrivilegeType(CreatePriv); priv != AllPriv; priv = priv << 1 {
		_, ok := Priv2UserCol[priv]
		c.Assert(ok, IsTrue, Commentf("priv fail %d", priv))
	}

	for _, v := range AllGlobalPrivs {
		_, ok := Priv2UserCol[v]
		c.Assert(ok, IsTrue)
	}

	c.Assert(len(Priv2UserCol), Equals, len(AllGlobalPrivs)+1)

	for _, v := range Priv2UserCol {
		_, ok := Col2PrivType[v]
		c.Assert(ok, IsTrue)
	}
	for _, v := range Col2PrivType {
		_, ok := Priv2UserCol[v]
		c.Assert(ok, IsTrue)
	}

	c.Assert(len(Priv2Str), Equals, len(Priv2UserCol))
}
