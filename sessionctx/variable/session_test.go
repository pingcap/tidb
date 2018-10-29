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

package variable_test

import (
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/util/mock"
)

var _ = Suite(&testSessionSuite{})

type testSessionSuite struct {
}

func (*testSessionSuite) TestSession(c *C) {
	ctx := mock.NewContext()

	ss := ctx.GetSessionVars().StmtCtx
	c.Assert(ss, NotNil)

	// For AffectedRows
	ss.AddAffectedRows(1)
	c.Assert(ss.AffectedRows(), Equals, uint64(1))
	ss.AddAffectedRows(1)
	c.Assert(ss.AffectedRows(), Equals, uint64(2))

	// For FoundRows
	ss.AddFoundRows(1)
	c.Assert(ss.FoundRows(), Equals, uint64(1))
	ss.AddFoundRows(1)
	c.Assert(ss.FoundRows(), Equals, uint64(2))

	// For last insert id
	ctx.GetSessionVars().SetLastInsertID(1)
	c.Assert(ctx.GetSessionVars().LastInsertID, Equals, uint64(1))
}
