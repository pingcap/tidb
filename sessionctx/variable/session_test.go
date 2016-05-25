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
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/util/mock"
)

var _ = Suite(&testSessionSuite{})

type testSessionSuite struct {
}

func (*testSessionSuite) TestSession(c *C) {
	ctx := mock.NewContext()

	variable.BindSessionVars(ctx)

	v := variable.GetSessionVars(ctx)
	c.Assert(v, NotNil)

	// For AffectedRows
	v.AddAffectedRows(1)
	c.Assert(v.AffectedRows, Equals, uint64(1))
	v.AddAffectedRows(1)
	c.Assert(v.AffectedRows, Equals, uint64(2))

	// For FoundRows
	v.AddFoundRows(1)
	c.Assert(v.FoundRows, Equals, uint64(1))
	v.AddFoundRows(1)
	c.Assert(v.FoundRows, Equals, uint64(2))

	// For last insert id
	v.SetLastInsertID(uint64(1))
	c.Assert(v.LastInsertInfo.ID, Equals, uint64(1))
}
