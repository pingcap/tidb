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

package mysql

import (
	. "github.com/pingcap/check"
)

var _ = Suite(&testSQLErrorSuite{})

type testSQLErrorSuite struct {
}

func (s *testSQLErrorSuite) TestSQLError(c *C) {
	e := NewErrf(ErrNoDB, "no db error")
	c.Assert(len(e.Error()), Greater, 0)

	e = NewErrf(0, "customized error")
	c.Assert(len(e.Error()), Greater, 0)

	e = NewErr(ErrNoDB)
	c.Assert(len(e.Error()), Greater, 0)

	e = NewErr(0, "customized error")
	c.Assert(len(e.Error()), Greater, 0)
}
