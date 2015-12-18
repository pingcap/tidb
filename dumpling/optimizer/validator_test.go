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

package optimizer_test

import (
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/optimizer"
	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/terror"
)

var _ = Suite(&testValidatorSuite{})

type testValidatorSuite struct {
}

func (s *testValidatorSuite) TestValidator(c *C) {
	cases := []struct {
		sql       string
		inPrepare bool
		err       error
	}{
		{"select ?", false, parser.ErrSyntax},
		{"select ?", true, nil},
	}
	store, err := tidb.NewStore(tidb.EngineGoLevelDBMemory)
	c.Assert(err, IsNil)
	se, err := tidb.CreateSession(store)
	c.Assert(err, IsNil)
	for _, ca := range cases {
		stmts, err1 := tidb.Parse(se.(context.Context), ca.sql)
		c.Assert(err1, IsNil)
		c.Assert(stmts, HasLen, 1)
		stmt := stmts[0]
		err = optimizer.Validate(stmt, ca.inPrepare)
		c.Assert(terror.ErrorEqual(err, ca.err), IsTrue)
	}
}
