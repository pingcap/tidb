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

package plan_test

import (
	"github.com/juju/errors"
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/plan"
	"github.com/pingcap/tidb/terror"
	"github.com/pingcap/tidb/util/testleak"
)

var _ = Suite(&testValidatorSuite{})

type testValidatorSuite struct {
}

func (s *testValidatorSuite) TestValidator(c *C) {
	defer testleak.AfterTest(c)()
	cases := []struct {
		sql       string
		inPrepare bool
		err       error
	}{
		{"select ?", false, parser.ErrSyntax},
		{"select ?", true, nil},
		{"create table t(id int not null auto_increment default 2, key (id))", true,
			errors.New("Invalid default value for 'id'")},
		{"create table t(id int not null default 2 auto_increment, key (id))", true,
			errors.New("Invalid default value for 'id'")},
		{"create table t(id int not null auto_increment)", true,
			errors.New("Incorrect table definition; there can be only one auto column and it must be defined as a key")},
		{"create table t(id int not null auto_increment, c int auto_increment, key (id, c))", true,
			errors.New("Incorrect table definition; there can be only one auto column and it must be defined as a key")},
		{"create table t(id int not null auto_increment, c int, key (c, id))", true,
			errors.New("Incorrect table definition; there can be only one auto column and it must be defined as a key")},
		{"create table t(id decimal auto_increment, key (id))", true,
			errors.New("Incorrect column specifier for column 'id'")},
		{"create table t(id float auto_increment, key (id))", true, nil},
		{"create table t(id int auto_increment) ENGINE=MYISAM", true, nil},
	}
	store, err := tidb.NewStore(tidb.EngineGoLevelDBMemory)
	c.Assert(err, IsNil)
	defer store.Close()
	se, err := tidb.CreateSession(store)
	c.Assert(err, IsNil)
	for _, ca := range cases {
		stmts, err1 := tidb.Parse(se.(context.Context), ca.sql)
		c.Assert(err1, IsNil)
		c.Assert(stmts, HasLen, 1)
		stmt := stmts[0]
		err = plan.Validate(stmt, ca.inPrepare)
		c.Assert(terror.ErrorEqual(err, ca.err), IsTrue)
	}
}
