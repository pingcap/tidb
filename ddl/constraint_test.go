// Copyright 2020 PingCAP, Inc.
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

package ddl_test

import (
	. "github.com/pingcap/check"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb/util/testkit"
)

var _ = Suite(&testCheckConstraintSuite{&testDBSuite{}})

type testCheckConstraintSuite struct{ *testDBSuite }

func (s *testSequenceSuite) TestCreateTableWithCheckConstraints(c *C) {
	s.tk = testkit.NewTestKit(c, s.store)
	s.tk.MustExec("use test")
	s.tk.MustExec("drop table if exists t")

	// Test column-type check constraint.
	s.tk.MustExec("create table t(a int not null check(a>0))")
	constraintTable := testGetTableByName(c, s.s, "test", "t")
	c.Assert(len(constraintTable.Meta().Columns), Equals, 1)
	c.Assert(len(constraintTable.Meta().Constraints), Equals, 1)
	constrs := constraintTable.Meta().Constraints
	c.Assert(constrs[0].ID, Equals, int64(1))
	c.Assert(constrs[0].InColumn, Equals, true)
	c.Assert(constrs[0].Enforced, Equals, true)
	c.Assert(constrs[0].Table.L, Equals, "t")
	c.Assert(constrs[0].State, Equals, model.StatePublic)
	c.Assert(len(constrs[0].ConstraintCols), Equals, 1)
	c.Assert(constrs[0].ConstraintCols[0], Equals, model.NewCIStr("a"))
	c.Assert(constrs[0].Name, Equals, model.NewCIStr("t_chk_1"))
	c.Assert(constrs[0].ExprString, Equals, "`a` > 0")

	s.tk.MustExec("drop table t")
	s.tk.MustExec("create table t(a bigint key constraint my_constr check(a<10), b int constraint check(b > 1) not enforced)")
	constraintTable = testGetTableByName(c, s.s, "test", "t")
	c.Assert(len(constraintTable.Meta().Columns), Equals, 2)
	c.Assert(len(constraintTable.Meta().Constraints), Equals, 2)
	constrs = constraintTable.Meta().Constraints
	c.Assert(constrs[0].ID, Equals, int64(1))
	c.Assert(constrs[0].InColumn, Equals, true)
	c.Assert(constrs[0].Enforced, Equals, true)
	c.Assert(constrs[0].Table.L, Equals, "t")
	c.Assert(constrs[0].State, Equals, model.StatePublic)
	c.Assert(len(constrs[0].ConstraintCols), Equals, 1)
	c.Assert(constrs[0].ConstraintCols[0], Equals, model.NewCIStr("a"))
	c.Assert(constrs[0].Name, Equals, model.NewCIStr("my_constr"))
	c.Assert(constrs[0].ExprString, Equals, "`a` < 10")

	c.Assert(constrs[1].ID, Equals, int64(2))
	c.Assert(constrs[1].InColumn, Equals, true)
	c.Assert(constrs[1].Enforced, Equals, false)
	c.Assert(constrs[1].Table.L, Equals, "t")
	c.Assert(constrs[1].State, Equals, model.StatePublic)
	c.Assert(len(constrs[1].ConstraintCols), Equals, 1)
	c.Assert(constrs[1].ConstraintCols[0], Equals, model.NewCIStr("b"))
	c.Assert(constrs[1].Name, Equals, model.NewCIStr("t_chk_1"))
	c.Assert(constrs[1].ExprString, Equals, "`b` > 1")

	// Test table-type check constraint.
	s.tk.MustExec("drop table t")
	s.tk.MustExec("create table t(a int constraint check(a > 1) not enforced, constraint my_constr check(a < 10))")
	constraintTable = testGetTableByName(c, s.s, "test", "t")
	c.Assert(len(constraintTable.Meta().Columns), Equals, 1)
	c.Assert(len(constraintTable.Meta().Constraints), Equals, 2)
	constrs = constraintTable.Meta().Constraints
	// table-type check constraint.
	c.Assert(constrs[0].ID, Equals, int64(1))
	c.Assert(constrs[0].InColumn, Equals, false)
	c.Assert(constrs[0].Enforced, Equals, true)
	c.Assert(constrs[0].Table.L, Equals, "t")
	c.Assert(constrs[0].State, Equals, model.StatePublic)
	c.Assert(len(constrs[0].ConstraintCols), Equals, 1)
	c.Assert(constrs[0].ConstraintCols[0], Equals, model.NewCIStr("a"))
	c.Assert(constrs[0].Name, Equals, model.NewCIStr("my_constr"))
	c.Assert(constrs[0].ExprString, Equals, "`a` < 10")

	// column-type check constraint.
	c.Assert(constrs[1].ID, Equals, int64(2))
	c.Assert(constrs[1].InColumn, Equals, true)
	c.Assert(constrs[1].Enforced, Equals, false)
	c.Assert(constrs[1].Table.L, Equals, "t")
	c.Assert(constrs[1].State, Equals, model.StatePublic)
	c.Assert(len(constrs[1].ConstraintCols), Equals, 1)
	c.Assert(constrs[1].ConstraintCols[0], Equals, model.NewCIStr("a"))
	c.Assert(constrs[1].Name, Equals, model.NewCIStr("t_chk_1"))
	c.Assert(constrs[1].ExprString, Equals, "`a` > 1")

	// Test column-type check constraint fail on dependency.
	s.tk.MustExec("drop table t")
	_, err := s.tk.Exec("create table t(a int not null check(b>0))")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "[ddl:3813]Column check constraint 't_chk_1' references other column.")

	_, err = s.tk.Exec("create table t(a int not null check(b>a))")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "[ddl:3813]Column check constraint 't_chk_1' references other column.")

	_, err = s.tk.Exec("create table t(a int not null check(a>0), b int, constraint check(c>b))")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "[ddl:3820]Check constraint 't_chk_1' refers to non-existing column 'c'.")

	s.tk.MustExec("create table t(a int not null check(a>0), b int, constraint check(a>b))")
	s.tk.MustExec("drop table t")

	s.tk.MustExec("create table t(a int not null check(a > '12345'))")
	s.tk.MustExec("drop table t")

	s.tk.MustExec("create table t(a int not null primary key check(a > '12345'))")
	s.tk.MustExec("drop table t")

	s.tk.MustExec("create table t(a varchar(10) not null primary key check(a > '12345'))")
	s.tk.MustExec("drop table t")
}
