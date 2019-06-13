// Copyright 2019 PingCAP, Inc.
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

package executor_test

import (
	. "github.com/pingcap/check"
	"github.com/pingcap/parser/auth"
	plannercore "github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/util/testkit"
)

func (s *testSuite1) TestExplainPriviliges(c *C) {
	se, err := session.CreateSession4Test(s.store)
	c.Assert(err, IsNil)
	c.Assert(se.Auth(&auth.UserIdentity{Username: "root", Hostname: "%"}, nil, nil), IsTrue)
	tk := testkit.NewTestKit(c, s.store)
	tk.Se = se

	tk.MustExec("create database explaindatabase")
	tk.MustExec("use explaindatabase")
	tk.MustExec("create table t (id int)")
	tk.MustExec("create view v as select * from t")
	tk.MustExec(`create user 'explain'@'%'`)
	tk.MustExec(`flush privileges`)

	tk1 := testkit.NewTestKit(c, s.store)
	se, err = session.CreateSession4Test(s.store)
	c.Assert(err, IsNil)
	c.Assert(se.Auth(&auth.UserIdentity{Username: "explain", Hostname: "%"}, nil, nil), IsTrue)
	tk1.Se = se

	tk.MustExec(`grant select on explaindatabase.v to 'explain'@'%'`)
	tk.MustExec(`flush privileges`)
	tk1.MustQuery("show databases").Check(testkit.Rows("INFORMATION_SCHEMA", "explaindatabase"))

	tk1.MustExec("use explaindatabase")
	tk1.MustQuery("select * from v")
	err = tk1.ExecToErr("explain select * from v")
	c.Assert(err.Error(), Equals, plannercore.ErrViewNoExplain.Error())

	tk.MustExec(`grant show view on explaindatabase.v to 'explain'@'%'`)
	tk.MustExec(`flush privileges`)
	tk1.MustQuery("explain select * from v")

	tk.MustExec(`revoke select on explaindatabase.v from 'explain'@'%'`)
	tk.MustExec(`flush privileges`)

	err = tk1.ExecToErr("explain select * from v")
	c.Assert(err.Error(), Equals, plannercore.ErrTableaccessDenied.GenWithStackByArgs("SELECT", "explain", "%", "v").Error())
}
