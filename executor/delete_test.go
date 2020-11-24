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

package executor_test

import (
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/util/testkit"
)

func (s *testSuite4) TestIssue21200(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("drop database if exists TEST1")
	tk.MustExec("create database TEST1")
	tk.MustExec("use TEST1")
	tk.MustExec("create table t(a int)")
	tk.MustExec("create table t1(a int)")
	tk.MustExec("insert into t values(1)")
	tk.MustExec("insert into t1 values(1)")
	tk.MustExec("delete a from t a where exists (select 1 from t1 where t1.a=a.a)")
	tk.MustQuery("select * from t").Check(testkit.Rows())

	tk.MustExec("insert into t values(1), (2)")
	tk.MustExec("insert into t1 values(2)")
	tk.MustExec("prepare stmt from 'delete a from t a where exists (select 1 from t1 where a.a=t1.a and t1.a=?)'")
	tk.MustExec("set @a=1")
	tk.MustExec("execute stmt using @a")
	tk.MustQuery("select * from t").Check(testkit.Rows("2"))
}
