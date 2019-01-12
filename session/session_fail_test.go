// Copyright 2018 PingCAP, Inc.
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

package session_test

import (
	. "github.com/pingcap/check"
	gofail "github.com/pingcap/gofail/runtime"
	"github.com/pingcap/tidb/util/testkit"
)

func (s *testSessionSuite) TestFailStatementCommit(c *C) {

	tk := testkit.NewTestKitWithInit(c, s.store)
	tk.MustExec("create table t (id int)")
	tk.MustExec("begin")
	tk.MustExec("insert into t values (1)")

	gofail.Enable("github.com/pingcap/tidb/session/mockStmtCommitError", `return(true)`)
	_, err := tk.Exec("insert into t values (2),(3),(4),(5)")
	c.Assert(err, NotNil)

	gofail.Disable("github.com/pingcap/tidb/session/mockStmtCommitError")

	_, err = tk.Exec("select * from t")
	c.Assert(err, NotNil)
	_, err = tk.Exec("insert into t values (3)")
	c.Assert(err, NotNil)
	_, err = tk.Exec("insert into t values (4)")
	c.Assert(err, NotNil)
	_, err = tk.Exec("commit")
	c.Assert(err, NotNil)

	tk.MustQuery(`select * from t`).Check(testkit.Rows())

	tk.MustExec("insert into t values (1)")

	tk.MustExec("begin")
	tk.MustExec("insert into t values (2)")
	tk.MustExec("commit")

	tk.MustExec("begin")
	tk.MustExec("insert into t values (3)")
	tk.MustExec("rollback")

	tk.MustQuery(`select * from t`).Check(testkit.Rows("1", "2"))
}

func (s *testSessionSuite) TestFailStatementCommitInRetry(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
	tk.MustExec("create table t (id int)")

	tk.MustExec("begin")
	tk.MustExec("insert into t values (1)")
	tk.MustExec("insert into t values (2),(3),(4),(5)")
	tk.MustExec("insert into t values (6)")

	gofail.Enable("github.com/pingcap/tidb/session/mockCommitError8942", `return(true)`)
	gofail.Enable("github.com/pingcap/tidb/session/mockStmtCommitError", `return(true)`)
	_, err := tk.Exec("commit")
	c.Assert(err, NotNil)
	gofail.Disable("github.com/pingcap/tidb/session/mockCommitError8942")
	gofail.Disable("github.com/pingcap/tidb/session/mockStmtCommitError")

	tk.MustExec("insert into t values (6)")
	tk.MustQuery(`select * from t`).Check(testkit.Rows("6"))
}
