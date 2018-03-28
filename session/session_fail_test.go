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
	gofail "github.com/coreos/gofail/runtime"
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/util/testkit"
)

func (s *testSessionSuite) TestFailStatementCommit(c *C) {
	defer gofail.Disable("github.com/pingcap/tidb/session/mockStmtCommitError")

	tk := testkit.NewTestKitWithInit(c, s.store)
	tk.MustExec("create table t (id int)")
	tk.MustExec("begin")
	tk.MustExec("insert into t values (1)")
	gofail.Enable("github.com/pingcap/tidb/session/mockStmtCommitError", `return(true)`)
	tk.MustExec("insert into t values (2)")
	_, err := tk.Exec("commit")
	c.Assert(err, NotNil)
	tk.MustQuery(`select * from t`).Check(testkit.Rows())
}

func (s *testSessionSuite) TestSetTransactionIsolationOneShot(c *C) {
	tk1 := testkit.NewTestKitWithInit(c, s.store)
	tk1.MustExec("create table t (k int, v int)")
	tk1.MustExec("insert t values (1, 42)")
	tk1.MustExec("set transaction isolation level read committed")

	tk2 := testkit.NewTestKitWithInit(c, s.store)
	gofail.Enable("github.com/pingcap/tidb/store/mockstore/mocktikv/rpcCommitResult", `return("keyError")`)
	tk2.Exec("update t set v = 43 where k = 1")
	gofail.Disable("github.com/pingcap/tidb/store/mockstore/mocktikv/rpcCommitResult")

	// In RC isolation level, the request meet lock and skip it, get 42.
	tk1.MustQuery("select v from t where k = 1").Check(testkit.Rows("42"))

	// set transaction isolation level read committed take effect just one time.
	tk2.MustExec("update t set v = 43 where k = 1")
	tk1.MustQuery("select v from t where k = 1").Check(testkit.Rows("43"))
}
