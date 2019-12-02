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
	"context"

	. "github.com/pingcap/check"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/util/testkit"
)

func (s *testSessionSuite2) TestFailStatementCommit(c *C) {

	tk := testkit.NewTestKitWithInit(c, s.store)
	tk.MustExec("create table t (id int)")
	tk.MustExec("begin")
	tk.MustExec("insert into t values (1)")

	c.Assert(failpoint.Enable("github.com/pingcap/tidb/session/mockStmtCommitError", `return(true)`), IsNil)
	_, err := tk.Exec("insert into t values (2),(3),(4),(5)")
	c.Assert(err, NotNil)

	c.Assert(failpoint.Disable("github.com/pingcap/tidb/session/mockStmtCommitError"), IsNil)

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

func (s *testSessionSuite2) TestFailStatementCommitInRetry(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
	tk.MustExec("create table t (id int)")

	tk.MustExec("begin")
	tk.MustExec("insert into t values (1)")
	tk.MustExec("insert into t values (2),(3),(4),(5)")
	tk.MustExec("insert into t values (6)")

	c.Assert(failpoint.Enable("github.com/pingcap/tidb/session/mockCommitError8942", `return(true)`), IsNil)
	c.Assert(failpoint.Enable("github.com/pingcap/tidb/session/mockStmtCommitError", `return(true)`), IsNil)
	_, err := tk.Exec("commit")
	c.Assert(err, NotNil)
	c.Assert(failpoint.Disable("github.com/pingcap/tidb/session/mockCommitError8942"), IsNil)
	c.Assert(failpoint.Disable("github.com/pingcap/tidb/session/mockStmtCommitError"), IsNil)

	tk.MustExec("insert into t values (6)")
	tk.MustQuery(`select * from t`).Check(testkit.Rows("6"))
}

func (s *testSessionSuite2) TestGetTSFailDirtyState(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
	tk.MustExec("create table t (id int)")

	c.Assert(failpoint.Enable("github.com/pingcap/tidb/session/mockGetTSFail", "return"), IsNil)
	ctx := failpoint.WithHook(context.Background(), func(ctx context.Context, fpname string) bool {
		return fpname == "github.com/pingcap/tidb/session/mockGetTSFail"
	})
	_, err := tk.Se.Execute(ctx, "select * from t")
	c.Assert(err, NotNil)

	// Fix a bug that active txn fail set TxnState.fail to error, and then the following write
	// affected by this fail flag.
	tk.MustExec("insert into t values (1)")
	tk.MustQuery(`select * from t`).Check(testkit.Rows("1"))
	c.Assert(failpoint.Disable("github.com/pingcap/tidb/session/mockGetTSFail"), IsNil)
}

func (s *testSessionSuite2) TestGetTSFailDirtyStateInretry(c *C) {
	defer func() {
		c.Assert(failpoint.Disable("github.com/pingcap/tidb/session/mockCommitError"), IsNil)
		c.Assert(failpoint.Disable("github.com/pingcap/tidb/store/tikv/mockGetTSErrorInRetry"), IsNil)
	}()

	tk := testkit.NewTestKitWithInit(c, s.store)
	tk.MustExec("create table t (id int)")

	c.Assert(failpoint.Enable("github.com/pingcap/tidb/session/mockCommitError", `return(true)`), IsNil)
	// This test will mock a PD timeout error, and recover then.
	// Just make mockGetTSErrorInRetry return true once, and then return false.
	c.Assert(failpoint.Enable("github.com/pingcap/tidb/store/tikv/mockGetTSErrorInRetry",
		`1*return(true)->return(false)`), IsNil)
	tk.MustExec("insert into t values (2)")
	tk.MustQuery(`select * from t`).Check(testkit.Rows("2"))
}
