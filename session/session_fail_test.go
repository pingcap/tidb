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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package session_test

import (
	"context"
	"strings"

	. "github.com/pingcap/check"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/util/testkit"
)

func (s *testSessionSerialSuite) TestFailStatementCommitInRetry(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
	tk.MustExec("create table t (id int)")

	tk.MustExec("begin")
	tk.MustExec("insert into t values (1)")
	tk.MustExec("insert into t values (2),(3),(4),(5)")
	tk.MustExec("insert into t values (6)")

	c.Assert(failpoint.Enable("github.com/pingcap/tidb/session/mockCommitError8942", `return(true)`), IsNil)
	_, err := tk.Exec("commit")
	c.Assert(err, NotNil)
	c.Assert(failpoint.Disable("github.com/pingcap/tidb/session/mockCommitError8942"), IsNil)

	tk.MustExec("insert into t values (6)")
	tk.MustQuery(`select * from t`).Check(testkit.Rows("6"))
}

func (s *testSessionSerialSuite) TestGetTSFailDirtyState(c *C) {
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

func (s *testSessionSerialSuite) TestGetTSFailDirtyStateInretry(c *C) {
	defer func() {
		c.Assert(failpoint.Disable("github.com/pingcap/tidb/session/mockCommitError"), IsNil)
		c.Assert(failpoint.Disable("tikvclient/mockGetTSErrorInRetry"), IsNil)
	}()

	tk := testkit.NewTestKitWithInit(c, s.store)
	tk.MustExec("create table t (id int)")

	c.Assert(failpoint.Enable("github.com/pingcap/tidb/session/mockCommitError", `return(true)`), IsNil)
	// This test will mock a PD timeout error, and recover then.
	// Just make mockGetTSErrorInRetry return true once, and then return false.
	c.Assert(failpoint.Enable("tikvclient/mockGetTSErrorInRetry",
		`1*return(true)->return(false)`), IsNil)
	tk.MustExec("insert into t values (2)")
	tk.MustQuery(`select * from t`).Check(testkit.Rows("2"))
}

func (s *testSessionSerialSuite) TestKillFlagInBackoff(c *C) {
	// This test checks the `killed` flag is passed down to the backoffer through
	// session.KVVars.
	tk := testkit.NewTestKitWithInit(c, s.store)
	tk.MustExec("create table kill_backoff (id int)")
	// Inject 1 time timeout. If `Killed` is not successfully passed, it will retry and complete query.
	c.Assert(failpoint.Enable("tikvclient/tikvStoreSendReqResult", `return("timeout")->return("")`), IsNil)
	defer failpoint.Disable("tikvclient/tikvStoreSendReqResult")
	// Set kill flag and check its passed to backoffer.
	tk.Se.GetSessionVars().Killed = 1
	rs, err := tk.Exec("select * from kill_backoff")
	c.Assert(err, IsNil)
	_, err = session.ResultSetToStringSlice(context.TODO(), tk.Se, rs)
	// `interrupted` is returned when `Killed` is set.
	c.Assert(strings.Contains(err.Error(), "Query execution was interrupted"), IsTrue)
}

func (s *testSessionSerialSuite) TestClusterTableSendError(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
	c.Assert(failpoint.Enable("tikvclient/tikvStoreSendReqResult", `return("requestTiDBStoreError")`), IsNil)
	defer failpoint.Disable("tikvclient/tikvStoreSendReqResult")
	tk.MustQuery("select * from information_schema.cluster_slow_query")
	c.Assert(tk.Se.GetSessionVars().StmtCtx.WarningCount(), Equals, uint16(1))
	c.Assert(tk.Se.GetSessionVars().StmtCtx.GetWarnings()[0].Err, ErrorMatches, ".*TiDB server timeout, address is.*")
}

func (s *testSessionSerialSuite) TestAutoCommitNeedNotLinearizability(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
	tk.MustExec("drop table if exists t1;")
	defer tk.MustExec("drop table if exists t1")
	tk.MustExec(`create table t1 (c int)`)

	c.Assert(failpoint.Enable("tikvclient/getMinCommitTSFromTSO", `panic`), IsNil)
	defer func() {
		c.Assert(failpoint.Disable("tikvclient/getMinCommitTSFromTSO"), IsNil)
	}()

	c.Assert(tk.Se.GetSessionVars().SetSystemVar("tidb_enable_async_commit", "1"), IsNil)
	c.Assert(tk.Se.GetSessionVars().SetSystemVar("tidb_guarantee_linearizability", "1"), IsNil)

	// Auto-commit transactions don't need to get minCommitTS from TSO
	tk.MustExec("INSERT INTO t1 VALUES (1)")

	tk.MustExec("BEGIN")
	tk.MustExec("INSERT INTO t1 VALUES (2)")
	// An explicit transaction needs to get minCommitTS from TSO
	func() {
		defer func() {
			err := recover()
			c.Assert(err, NotNil)
		}()
		tk.MustExec("COMMIT")
	}()

	tk.MustExec("set autocommit = 0")
	tk.MustExec("INSERT INTO t1 VALUES (3)")
	func() {
		defer func() {
			err := recover()
			c.Assert(err, NotNil)
		}()
		tk.MustExec("COMMIT")
	}()

	// Same for 1PC
	tk.MustExec("set autocommit = 1")
	c.Assert(tk.Se.GetSessionVars().SetSystemVar("tidb_enable_1pc", "1"), IsNil)
	tk.MustExec("INSERT INTO t1 VALUES (4)")

	tk.MustExec("BEGIN")
	tk.MustExec("INSERT INTO t1 VALUES (5)")
	func() {
		defer func() {
			err := recover()
			c.Assert(err, NotNil)
		}()
		tk.MustExec("COMMIT")
	}()

	tk.MustExec("set autocommit = 0")
	tk.MustExec("INSERT INTO t1 VALUES (6)")
	func() {
		defer func() {
			err := recover()
			c.Assert(err, NotNil)
		}()
		tk.MustExec("COMMIT")
	}()
}
