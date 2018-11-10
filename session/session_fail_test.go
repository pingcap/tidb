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
	gofail "github.com/etcd-io/gofail/runtime"
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
