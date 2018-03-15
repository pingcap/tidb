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

package executor_test

import (
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/util/testkit"
)

func (s *testSuite) TestExportRowID(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int, b int)")
	tk.MustExec("insert t values (1, 7), (1, 8), (1, 9)")
	tk.MustQuery("select *, _tidb_rowid from t").
		Check(testkit.Rows("1 7 1", "1 8 2", "1 9 3"))
	tk.MustExec("update t set a = 2 where _tidb_rowid = 2")
	tk.MustQuery("select *, _tidb_rowid from t").
		Check(testkit.Rows("1 7 1", "2 8 2", "1 9 3"))

	tk.MustExec("delete from t where _tidb_rowid = 2")
	tk.MustQuery("select *, _tidb_rowid from t").
		Check(testkit.Rows("1 7 1", "1 9 3"))

	tk.MustExec("insert t (a, b, _tidb_rowid) values (2, 2, 2), (5, 5, 5)")
	tk.MustQuery("select *, _tidb_rowid from t").
		Check(testkit.Rows("1 7 1", "2 2 2", "1 9 3", "5 5 5"))

	// If PK is handle, _tidb_rowid is unknown column.
	tk.MustExec("create table s (a int primary key)")
	tk.MustExec("insert s values (1)")
	_, err := tk.Exec("insert s (a, _tidb_rowid) values (1, 2)")
	c.Assert(err, NotNil)
	_, err = tk.Exec("select _tidb_rowid from s")
	c.Assert(err, NotNil)
	_, err = tk.Exec("update s set a = 2 where _tidb_rowid = 1")
	c.Assert(err, NotNil)
	_, err = tk.Exec("delete from s where _tidb_rowid = 1")
	c.Assert(err, NotNil)
}
