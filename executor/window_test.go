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
	"github.com/pingcap/tidb/util/testkit"
)

func (s *testSuite2) TestWindowFunctions(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int, b int, c int)")
	tk.MustExec("set @@tidb_enable_window_function = 1")
	defer func() {
		tk.MustExec("set @@tidb_enable_window_function = 0")
	}()
	tk.MustExec("insert into t values (1,2,3),(4,3,2),(2,3,4)")
	result := tk.MustQuery("select count(a) over () from t")
	result.Check(testkit.Rows("3", "3", "3"))
	result = tk.MustQuery("select sum(a) over () + count(a) over () from t")
	result.Check(testkit.Rows("10", "10", "10"))
	result = tk.MustQuery("select sum(a) over (partition by a) from t")
	result.Check(testkit.Rows("1", "2", "4"))
	result = tk.MustQuery("select 1 + sum(a) over (), count(a) over () from t")
	result.Check(testkit.Rows("8 3", "8 3", "8 3"))
	result = tk.MustQuery("select sum(t1.a) over() from t t1, t t2")
	result.Check(testkit.Rows("21", "21", "21", "21", "21", "21", "21", "21", "21"))
	result = tk.MustQuery("select _tidb_rowid, sum(t.a) over() from t")
	result.Check(testkit.Rows("1 7", "2 7", "3 7"))
}
