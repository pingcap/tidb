// Copyright 2017 PingCAP, Inc.
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

func (s *testSuite) TestWindowFunction(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int, b int, c int)")
	tk.MustExec("insert into t values (1,2,3),(4,3,2),(2,3,4),(3,4,5),(3,4,5),(4,5,6),(5,6,7)")
	result := tk.MustQuery("select row_number() over () from t")
	result.Check(testkit.Rows("1", "2", "3", "4", "5", "6", "7"))
	result = tk.MustQuery("select rank() over () from t")
	result.Check(testkit.Rows("1", "1", "1", "1", "1", "1", "1"))
	result = tk.MustQuery("select RANK() over () from t")
	result.Check(testkit.Rows("1", "1", "1", "1", "1", "1", "1"))
	result = tk.MustQuery("select rank() over () + row_number() over () from t")
	result.Check(testkit.Rows("2", "3", "4", "5", "6", "7", "8"))
	result = tk.MustQuery("select row_number() over () + row_number() over () from t")
	result.Check(testkit.Rows("2", "4", "6", "8", "10", "12", "14"))
	result = tk.MustQuery("select 1 + row_number() over () from t")
	result.Check(testkit.Rows("2", "3", "4", "5", "6", "7", "8"))
	result = tk.MustQuery("select 1 + row_number() over (), row_number() over () from t")
	result.Check(testkit.Rows("2 1", "3 2", "4 3", "5 4", "6 5", "7 6", "8 7"))
}
