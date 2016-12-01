// Copyright 2016 PingCAP, Inc.
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
	"github.com/pingcap/tidb/terror"
	"github.com/pingcap/tidb/util/testkit"
	"github.com/pingcap/tidb/util/types"
)

func (s *testSuite) TestStatementContext(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("create table sc (a int)")
	tk.MustExec("insert sc values (1), (2)")
	tk.MustQuery("select * from sc where a > cast(1.1 as decimal)").Check(testkit.Rows("2"))
	_, err := tk.Exec("update sc set a = 4 where a > cast(1.1 as decimal)")
	c.Check(terror.ErrorEqual(err, types.ErrTruncated), IsTrue)
	tk.MustExec("set sql_mode = 0")
	tk.MustExec("update sc set a = 3 where a > cast(1.1 as decimal)")
	tk.MustQuery("select * from sc").Check(testkit.Rows("1", "3"))
}
