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
	"fmt"
	"os"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/util/testkit"
)

func (s *testSuiteP2) TestQueryTime(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")

	costTime := time.Since(tk.Se.GetSessionVars().StartTime)
	c.Assert(costTime < 1*time.Second, IsTrue)

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int)")
	tk.MustExec("insert into t values(1), (1), (1), (1), (1)")
	tk.MustExec("select * from t t1 join t t2 on t1.a = t2.a")

	costTime = time.Since(tk.Se.GetSessionVars().StartTime)
	c.Assert(costTime < 1*time.Second, IsTrue)
}

func (s *testSuiteP2) TestPreparedSQL(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	slowLogFileName := "tidb_slow.log"
	f, err := os.OpenFile(slowLogFileName, os.O_CREATE|os.O_WRONLY, 0644)
	c.Assert(err, IsNil)
	c.Assert(f.Close(), IsNil)
	c.Assert(err, IsNil)

	tk.MustExec(fmt.Sprintf("set @@tidb_slow_query_file='%v'", slowLogFileName))
	tk.MustExec("prepare mysleep from 'select sleep(?+?)'")
	tk.MustExec("set @a=2, @b=3.0;")
	tk.MustExec("execute mysleep using @a, @b;")
	tk.MustQuery("select Query from INFORMATION_SCHEMA.SLOW_QUERY;").Check(testkit.Rows("select sleep ( 2 + 3.0 );"))
}
