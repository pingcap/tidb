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
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/util/logutil"
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

func (s testSuiteP2) TestTurnOffSlowLog(c *C) {
	tk := testkit.NewTestKit(c, s.store)

	c.Assert(config.GetGlobalConfig().Log.EnableSlowLog, Equals, uint32(logutil.DefaultTiDBEnableSlowLog))
	tk.MustExec("set @@tidb_enable_slow_log=0")
	c.Assert(config.GetGlobalConfig().Log.EnableSlowLog, Equals, uint32(0))
	tk.MustExec("set @@tidb_enable_slow_log=1")
	c.Assert(config.GetGlobalConfig().Log.EnableSlowLog, Equals, uint32(logutil.DefaultTiDBEnableSlowLog))
}
