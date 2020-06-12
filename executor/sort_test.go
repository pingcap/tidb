// Copyright 2020 PingCAP, Inc.
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
	"strings"

	. "github.com/pingcap/check"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/testkit"
)

func (s *testSerialSuite1) TestSortInDisk(c *C) {
	defer config.RestoreFunc()()
	config.UpdateGlobal(func(conf *config.Config) {
		conf.OOMUseTmpStorage = true
	})
	c.Assert(failpoint.Enable("github.com/pingcap/tidb/executor/testSortedRowContainerSpill", "return(true)"), IsNil)
	defer func() {
		c.Assert(failpoint.Disable("github.com/pingcap/tidb/executor/testSortedRowContainerSpill"), IsNil)
	}()

	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")

	sm := &mockSessionManager1{
		PS: make([]*util.ProcessInfo, 0),
	}
	tk.Se.SetSessionManager(sm)
	s.domain.ExpensiveQueryHandle().SetSessionManager(sm)

	tk.MustExec("set @@tidb_mem_quota_query=1;")
	tk.MustExec("set @@tidb_max_chunk_size=32;")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(c1 int, c2 int, c3 int)")
	for i := 0; i < 5; i++ {
		for j := i; j < 1024; j += 5 {
			tk.MustExec(fmt.Sprintf("insert into t values(%v, %v, %v)", j, j, j))
		}
	}
	result := tk.MustQuery("select * from t order by c1")
	for i := 0; i < 1024; i++ {
		c.Assert(result.Rows()[i][0].(string), Equals, fmt.Sprint(i))
		c.Assert(result.Rows()[i][1].(string), Equals, fmt.Sprint(i))
		c.Assert(result.Rows()[i][2].(string), Equals, fmt.Sprint(i))
	}
	c.Assert(tk.Se.GetSessionVars().StmtCtx.MemTracker.BytesConsumed(), Equals, int64(0))
	c.Assert(tk.Se.GetSessionVars().StmtCtx.MemTracker.MaxConsumed(), Greater, int64(0))
	c.Assert(tk.Se.GetSessionVars().StmtCtx.DiskTracker.BytesConsumed(), Equals, int64(0))
	c.Assert(tk.Se.GetSessionVars().StmtCtx.DiskTracker.MaxConsumed(), Greater, int64(0))
}

func (s *testSerialSuite1) TestIssue16696(c *C) {
	originCfg := config.GetGlobalConfig()
	newConf := *originCfg
	newConf.OOMUseTmpStorage = true
	config.StoreGlobalConfig(&newConf)
	defer config.StoreGlobalConfig(originCfg)
	c.Assert(failpoint.Enable("github.com/pingcap/tidb/executor/testSortedRowContainerSpill", "return(true)"), IsNil)
	defer func() {
		c.Assert(failpoint.Disable("github.com/pingcap/tidb/executor/testSortedRowContainerSpill"), IsNil)
	}()
	c.Assert(failpoint.Enable("github.com/pingcap/tidb/executor/testRowContainerSpill", "return(true)"), IsNil)
	defer func() { c.Assert(failpoint.Disable("github.com/pingcap/tidb/executor/testRowContainerSpill"), IsNil) }()
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("CREATE TABLE `t` (`a` int(11) DEFAULT NULL,`b` int(11) DEFAULT NULL)")
	tk.MustExec("insert into t values (1, 1)")
	for i := 0; i < 6; i++ {
		tk.MustExec("insert into t select * from t")
	}
	tk.MustExec("set tidb_mem_quota_query = 1;")
	rows := tk.MustQuery("explain analyze  select t1.a, t1.a +1 from t t1 join t t2 join t t3 order by t1.a").Rows()
	for _, row := range rows {
		length := len(row)
		line := fmt.Sprintf("%v", row)
		disk := fmt.Sprintf("%v", row[length-1])
		if strings.Contains(line, "Sort") || strings.Contains(line, "HashJoin") {
			c.Assert(strings.Contains(disk, "0 Bytes"), IsFalse)
			c.Assert(strings.Contains(disk, "MB") ||
				strings.Contains(disk, "KB") ||
				strings.Contains(disk, "Bytes"), IsTrue)
		}
	}
}
