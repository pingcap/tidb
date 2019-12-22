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
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/testkit"
)

func (s *testSuite) TestSortInDisk(c *C) {
	originCfg := config.GetGlobalConfig()
	newConf := *originCfg
	newConf.OOMUseTmpStorage = true
	config.StoreGlobalConfig(&newConf)
	defer config.StoreGlobalConfig(originCfg)

	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")

	sm := &mockSessionManager1{
		PS: make([]*util.ProcessInfo, 0),
	}
	tk.Se.SetSessionManager(sm)
	s.domain.ExpensiveQueryHandle().SetSessionManager(sm)

	tk.MustExec("set @@tidb_mem_quota_query=1;")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(c1 int, c2 int)")
	tk.MustExec("insert into t values(1,5),(2,4),(3,3),(4,2),(5,1)")
	result := tk.MustQuery("select * from t order by c2")
	result.Check(testkit.Rows("5 1", "4 2", "3 3", "2 4", "1 5"))
}
