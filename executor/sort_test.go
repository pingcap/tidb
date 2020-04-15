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

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/v4/config"
	"github.com/pingcap/tidb/v4/util"
	"github.com/pingcap/tidb/v4/util/testkit"
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
}
