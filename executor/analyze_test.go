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
	"fmt"
	"strings"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/util/testkit"
	"github.com/pingcap/tidb/util/testleak"
)

func (s *testSuite) TestAnalyzeTable(c *C) {
	defer func() {
		testleak.AfterTest(c)()
	}()
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t1 (a int)")
	tk.MustExec("create index ind_a on t1 (a)")
	tk.MustExec("insert into t1 (a) values (1)")
	result := tk.MustQuery("explain select * from t1 where t1.a = 1")
	rowStr := fmt.Sprintf("%s", result.Rows())
	c.Check(strings.Split(rowStr, "{")[0], Equals, "[[IndexScan_5 ")
	tk.MustExec("analyze table t1")
	result = tk.MustQuery("explain select * from t1 where t1.a = 1")
	rowStr = fmt.Sprintf("%s", result.Rows())
	c.Check(strings.Split(rowStr, "{")[0], Equals, "[[TableScan_4 ")
}
