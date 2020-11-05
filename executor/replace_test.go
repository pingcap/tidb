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
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/util/collate"
	"github.com/pingcap/tidb/util/testkit"
)

func (s *testSerialSuite) TestIssue20849(c *C) {
	collate.SetNewCollationEnabledForTest(true)
	defer collate.SetNewCollationEnabledForTest(false)

	tk := testkit.NewTestKitWithInit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1")
	tk.MustExec("set tidb_enable_clustered_index = 0")
	tk.MustExec("create table t1 (i varchar(20) unique key) collate=utf8mb4_general_ci")
	tk.MustExec("insert into t1 values ('a')")
	tk.MustExec("replace into t1 values ('A')")
	tk.MustQuery("select * from t1").Check(testkit.Rows("A"))
	tk.MustExec("drop table t1")
}
