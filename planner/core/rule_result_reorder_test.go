// Copyright 2021 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package core_test

import (
	"testing"

	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/testkit"
	"github.com/stretchr/testify/require"
)

func TestPlanCache(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`set tidb_enable_prepared_plan_cache=1`)

	tk.MustExec("use test")
	tk.MustExec("set tidb_enable_ordered_result_mode=1")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int primary key, b int, c int, d int, key(b))")
	tk.MustExec("prepare s1 from 'select * from t where a > ? limit 10'")
	tk.MustExec("set @a = 10")
	tk.MustQuery("execute s1 using @a").Check(testkit.Rows())
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("0"))
	tk.MustQuery("execute s1 using @a").Check(testkit.Rows())
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("1")) // plan cache is still working
}

func TestSQLBinding(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set tidb_enable_ordered_result_mode=1")
	tk.MustExec("set tidb_opt_limit_push_down_threshold=0")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int primary key, b int, c int, d int, key(b))")
	tk.MustQuery("explain select * from t where a > 0 limit 1").Check(testkit.Rows(
		"Limit_12 1.00 root  offset:0, count:1",
		"└─TableReader_22 1.00 root  data:Limit_21",
		"  └─Limit_21 1.00 cop[tikv]  offset:0, count:1",
		"    └─TableRangeScan_20 1.00 cop[tikv] table:t range:(0,+inf], keep order:true, stats:pseudo"))

	tk.MustExec("create session binding for select * from t where a>0 limit 1 using select * from t use index(b) where a>0 limit 1")
	tk.MustQuery("explain select * from t where a > 0 limit 1").Check(testkit.Rows(
		"TopN_9 1.00 root  test.t.a, offset:0, count:1",
		"└─IndexLookUp_19 1.00 root  ",
		"  ├─TopN_18(Build) 1.00 cop[tikv]  test.t.a, offset:0, count:1",
		"  │ └─Selection_17 3333.33 cop[tikv]  gt(test.t.a, 0)",
		"  │   └─IndexFullScan_15 10000.00 cop[tikv] table:t, index:b(b) keep order:false, stats:pseudo",
		"  └─TableRowIDScan_16(Probe) 1.00 cop[tikv] table:t keep order:false, stats:pseudo"))
}

func TestClusteredIndex(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set tidb_enable_ordered_result_mode=1")
	tk.Session().GetSessionVars().EnableClusteredIndex = variable.ClusteredIndexDefModeOn
	tk.MustExec("drop table if exists t")
	tk.MustExec("CREATE TABLE t (a int,b int,c int, PRIMARY KEY (a,b))")
	tk.MustQuery("explain format=brief select * from t limit 10").Check(testkit.Rows(
		"TopN 10.00 root  test.t.a, test.t.b, test.t.c, offset:0, count:10",
		"└─TableReader 10.00 root  data:TopN",
		"  └─TopN 10.00 cop[tikv]  test.t.a, test.t.b, test.t.c, offset:0, count:10",
		"    └─TableFullScan 10000.00 cop[tikv] table:t keep order:false, stats:pseudo"))
	tk.Session().GetSessionVars().EnableClusteredIndex = variable.ClusteredIndexDefModeOff
}

func TestStableResultSwitch(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	rows := tk.MustQuery("show variables where variable_name like 'tidb_enable_ordered_result_mode'").Rows()
	require.Len(t, rows, 1)
}
