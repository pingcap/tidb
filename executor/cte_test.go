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

package executor_test

import (
	"fmt"
	"math/rand"
	"sort"
	"testing"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/parser/terror"
	"github.com/pingcap/tidb/testkit"
	"github.com/pingcap/tidb/types"
	"github.com/stretchr/testify/require"
)

func TestBasicCTE(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	rows := tk.MustQuery("with recursive cte1 as (" +
		"select 1 c1 " +
		"union all " +
		"select c1 + 1 c1 from cte1 where c1 < 5) " +
		"select * from cte1")
	rows.Check(testkit.Rows("1", "2", "3", "4", "5"))

	// Two seed parts.
	rows = tk.MustQuery("with recursive cte1 as (" +
		"select 1 c1 " +
		"union all " +
		"select 2 c1 " +
		"union all " +
		"select c1 + 1 c1 from cte1 where c1 < 10) " +
		"select * from cte1 order by c1")
	rows.Check(testkit.Rows("1", "2", "2", "3", "3", "4", "4", "5", "5", "6", "6", "7", "7", "8", "8", "9", "9", "10", "10"))

	// Two recursive parts.
	rows = tk.MustQuery("with recursive cte1 as (" +
		"select 1 c1 " +
		"union all " +
		"select 2 c1 " +
		"union all " +
		"select c1 + 1 c1 from cte1 where c1 < 3 " +
		"union all " +
		"select c1 + 2 c1 from cte1 where c1 < 5) " +
		"select * from cte1 order by c1")
	rows.Check(testkit.Rows("1", "2", "2", "3", "3", "3", "4", "4", "5", "5", "5", "6", "6"))

	tk.MustExec("drop table if exists t1;")
	tk.MustExec("create table t1(a int);")
	tk.MustExec("insert into t1 values(1);")
	tk.MustExec("insert into t1 values(2);")
	rows = tk.MustQuery("SELECT * FROM t1 dt WHERE EXISTS(WITH RECURSIVE qn AS (SELECT a*0 AS b UNION ALL SELECT b+1 FROM qn WHERE b=0) SELECT * FROM qn WHERE b=a);")
	rows.Check(testkit.Rows("1"))
	rows = tk.MustQuery("SELECT * FROM t1 dt WHERE EXISTS( WITH RECURSIVE qn AS (SELECT a*0 AS b UNION ALL SELECT b+1 FROM qn WHERE b=0 or b = 1) SELECT * FROM qn WHERE b=a );")
	rows.Check(testkit.Rows("1", "2"))

	rows = tk.MustQuery("with recursive  c(p) as (select 1), cte(a, b) as (select 1, 1 union select a+1, 1 from cte, c where a < 5)  select * from cte order by 1, 2;")
	rows.Check(testkit.Rows("1 1", "2 1", "3 1", "4 1", "5 1"))
}

func TestUnionDistinct(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test;")

	// Basic test. UNION/UNION ALL intersects.
	rows := tk.MustQuery("with recursive cte1(c1) as (select 1 union select 1 union select 1 union all select c1 + 1 from cte1 where c1 < 3) select * from cte1 order by c1;")
	rows.Check(testkit.Rows("1", "2", "3"))

	rows = tk.MustQuery("with recursive cte1(c1) as (select 1 union all select 1 union select 1 union all select c1 + 1 from cte1 where c1 < 3) select * from cte1 order by c1;")
	rows.Check(testkit.Rows("1", "2", "3"))

	tk.MustExec("drop table if exists t1;")
	tk.MustExec("create table t1(c1 int, c2 int);")
	tk.MustExec("insert into t1 values(1, 1), (1, 2), (2, 2);")
	rows = tk.MustQuery("with recursive cte1(c1) as (select c1 from t1 union select c1 + 1 c1 from t1) select * from cte1 order by c1;")
	rows.Check(testkit.Rows("1", "2", "3"))

	tk.MustExec("drop table if exists t1;")
	tk.MustExec("create table t1(c1 int);")
	tk.MustExec("insert into t1 values(1), (1), (1), (2), (2), (2);")
	rows = tk.MustQuery("with recursive cte1(c1) as (select c1 from t1 union select c1 + 1 c1 from cte1 where c1 < 4) select * from cte1 order by c1;")
	rows.Check(testkit.Rows("1", "2", "3", "4"))
}

func TestCTEMaxRecursionDepth(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test;")

	tk.MustExec("set @@cte_max_recursion_depth = -1;")
	err := tk.QueryToErr("with recursive cte1(c1) as (select 1 union select c1 + 1 c1 from cte1 where c1 < 100) select * from cte1;")
	require.EqualError(t, err, "[executor:3636]Recursive query aborted after 1 iterations. Try increasing @@cte_max_recursion_depth to a larger value")
	// If there is no recursive part, query runs ok.
	rows := tk.MustQuery("with recursive cte1(c1) as (select 1 union select 2) select * from cte1 order by c1;")
	rows.Check(testkit.Rows("1", "2"))
	rows = tk.MustQuery("with cte1(c1) as (select 1 union select 2) select * from cte1 order by c1;")
	rows.Check(testkit.Rows("1", "2"))

	tk.MustExec("set @@cte_max_recursion_depth = 0;")
	err = tk.QueryToErr("with recursive cte1(c1) as (select 1 union select c1 + 1 c1 from cte1 where c1 < 0) select * from cte1;")
	require.EqualError(t, err, "[executor:3636]Recursive query aborted after 1 iterations. Try increasing @@cte_max_recursion_depth to a larger value")
	err = tk.QueryToErr("with recursive cte1(c1) as (select 1 union select c1 + 1 c1 from cte1 where c1 < 1) select * from cte1;")
	require.EqualError(t, err, "[executor:3636]Recursive query aborted after 1 iterations. Try increasing @@cte_max_recursion_depth to a larger value")
	// If there is no recursive part, query runs ok.
	rows = tk.MustQuery("with recursive cte1(c1) as (select 1 union select 2) select * from cte1 order by c1;")
	rows.Check(testkit.Rows("1", "2"))
	rows = tk.MustQuery("with cte1(c1) as (select 1 union select 2) select * from cte1 order by c1;")
	rows.Check(testkit.Rows("1", "2"))

	tk.MustExec("set @@cte_max_recursion_depth = 1;")
	rows = tk.MustQuery("with recursive cte1(c1) as (select 1 union select c1 + 1 c1 from cte1 where c1 < 0) select * from cte1;")
	rows.Check(testkit.Rows("1"))
	rows = tk.MustQuery("with recursive cte1(c1) as (select 1 union select c1 + 1 c1 from cte1 where c1 < 1) select * from cte1;")
	rows.Check(testkit.Rows("1"))
	err = tk.QueryToErr("with recursive cte1(c1) as (select 1 union select c1 + 1 c1 from cte1 where c1 < 2) select * from cte1;")
	require.EqualError(t, err, "[executor:3636]Recursive query aborted after 2 iterations. Try increasing @@cte_max_recursion_depth to a larger value")
	// If there is no recursive part, query runs ok.
	rows = tk.MustQuery("with recursive cte1(c1) as (select 1 union select 2) select * from cte1 order by c1;")
	rows.Check(testkit.Rows("1", "2"))
	rows = tk.MustQuery("with cte1(c1) as (select 1 union select 2) select * from cte1 order by c1;")
	rows.Check(testkit.Rows("1", "2"))
}

func TestCTEWithLimit(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test;")

	// Basic recursive tests.
	rows := tk.MustQuery("with recursive cte1(c1) as (select 1 union select c1 + 1 from cte1 limit 5 offset 0) select * from cte1")
	rows.Check(testkit.Rows("1", "2", "3", "4", "5"))

	rows = tk.MustQuery("with recursive cte1(c1) as (select 1 union select c1 + 1 from cte1 limit 5 offset 1) select * from cte1")
	rows.Check(testkit.Rows("2", "3", "4", "5", "6"))

	rows = tk.MustQuery("with recursive cte1(c1) as (select 1 union select c1 + 1 from cte1 limit 5 offset 10) select * from cte1")
	rows.Check(testkit.Rows("11", "12", "13", "14", "15"))

	rows = tk.MustQuery("with recursive cte1(c1) as (select 1 union select c1 + 1 from cte1 limit 5 offset 995) select * from cte1")
	rows.Check(testkit.Rows("996", "997", "998", "999", "1000"))

	rows = tk.MustQuery("with recursive cte1(c1) as (select 1 union select c1 + 1 from cte1 limit 5 offset 6) select * from cte1;")
	rows.Check(testkit.Rows("7", "8", "9", "10", "11"))

	// Test with cte_max_recursion_depth
	tk.MustExec("set cte_max_recursion_depth=2;")
	rows = tk.MustQuery("with recursive cte1(c1) as (select 0 union select c1 + 1 from cte1 limit 1 offset 2) select * from cte1;")
	rows.Check(testkit.Rows("2"))

	err := tk.QueryToErr("with recursive cte1(c1) as (select 0 union select c1 + 1 from cte1 limit 1 offset 3) select * from cte1;")
	require.EqualError(t, err, "[executor:3636]Recursive query aborted after 3 iterations. Try increasing @@cte_max_recursion_depth to a larger value")

	tk.MustExec("set cte_max_recursion_depth=1000;")
	rows = tk.MustQuery("with recursive cte1(c1) as (select 0 union select c1 + 1 from cte1 limit 5 offset 996) select * from cte1;")
	rows.Check(testkit.Rows("996", "997", "998", "999", "1000"))

	err = tk.QueryToErr("with recursive cte1(c1) as (select 0 union select c1 + 1 from cte1 limit 5 offset 997) select * from cte1;")
	require.EqualError(t, err, "[executor:3636]Recursive query aborted after 1001 iterations. Try increasing @@cte_max_recursion_depth to a larger value")

	rows = tk.MustQuery("with recursive cte1(c1) as (select 1 union select c1 + 1 from cte1 limit 0 offset 1) select * from cte1")
	rows.Check(testkit.Rows())

	rows = tk.MustQuery("with recursive cte1(c1) as (select 1 union select c1 + 1 from cte1 limit 0 offset 10) select * from cte1")
	rows.Check(testkit.Rows())

	// Test join.
	rows = tk.MustQuery("with recursive cte1(c1) as (select 1 union select c1 + 1 from cte1 limit 2 offset 1) select * from cte1 dt1 join cte1 dt2 order by dt1.c1, dt2.c1;")
	rows.Check(testkit.Rows("2 2", "2 3", "3 2", "3 3"))

	rows = tk.MustQuery("with recursive cte1(c1) as (select 1 union select c1 + 1 from cte1 limit 2 offset 1) select * from cte1 dt1 join cte1 dt2 on dt1.c1 = dt2.c1 order by dt1.c1, dt1.c1;")
	rows.Check(testkit.Rows("2 2", "3 3"))

	// Test subquery.
	// Different with mysql, maybe it's mysql bug?(https://bugs.mysql.com/bug.php?id=103890&thanks=4)
	rows = tk.MustQuery("with recursive cte1(c1) as (select 1 union select c1 + 1 from cte1 limit 2 offset 1) select c1 from cte1 where c1 in (select 2);")
	rows.Check(testkit.Rows("2"))

	rows = tk.MustQuery("with recursive cte1(c1) as (select 1 union select c1 + 1 from cte1 limit 2 offset 1) select c1 from cte1 dt where c1 in (select c1 from cte1 where 1 = dt.c1 - 1);")
	rows.Check(testkit.Rows("2"))

	// Test Apply.
	rows = tk.MustQuery("with recursive cte1(c1) as (select 1 union select c1 + 1 from cte1 limit 2 offset 1) select c1 from cte1 where cte1.c1 = (select dt1.c1 from cte1 dt1 where dt1.c1 = cte1.c1);")
	rows.Check(testkit.Rows("2", "3"))

	// Recursive tests with table.
	tk.MustExec("drop table if exists t1;")
	tk.MustExec("create table t1(c1 int);")
	tk.MustExec("insert into t1 values(1), (2), (3);")

	// Error: ERROR 1221 (HY000): Incorrect usage of UNION and LIMIT.
	// Limit can only be at the end of SQL stmt.
	err = tk.ExecToErr("with recursive cte1(c1) as (select c1 from t1 limit 1 offset 1 union select c1 + 1 from cte1 limit 0 offset 1) select * from cte1")
	require.EqualError(t, err, "[planner:1221]Incorrect usage of UNION and LIMIT")

	// Basic non-recusive tests.
	rows = tk.MustQuery("with recursive cte1(c1) as (select 1 union select 2 order by 1 limit 1 offset 1) select * from cte1")
	rows.Check(testkit.Rows("2"))

	rows = tk.MustQuery("with recursive cte1(c1) as (select 1 union select 2 order by 1 limit 0 offset 1) select * from cte1")
	rows.Check(testkit.Rows())

	rows = tk.MustQuery("with recursive cte1(c1) as (select 1 union select 2 order by 1 limit 2 offset 0) select * from cte1")
	rows.Check(testkit.Rows("1", "2"))

	// Test with table.
	tk.MustExec("drop table if exists t1;")
	insertStr := "insert into t1 values(0)"
	for i := 1; i < 300; i++ {
		insertStr += fmt.Sprintf(", (%d)", i)
	}

	tk.MustExec("drop table if exists t1;")
	tk.MustExec("create table t1(c1 int);")
	tk.MustExec(insertStr)

	rows = tk.MustQuery("with recursive cte1(c1) as (select c1 from t1 union select c1 + 1 c1 from cte1 limit 1) select * from cte1")
	rows.Check(testkit.Rows("0"))

	rows = tk.MustQuery("with recursive cte1(c1) as (select c1 from t1 union select c1 + 1 c1 from cte1 limit 1 offset 100) select * from cte1")
	rows.Check(testkit.Rows("100"))

	rows = tk.MustQuery("with recursive cte1(c1) as (select c1 from t1 union select c1 + 1 c1 from cte1 limit 5 offset 100) select * from cte1")
	rows.Check(testkit.Rows("100", "101", "102", "103", "104"))

	// Basic non-recursive tests.
	rows = tk.MustQuery("with cte1 as (select c1 from t1 limit 2 offset 1) select * from cte1")
	rows.Check(testkit.Rows("1", "2"))

	rows = tk.MustQuery("with cte1 as (select c1 from t1 limit 2 offset 1) select * from cte1 dt1 join cte1 dt2 on dt1.c1 = dt2.c1")
	rows.Check(testkit.Rows("1 1", "2 2"))

	rows = tk.MustQuery("with recursive cte1(c1) as (select c1 from t1 union select 2 limit 0 offset 1) select * from cte1")
	rows.Check(testkit.Rows())

	rows = tk.MustQuery("with recursive cte1(c1) as (select c1 from t1 union select 2 limit 0 offset 1) select * from cte1 dt1 join cte1 dt2 on dt1.c1 = dt2.c1")
	rows.Check(testkit.Rows())

	// rows = tk.MustQuery("with recursive cte1(c1) as (select c1 from t1 union select 2 limit 5 offset 100) select * from cte1")
	// rows.Check(testkit.Rows("100", "101", "102", "103", "104"))

	rows = tk.MustQuery("with recursive cte1(c1) as (select c1 from t1 limit 3 offset 100) select * from cte1")
	rows.Check(testkit.Rows("100", "101", "102"))

	rows = tk.MustQuery("with recursive cte1(c1) as (select c1 from t1 limit 3 offset 100) select * from cte1 dt1 join cte1 dt2 on dt1.c1 = dt2.c1")
	rows.Check(testkit.Rows("100 100", "101 101", "102 102"))

	// Test limit 0.
	tk.MustExec("set cte_max_recursion_depth = 0;")
	tk.MustExec("drop table if exists t1;")
	tk.MustExec("create table t1(c1 int);")
	tk.MustExec("insert into t1 values(0);")
	rows = tk.MustQuery("with recursive cte1 as (select 1/c1 c1 from t1 union select c1 + 1 c1 from cte1 where c1 < 2 limit 0) select * from cte1;")
	rows.Check(testkit.Rows())
	// MySQL err: ERROR 1365 (22012): Division by 0. Because it gives error when computing 1/c1.
	err = tk.QueryToErr("with recursive cte1 as (select 1/c1 c1 from t1 union select c1 + 1 c1 from cte1 where c1 < 2 limit 1) select * from cte1;")
	require.EqualError(t, err, "[executor:3636]Recursive query aborted after 1 iterations. Try increasing @@cte_max_recursion_depth to a larger value")

	tk.MustExec("set cte_max_recursion_depth = 1000;")
	tk.MustExec("drop table if exists t1;")
	tk.MustExec("create table t1(c1 int);")
	tk.MustExec("insert into t1 values(1), (2), (3);")

	rows = tk.MustQuery("with recursive cte1(c1) as (select c1 from t1 union select c1 + 1 from cte1 limit 0 offset 2) select * from cte1;")
	rows.Check(testkit.Rows())
	rows = tk.MustQuery("with recursive cte1(c1) as (select c1 from t1 union select c1 + 1 from cte1 limit 1 offset 2) select * from cte1;")
	rows.Check(testkit.Rows("3"))
	rows = tk.MustQuery("with recursive cte1(c1) as (select c1 from t1 union select c1 + 1 from cte1 limit 2 offset 2) select * from cte1;")
	rows.Check(testkit.Rows("3", "4"))
	rows = tk.MustQuery("with recursive cte1(c1) as (select c1 from t1 union select c1 + 1 from cte1 limit 3 offset 2) select * from cte1;")
	rows.Check(testkit.Rows("3", "4", "5"))
	rows = tk.MustQuery("with recursive cte1(c1) as (select c1 from t1 union select c1 + 1 from cte1 limit 4 offset 2) select * from cte1;")
	rows.Check(testkit.Rows("3", "4", "5", "6"))

	rows = tk.MustQuery("with recursive cte1(c1) as (select c1 from t1 union select c1 + 1 from cte1 limit 0 offset 3) select * from cte1;")
	rows.Check(testkit.Rows())
	rows = tk.MustQuery("with recursive cte1(c1) as (select c1 from t1 union select c1 + 1 from cte1 limit 1 offset 3) select * from cte1;")
	rows.Check(testkit.Rows("4"))
	rows = tk.MustQuery("with recursive cte1(c1) as (select c1 from t1 union select c1 + 1 from cte1 limit 2 offset 3) select * from cte1;")
	rows.Check(testkit.Rows("4", "5"))
	rows = tk.MustQuery("with recursive cte1(c1) as (select c1 from t1 union select c1 + 1 from cte1 limit 3 offset 3) select * from cte1;")
	rows.Check(testkit.Rows("4", "5", "6"))
	rows = tk.MustQuery("with recursive cte1(c1) as (select c1 from t1 union select c1 + 1 from cte1 limit 4 offset 3) select * from cte1;")
	rows.Check(testkit.Rows("4", "5", "6", "7"))

	rows = tk.MustQuery("with recursive cte1(c1) as (select c1 from t1 union select c1 + 1 from cte1 limit 0 offset 4) select * from cte1;")
	rows.Check(testkit.Rows())
	rows = tk.MustQuery("with recursive cte1(c1) as (select c1 from t1 union select c1 + 1 from cte1 limit 1 offset 4) select * from cte1;")
	rows.Check(testkit.Rows("5"))
	rows = tk.MustQuery("with recursive cte1(c1) as (select c1 from t1 union select c1 + 1 from cte1 limit 2 offset 4) select * from cte1;")
	rows.Check(testkit.Rows("5", "6"))
	rows = tk.MustQuery("with recursive cte1(c1) as (select c1 from t1 union select c1 + 1 from cte1 limit 3 offset 4) select * from cte1;")
	rows.Check(testkit.Rows("5", "6", "7"))
	rows = tk.MustQuery("with recursive cte1(c1) as (select c1 from t1 union select c1 + 1 from cte1 limit 4 offset 4) select * from cte1;")
	rows.Check(testkit.Rows("5", "6", "7", "8"))

	rows = tk.MustQuery("with recursive cte1(c1) as (select c1 from t1 union all select c1 + 1 from cte1 limit 0 offset 2) select * from cte1;")
	rows.Check(testkit.Rows())
	rows = tk.MustQuery("with recursive cte1(c1) as (select c1 from t1 union all select c1 + 1 from cte1 limit 1 offset 2) select * from cte1;")
	rows.Check(testkit.Rows("3"))
	rows = tk.MustQuery("with recursive cte1(c1) as (select c1 from t1 union all select c1 + 1 from cte1 limit 2 offset 2) select * from cte1;")
	rows.Check(testkit.Rows("3", "2"))
	rows = tk.MustQuery("with recursive cte1(c1) as (select c1 from t1 union all select c1 + 1 from cte1 limit 3 offset 2) select * from cte1;")
	rows.Check(testkit.Rows("3", "2", "3"))
	rows = tk.MustQuery("with recursive cte1(c1) as (select c1 from t1 union all select c1 + 1 from cte1 limit 4 offset 2) select * from cte1;")
	rows.Check(testkit.Rows("3", "2", "3", "4"))

	rows = tk.MustQuery("with recursive cte1(c1) as (select c1 from t1 union all select c1 + 1 from cte1 limit 0 offset 3) select * from cte1;")
	rows.Check(testkit.Rows())
	rows = tk.MustQuery("with recursive cte1(c1) as (select c1 from t1 union all select c1 + 1 from cte1 limit 1 offset 3) select * from cte1;")
	rows.Check(testkit.Rows("2"))
	rows = tk.MustQuery("with recursive cte1(c1) as (select c1 from t1 union all select c1 + 1 from cte1 limit 2 offset 3) select * from cte1;")
	rows.Check(testkit.Rows("2", "3"))
	rows = tk.MustQuery("with recursive cte1(c1) as (select c1 from t1 union all select c1 + 1 from cte1 limit 3 offset 3) select * from cte1;")
	rows.Check(testkit.Rows("2", "3", "4"))
	rows = tk.MustQuery("with recursive cte1(c1) as (select c1 from t1 union all select c1 + 1 from cte1 limit 4 offset 3) select * from cte1;")
	rows.Check(testkit.Rows("2", "3", "4", "3"))

	rows = tk.MustQuery("with recursive cte1(c1) as (select c1 from t1 union all select c1 + 1 from cte1 limit 0 offset 4) select * from cte1;")
	rows.Check(testkit.Rows())
	rows = tk.MustQuery("with recursive cte1(c1) as (select c1 from t1 union all select c1 + 1 from cte1 limit 1 offset 4) select * from cte1;")
	rows.Check(testkit.Rows("3"))
	rows = tk.MustQuery("with recursive cte1(c1) as (select c1 from t1 union all select c1 + 1 from cte1 limit 2 offset 4) select * from cte1;")
	rows.Check(testkit.Rows("3", "4"))
	rows = tk.MustQuery("with recursive cte1(c1) as (select c1 from t1 union all select c1 + 1 from cte1 limit 3 offset 4) select * from cte1;")
	rows.Check(testkit.Rows("3", "4", "3"))
	rows = tk.MustQuery("with recursive cte1(c1) as (select c1 from t1 union all select c1 + 1 from cte1 limit 4 offset 4) select * from cte1;")
	rows.Check(testkit.Rows("3", "4", "3", "4"))
}

func TestSpillToDisk(t *testing.T) {
	defer config.RestoreFunc()()
	config.UpdateGlobal(func(conf *config.Config) {
		conf.OOMUseTmpStorage = true
	})

	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test;")

	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/executor/testCTEStorageSpill", "return(true)"))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/executor/testCTEStorageSpill"))
		tk.MustExec("set tidb_mem_quota_query = 1073741824;")
	}()
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/executor/testSortedRowContainerSpill", "return(true)"))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/executor/testSortedRowContainerSpill"))
	}()

	// Use duplicated rows to test UNION DISTINCT.
	tk.MustExec("set tidb_mem_quota_query = 1073741824;")
	insertStr := "insert into t1 values(0)"
	rowNum := 1000
	vals := make([]int, rowNum)
	vals[0] = 0
	for i := 1; i < rowNum; i++ {
		v := rand.Intn(100)
		vals[i] = v
		insertStr += fmt.Sprintf(", (%d)", v)
	}
	tk.MustExec("drop table if exists t1;")
	tk.MustExec("create table t1(c1 int);")
	tk.MustExec(insertStr)
	tk.MustExec("set tidb_mem_quota_query = 40000;")
	tk.MustExec("set cte_max_recursion_depth = 500000;")
	sql := fmt.Sprintf("with recursive cte1 as ( "+
		"select c1 from t1 "+
		"union "+
		"select c1 + 1 c1 from cte1 where c1 < %d) "+
		"select c1 from cte1 order by c1;", rowNum)
	rows := tk.MustQuery(sql)

	memTracker := tk.Session().GetSessionVars().StmtCtx.MemTracker
	diskTracker := tk.Session().GetSessionVars().StmtCtx.DiskTracker
	require.Greater(t, memTracker.MaxConsumed(), int64(0))
	require.Greater(t, diskTracker.MaxConsumed(), int64(0))

	sort.Ints(vals)
	resRows := make([]string, 0, rowNum)
	for i := vals[0]; i <= rowNum; i++ {
		resRows = append(resRows, fmt.Sprintf("%d", i))
	}
	rows.Check(testkit.Rows(resRows...))
}

func TestCTEExecError(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test;")
	tk.MustExec("drop table if exists src;")
	tk.MustExec("create table src(first int, second int);")

	insertStr := fmt.Sprintf("insert into src values (%d, %d)", rand.Intn(1000), rand.Intn(1000))
	for i := 0; i < 1000; i++ {
		insertStr += fmt.Sprintf(",(%d, %d)", rand.Intn(1000), rand.Intn(1000))
	}
	insertStr += ";"
	tk.MustExec(insertStr)

	// Increase projection concurrency and decrease chunk size
	// to increase the probability of reproducing the problem.
	tk.MustExec("set tidb_max_chunk_size = 32")
	tk.MustExec("set tidb_projection_concurrency = 20")
	for i := 0; i < 10; i++ {
		err := tk.QueryToErr("with recursive cte(iter, first, second, result) as " +
			"(select 1, first, second, first+second from src " +
			" union all " +
			"select iter+1, second, result, second+result from cte where iter < 80 )" +
			"select * from cte")
		require.True(t, terror.ErrorEqual(err, types.ErrOverflow))
	}
}

// https://github.com/pingcap/tidb/issues/33965.
func TestCTEsInView(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test;")

	tk.MustExec("create database if not exists test1;")
	tk.MustExec("create table test.t (a int);")
	tk.MustExec("create table test1.t (a int);")
	tk.MustExec("insert into test.t values (1);")
	tk.MustExec("insert into test1.t values (2);")

	tk.MustExec("use test;")
	tk.MustExec("create definer='root'@'localhost' view test.v as with tt as (select * from t) select * from tt;")
	tk.MustQuery("select * from test.v;").Check(testkit.Rows("1"))
	tk.MustExec("use test1;")
	tk.MustQuery("select * from test.v;").Check(testkit.Rows("1"))
}
