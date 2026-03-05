// Copyright 2025 PingCAP, Inc.
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

package plancache

import (
	"strconv"
	"testing"

	plannercore "github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/util/benchdaily"
	"github.com/stretchr/testify/require"
)

func TestDropPrepare(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int not null, b int not null, key(a), key(b))")
	tk.MustExec(`insert into t values (1, 1), (2, 2), (3, 3)`)
	tk.MustExec("set @@tidb_enable_prepared_plan_cache=1")

	// 1. Prepare and execute statement A to cache its plan.
	tk.MustExec(`prepare stmt from 'select * from t where a = ?'`)
	tk.MustExec(`set @a=1`)
	tk.MustQuery(`execute stmt using @a`) // First execution to generate a plan.
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("0"))
	tk.MustExec(`set @a=2`)
	tk.MustQuery(`execute stmt using @a`) // Second execution to cache the plan.
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("1"))

	// 2. Get the cached plan for statement A.
	// Note: `explain for connection` is used to get the real cached plan.
	tk.MustQuery(`execute stmt using @a`)
	planA := tk.MustQuery("explain for connection " + strconv.FormatUint(tk.Session().ShowProcess().ID, 10))
	// 3. Deallocate/drop the prepared statement.
	tk.MustExec(`deallocate prepare stmt`)

	// 4. Prepare and execute statement B with the same name.
	// This statement has a different structure and should generate a different plan.
	tk.MustExec(`prepare stmt from 'select * from t where b = ?'`)
	tk.MustExec(`set @b=1`)
	tk.MustExec(`execute stmt using @b`)

	// 5. Check if the cache was hit. It must NOT be hit because of the deallocation.
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("0"))
	tk.MustExec(`set @b=3`)
	tk.MustExec(`execute stmt using @b`)
	// 6. Get the new plan for statement B.
	planB := tk.MustQuery("explain for connection " + strconv.FormatUint(tk.Session().ShowProcess().ID, 10))

	// 7. Core Assertions:
	//    a. Assert that Plan A and Plan B are different.
	require.NotEqual(t, planA.Rows(), planB.Rows(), "Plan A and Plan B should be different")
	//    b. Check the content of the plans to be sure.
	//       Plan A should use index `a`. Plan B should use index `b`.
	planA.Check(testkit.Rows(
		`IndexLookUp_8 10.00 0 root  time:0s, open:0s, close:0s, loops:0  N/A N/A`,
		`├─IndexRangeScan_6(Build) 10.00 0 cop[tikv] table:t, index:a(a)  range:[2,2], keep order:false, stats:pseudo N/A N/A`,
		`└─TableRowIDScan_7(Probe) 10.00 0 cop[tikv] table:t  keep order:false, stats:pseudo N/A N/A`))
	planB.Check(testkit.Rows(
		`IndexLookUp_8 10.00 0 root  time:0s, open:0s, close:0s, loops:0  N/A N/A`,
		`├─IndexRangeScan_6(Build) 10.00 0 cop[tikv] table:t, index:b(b)  range:[3,3], keep order:false, stats:pseudo N/A N/A`,
		`└─TableRowIDScan_7(Probe) 10.00 0 cop[tikv] table:t  keep order:false, stats:pseudo N/A N/A`))

	// 8. Execute statement B again and verify it hits the cache now.
	tk.MustExec(`execute stmt using @b`)
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("1"))
}

func BenchmarkNewPlanCacheKey(b *testing.B) {
	store := testkit.CreateMockStore(b)
	tk := testkit.NewTestKit(b, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (a int, b int, c varchar(100), key(a), key(b, c))")
	tk.MustExec("prepare st from 'select * from t where a = ? and b = ? and c = ?'")
	tk.MustExec("set @a=1, @b=2, @c='test'")
	tk.MustExec("execute st using @a, @b, @c") // first execution to prepare the statement

	sctx := tk.Session().(sessionctx.Context)
	stmtID, _, _, err := tk.Session().PrepareStmt("select * from t where a = ? and b = ? and c = ?")
	require.NoError(b, err)
	prepStmt, err := sctx.GetSessionVars().GetPreparedStmtByID(stmtID)
	require.NoError(b, err)
	stmt := prepStmt.(*plannercore.PlanCacheStmt)

	b.ResetTimer()
	for b.Loop() {
		_, _, _, _, _ = plannercore.NewPlanCacheKey(sctx, stmt)
	}
}

func BenchmarkNewPlanCacheKeyInTxn(b *testing.B) {
	store := testkit.CreateMockStore(b)
	tk := testkit.NewTestKit(b, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (a int, b int, key(a))")
	tk.MustExec("insert into t values (1, 1), (2, 2)")

	stmtID, _, _, err := tk.Session().PrepareStmt("select * from t where a = ?")
	require.NoError(b, err)
	sctx := tk.Session().(sessionctx.Context)
	prepStmt, err := sctx.GetSessionVars().GetPreparedStmtByID(stmtID)
	require.NoError(b, err)
	stmt := prepStmt.(*plannercore.PlanCacheStmt)

	// Start a transaction and make the table dirty
	tk.MustExec("begin")
	tk.MustExec("insert into t values (3, 3)")

	b.ResetTimer()
	for b.Loop() {
		_, _, _, _, _ = plannercore.NewPlanCacheKey(sctx, stmt)
	}
	b.StopTimer()
	tk.MustExec("rollback")
}

func TestBenchDaily(t *testing.T) {
	benchdaily.Run(
		BenchmarkNewPlanCacheKey,
		BenchmarkNewPlanCacheKeyInTxn,
		BenchmarkPlanCacheBindingMatch,
		BenchmarkPlanCacheInsert,
		BenchmarkNonPreparedPlanCacheDML,
	)
}
