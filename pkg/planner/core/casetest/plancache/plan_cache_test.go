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
	"testing"

	plannercore "github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/util/benchdaily"
	"github.com/stretchr/testify/require"
)

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
	)
}
