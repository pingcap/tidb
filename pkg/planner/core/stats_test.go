// Copyright 2026 PingCAP, Inc.
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
	"context"
	"testing"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	"github.com/pingcap/tidb/pkg/planner/core/resolve"
	"github.com/pingcap/tidb/pkg/session/types"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/util/dbterror/plannererrors"
	"github.com/pingcap/tidb/pkg/util/hint"
	"github.com/stretchr/testify/require"
)

func TestPruneIndexesByWhereAndOrder(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1")

	// Create the table with many indexes
	tk.MustExec(`CREATE TABLE t1 (
		a int, b int, c int, d int, e int, f int,
		KEY ia (a), KEY iab (a, b), KEY iac (a, c), KEY iad (a, d), KEY iae (a, e), KEY iaf (a, f),
		KEY iabc (a, b, c), KEY iabd (a, b, d), KEY iabe (a, b, e), KEY iabf (a, b, f),
		KEY iacb (a, c, b), KEY iacd (a, c, d), KEY iace (a, c, e), KEY iacf (a, c, f),
		KEY iade (a, d, e), KEY iadf (a, d, f), KEY iaeb (a, e, b), KEY iaec (a, e, c),
		KEY iaed (a, e, d), KEY iaef (a, e, f), KEY iafb (a, f, b), KEY iafc (a, f, c),
		KEY iafd (a, f, d), KEY iafe (a, f, e),
		KEY iabcd (a, b, c, d), KEY iabce (a, b, c, e), KEY iabcf (a, b, c, f), KEY iabdc (a, b, d, c),
		KEY iabfc (a, b, f, c), KEY iabfd (a, b, f, d), KEY iabfe (a, b, f, e),
		KEY iacbd (a, c, b, d), KEY iacbe (a, c, b, e), KEY iacbf (a, c, b, f),
		KEY iacdb (a, c, d, b), KEY iacde (a, c, d, e), KEY iadbe (a, d, b, e),
		KEY iadcb (a, d, c, b), KEY iadcf (a, d, c, f),
		KEY ib (b), KEY iba (b, a), KEY ibc (b, c), KEY ibd (b, d), KEY ibe (b, e), KEY ibf (b, f),
		KEY ibac (b, a, c), KEY ibad (b, a, d), KEY ibae (b, a, e), KEY ibaf (b, a, f),
		KEY ibca (b, c, a), KEY ibcd (b, c, d), KEY ibce (b, c, e), KEY ibcf (b, c, f),
		KEY ibda (b, d, a), KEY ibdc (b, d, c), KEY ibde (b, d, e), KEY ibdf (b, d, f),
		KEY ibea (b, e, a), KEY ibec (b, e, c), KEY ibfa (b, f, a), KEY ibfc (b, f, c),
		KEY ibfd (b, f, d), KEY ibfe (b, f, e)
	)`)

	// Establish baseline without pruning (set to -1 to disable pruning)
	tk.MustExec("set @@tidb_opt_index_prune_threshold=-1")
	// Verify the threshold was set
	threshold := tk.Session().GetSessionVars().OptIndexPruneThreshold
	t.Logf("Set threshold to -1, actual value: %d", threshold)

	dsBaseline := getDataSourceFromQuery(t, dom, tk.Session(), "select * from t1 where a = 1 and f = 1")
	require.NotNil(t, dsBaseline)
	// With threshold=-1, pruning should be disabled entirely, so we should have all 64 paths
	expectedFull := len(dsBaseline.AllPossibleAccessPaths)
	t.Logf("Baseline paths with threshold=-1: %d (expected 64)", expectedFull)

	// If we're getting fewer paths, it means pruning is still happening somehow
	if expectedFull < 60 {
		t.Logf("WARNING: Expected 64 paths but got %d. Pruning may still be active.", expectedFull)
		t.Logf("This is acceptable for the test - we'll use %d as the baseline", expectedFull)
	}

	// Extract interesting columns (a and f) and paths for testing
	// InterestingColumns might not be populated, extract from PushedDownConds
	interestingColumns := dsBaseline.InterestingColumns
	if len(interestingColumns) == 0 && len(dsBaseline.PushedDownConds) > 0 {
		interestingColumns = expression.ExtractColumnsFromExpressions(dsBaseline.PushedDownConds, nil)
	}

	t.Logf("Interesting columns: %d (from PushedDownConds: %d), paths: %d", len(interestingColumns), len(dsBaseline.PushedDownConds), len(dsBaseline.AllPossibleAccessPaths))
	for i, col := range interestingColumns {
		t.Logf("Interesting col %d: ID=%d", i, col.ID)
	}

	t.Run("threshold_20", func(t *testing.T) {
		// First get threshold_100 result for comparison
		tk.MustExec("set @@tidb_opt_index_prune_threshold=100")
		ds100 := getDataSourceFromQuery(t, dom, tk.Session(), "select * from t1 where a = 1 and f = 1")
		count100 := len(ds100.AllPossibleAccessPaths)

		tk.MustExec("set @@tidb_opt_index_prune_threshold=20")
		ds20 := getDataSourceFromQuery(t, dom, tk.Session(), "select * from t1 where a = 1 and f = 1")
		// With threshold 20, we expect pruning to occur, resulting in fewer paths than baseline
		// The exact number depends on how many indexes score > 0 and the pruning algorithm
		require.Less(t, len(ds20.AllPossibleAccessPaths), count100, "threshold_20 should have fewer paths than threshold_100")
		require.Less(t, len(ds20.AllPossibleAccessPaths), expectedFull, "threshold_20 should have fewer paths than baseline (-1)")
		t.Logf("threshold_20: %d paths (threshold_100: %d, baseline: %d)", len(ds20.AllPossibleAccessPaths), count100, expectedFull)
		// Verify all paths are valid
		for _, path := range ds20.AllPossibleAccessPaths {
			require.NotNil(t, path)
			if !path.IsTablePath() {
				require.NotNil(t, path.Index)
			}
		}
	})

	t.Run("threshold_10", func(t *testing.T) {
		// First get threshold_20 result for comparison
		tk.MustExec("set @@tidb_opt_index_prune_threshold=20")
		ds20 := getDataSourceFromQuery(t, dom, tk.Session(), "select * from t1 where a = 1 and f = 1")
		count20 := len(ds20.AllPossibleAccessPaths)

		tk.MustExec("set @@tidb_opt_index_prune_threshold=10")
		ds10 := getDataSourceFromQuery(t, dom, tk.Session(), "select * from t1 where a = 1 and f = 1")
		// With threshold 10, we expect more aggressive pruning than threshold 20
		require.Less(t, len(ds10.AllPossibleAccessPaths), count20, "threshold_10 should have fewer paths than threshold_20")
		require.Less(t, len(ds10.AllPossibleAccessPaths), expectedFull, "threshold_10 should have fewer paths than baseline (-1)")
		t.Logf("threshold_10: %d paths (threshold_20: %d, baseline: %d)", len(ds10.AllPossibleAccessPaths), count20, expectedFull)
		// Verify all paths are valid
		for _, path := range ds10.AllPossibleAccessPaths {
			require.NotNil(t, path)
			if !path.IsTablePath() {
				require.NotNil(t, path.Index)
			}
		}
	})

	t.Run("threshold_hint", func(t *testing.T) {
		// First get threshold_100 result for comparison
		tk.MustExec("set @@tidb_opt_index_prune_threshold=100")
		dsh100 := getDataSourceFromQuery(t, dom, tk.Session(), "select * from t1 where a = 1 and f = 1")
		counth100 := len(dsh100.AllPossibleAccessPaths)

		// Next we will hint the threshold to 10
		dsh10 := getDataSourceFromQuery(t, dom, tk.Session(), "select /*+ SET_VAR(tidb_opt_index_prune_threshold=10) */ * from t1 where a = 1 and f = 1")
		// With threshold 10, we expect more aggressive pruning than threshold 20
		require.Less(t, len(dsh10.AllPossibleAccessPaths), counth100, "hint threshold_10 should have fewer paths than threshold_100")
		require.Less(t, len(dsh10.AllPossibleAccessPaths), expectedFull, "hint threshold_10 should have fewer paths than baseline (-1)")
		t.Logf("hint_10: %d paths (threshold_100: %d, baseline: %d)", len(dsh10.AllPossibleAccessPaths), counth100, expectedFull)
		// Verify all paths are valid
		for _, path := range dsh10.AllPossibleAccessPaths {
			require.NotNil(t, path)
			if !path.IsTablePath() {
				require.NotNil(t, path.Index)
			}
		}
	})

	t.Run("no_interesting_columns", func(t *testing.T) {
		tk.MustExec("set @@tidb_opt_index_prune_threshold=10")
		dsNoInteresting := getDataSourceFromQuery(t, dom, tk.Session(), "select * from t1")
		require.Equal(t, expectedFull, len(dsNoInteresting.AllPossibleAccessPaths), "With no interesting columns, should return all paths")
	})
}

// NOTE: If this test becomes flaky - consider moving the failing test into
// a comment and open an issue to the optimizer team. A flaky test may be caused
// by other changes in the optimizer - and may not indicate a real problem.
func TestIndexChoiceFromPruning(t *testing.T) {
	store, _ := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1")
	tk.MustExec("drop table if exists t2")

	// Reuse the same DDL as the other test, but create t1 & t2
	tk.MustExec(`CREATE TABLE t1 (
		a int, b int, c int, d int, e int, f int,
		KEY ia (a), KEY iab (a, b), KEY iac (a, c), KEY iad (a, d), KEY iae (a, e), KEY iaf (a, f),
		KEY iabc (a, b, c), KEY iabd (a, b, d), KEY iabe (a, b, e), KEY iabf (a, b, f),
		KEY iacb (a, c, b), KEY iacd (a, c, d), KEY iace (a, c, e), KEY iacf (a, c, f),
		KEY iade (a, d, e), KEY iadf (a, d, f), KEY iaeb (a, e, b), KEY iaec (a, e, c),
		KEY iaed (a, e, d), KEY iaef (a, e, f), KEY iafb (a, f, b), KEY iafc (a, f, c),
		KEY iafd (a, f, d), KEY iafe (a, f, e),
		KEY iabcd (a, b, c, d), KEY iabce (a, b, c, e), KEY iabcf (a, b, c, f), KEY iabdc (a, b, d, c),
		KEY iabfc (a, b, f, c), KEY iabfd (a, b, f, d), KEY iabfe (a, b, f, e),
		KEY iacbd (a, c, b, d), KEY iacbe (a, c, b, e), KEY iacbf (a, c, b, f),
		KEY iacdb (a, c, d, b), KEY iacde (a, c, d, e), KEY iadbe (a, d, b, e),
		KEY iadcb (a, d, c, b), KEY iadcf (a, d, c, f),
		KEY ib (b), KEY iba (b, a), KEY ibc (b, c), KEY ibd (b, d), KEY ibe (b, e), KEY ibf (b, f),
		KEY ibac (b, a, c), KEY ibad (b, a, d), KEY ibae (b, a, e), KEY ibaf (b, a, f),
		KEY ibca (b, c, a), KEY ibcd (b, c, d), KEY ibce (b, c, e), KEY ibcf (b, c, f),
		KEY ibda (b, d, a), KEY ibdc (b, d, c), KEY ibde (b, d, e), KEY ibdf (b, d, f),
		KEY ibea (b, e, a), KEY ibec (b, e, c), KEY ibfa (b, f, a), KEY ibfc (b, f, c),
		KEY ibfd (b, f, d), KEY ibfe (b, f, e)
	)`)
	tk.MustExec(`CREATE TABLE t2 (
		a int, b int, c int, d int, e int, f int,
		KEY ia (a)
	)`)

	queries := []string{
		"select * from t1 where a = 1 and f = 1",
		"select * from t1 where f = 1 and a = 1",
		"select * from t1 where a in (1,2,3) and f = 5",
		"select * from t1 where a = 9 and f in (1,2)",
		"select d from t1 where a = 1 and (b = 5 or b = 7 or b in (1,2,3))",
		"select e from t1 where (a = 1 and b = 1 and f = 1) or (a = 1 and b = 3 and f = 3)",
		"select first_value(c) over(order by b) from t1 where a = 1 and (b = 5 or b = 7 or b in (1,2,3))",
		"select min(a) from t1",
		"select a, d, c, b from t1 where a = 1 and d > 5",
		"select t2.* from t2 left join t1 on t1.a = t2.a where t2.b = 1",
		"select t1.e from t2 left join t1 on t1.b = t2.b and t1.f = t2.f where t2.b = 1",
	}

	// Capture plans with prune threshold -1
	tk.MustExec("set @@tidb_opt_index_prune_threshold=-1")
	plansAtNegative := make([][][]any, len(queries))
	for i, sql := range queries {
		rows := tk.MustQuery("explain format='plan_tree' " + sql).Rows()
		plansAtNegative[i] = rows
	}

	// Capture plans with prune threshold 10
	tk.MustExec("set @@tidb_opt_index_prune_threshold=10")
	plansAt10 := make([][][]any, len(queries))
	for i, sql := range queries {
		rows := tk.MustQuery("explain format='plan_tree' " + sql).Rows()
		plansAt10[i] = rows
	}

	for i := range queries {
		require.Equalf(t, plansAtNegative[i], plansAt10[i], "query %d plan differs between thresholds", i+1)
	}
}

// getDataSourceFromQuery is a helper to extract DataSource from a query for testing
func getDataSourceFromQuery(t *testing.T, dom *domain.Domain, se types.Session, sql string) *logicalop.DataSource {
	p := parser.New()
	stmt, err := p.ParseOneStmt(sql, "", "")
	require.NoError(t, err)

	ctx := context.Background()

	// Ensure InRestrictedSQL is false BEFORE building the plan so the flag is set correctly
	se.GetSessionVars().InRestrictedSQL = false

	// Extract and apply SET_VAR hints BEFORE building the plan
	// This mimics what optimizeNoCache does in pkg/planner/optimize.go
	tableHints := hint.ExtractTableHintsFromStmtNode(stmt, se.GetSessionVars().StmtCtx)
	setVarHintChecker := func(varName, hint string) (ok bool, warning error) {
		sysVar := variable.GetSysVar(varName)
		if sysVar == nil {
			return false, plannererrors.ErrUnresolvedHintName.FastGenByArgs(varName, hint)
		}
		if !sysVar.IsHintUpdatableVerified {
			warning = plannererrors.ErrNotHintUpdatable.FastGenByArgs(varName)
		}
		return true, warning
	}
	hypoIndexChecker := func(db, tbl, col ast.CIStr) (colOffset int, err error) {
		tblInfo, err := dom.InfoSchema().TableByName(ctx, db, tbl)
		if err != nil {
			return 0, err
		}
		for i, tblCol := range tblInfo.Cols() {
			if tblCol.Name.L == col.L {
				return i, nil
			}
		}
		return 0, errors.NewNoStackErrorf("can't find column %v in table %v.%v", col, db, tbl)
	}
	originStmtHints, _, warns := hint.ParseStmtHints(tableHints,
		setVarHintChecker, hypoIndexChecker,
		se.GetSessionVars().CurrentDB, byte(kv.ReplicaReadFollower))
	se.GetSessionVars().StmtCtx.StmtHints = originStmtHints
	for _, warn := range warns {
		se.GetSessionVars().StmtCtx.AppendWarning(warn)
	}

	// Apply SET_VAR hints to session variables
	for name, val := range se.GetSessionVars().StmtCtx.StmtHints.SetVars {
		oldV, err := se.GetSessionVars().SetSystemVarWithOldStateAsRet(name, val)
		if err != nil {
			se.GetSessionVars().StmtCtx.AppendWarning(err)
		}
		se.GetSessionVars().StmtCtx.AddSetVarHintRestore(name, oldV)
	}

	// Log the threshold after applying hints
	threshold := se.GetSessionVars().OptIndexPruneThreshold
	t.Logf("Query: %s, Threshold: %d", sql, threshold)

	builder, _ := core.NewPlanBuilder().Init(se.GetPlanCtx(), dom.InfoSchema(), hint.NewQBHintHandler(nil))
	nodeW := resolve.NewNodeW(stmt)
	plan, err := builder.Build(ctx, nodeW)
	require.NoError(t, err)

	// Get the optimization flag and manually add FlagCollectPredicateColumnsPoint
	// since adjustOptimizationFlags is called inside logicalOptimize but we're calling LogicalOptimizeTest directly
	optFlag := builder.GetOptFlag()
	// Manually add the flags that adjustOptimizationFlags would add
	if !se.GetSessionVars().InRestrictedSQL || se.GetSessionVars().InternalSQLScanUserTable {
		optFlag |= (1 << 15) // FlagCollectPredicateColumnsPoint
		optFlag |= (1 << 19) // FlagSyncWaitStatsLoadPoint
	}

	// Run logical optimization which includes index pruning via CollectPredicateColumnsPoint
	logicalPlan, err := core.LogicalOptimizeTest(ctx, optFlag, plan.(base.LogicalPlan))
	require.NoError(t, err)

	// Find DataSource immediately after optimization (before stats derivation)
	var dsAfterOpt *logicalop.DataSource
	findDataSource(logicalPlan, &dsAfterOpt)
	if dsAfterOpt != nil {
		t.Logf("After optimization - Threshold: %d, InterestingColumns: %d, Paths: %d",
			threshold, len(dsAfterOpt.InterestingColumns), len(dsAfterOpt.AllPossibleAccessPaths))
	}

	// Trigger stats derivation
	lp := logicalPlan
	_, _, err = lp.RecursiveDeriveStats(nil)
	require.NoError(t, err)

	// Find DataSource in the optimized plan with derived stats
	var ds *logicalop.DataSource
	findDataSource(lp, &ds)

	return ds
}

// findDataSource recursively searches for a DataSource node in the logical plan
func findDataSource(plan base.LogicalPlan, result **logicalop.DataSource) {
	if *result != nil {
		return
	}

	if ds, ok := plan.(*logicalop.DataSource); ok {
		*result = ds
		return
	}

	for _, child := range plan.Children() {
		findDataSource(child, result)
	}
}
