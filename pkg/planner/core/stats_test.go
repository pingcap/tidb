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

package core_test

import (
	"context"
	"testing"

	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	"github.com/pingcap/tidb/pkg/planner/core/resolve"
	"github.com/pingcap/tidb/pkg/session/sessionapi"
	"github.com/pingcap/tidb/pkg/testkit"
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

	// Establish baseline without pruning (very large threshold)
	tk.MustExec("set @@tidb_opt_index_prune_threshold=100000")
	dsBaseline := getDataSourceFromQuery(t, dom, tk.Session(), "select * from t1 where a = 1 and f = 1")
	require.NotNil(t, dsBaseline)
	require.Greater(t, len(dsBaseline.AllPossibleAccessPaths), 60, "Should have 60+ paths")
	expectedFull := len(dsBaseline.AllPossibleAccessPaths)

	// Extract WHERE columns (a and f) and paths for testing
	// WhereColumns might not be populated, extract from PushedDownConds
	whereColumns := dsBaseline.WhereColumns
	if len(whereColumns) == 0 && len(dsBaseline.PushedDownConds) > 0 {
		whereColumns = expression.ExtractColumnsFromExpressions(dsBaseline.PushedDownConds, nil)
	}
	//orderingColumns := []*expression.Column{}
	paths := dsBaseline.AllPossibleAccessPaths

	t.Logf("WHERE columns: %d (from PushedDownConds: %d), paths: %d", len(whereColumns), len(dsBaseline.PushedDownConds), len(paths))
	for i, col := range whereColumns {
		t.Logf("WHERE col %d: ID=%d", i, col.ID)
	}

	t.Run("threshold_100", func(t *testing.T) {
		// With threshold 100 (> full count), no pruning should happen in the pipeline
		tk.MustExec("set @@tidb_opt_index_prune_threshold=100")
		ds100 := getDataSourceFromQuery(t, dom, tk.Session(), "select * from t1 where a = 1 and f = 1")
		require.Equal(t, expectedFull, len(ds100.AllPossibleAccessPaths), "With threshold 100, should keep all indexes")
	})

	t.Run("threshold_20", func(t *testing.T) {
		tk.MustExec("set @@tidb_opt_index_prune_threshold=20")
		ds20 := getDataSourceFromQuery(t, dom, tk.Session(), "select * from t1 where a = 1 and f = 1")
		require.Equal(t, 20, len(ds20.AllPossibleAccessPaths), "With threshold 20, should prune to exactly 20 indexes")

		// Verify all paths are valid
		for _, path := range ds20.AllPossibleAccessPaths {
			require.NotNil(t, path)
			if !path.IsTablePath() {
				require.NotNil(t, path.Index)
			}
		}
	})

	t.Run("threshold_10", func(t *testing.T) {
		tk.MustExec("set @@tidb_opt_index_prune_threshold=10")
		ds10 := getDataSourceFromQuery(t, dom, tk.Session(), "select * from t1 where a = 1 and f = 1")
		require.Equal(t, 10, len(ds10.AllPossibleAccessPaths), "With threshold 10, should prune to exactly 10 indexes")
	})

	t.Run("no_where_columns", func(t *testing.T) {
		tk.MustExec("set @@tidb_opt_index_prune_threshold=10")
		dsNoWhere := getDataSourceFromQuery(t, dom, tk.Session(), "select * from t1")
		require.Equal(t, expectedFull, len(dsNoWhere.AllPossibleAccessPaths), "With no WHERE columns, should return all paths")
	})

	// Unit-level empty/single path cases are omitted in E2E pipeline test
}

// getDataSourceFromQuery is a helper to extract DataSource from a query for testing
func getDataSourceFromQuery(t *testing.T, dom *domain.Domain, se sessionapi.Session, sql string) *logicalop.DataSource {
	p := parser.New()
	stmt, err := p.ParseOneStmt(sql, "", "")
	require.NoError(t, err)

	ctx := context.Background()
	builder, _ := core.NewPlanBuilder().Init(se.GetPlanCtx(), dom.InfoSchema(), hint.NewQBHintHandler(nil))
	nodeW := resolve.NewNodeW(stmt)
	plan, err := builder.Build(ctx, nodeW)
	require.NoError(t, err)

	// Run logical optimization to populate WhereColumns and OrderingColumns
	logicalPlan, err := core.LogicalOptimizeTest(ctx, builder.GetOptFlag(), plan.(base.LogicalPlan))
	require.NoError(t, err)

	// Populate WhereColumns on DataSource before stats derivation
	var dsForPopulate *logicalop.DataSource
	findDataSource(logicalPlan.(base.LogicalPlan), &dsForPopulate)
	if dsForPopulate != nil && len(dsForPopulate.WhereColumns) == 0 && len(dsForPopulate.PushedDownConds) > 0 {
		dsForPopulate.WhereColumns = expression.ExtractColumnsFromExpressions(dsForPopulate.PushedDownConds, nil)
	}

	// Trigger stats derivation; pruning happens here
	lp := logicalPlan.(base.LogicalPlan)
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
		if lp, ok := child.(base.LogicalPlan); ok {
			findDataSource(lp, result)
		}
	}
}
