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
	"context"
	"fmt"
	"testing"

	"github.com/pingcap/tidb/parser/model"
	plannercore "github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/testkit"
	"github.com/pingcap/tidb/util/hint"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/stretchr/testify/require"
)

func TestCollectPredicateColumns(t *testing.T) {
	store, dom, clean := testkit.CreateMockStoreAndDomain(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1, t2")
	tk.MustExec("set @@session.tidb_partition_prune_mode = 'static'")
	tk.MustExec("create table t1(a int, b int, c int)")
	tk.MustExec("create table t2(a int, b int, c int)")
	tk.MustExec("create table t3(a int, b int, c int) partition by range(a) (partition p0 values less than (10), partition p1 values less than (20), partition p2 values less than maxvalue)")

	tests := []struct {
		sql string
		res []string
	}{
		{
			// DataSource
			sql: "select * from t1 where a > 2",
			res: []string{"t1.a"},
		},
		{
			// DataSource
			sql: "select * from t1 where b in (2, 5) or c = 5",
			res: []string{"t1.b", "t1.c"},
		},
		{
			// LogicalProjection
			sql: "select * from (select a + b as ab, c from t1) as tmp where ab > 4",
			res: []string{"t1.a", "t1.b"},
		},
		{
			// LogicalAggregation
			sql: "select b, count(*) from t1 group by b",
			res: []string{"t1.b"},
		},
		{
			// LogicalAggregation
			sql: "select b, sum(a) from t1 group by b having sum(a) > 3",
			res: []string{"t1.a", "t1.b"},
		},
		{
			// LogicalAggregation
			sql: "select count(*), sum(a), sum(c) from t1",
			res: []string{},
		},
		{
			// LogicalAggregation
			sql: "(select a, b from t1) union (select a, c from t2)",
			res: []string{"t1.a", "t1.b", "t2.a", "t2.c"},
		},
		{
			// LogicalWindow
			sql: "select avg(b) over(partition by a) from t1",
			res: []string{"t1.a"},
		},
		{
			// LogicalWindow
			sql: "select * from (select avg(b) over(partition by a) as w from t1) as tmp where w > 4",
			res: []string{"t1.a", "t1.b"},
		},
		{
			// LogicalWindow
			sql: "select row_number() over(partition by a order by c) from t1",
			res: []string{"t1.a"},
		},
		{
			// LogicalJoin
			sql: "select * from t1, t2 where t1.a = t2.a",
			res: []string{"t1.a", "t2.a"},
		},
		{
			// LogicalJoin
			sql: "select * from t1 as x join t2 as y on x.b + y.c > 2",
			res: []string{"t1.b", "t2.c"},
		},
		{
			// LogicalJoin
			sql: "select * from t1 as x join t2 as y on x.a = y.a and x.b < 3 and y.c > 2",
			res: []string{"t1.a", "t1.b", "t2.a", "t2.c"},
		},
		{
			// LogicalJoin
			sql: "select x.b, y.c, sum(x.c), sum(y.b) from t1 as x join t2 as y on x.a = y.a group by x.b, y.c order by x.b",
			res: []string{"t1.a", "t1.b", "t2.a", "t2.c"},
		},
		{
			// LogicalApply
			sql: "select * from t1 where t1.b > all(select b from t2 where t2.c > 2)",
			res: []string{"t1.b", "t2.b", "t2.c"},
		},
		{
			// LogicalApply
			sql: "select * from t1 where t1.b > (select count(b) from t2 where t2.c > t1.a)",
			res: []string{"t1.a", "t1.b", "t2.b", "t2.c"},
		},
		{
			// LogicalApply
			sql: "select * from t1 where t1.b > (select count(*) from t2 where t2.c > t1.a)",
			res: []string{"t1.a", "t1.b", "t2.c"},
		},
		{
			// LogicalSort
			sql: "select * from t1 order by c",
			res: []string{"t1.c"},
		},
		{
			// LogicalTopN
			sql: "select * from t1 order by a + b limit 10",
			res: []string{"t1.a", "t1.b"},
		},
		{
			// LogicalUnionAll
			sql: "select * from ((select a, b from t1) union all (select a, c from t2)) as tmp where tmp.b > 2",
			res: []string{"t1.b", "t2.c"},
		},
		{
			// LogicalPartitionUnionAll
			sql: "select * from t3 where a < 15 and b > 1",
			res: []string{"t3.a", "t3.b"},
		},
		{
			// LogicalCTE
			sql: "with cte(x, y) as (select a + 1, b from t1 where b > 1) select * from cte where x > 3",
			res: []string{"t1.a", "t1.b"},
		},
		{
			// LogicalCTE, LogicalCTETable
			sql: "with recursive cte(x, y) as (select c, 1 from t1 union all select x + 1, y from cte where x < 5) select * from cte",
			res: []string{"t1.c"},
		},
		{
			// LogicalCTE, LogicalCTETable
			sql: "with recursive cte(x, y) as (select 1, c from t1 union all select x + 1, y from cte where x < 5) select * from cte where y > 1",
			res: []string{"t1.c"},
		},
		{
			// LogicalCTE, LogicalCTETable
			sql: "with recursive cte(x, y) as (select a, b from t1 union select x + 1, y from cte where x < 5) select * from cte",
			res: []string{"t1.a", "t1.b"},
		},
	}

	ctx := context.Background()
	sctx := tk.Session()
	is := dom.InfoSchema()
	getColName := func(tblColID model.TableColumnID) (string, bool) {
		tbl, ok := is.TableByID(tblColID.TableID)
		if !ok {
			return "", false
		}
		tblInfo := tbl.Meta()
		for _, col := range tblInfo.Columns {
			if tblColID.ColumnID == col.ID {
				return tblInfo.Name.L + "." + col.Name.L, true
			}
		}
		return "", false
	}
	checkPredicateColumns := func(lp plannercore.LogicalPlan, expected []string, comment string) {
		tblColIDs := plannercore.CollectPredicateColumnsForTest(lp)
		cols := make([]string, 0, len(tblColIDs))
		for _, tblColID := range tblColIDs {
			col, ok := getColName(tblColID)
			require.True(t, ok, comment)
			cols = append(cols, col)
		}
		require.ElementsMatch(t, expected, cols, comment)
	}

	for _, tt := range tests {
		comment := fmt.Sprintf("for %s", tt.sql)
		logutil.BgLogger().Info(comment)
		stmts, err := tk.Session().Parse(ctx, tt.sql)
		require.NoError(t, err, comment)
		stmt := stmts[0]
		err = plannercore.Preprocess(sctx, stmt, plannercore.WithPreprocessorReturn(&plannercore.PreprocessorReturn{InfoSchema: is}))
		require.NoError(t, err, comment)
		builder, _ := plannercore.NewPlanBuilder().Init(sctx, is, &hint.BlockHintProcessor{})
		p, err := builder.Build(ctx, stmt)
		require.NoError(t, err, comment)
		lp, ok := p.(plannercore.LogicalPlan)
		require.True(t, ok, comment)
		// We check predicate columns twice, before and after logical optimization. Some logical plan patterns may occur before
		// logical optimization while others may occur after logical optimization.
		// logutil.BgLogger().Info("before logical opt", zap.String("lp", plannercore.ToString(lp)))
		checkPredicateColumns(lp, tt.res, comment)
		lp, err = plannercore.LogicalOptimize(ctx, builder.GetOptFlag(), lp)
		require.NoError(t, err, comment)
		// logutil.BgLogger().Info("after logical opt", zap.String("lp", plannercore.ToString(lp)))
		checkPredicateColumns(lp, tt.res, comment)
	}
}
