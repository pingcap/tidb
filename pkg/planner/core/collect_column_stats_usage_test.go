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

package core

import (
	"context"
	"fmt"
	"sort"
	"testing"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/meta/model"
	pmodel "github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/resolve"
	"github.com/pingcap/tidb/pkg/planner/core/rule"
	"github.com/pingcap/tidb/pkg/util/hint"
	"github.com/stretchr/testify/require"
)

func getTblInfoByPhyID(t *testing.T, is infoschema.InfoSchema, physicalTblID int64) (*model.TableInfo, string) {
	if tbl, ok := is.TableByID(context.Background(), physicalTblID); ok {
		tblInfo := tbl.Meta()
		return tblInfo, tblInfo.Name.L
	}
	db, exists := is.SchemaByName(pmodel.NewCIStr("test"))
	require.True(t, exists)
	tblInfos, err := is.SchemaTableInfos(context.Background(), db.Name)
	require.NoError(t, err)
	for _, tbl := range tblInfos {
		pi := tbl.GetPartitionInfo()
		if pi == nil {
			continue
		}
		for _, def := range pi.Definitions {
			if def.ID == physicalTblID {
				return tbl, tbl.Name.L + "." + def.Name.L
			}
		}
	}
	require.Fail(t, "table not found, physical ID: %d", physicalTblID)
	return nil, ""
}

func getColumnName(t *testing.T, is infoschema.InfoSchema, tblColID model.TableItemID, comment string) string {
	tblInfo, prefix := getTblInfoByPhyID(t, is, tblColID.TableID)
	prefix += "."

	var colName string
	for _, col := range tblInfo.Columns {
		if tblColID.ID == col.ID {
			colName = prefix + col.Name.L
		}
	}
	require.NotEmpty(t, colName, comment)
	return colName
}

func getStatsLoadItem(t *testing.T, is infoschema.InfoSchema, item model.StatsLoadItem, comment string) string {
	str := getColumnName(t, is, item.TableItemID, comment)
	if item.FullLoad {
		str += " full"
	} else {
		str += " meta"
	}
	return str
}

func checkColumnStatsUsageForPredicates(t *testing.T, is infoschema.InfoSchema, lp base.LogicalPlan, expected []string, comment string) {
	tblColIDs, _, _ := CollectColumnStatsUsage(lp, false)
	cols := make([]string, 0, len(tblColIDs))
	for tblColID := range tblColIDs {
		col := getColumnName(t, is, tblColID, comment)
		cols = append(cols, col)
	}
	sort.Strings(cols)
	require.Equal(t, expected, cols, comment)
}

func checkColumnStatsUsageForStatsLoad(t *testing.T, is infoschema.InfoSchema, lp base.LogicalPlan, expectedCols []string, expectedParts map[string][]string, comment string) {
	predicateCols, _, expandedPartitions := CollectColumnStatsUsage(lp, true)
	loadItems := make([]model.StatsLoadItem, 0, len(predicateCols))
	for tblColID, fullLoad := range predicateCols {
		loadItems = append(loadItems, model.StatsLoadItem{TableItemID: tblColID, FullLoad: fullLoad})
	}
	cols := make([]string, 0, len(loadItems))
	for _, item := range loadItems {
		col := getStatsLoadItem(t, is, item, comment)
		cols = append(cols, col)
	}
	sort.Strings(cols)
	require.Equal(t, expectedCols, cols, comment+", we get %v", cols)
	if len(expectedParts) == 0 {
		require.Empty(t, expandedPartitions, comment)
		return
	}
	expanded := make(map[string][]string, len(expandedPartitions))
	for tblID, partIDs := range expandedPartitions {
		_, tblName := getTblInfoByPhyID(t, is, tblID)
		parts := make([]string, 0, len(partIDs))
		for _, partID := range partIDs {
			_, partName := getTblInfoByPhyID(t, is, partID)
			parts = append(parts, partName)
		}
		sort.Strings(parts)
		expanded[tblName] = parts
	}
	require.Equal(t, expectedParts, expanded, comment)
}

func TestSkipSystemTables(t *testing.T) {
	sql := "select * from mysql.stats_meta where a > 1"
	res := []string{}
	s := createPlannerSuite()
	defer s.Close()
	ctx := context.Background()
	stmt, err := s.p.ParseOneStmt(sql, "", "")
	require.NoError(t, err)
	nodeW := resolve.NewNodeW(stmt)
	err = Preprocess(context.Background(), s.sctx, nodeW, WithPreprocessorReturn(&PreprocessorReturn{InfoSchema: s.is}))
	require.NoError(t, err)
	builder, _ := NewPlanBuilder().Init(s.ctx, s.is, hint.NewQBHintHandler(nil))
	p, err := builder.Build(ctx, nodeW)
	require.NoError(t, err)
	lp, ok := p.(base.LogicalPlan)
	require.True(t, ok)
	// We check predicate columns twice, before and after logical optimization. Some logical plan patterns may occur before
	// logical optimization while others may occur after logical optimization.
	checkColumnStatsUsageForPredicates(t, s.is, lp, res, sql)
	lp, err = logicalOptimize(ctx, builder.GetOptFlag(), lp)
	require.NoError(t, err)
	checkColumnStatsUsageForPredicates(t, s.is, lp, res, sql)
}

func TestCollectPredicateColumns(t *testing.T) {
	tests := []struct {
		pruneMode string
		sql       string
		res       []string
	}{
		{
			// DataSource
			sql: "select * from t where a > 2",
			res: []string{"t.a"},
		},
		{
			// DataSource
			sql: "select * from t where b in (2, 5) or c = 5",
			res: []string{"t.b", "t.c"},
		},
		{
			// LogicalProjection
			sql: "select * from (select a + b as ab, c from t) as tmp where ab > 4",
			res: []string{"t.a", "t.b"},
		},
		{
			// LogicalAggregation
			sql: "select b, count(*) from t group by b",
			res: []string{"t.b"},
		},
		{
			// LogicalAggregation
			sql: "select b, sum(a) from t group by b having sum(a) > 3",
			res: []string{"t.a", "t.b"},
		},
		{
			// LogicalAggregation
			sql: "select count(*), sum(a), sum(c) from t",
			res: []string{},
		},
		{
			// LogicalAggregation
			sql: "(select a, c from t) union (select a, b from t2)",
			res: []string{"t.a", "t.c", "t2.a", "t2.b"},
		},
		{
			// LogicalWindow
			sql: "select avg(b) over(partition by a) from t",
			res: []string{"t.a"},
		},
		{
			// LogicalWindow
			sql: "select * from (select avg(b) over(partition by a) as w from t) as tmp where w > 4",
			res: []string{"t.a", "t.b"},
		},
		{
			// LogicalWindow
			sql: "select row_number() over(partition by a order by c) from t",
			res: []string{"t.a"},
		},
		{
			// LogicalJoin
			sql: "select * from t, t2 where t.a = t2.a",
			res: []string{"t.a", "t2.a"},
		},
		{
			// LogicalJoin
			sql: "select * from t as x join t2 as y on x.c + y.b > 2",
			res: []string{"t.c", "t2.b"},
		},
		{
			// LogicalJoin
			sql: "select * from t as x join t2 as y on x.a = y.a and x.c < 3 and y.b > 2",
			res: []string{"t.a", "t.c", "t2.a", "t2.b"},
		},
		{
			// LogicalJoin
			sql: "select x.c, y.b, sum(x.b), sum(y.a) from t as x join t2 as y on x.a < y.a group by x.c, y.b order by x.c",
			res: []string{"t.a", "t.c", "t2.a", "t2.b"},
		},
		{
			// LogicalApply, LogicalJoin
			sql: "select * from t2 where t2.b > all(select b from t where t.c > 2)",
			res: []string{"t.b", "t.c", "t2.b"},
		},
		{
			// LogicalApply, LogicalJoin
			sql: "select * from t2 where t2.b > any(select b from t where t.c > 2)",
			res: []string{"t.b", "t.c", "t2.b"},
		},
		{
			// LogicalApply, LogicalJoin
			sql: "select * from t2 where t2.b > (select sum(b) from t where t.c > t2.a)",
			res: []string{"t.b", "t.c", "t2.a", "t2.b"},
		},
		{
			// LogicalApply
			sql: "select * from t2 where t2.b > (select count(*) from t where t.a > t2.a)",
			res: []string{"t.a", "t2.a", "t2.b"},
		},
		{
			// LogicalApply, LogicalJoin
			sql: "select * from t2 where exists (select * from t where t.a > t2.b)",
			res: []string{"t.a", "t2.b"},
		},
		{
			// LogicalApply, LogicalJoin
			sql: "select * from t2 where not exists (select * from t where t.a > t2.b)",
			res: []string{"t.a", "t2.b"},
		},
		{
			// LogicalJoin
			sql: "select * from t2 where t2.a in (select b from t)",
			res: []string{"t.b", "t2.a"},
		},
		{
			// LogicalApply, LogicalJoin
			sql: "select * from t2 where t2.a not in (select b from t)",
			res: []string{"t.b", "t2.a"},
		},
		{
			// LogicalSort
			sql: "select * from t order by c",
			res: []string{"t.c"},
		},
		{
			// LogicalTopN
			sql: "select * from t order by a + b limit 10",
			res: []string{"t.a", "t.b"},
		},
		{
			// LogicalUnionAll
			sql: "select * from ((select a, c from t) union all (select a, b from t2)) as tmp where tmp.c > 2",
			res: []string{"t.c", "t2.b"},
		},
		{
			// LogicalCTE
			sql: "with cte(x, y) as (select a + 1, b from t where b > 1) select * from cte where x > 3",
			res: []string{"t.a", "t.b"},
		},
		{
			// LogicalCTE, LogicalCTETable
			sql: "with recursive cte(x, y) as (select c, 1 from t union all select x + 1, y from cte where x < 5) select * from cte",
			res: []string{"t.c"},
		},
		{
			// LogicalCTE, LogicalCTETable
			sql: "with recursive cte(x, y) as (select 1, c from t union all select x + 1, y from cte where x < 5) select * from cte where y > 1",
			res: []string{"t.c"},
		},
		{
			// LogicalCTE, LogicalCTETable
			sql: "with recursive cte(x, y) as (select a, b from t union select x + 1, y from cte where x < 5) select * from cte",
			res: []string{"t.a", "t.b"},
		},
		{
			// LogicalPartitionUnionAll, static partition prune mode, use table ID rather than partition ID
			pruneMode: "static",
			sql:       "select * from pt1 where ptn < 20 and b > 1",
			res:       []string{"pt1.b", "pt1.ptn"},
		},
		{
			// dynamic partition prune mode, use table ID rather than partition ID
			pruneMode: "dynamic",
			sql:       "select * from pt1 where ptn < 20 and b > 1",
			res:       []string{"pt1.b", "pt1.ptn"},
		},
	}

	s := createPlannerSuite()
	defer s.Close()
	ctx := context.Background()
	for _, tt := range tests {
		comment := fmt.Sprintf("sql: %s", tt.sql)
		if len(tt.pruneMode) > 0 {
			s.ctx.GetSessionVars().PartitionPruneMode.Store(tt.pruneMode)
		}
		stmt, err := s.p.ParseOneStmt(tt.sql, "", "")
		require.NoError(t, err, comment)
		nodeW := resolve.NewNodeW(stmt)
		err = Preprocess(context.Background(), s.sctx, nodeW, WithPreprocessorReturn(&PreprocessorReturn{InfoSchema: s.is}))
		require.NoError(t, err, comment)
		builder, _ := NewPlanBuilder().Init(s.ctx, s.is, hint.NewQBHintHandler(nil))
		p, err := builder.Build(ctx, nodeW)
		require.NoError(t, err, comment)
		lp, ok := p.(base.LogicalPlan)
		require.True(t, ok, comment)
		// We check predicate columns twice, before and after logical optimization. Some logical plan patterns may occur before
		// logical optimization while others may occur after logical optimization.
		checkColumnStatsUsageForPredicates(t, s.is, lp, tt.res, comment)
		lp, err = logicalOptimize(ctx, builder.GetOptFlag(), lp)
		require.NoError(t, err, comment)
		checkColumnStatsUsageForPredicates(t, s.is, lp, tt.res, comment)
	}
}

func TestCollectHistNeededColumns(t *testing.T) {
	failpoint.Enable("github.com/pingcap/tidb/pkg/planner/core/forceDynamicPrune", `return(true)`)
	defer failpoint.Disable("github.com/pingcap/tidb/pkg/planner/core/forceDynamicPrune")
	tests := []struct {
		pruneMode     string
		sql           string
		res           []string
		expandedParts map[string][]string
	}{
		{
			sql: "select * from t where a > 2",
			res: []string{"t.a full"},
		},
		{
			sql: "select * from t where b in (2, 5) or c = 5",
			res: []string{"t.b full", "t.c full"},
		},
		{
			sql: "select * from t where a + b > 1",
			res: []string{"t.a full", "t.b full"},
		},
		{
			sql: "select b, count(a) from t where b > 1 group by b having count(a) > 2",
			res: []string{"t.a meta", "t.b full"},
		},
		{
			sql: "select * from t as x join t2 as y on x.b + y.b > 2 and x.c > 1 and y.a < 1",
			res: []string{"t.b meta", "t.c full", "t2.a full", "t2.b meta"},
		},
		{
			sql: "select * from t2 where t2.b > all(select b from t where t.c > 2)",
			res: []string{"t.b meta", "t.c full", "t2.b meta"},
		},
		{
			sql: "select * from t2 where t2.b > any(select b from t where t.c > 2)",
			res: []string{"t.b meta", "t.c full", "t2.b meta"},
		},
		{
			sql: "select * from t2 where t2.b in (select b from t where t.c > 2)",
			res: []string{"t.b meta", "t.c full", "t2.b meta"},
		},
		{
			pruneMode: "static",
			sql:       "select * from pt1 where ptn < 20 and b > 1",
			res:       []string{"pt1.b full", "pt1.ptn full"},
			expandedParts: map[string][]string{
				"pt1": {"pt1.p1", "pt1.p2"},
			},
		},
		{
			pruneMode: "dynamic",
			sql:       "select * from pt1 where ptn < 20 and b > 1",
			res:       []string{"pt1.b full", "pt1.ptn full"},
		},
	}

	s := createPlannerSuite()
	defer s.Close()
	ctx := context.Background()
	for _, tt := range tests {
		comment := fmt.Sprintf("sql: %s", tt.sql)
		if len(tt.pruneMode) > 0 {
			s.ctx.GetSessionVars().PartitionPruneMode.Store(tt.pruneMode)
		}
		if s.ctx.GetSessionVars().IsDynamicPartitionPruneEnabled() {
			s.ctx.GetSessionVars().StmtCtx.UseDynamicPruneMode = true
		} else {
			s.ctx.GetSessionVars().StmtCtx.UseDynamicPruneMode = false
		}
		stmt, err := s.p.ParseOneStmt(tt.sql, "", "")
		require.NoError(t, err, comment)
		nodeW := resolve.NewNodeW(stmt)
		err = Preprocess(context.Background(), s.sctx, nodeW, WithPreprocessorReturn(&PreprocessorReturn{InfoSchema: s.is}))
		require.NoError(t, err, comment)
		builder, _ := NewPlanBuilder().Init(s.ctx, s.is, hint.NewQBHintHandler(nil))
		p, err := builder.Build(ctx, nodeW)
		require.NoError(t, err, comment)
		lp, ok := p.(base.LogicalPlan)
		require.True(t, ok, comment)
		flags := builder.GetOptFlag()
		// JoinReOrder may need columns stats so collecting hist-needed columns must happen before JoinReOrder.
		// Hence, we disable JoinReOrder and PruneColumnsAgain here.
		flags &= ^(rule.FlagJoinReOrder | rule.FlagPruneColumnsAgain)
		lp, err = logicalOptimize(ctx, flags, lp)
		require.NoError(t, err, comment)
		checkColumnStatsUsageForStatsLoad(t, s.is, lp, tt.res, tt.expandedParts, comment)
	}
}
