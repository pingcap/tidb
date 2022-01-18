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
	"sort"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/util/hint"
	"github.com/pingcap/tidb/util/testleak"
)

func getColumnName(c *C, is infoschema.InfoSchema, tblColID model.TableColumnID, comment CommentInterface) (string, bool) {
	var tblInfo *model.TableInfo
	var prefix string
	if tbl, ok := is.TableByID(tblColID.TableID); ok {
		tblInfo = tbl.Meta()
		prefix = tblInfo.Name.L + "."
	} else {
		db, exists := is.SchemaByName(model.NewCIStr("test"))
		c.Assert(exists, IsTrue, comment)
		for _, tbl := range db.Tables {
			pi := tbl.GetPartitionInfo()
			if pi == nil {
				continue
			}
			for _, def := range pi.Definitions {
				if def.ID == tblColID.TableID {
					tblInfo = tbl
					prefix = tbl.Name.L + "." + def.Name.L + "."
					break
				}
			}
			if tblInfo != nil {
				break
			}
		}
		if tblInfo == nil {
			return "", false
		}
	}
	for _, col := range tblInfo.Columns {
		if tblColID.ColumnID == col.ID {
			return prefix + col.Name.L, true
		}
	}
	return "", false
}

func checkColumnStatsUsage(c *C, is infoschema.InfoSchema, lp LogicalPlan, histNeededOnly bool, expected []string, comment CommentInterface) {
	var tblColIDs []model.TableColumnID
	if histNeededOnly {
		_, tblColIDs = CollectColumnStatsUsage(lp, false, true)
	} else {
		tblColIDs, _ = CollectColumnStatsUsage(lp, true, false)
	}
	cols := make([]string, 0, len(tblColIDs))
	for _, tblColID := range tblColIDs {
		col, ok := getColumnName(c, is, tblColID, comment)
		c.Assert(ok, IsTrue, comment)
		cols = append(cols, col)
	}
	sort.Strings(cols)
	c.Assert(cols, DeepEquals, expected, comment)
}

func (s *testPlanSuite) TestCollectPredicateColumns(c *C) {
	defer testleak.AfterTest(c)()
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

	ctx := context.Background()
	for _, tt := range tests {
		comment := Commentf("for %s", tt.sql)
		if len(tt.pruneMode) > 0 {
			s.ctx.GetSessionVars().PartitionPruneMode.Store(tt.pruneMode)
		}
		stmt, err := s.ParseOneStmt(tt.sql, "", "")
		c.Assert(err, IsNil, comment)
		err = Preprocess(s.ctx, stmt, WithPreprocessorReturn(&PreprocessorReturn{InfoSchema: s.is}))
		c.Assert(err, IsNil, comment)
		builder, _ := NewPlanBuilder().Init(s.ctx, s.is, &hint.BlockHintProcessor{})
		p, err := builder.Build(ctx, stmt)
		c.Assert(err, IsNil, comment)
		lp, ok := p.(LogicalPlan)
		c.Assert(ok, IsTrue, comment)
		// We check predicate columns twice, before and after logical optimization. Some logical plan patterns may occur before
		// logical optimization while others may occur after logical optimization.
		checkColumnStatsUsage(c, s.is, lp, false, tt.res, comment)
		lp, err = logicalOptimize(ctx, builder.GetOptFlag(), lp)
		c.Assert(err, IsNil, comment)
		checkColumnStatsUsage(c, s.is, lp, false, tt.res, comment)
	}
}

func (s *testPlanSuite) TestCollectHistNeededColumns(c *C) {
	defer testleak.AfterTest(c)()
	tests := []struct {
		pruneMode string
		sql       string
		res       []string
	}{
		{
			sql: "select * from t where a > 2",
			res: []string{"t.a"},
		},
		{
			sql: "select * from t where b in (2, 5) or c = 5",
			res: []string{"t.b", "t.c"},
		},
		{
			sql: "select * from t where a + b > 1",
			res: []string{"t.a", "t.b"},
		},
		{
			sql: "select b, count(a) from t where b > 1 group by b having count(a) > 2",
			res: []string{"t.b"},
		},
		{
			sql: "select * from t as x join t2 as y on x.b + y.b > 2 and x.c > 1 and y.a < 1",
			res: []string{"t.c", "t2.a"},
		},
		{
			sql: "select * from t2 where t2.b > all(select b from t where t.c > 2)",
			res: []string{"t.c"},
		},
		{
			sql: "select * from t2 where t2.b > any(select b from t where t.c > 2)",
			res: []string{"t.c"},
		},
		{
			sql: "select * from t2 where t2.b in (select b from t where t.c > 2)",
			res: []string{"t.c"},
		},
		{
			pruneMode: "static",
			sql:       "select * from pt1 where ptn < 20 and b > 1",
			res:       []string{"pt1.p1.b", "pt1.p1.ptn", "pt1.p2.b", "pt1.p2.ptn"},
		},
		{
			pruneMode: "dynamic",
			sql:       "select * from pt1 where ptn < 20 and b > 1",
			res:       []string{"pt1.b", "pt1.ptn"},
		},
	}

	ctx := context.Background()
	for _, tt := range tests {
		comment := Commentf("for %s", tt.sql)
		if len(tt.pruneMode) > 0 {
			s.ctx.GetSessionVars().PartitionPruneMode.Store(tt.pruneMode)
		}
		stmt, err := s.ParseOneStmt(tt.sql, "", "")
		c.Assert(err, IsNil, comment)
		err = Preprocess(s.ctx, stmt, WithPreprocessorReturn(&PreprocessorReturn{InfoSchema: s.is}))
		c.Assert(err, IsNil, comment)
		builder, _ := NewPlanBuilder().Init(s.ctx, s.is, &hint.BlockHintProcessor{})
		p, err := builder.Build(ctx, stmt)
		c.Assert(err, IsNil, comment)
		lp, ok := p.(LogicalPlan)
		c.Assert(ok, IsTrue, comment)
		flags := builder.GetOptFlag()
		// JoinReOrder may need columns stats so collecting hist-needed columns must happen before JoinReOrder.
		// Hence we disable JoinReOrder and PruneColumnsAgain here.
		flags &= ^(flagJoinReOrder | flagPrunColumnsAgain)
		lp, err = logicalOptimize(ctx, flags, lp)
		c.Assert(err, IsNil, comment)
		checkColumnStatsUsage(c, s.is, lp, true, tt.res, comment)
	}
}
