// Copyright 2017 PingCAP, Inc.
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

package plan

import (
	"math"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/statistics"
	"github.com/pingcap/tidb/util/testleak"
)

const eps = 1e-9

func (s *testPlanSuite) TestSelectivity(c *C) {
	defer testleak.AfterTest(c)()

	is := mockInfoSchema()
	ctx := mockContext()
	handle := sessionctx.GetDomain(ctx).StatsHandle()
	tb, _ := is.TableByID(0)
	tbl := tb.Meta()

	// mock the statistic table and set the value of pk column.
	statsTbl := mockStatsTable(tbl, 540)

	// generate 40 distinct values for pk.
	pkValues, _ := generateIntDatum(1, 54)
	// make the statistic col info for pk, every distinct value occurs 10 times.
	statsTbl.Columns[1] = &statistics.Column{Histogram: *mockStatsHistogram(1, pkValues, 10)}

	// for index (c, d, e)
	idx1Values, err := generateIntDatum(3, 3)
	c.Assert(err, IsNil)
	statsTbl.Indices[1] = &statistics.Index{Histogram: *mockStatsHistogram(1, idx1Values, 20), NumColumns: 3}

	// for index f, g and (f, g)
	singleValues, _ := generateIntDatum(1, 3)
	statsTbl.Indices[3] = &statistics.Index{Histogram: *mockStatsHistogram(3, singleValues, 180), NumColumns: 1}
	statsTbl.Indices[4] = &statistics.Index{Histogram: *mockStatsHistogram(4, singleValues, 180), NumColumns: 1}
	fgValues, err := generateIntDatum(2, 3)
	c.Assert(err, IsNil)
	statsTbl.Indices[5] = &statistics.Index{Histogram: *mockStatsHistogram(5, fgValues, 60), NumColumns: 3}

	handle.UpdateTableStats([]*statistics.Table{statsTbl}, nil)

	defer func() {
		handle.Clear()
	}()

	tests := []struct {
		exprs       string
		selectivity float64
	}{
		// This case is tested for pr#3146
		{
			exprs:       "a > 0 and a < 2",
			selectivity: 0.01851851851,
		},
		{
			exprs:       "a >= 1 and a < 2",
			selectivity: 0.01851851851,
		},
		{
			exprs:       "a >= 1 and b > 1 and a < 2",
			selectivity: 0.01481481481,
		},
		{
			exprs:       "a >= 1 and c > 1 and a < 2",
			selectivity: 0.00617283950,
		},
		{
			exprs:       "a >= 1 and c >= 1 and a < 2",
			selectivity: 0.01234567901,
		},
		{
			exprs:       "g = 0 and f = 1",
			selectivity: 0.03888888888,
		},
	}
	for _, tt := range tests {
		sql := "select * from t where " + tt.exprs
		comment := Commentf("for %s", sql)
		stmt, err := s.ParseOneStmt(sql, "", "")
		c.Assert(err, IsNil, comment)
		ast.SetFlag(stmt)

		err = MockResolveName(stmt, is, "test", ctx)
		c.Assert(err, IsNil, comment)
		err = InferType(ctx.GetSessionVars().StmtCtx, stmt)
		c.Assert(err, IsNil, comment)

		builder := &planBuilder{
			allocator: new(idAllocator),
			ctx:       ctx,
			colMapper: make(map[*ast.ColumnNameExpr]int),
			is:        is,
		}
		p := builder.build(stmt)
		c.Assert(builder.err, IsNil, comment)
		lp := p.(LogicalPlan)
		sel, ok := lp.Children()[0].(*Selection)
		c.Assert(ok, IsTrue, comment)
		ds, ok := sel.children[0].(*DataSource)
		c.Assert(ok, IsTrue, comment)
		indices, _ := availableIndices(ds.indexHints, ds.tableInfo)
		var pkColID int64
		for _, col := range ds.Columns {
			if mysql.HasPriKeyFlag(col.Flag) {
				pkColID = col.ID
				break
			}
		}
		ratio, err := selectivity(sel.Conditions, indices, ds, pkColID)
		c.Assert(err, IsNil, comment)
		c.Assert(math.Abs(ratio-tt.selectivity) < eps, IsTrue, comment)
	}
}
