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
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/util/testleak"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/statistics"
	"github.com/pingcap/tidb/util/types"
	"github.com/pingcap/tidb/ast"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb/mysql"
)

func (s *testPlanSuite) TestSelectivity(c *C) {
	defer testleak.AfterTest(c)()
	tests := []struct{
		exprs string
		selectivity float64
	}{
		{
			exprs: "a >= 39",
			selectivity: 0.1,
		},
	}
	for _, tt := range tests {
		sql := "select * from t where " + tt.exprs
		comment := Commentf("for %s", sql)
		stmt, err := s.ParseOneStmt(sql, "", "")
		c.Assert(err, IsNil, comment)
		ast.SetFlag(stmt)
		is, err := mockResolve(stmt)
		ctx := mockContext()
		handle := sessionctx.GetDomain(ctx).StatsHandle()
		tb, _ := is.TableByID(0)
		tbl := tb.Meta()
		// generate 40 distinct values for pk.
		pkValues := make([]types.Datum, 40)
		for i := 0; i < 40; i++ {
			pkValues[i] = types.NewIntDatum(int64(i))
		}
		// make the statistic col info for pk, every distinct value occurs 10 times.
		pkStatsCol := &statistics.Column{Histogram: *mockStatsHistogram(1, pkValues, 10)}
		// mock the statistic table and set the value of pk column.
		statsTbl := mockStatsTable(tbl, 400)
		statsTbl.Columns[1] = pkStatsCol
		handle.UpdateTableStats([]*statistics.Table{statsTbl}, nil)
		c.Assert(err, IsNil)
		builder := &planBuilder{
			allocator: new(idAllocator),
			ctx:       ctx,
			colMapper: make(map[*ast.ColumnNameExpr]int),
			is:        is,
		}
		p := builder.build(stmt)
		c.Assert(builder.err, IsNil)
		lp := p.(LogicalPlan)
		sel, ok := lp.Children()[0].(*Selection)
		c.Assert(ok, IsTrue)
		ds, ok := sel.children[0].(*DataSource)
		c.Assert(ok, IsTrue)
		indices, _ := availableIndices(ds.indexHints, ds.tableInfo)
		var pkColID int64
		for _, col := range ds.Columns {
			if mysql.HasPriKeyFlag(col.Flag) {
				pkColID = col.ID
				break
			}
		}
		ratio, err := selectivity(sel.Conditions, indices, ds, pkColID)
		log.Warnf("%v xxxxx", ratio)
		c.Assert(false, IsTrue)
		//ret := sele
	}
}
