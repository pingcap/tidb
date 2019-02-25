// Copyright 2018 PingCAP, Inc.
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

package core

import (
	"encoding/json"
	"io/ioutil"
	"path/filepath"

	. "github.com/pingcap/check"
	"github.com/pingcap/errors"
	"github.com/pingcap/parser"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb/ddl"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/planner/property"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/statistics"
	"github.com/pingcap/tidb/util/mock"
)

var _ = Suite(&testStatsSuite{})

type testStatsSuite struct {
	ctx sessionctx.Context
}

func (s *testStatsSuite) SetUpTest(c *C) {
	s.ctx = mock.NewContext()
}

func (s *testStatsSuite) loadStatsByFileName(t string, tbl *model.TableInfo) (*statistics.HistColl, error) {
	statsTbl := &statistics.JSONTable{}
	path := filepath.Join("testdata", t)
	bytes, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, errors.Trace(err)
	}
	err = json.Unmarshal(bytes, statsTbl)
	if err != nil {
		return nil, errors.Trace(err)
	}

	coll, err := statistics.HistCollFromJSON(tbl, tbl.ID, statsTbl)

	return coll, err
}

func (s *testStatsSuite) mockTableInfo(sql string, tblID int64) (*model.TableInfo, error) {
	stmt, err := parser.New().ParseOneStmt(sql, "", "")
	if err != nil {
		return nil, errors.Trace(err)
	}
	mockTbl, err := ddl.MockTableInfo(mock.NewContext(), stmt.(*ast.CreateTableStmt), tblID)
	return mockTbl, err
}

func (s *testStatsSuite) TestInnerJoinStats(c *C) {
	// t1's values are (i, i) where i = 1..9. Each pair appears once.
	t1Tbl, err := s.mockTableInfo("create table t1(a int, b int, index a(a, b))", 1)
	c.Assert(err, IsNil)
	t1Coll, err := s.loadStatsByFileName("t1.json", t1Tbl)
	c.Assert(err, IsNil)
	t1ExprCols := expression.ColumnInfos2ColumnsWithDBName(s.ctx, model.NewCIStr("test"), t1Tbl.Name, t1Tbl.Columns)
	t1FinalColl := t1Coll.GenerateHistCollFromColumnInfo(t1Tbl.Columns, t1ExprCols)
	t1StatsInfo := &property.StatsInfo{
		RowCount:    float64(t1Coll.Count),
		Cardinality: make([]float64, len(t1ExprCols)),
		HistColl:    t1FinalColl,
	}
	for i := range t1ExprCols {
		t1StatsInfo.Cardinality[i] = float64(t1FinalColl.Columns[t1ExprCols[i].UniqueID].NDV)
	}
	t1Child := DataSource{}.Init(s.ctx)
	t1Child.schema = expression.NewSchema(t1ExprCols...)
	t1Child.stats = t1StatsInfo

	t2Tbl, err := s.mockTableInfo("create table t2(a int, b int, index a(a, b))", 2)
	c.Assert(err, IsNil)
	// t2's values are (i, i) where i = 8..15. Each pair appears once.
	t2Coll, err := s.loadStatsByFileName("t2.json", t2Tbl)
	c.Assert(err, IsNil)
	t2ExprCols := expression.ColumnInfos2ColumnsWithDBName(s.ctx, model.NewCIStr("test"), t2Tbl.Name, t2Tbl.Columns)
	t2FinalColl := t2Coll.GenerateHistCollFromColumnInfo(t2Tbl.Columns, t2ExprCols)
	t2StatsInfo := &property.StatsInfo{
		RowCount:    float64(t2Coll.Count),
		Cardinality: make([]float64, len(t2ExprCols)),
		HistColl:    t2FinalColl,
	}
	for i := range t2ExprCols {
		t2StatsInfo.Cardinality[i] = float64(t2FinalColl.Columns[t2ExprCols[i].UniqueID].NDV)
	}
	t2Child := DataSource{}.Init(s.ctx)
	t2Child.schema = expression.NewSchema(t2ExprCols...)
	t2Child.stats = t2StatsInfo

	join := LogicalJoin{}.Init(s.ctx)
	join.SetChildren(t1Child, t2Child)
	join.schema = expression.MergeSchema(t1Child.schema, t2Child.schema)
	finalStats, err := join.deriveInnerJoinStatsWithHist([]*expression.Column{t1Child.schema.Columns[0]}, []*expression.Column{t2Child.schema.Columns[0]}, []*property.StatsInfo{t1StatsInfo, t2StatsInfo})
	c.Assert(err, IsNil)
	c.Assert(finalStats.RowCount, Equals, float64(2))
	c.Assert(len(finalStats.HistColl.Columns), Equals, 4)
	ans1 := `column:0 ndv:2 totColSize:0
num: 1 lower_bound: 8 upper_bound: 8 repeats: 1
num: 1 lower_bound: 9 upper_bound: 9 repeats: 1`
	c.Assert(finalStats.HistColl.Columns[1].String(), Equals, ans1)
	ans2 := `column:2 ndv:9 totColSize:9
num: 1 lower_bound: 1 upper_bound: 1 repeats: 1
num: 1 lower_bound: 2 upper_bound: 2 repeats: 1
num: 1 lower_bound: 3 upper_bound: 3 repeats: 1
num: 1 lower_bound: 4 upper_bound: 4 repeats: 1
num: 1 lower_bound: 5 upper_bound: 5 repeats: 1
num: 1 lower_bound: 6 upper_bound: 6 repeats: 1
num: 1 lower_bound: 7 upper_bound: 7 repeats: 1
num: 1 lower_bound: 8 upper_bound: 8 repeats: 1
num: 1 lower_bound: 9 upper_bound: 9 repeats: 1`
	c.Assert(finalStats.HistColl.Columns[2].String(), Equals, ans2)
	c.Assert(finalStats.HistColl.Columns[3].String(), Equals, ans1)
	ans4 := `column:2 ndv:8 totColSize:8
num: 1 lower_bound: 8 upper_bound: 8 repeats: 1
num: 1 lower_bound: 9 upper_bound: 9 repeats: 1
num: 1 lower_bound: 10 upper_bound: 10 repeats: 1
num: 1 lower_bound: 11 upper_bound: 11 repeats: 1
num: 1 lower_bound: 12 upper_bound: 12 repeats: 1
num: 1 lower_bound: 13 upper_bound: 13 repeats: 1
num: 1 lower_bound: 14 upper_bound: 14 repeats: 1
num: 1 lower_bound: 15 upper_bound: 15 repeats: 1`
	c.Assert(finalStats.HistColl.Columns[4].String(), Equals, ans4)
	t2StatsInfo.RowCount /= 2
	t2StatsInfo.HistColl.Count /= 2
	for i := range t2StatsInfo.Cardinality {
		t2StatsInfo.Cardinality[i] /= 2
	}
	finalStats, err = join.deriveInnerJoinStatsWithHist([]*expression.Column{t1Child.schema.Columns[0]}, []*expression.Column{t2Child.schema.Columns[0]}, []*property.StatsInfo{t1StatsInfo, t2StatsInfo})
	c.Assert(err, IsNil)
	c.Assert(finalStats.RowCount, Equals, float64(1))
}
