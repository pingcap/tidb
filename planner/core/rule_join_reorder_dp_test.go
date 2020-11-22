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
	"fmt"

	. "github.com/pingcap/check"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/planner/property"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/types"
)

var _ = Suite(&testJoinReorderDPSuite{})

type testJoinReorderDPSuite struct {
	ctx      sessionctx.Context
	statsMap map[int]*property.StatsInfo
}

func (s *testJoinReorderDPSuite) SetUpTest(c *C) {
	s.ctx = MockContext()
	s.ctx.GetSessionVars().PlanID = -1
}

type mockLogicalJoin struct {
	logicalSchemaProducer
	involvedNodeSet int
	statsMap        map[int]*property.StatsInfo
}

func (mj mockLogicalJoin) init(ctx sessionctx.Context) *mockLogicalJoin {
	mj.baseLogicalPlan = newBaseLogicalPlan(ctx, "MockLogicalJoin", &mj, 0)
	return &mj
}

func (mj *mockLogicalJoin) recursiveDeriveStats(_ [][]*expression.Column) (*property.StatsInfo, error) {
	if mj.stats == nil {
		mj.stats = mj.statsMap[mj.involvedNodeSet]
	}
	return mj.statsMap[mj.involvedNodeSet], nil
}

func (s *testJoinReorderDPSuite) newMockJoin(lChild, rChild LogicalPlan, eqConds []*expression.ScalarFunction, _ []expression.Expression) LogicalPlan {
	retJoin := mockLogicalJoin{}.init(s.ctx)
	retJoin.schema = expression.MergeSchema(lChild.Schema(), rChild.Schema())
	retJoin.statsMap = s.statsMap
	if mj, ok := lChild.(*mockLogicalJoin); ok {
		retJoin.involvedNodeSet = mj.involvedNodeSet
	} else {
		retJoin.involvedNodeSet = 1 << uint(lChild.ID())
	}
	if mj, ok := rChild.(*mockLogicalJoin); ok {
		retJoin.involvedNodeSet |= mj.involvedNodeSet
	} else {
		retJoin.involvedNodeSet |= 1 << uint(rChild.ID())
	}
	retJoin.SetChildren(lChild, rChild)
	return retJoin
}

func (s *testJoinReorderDPSuite) mockStatsInfo(state int, count float64) {
	s.statsMap[state] = &property.StatsInfo{
		RowCount: count,
	}
}

func (s *testJoinReorderDPSuite) makeStatsMapForTPCHQ5() {
	// Labeled as lineitem -> 0, orders -> 1, customer -> 2, supplier 3, nation 4, region 5
	// This graph can be shown as following:
	// +---------------+            +---------------+
	// |               |            |               |
	// |    lineitem   +------------+    orders     |
	// |               |            |               |
	// +-------+-------+            +-------+-------+
	//         |                            |
	//         |                            |
	//         |                            |
	// +-------+-------+            +-------+-------+
	// |               |            |               |
	// |   supplier    +------------+    customer   |
	// |               |            |               |
	// +-------+-------+            +-------+-------+
	//         |                            |
	//         |                            |
	//         |                            |
	//         |                            |
	//         |      +---------------+     |
	//         |      |               |     |
	//         +------+    nation     +-----+
	//                |               |
	//                +---------------+
	//                        |
	//                +---------------+
	//                |               |
	//                |    region     |
	//                |               |
	//                +---------------+
	s.statsMap = make(map[int]*property.StatsInfo)
	s.mockStatsInfo(3, 9103367)
	s.mockStatsInfo(6, 2275919)
	s.mockStatsInfo(7, 9103367)
	s.mockStatsInfo(9, 59986052)
	s.mockStatsInfo(11, 9103367)
	s.mockStatsInfo(12, 5999974575)
	s.mockStatsInfo(13, 59999974575)
	s.mockStatsInfo(14, 9103543072)
	s.mockStatsInfo(15, 99103543072)
	s.mockStatsInfo(20, 1500000)
	s.mockStatsInfo(22, 2275919)
	s.mockStatsInfo(23, 7982159)
	s.mockStatsInfo(24, 100000)
	s.mockStatsInfo(25, 59986052)
	s.mockStatsInfo(27, 9103367)
	s.mockStatsInfo(28, 5999974575)
	s.mockStatsInfo(29, 59999974575)
	s.mockStatsInfo(30, 59999974575)
	s.mockStatsInfo(31, 59999974575)
	s.mockStatsInfo(48, 5)
	s.mockStatsInfo(52, 299838)
	s.mockStatsInfo(54, 454183)
	s.mockStatsInfo(55, 1815222)
	s.mockStatsInfo(56, 20042)
	s.mockStatsInfo(57, 12022687)
	s.mockStatsInfo(59, 1823514)
	s.mockStatsInfo(60, 1201884359)
	s.mockStatsInfo(61, 12001884359)
	s.mockStatsInfo(62, 12001884359)
	s.mockStatsInfo(63, 72985)

}

func (s *testJoinReorderDPSuite) newDataSource(name string, count int) LogicalPlan {
	ds := DataSource{}.Init(s.ctx, 0)
	tan := model.NewCIStr(name)
	ds.TableAsName = &tan
	ds.schema = expression.NewSchema()
	s.ctx.GetSessionVars().PlanColumnID++
	ds.schema.Append(&expression.Column{
		UniqueID: s.ctx.GetSessionVars().PlanColumnID,
		RetType:  types.NewFieldType(mysql.TypeLonglong),
	})
	ds.stats = &property.StatsInfo{
		RowCount: float64(count),
	}
	return ds
}

func (s *testJoinReorderDPSuite) planToString(plan LogicalPlan) string {
	switch x := plan.(type) {
	case *mockLogicalJoin:
		return fmt.Sprintf("MockJoin{%v, %v}", s.planToString(x.children[0]), s.planToString(x.children[1]))
	case *DataSource:
		return x.TableAsName.L
	}
	return ""
}

func (s *testJoinReorderDPSuite) TestDPReorderTPCHQ5(c *C) {
	s.makeStatsMapForTPCHQ5()
	joinGroups := make([]LogicalPlan, 0, 6)
	joinGroups = append(joinGroups, s.newDataSource("lineitem", 59986052))
	joinGroups = append(joinGroups, s.newDataSource("orders", 15000000))
	joinGroups = append(joinGroups, s.newDataSource("customer", 1500000))
	joinGroups = append(joinGroups, s.newDataSource("supplier", 100000))
	joinGroups = append(joinGroups, s.newDataSource("nation", 25))
	joinGroups = append(joinGroups, s.newDataSource("region", 5))
	var eqConds []expression.Expression
	eqConds = append(eqConds, expression.NewFunctionInternal(s.ctx, ast.EQ, types.NewFieldType(mysql.TypeTiny), joinGroups[0].Schema().Columns[0], joinGroups[1].Schema().Columns[0]))
	eqConds = append(eqConds, expression.NewFunctionInternal(s.ctx, ast.EQ, types.NewFieldType(mysql.TypeTiny), joinGroups[1].Schema().Columns[0], joinGroups[2].Schema().Columns[0]))
	eqConds = append(eqConds, expression.NewFunctionInternal(s.ctx, ast.EQ, types.NewFieldType(mysql.TypeTiny), joinGroups[2].Schema().Columns[0], joinGroups[3].Schema().Columns[0]))
	eqConds = append(eqConds, expression.NewFunctionInternal(s.ctx, ast.EQ, types.NewFieldType(mysql.TypeTiny), joinGroups[0].Schema().Columns[0], joinGroups[3].Schema().Columns[0]))
	eqConds = append(eqConds, expression.NewFunctionInternal(s.ctx, ast.EQ, types.NewFieldType(mysql.TypeTiny), joinGroups[2].Schema().Columns[0], joinGroups[4].Schema().Columns[0]))
	eqConds = append(eqConds, expression.NewFunctionInternal(s.ctx, ast.EQ, types.NewFieldType(mysql.TypeTiny), joinGroups[3].Schema().Columns[0], joinGroups[4].Schema().Columns[0]))
	eqConds = append(eqConds, expression.NewFunctionInternal(s.ctx, ast.EQ, types.NewFieldType(mysql.TypeTiny), joinGroups[4].Schema().Columns[0], joinGroups[5].Schema().Columns[0]))
	solver := &joinReorderDPSolver{
		baseSingleGroupJoinOrderSolver: &baseSingleGroupJoinOrderSolver{
			ctx: s.ctx,
		},
		newJoin: s.newMockJoin,
	}
	result, err := solver.solve(joinGroups, eqConds)
	c.Assert(err, IsNil)
	c.Assert(s.planToString(result), Equals, "MockJoin{supplier, MockJoin{lineitem, MockJoin{orders, MockJoin{customer, MockJoin{nation, region}}}}}")
}

func (s *testJoinReorderDPSuite) TestDPReorderAllCartesian(c *C) {
	joinGroup := make([]LogicalPlan, 0, 4)
	joinGroup = append(joinGroup, s.newDataSource("a", 100))
	joinGroup = append(joinGroup, s.newDataSource("b", 100))
	joinGroup = append(joinGroup, s.newDataSource("c", 100))
	joinGroup = append(joinGroup, s.newDataSource("d", 100))
	solver := &joinReorderDPSolver{
		baseSingleGroupJoinOrderSolver: &baseSingleGroupJoinOrderSolver{
			ctx: s.ctx,
		},
		newJoin: s.newMockJoin,
	}
	result, err := solver.solve(joinGroup, nil)
	c.Assert(err, IsNil)
	c.Assert(s.planToString(result), Equals, "MockJoin{MockJoin{a, b}, MockJoin{c, d}}")
}
