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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package core

import (
	"fmt"

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/property"
	"github.com/pingcap/tidb/pkg/types"
)

type mockLogicalJoinForJoinOrder struct {
	logicalSchemaProducer
	involvedNodeSet int
	statsMap        map[int]*property.StatsInfo
	JoinType        JoinType
}

func (mj mockLogicalJoinForJoinOrder) init(ctx base.PlanContext) *mockLogicalJoinForJoinOrder {
	mj.baseLogicalPlan = newBaseLogicalPlan(ctx, "MockLogicalJoin", &mj, 0)
	return &mj
}

// NewMockJoinForJoinOrder creates a LogicalJoin node for join order testing testing.
func NewMockJoinForJoinOrder(ctx base.PlanContext, statsMap map[int]*property.StatsInfo) func(lChild, rChild base.LogicalPlan, _ []*expression.ScalarFunction, _, _, _ []expression.Expression, joinType JoinType) base.LogicalPlan {
	return func(lChild, rChild base.LogicalPlan, _ []*expression.ScalarFunction, _, _, _ []expression.Expression, joinType JoinType) base.LogicalPlan {
		retJoin := mockLogicalJoinForJoinOrder{}.init(ctx)
		retJoin.schema = expression.MergeSchema(lChild.Schema(), rChild.Schema())
		retJoin.statsMap = statsMap
		if mj, ok := lChild.(*mockLogicalJoinForJoinOrder); ok {
			retJoin.involvedNodeSet = mj.involvedNodeSet
		} else {
			retJoin.involvedNodeSet = 1 << uint(lChild.ID())
		}
		if mj, ok := rChild.(*mockLogicalJoinForJoinOrder); ok {
			retJoin.involvedNodeSet |= mj.involvedNodeSet
		} else {
			retJoin.involvedNodeSet |= 1 << uint(rChild.ID())
		}
		retJoin.SetChildren(lChild, rChild)
		retJoin.JoinType = joinType
		return retJoin
	}
}

// MockDataSource make logical DataSource.
func MockJoin(ctx base.PlanContext, leftPlan base.LogicalPlan, rightPlan base.LogicalPlan, joinType JoinType) base.LogicalPlan {
	joinPlan := LogicalJoin{}.Init(ctx, 0)
	joinPlan.SetChildren(leftPlan, rightPlan)
	joinPlan.SetSchema(expression.MergeSchema(leftPlan.Schema(), rightPlan.Schema()))
	joinPlan.names = make([]*types.FieldName, leftPlan.Schema().Len()+rightPlan.Schema().Len())
	copy(joinPlan.names, leftPlan.OutputNames())
	copy(joinPlan.names[leftPlan.Schema().Len():], rightPlan.OutputNames())
	joinPlan.JoinType = joinType
	return joinPlan
}

// MockDataSource make logical DataSource.
func MockDataSource(ctx base.PlanContext, name string, count int) base.LogicalPlan {
	ds := DataSource{}.Init(ctx, 0)
	tan := model.NewCIStr(name)
	ds.TableAsName = &tan
	ds.schema = expression.NewSchema()
	ds.schema.Append(&expression.Column{
		UniqueID: ctx.GetSessionVars().PlanColumnID.Add(1),
		RetType:  types.NewFieldType(mysql.TypeLonglong),
	})
	ds.SetStats(&property.StatsInfo{
		RowCount: float64(count),
	})
	return ds
}

// PlanToString serializes some logical plan nodes to string.
func PlanToString(plan base.LogicalPlan) string {
	switch x := plan.(type) {
	case *mockLogicalJoinForJoinOrder:
		return fmt.Sprintf("MockJoin{%v, %v}", PlanToString(x.children[0]), PlanToString(x.children[1]))
	case *LogicalJoin:
		return fmt.Sprintf("MockJoin{%v, %v}", PlanToString(x.children[0]), PlanToString(x.children[1]))
	case *DataSource:
		return x.TableAsName.L
	}
	return ""
}

