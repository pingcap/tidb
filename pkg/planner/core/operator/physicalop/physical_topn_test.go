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

package physicalop

import (
	"testing"

	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	"github.com/pingcap/tidb/pkg/planner/property"
	"github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/planner/util/coretestsdk"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/stretchr/testify/require"
)

func TestGetPhysTopNAppendsMPPAndTiCIVectorTasks(t *testing.T) {
	ctx := coretestsdk.MockContext()
	defer func() {
		domain.GetDomain(ctx).StatsHandle().Close()
	}()

	require.True(t, ctx.GetSessionVars().IsMPPAllowed())

	vectorType := types.NewFieldType(mysql.TypeTiDBVectorFloat32)
	vectorCol := &expression.Column{
		UniqueID: 1,
		ID:       1,
		RetType:  vectorType,
	}
	vectorConst := &expression.Constant{
		Value:   types.NewVectorFloat32Datum(types.MustCreateVectorFloat32([]float32{1, 2, 3})),
		RetType: vectorType,
	}
	orderExpr := expression.NewFunctionInternal(
		ctx.GetExprCtx(),
		ast.VecL2Distance,
		types.NewFieldType(mysql.TypeDouble),
		vectorCol,
		vectorConst,
	)

	ds := logicalop.DataSource{}.Init(ctx.GetPlanCtx(), 0)
	ds.PossibleAccessPaths = []*util.AccessPath{{
		Index: &model.IndexInfo{
			HybridInfo: &model.HybridIndexInfo{
				Vector: []*model.HybridVectorSpec{{}},
			},
		},
	}}
	ds.SetSchema(expression.NewSchema(vectorCol))

	lt := logicalop.LogicalTopN{
		ByItems: []*util.ByItems{{Expr: orderExpr}},
		Count:   10,
		Offset:  2,
	}.Init(ctx.GetPlanCtx(), 0)
	lt.SetChildren(ds)
	lt.SetSchema(ds.Schema().Clone())

	plans := getPhysTopN(lt, &property.PhysicalProperty{})
	vectorTaskTypes := make([]property.TaskType, 0, 2)
	for _, plan := range plans {
		topN := plan.(*PhysicalTopN)
		childProp := topN.GetChildReqProps(0)
		if childProp.VectorProp.VSInfo == nil {
			continue
		}
		vectorTaskTypes = append(vectorTaskTypes, childProp.TaskTp)
		require.Equal(t, uint32(12), childProp.VectorProp.TopK)
		require.Equal(t, ast.VecL2Distance, childProp.VectorProp.DistanceFnName.L)
	}
	require.ElementsMatch(t, []property.TaskType{
		property.MppTaskType,
		property.CopSingleReadTaskType,
	}, vectorTaskTypes)
}
