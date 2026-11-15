// Copyright 2026 PingCAP, Inc.
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
	"testing"

	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser"
	pmodel "github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	"github.com/pingcap/tidb/pkg/planner/core/resolve"
	"github.com/pingcap/tidb/pkg/planner/property"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/hint"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/stretchr/testify/require"
)

func findLogicalTopNForTest(p base.LogicalPlan) *logicalop.LogicalTopN {
	if p == nil {
		return nil
	}
	if topN, ok := p.(*logicalop.LogicalTopN); ok {
		return topN
	}
	for _, child := range p.Children() {
		if res := findLogicalTopNForTest(child); res != nil {
			return res
		}
	}
	return nil
}

func TestGetPhysTopNVectorPropExtractFromSQLIdOnly(t *testing.T) {
	tbl := &model.TableInfo{
		ID:   1,
		Name: pmodel.NewCIStr("t_spfresh_prop"),
		Columns: []*model.ColumnInfo{
			{
				ID:        1,
				Name:      pmodel.NewCIStr("id"),
				Offset:    0,
				State:     model.StatePublic,
				FieldType: *types.NewFieldType(mysql.TypeLong),
			},
			{
				ID:     2,
				Name:   pmodel.NewCIStr("vec"),
				Offset: 1,
				State:  model.StatePublic,
				FieldType: func() types.FieldType {
					ft := types.NewFieldType(mysql.TypeTiDBVectorFloat32)
					ft.SetFlen(3)
					ft.SetDecimal(0)
					return *ft
				}(),
			},
		},
		State: model.StatePublic,
		Indices: []*model.IndexInfo{
			{
				ID:    11,
				Name:  pmodel.NewCIStr("idx_vec"),
				Tp:    pmodel.IndexTypeSPFresh,
				State: model.StatePublic,
				Columns: []*model.IndexColumn{
					{Name: pmodel.NewCIStr("vec"), Offset: 1},
				},
				VectorInfo: &model.VectorIndexInfo{
					Kind:           model.VectorIndexKindSPFresh,
					Dimension:      3,
					DistanceMetric: model.DistanceMetricCosine,
				},
			},
		},
	}

	is := infoschema.MockInfoSchema([]*model.TableInfo{tbl})

	ctx := mock.NewContext()
	ctx.Store = &mock.Store{Client: &mock.Client{}}
	ctx.GetSessionVars().CurrentDB = "test"
	initStatsCtx := mock.NewContext()
	initStatsCtx.Store = &mock.Store{Client: &mock.Client{}}
	do := domain.NewMockDomain()
	require.NoError(t, do.CreateStatsHandle(context.Background(), initStatsCtx))
	ctx.BindDomain(do)
	ctx.SetInfoSchema(is)
	domain.GetDomain(ctx).MockInfoCacheAndLoadInfoSchema(is)

	sql := "select id from t_spfresh_prop order by vec_cosine_distance(vec, '[0,0,0]') limit 1"
	p := parser.New()
	stmt, err := p.ParseOneStmt(sql, "", "")
	require.NoError(t, err)

	nodeW := resolve.NewNodeW(stmt)
	builder, _ := NewPlanBuilder().Init(ctx.GetPlanCtx(), is, hint.NewQBHintHandler(nil))
	plan, err := builder.Build(context.Background(), nodeW)
	require.NoError(t, err)

	logic, ok := plan.(base.LogicalPlan)
	require.True(t, ok)
	logic, err = LogicalOptimizeTest(context.Background(), builder.GetOptFlag(), logic)
	require.NoError(t, err)

	lt := findLogicalTopNForTest(logic)
	require.NotNil(t, lt, "expect LogicalTopN in logical plan")

	plans := getPhysTopN(lt, &property.PhysicalProperty{})
	found := false
	for _, p := range plans {
		topN, ok := p.(*PhysicalTopN)
		if !ok {
			continue
		}
		childProp := topN.GetChildReqProps(0)
		if childProp.TaskTp == property.RootTaskType && childProp.VectorProp.VectorHelper != nil {
			found = true
			break
		}
	}
	require.True(t, found, "expect RootTaskType + VectorProp candidate")
}
