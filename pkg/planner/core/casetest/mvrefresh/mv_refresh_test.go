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

package mvrefresh

import (
	"context"
	"testing"

	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	pmodel "github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	plannercore "github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/planner/core/resolve"
	"github.com/pingcap/tidb/pkg/planner/mvmerge"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/hint"
	"github.com/stretchr/testify/require"
)

func TestBuildRefreshMaterializedViewImplementFastBuildsMVDeltaMergePlan(t *testing.T) {
	sctx := plannercore.MockContext()
	// Ensure we have a non-zero StartTS; mock.Store.Begin returns nil, so create a fake txn first.
	savedStore := sctx.Store
	sctx.Store = nil
	_, err := sctx.Txn(true)
	require.NoError(t, err)
	sctx.Store = savedStore

	baseID := int64(1)
	mlogID := int64(2)
	mvID := int64(3)

	baseTbl := &model.TableInfo{
		ID:    baseID,
		Name:  pmodel.NewCIStr("t"),
		State: model.StatePublic,
		Columns: []*model.ColumnInfo{
			mkTestCol(1, "a", 0, mysql.TypeLong),
			mkTestCol(2, "b", 1, mysql.TypeLong),
		},
		MaterializedViewBase: &model.MaterializedViewBaseInfo{MLogID: mlogID},
	}
	mlogTbl := &model.TableInfo{
		ID:    mlogID,
		Name:  pmodel.NewCIStr("$mlog$t"),
		State: model.StatePublic,
		Columns: []*model.ColumnInfo{
			mkTestCol(1, "a", 0, mysql.TypeLong),
			mkTestCol(2, "b", 1, mysql.TypeLong),
			mkTestCol(3, model.MaterializedViewLogDMLTypeColumnName, 2, mysql.TypeVarchar),
			mkTestCol(4, model.MaterializedViewLogOldNewColumnName, 3, mysql.TypeTiny),
		},
		MaterializedViewLog: &model.MaterializedViewLogInfo{
			BaseTableID: baseID,
			Columns:     []pmodel.CIStr{pmodel.NewCIStr("a"), pmodel.NewCIStr("b")},
		},
	}
	mvTbl := &model.TableInfo{
		ID:    mvID,
		Name:  pmodel.NewCIStr("mv_tbl"),
		State: model.StatePublic,
		Columns: []*model.ColumnInfo{
			mkTestCol(1, "x", 0, mysql.TypeLong),
			mkTestCol(2, "cnt", 1, mysql.TypeLonglong),
			mkTestCol(3, "cnt_b", 2, mysql.TypeLonglong),
			mkTestCol(4, "s", 3, mysql.TypeLonglong),
		},
		MaterializedView: &model.MaterializedViewInfo{
			BaseTableIDs: []int64{baseID},
			SQLContent:   "select a, count(1), count(b), sum(b) from t group by a",
		},
	}

	is := infoschema.MockInfoSchema([]*model.TableInfo{baseTbl, mlogTbl, mvTbl})
	domain.GetDomain(sctx).MockInfoCacheAndLoadInfoSchema(is)

	implementStmt := &ast.RefreshMaterializedViewImplementStmt{
		RefreshStmt: &ast.RefreshMaterializedViewStmt{
			ViewName: &ast.TableName{Name: mvTbl.Name},
			Type:     ast.RefreshMaterializedViewTypeFast,
		},
		LastSuccessfulRefreshReadTSO: 0,
	}

	builder, _ := plannercore.NewPlanBuilder().Init(sctx.GetPlanCtx(), is, hint.NewQBHintHandler(nil))
	p, err := builder.Build(context.Background(), resolve.NewNodeW(implementStmt))
	require.NoError(t, err)

	mergePlan, ok := p.(*plannercore.MVDeltaMerge)
	require.True(t, ok)
	require.NotNil(t, mergePlan.Source)
	require.Equal(t, mvID, mergePlan.MVTableID)
	require.Equal(t, baseID, mergePlan.BaseTableID)
	require.Equal(t, mlogID, mergePlan.MLogTableID)
	require.Equal(t, len(mvTbl.Columns), mergePlan.MVColumnCount)
	require.Equal(t, []int{0}, mergePlan.GroupKeyMVOffsets)
	require.Equal(t, 1, mergePlan.CountStarMVOffset)
	require.Len(t, mergePlan.SourceOutputNames, 7)
	require.Len(t, mergePlan.AggInfos, 3)

	var hasCountStar, hasCountExpr, hasSum bool
	for _, aggInfo := range mergePlan.AggInfos {
		switch aggInfo.Kind {
		case mvmerge.AggCountStar:
			hasCountStar = true
			require.Equal(t, []int{4}, aggInfo.Dependencies)
		case mvmerge.AggCount:
			hasCountExpr = true
			require.Equal(t, "b", aggInfo.ArgColName)
			require.Equal(t, []int{5}, aggInfo.Dependencies)
		case mvmerge.AggSum:
			hasSum = true
			require.Equal(t, "b", aggInfo.ArgColName)
			require.Equal(t, []int{6, 2}, aggInfo.Dependencies)
		}
	}
	require.True(t, hasCountStar)
	require.True(t, hasCountExpr)
	require.True(t, hasSum)

	savedIgnoreExplainIDSuffix := sctx.GetSessionVars().StmtCtx.IgnoreExplainIDSuffix
	sctx.GetSessionVars().StmtCtx.IgnoreExplainIDSuffix = true
	defer func() {
		sctx.GetSessionVars().StmtCtx.IgnoreExplainIDSuffix = savedIgnoreExplainIDSuffix
	}()
	explain := &plannercore.Explain{
		TargetPlan: p,
		Format:     types.ExplainFormatBrief,
		Analyze:    false,
	}
	explain.SetSCtx(p.SCtx())
	require.NoError(t, explain.RenderResult())
	require.NotEmpty(t, explain.Rows)
	require.Equal(t,
		[]string{"MVDeltaMerge", "N/A", "root", "", "agg_deps:[count(*)@1->[4], count(b)@2->[5], sum(b)@3->[6,2]]"},
		explain.Rows[0],
	)
}

func TestExplainRefreshMaterializedViewImplementFastOutputsFullPlanTree(t *testing.T) {
	sctx := plannercore.MockContext()
	// Ensure we have a non-zero StartTS; mock.Store.Begin returns nil, so create a fake txn first.
	savedStore := sctx.Store
	sctx.Store = nil
	_, err := sctx.Txn(true)
	require.NoError(t, err)
	sctx.Store = savedStore

	baseID := int64(101)
	mlogID := int64(102)
	mvID := int64(103)

	baseTbl := &model.TableInfo{
		ID:    baseID,
		Name:  pmodel.NewCIStr("t"),
		State: model.StatePublic,
		Columns: []*model.ColumnInfo{
			mkTestCol(1, "a", 0, mysql.TypeLong),
		},
		MaterializedViewBase: &model.MaterializedViewBaseInfo{MLogID: mlogID},
	}
	mlogTbl := &model.TableInfo{
		ID:    mlogID,
		Name:  pmodel.NewCIStr("$mlog$t"),
		State: model.StatePublic,
		Columns: []*model.ColumnInfo{
			mkTestCol(1, "a", 0, mysql.TypeLong),
			mkTestCol(2, model.MaterializedViewLogDMLTypeColumnName, 1, mysql.TypeVarchar),
			mkTestCol(3, model.MaterializedViewLogOldNewColumnName, 2, mysql.TypeTiny),
		},
		MaterializedViewLog: &model.MaterializedViewLogInfo{
			BaseTableID: baseID,
			Columns:     []pmodel.CIStr{pmodel.NewCIStr("a")},
		},
	}
	mvTbl := &model.TableInfo{
		ID:    mvID,
		Name:  pmodel.NewCIStr("mv_tbl_explain"),
		State: model.StatePublic,
		Columns: []*model.ColumnInfo{
			mkTestCol(1, "a", 0, mysql.TypeLong),
			mkTestCol(2, "cnt", 1, mysql.TypeLonglong),
		},
		MaterializedView: &model.MaterializedViewInfo{
			BaseTableIDs: []int64{baseID},
			SQLContent:   "select a, count(1) from t group by a",
		},
	}

	is := infoschema.MockInfoSchema([]*model.TableInfo{baseTbl, mlogTbl, mvTbl})
	domain.GetDomain(sctx).MockInfoCacheAndLoadInfoSchema(is)

	implementStmt := &ast.RefreshMaterializedViewImplementStmt{
		RefreshStmt: &ast.RefreshMaterializedViewStmt{
			ViewName: &ast.TableName{Name: mvTbl.Name},
			Type:     ast.RefreshMaterializedViewTypeFast,
		},
		LastSuccessfulRefreshReadTSO: 0,
	}

	builder, _ := plannercore.NewPlanBuilder().Init(sctx.GetPlanCtx(), is, hint.NewQBHintHandler(nil))
	p, err := builder.Build(context.Background(), resolve.NewNodeW(implementStmt))
	require.NoError(t, err)

	savedIgnoreExplainIDSuffix := sctx.GetSessionVars().StmtCtx.IgnoreExplainIDSuffix
	sctx.GetSessionVars().StmtCtx.IgnoreExplainIDSuffix = true
	defer func() {
		sctx.GetSessionVars().StmtCtx.IgnoreExplainIDSuffix = savedIgnoreExplainIDSuffix
	}()

	explain := &plannercore.Explain{
		TargetPlan: p,
		Format:     types.ExplainFormatBrief,
		Analyze:    false,
	}
	explain.SetSCtx(p.SCtx())
	require.NoError(t, explain.RenderResult())
	require.Equal(t, [][]string{
		{"MVDeltaMerge", "N/A", "root", "", "agg_deps:[count(*)@1->[2]]"},
		{"└─Projection", "8000.00", "root", "", "coalesce(test.mv_tbl_explain.a, test.$mlog$t.a)->Column#11, test.mv_tbl_explain.cnt, Column#6"},
		{"  └─HashJoin", "8000.00", "root", "", "left outer join, equal:[nulleq(test.$mlog$t.a, test.mv_tbl_explain.a)]"},
		{"    ├─HashAgg(Build)", "6400.00", "root", "", "group by:test.$mlog$t.a, funcs:sum_int(Column#13)->Column#6, funcs:firstrow(test.$mlog$t.a)->test.$mlog$t.a"},
		{"    │ └─TableReader", "6400.00", "root", "", "data:HashAgg"},
		{"    │   └─HashAgg", "6400.00", "cop[tikv]", "", "group by:test.$mlog$t.a, funcs:sum_int(test.$mlog$t._mlog$_old_new)->Column#13"},
		{"    │     └─Selection", "8000.00", "cop[tikv]", "", "gt(test.$mlog$t._tidb_commit_ts, 0), le(test.$mlog$t._tidb_commit_ts, 1)"},
		{"    │       └─TableFullScan", "10000.00", "cop[tikv]", "table:$mlog$t", "keep order:false, stats:pseudo"},
		{"    └─TableReader(Probe)", "10000.00", "root", "", "data:TableFullScan"},
		{"      └─TableFullScan", "10000.00", "cop[tikv]", "table:mv", "keep order:false, stats:pseudo"},
	}, explain.Rows)
}

func TestBuildRefreshMaterializedViewImplementFastSumNotNullDoesNotRequireCountExpr(t *testing.T) {
	sctx := plannercore.MockContext()
	// Ensure we have a non-zero StartTS; mock.Store.Begin returns nil, so create a fake txn first.
	savedStore := sctx.Store
	sctx.Store = nil
	_, err := sctx.Txn(true)
	require.NoError(t, err)
	sctx.Store = savedStore

	baseID := int64(11)
	mlogID := int64(22)
	mvID := int64(33)

	baseColA := mkTestCol(1, "a", 0, mysql.TypeLong)
	baseColB := mkTestCol(2, "b", 1, mysql.TypeLong)
	baseColB.FieldType.AddFlag(mysql.NotNullFlag)
	baseTbl := &model.TableInfo{
		ID:    baseID,
		Name:  pmodel.NewCIStr("t"),
		State: model.StatePublic,
		Columns: []*model.ColumnInfo{
			baseColA,
			baseColB,
		},
		MaterializedViewBase: &model.MaterializedViewBaseInfo{MLogID: mlogID},
	}

	mlogColA := mkTestCol(1, "a", 0, mysql.TypeLong)
	mlogColB := mkTestCol(2, "b", 1, mysql.TypeLong)
	mlogColB.FieldType.AddFlag(mysql.NotNullFlag)
	mlogTbl := &model.TableInfo{
		ID:    mlogID,
		Name:  pmodel.NewCIStr("$mlog$t"),
		State: model.StatePublic,
		Columns: []*model.ColumnInfo{
			mlogColA,
			mlogColB,
			mkTestCol(3, model.MaterializedViewLogDMLTypeColumnName, 2, mysql.TypeVarchar),
			mkTestCol(4, model.MaterializedViewLogOldNewColumnName, 3, mysql.TypeTiny),
		},
		MaterializedViewLog: &model.MaterializedViewLogInfo{
			BaseTableID: baseID,
			Columns:     []pmodel.CIStr{pmodel.NewCIStr("a"), pmodel.NewCIStr("b")},
		},
	}

	mvTbl := &model.TableInfo{
		ID:    mvID,
		Name:  pmodel.NewCIStr("mv_tbl_not_null"),
		State: model.StatePublic,
		Columns: []*model.ColumnInfo{
			mkTestCol(1, "x", 0, mysql.TypeLong),
			mkTestCol(2, "cnt", 1, mysql.TypeLonglong),
			mkTestCol(3, "s", 2, mysql.TypeLonglong),
		},
		MaterializedView: &model.MaterializedViewInfo{
			BaseTableIDs: []int64{baseID},
			SQLContent:   "select a, count(1), sum(b) from t group by a",
		},
	}

	is := infoschema.MockInfoSchema([]*model.TableInfo{baseTbl, mlogTbl, mvTbl})
	domain.GetDomain(sctx).MockInfoCacheAndLoadInfoSchema(is)

	implementStmt := &ast.RefreshMaterializedViewImplementStmt{
		RefreshStmt: &ast.RefreshMaterializedViewStmt{
			ViewName: &ast.TableName{Name: mvTbl.Name},
			Type:     ast.RefreshMaterializedViewTypeFast,
		},
		LastSuccessfulRefreshReadTSO: 0,
	}

	builder, _ := plannercore.NewPlanBuilder().Init(sctx.GetPlanCtx(), is, hint.NewQBHintHandler(nil))
	p, err := builder.Build(context.Background(), resolve.NewNodeW(implementStmt))
	require.NoError(t, err)

	mergePlan, ok := p.(*plannercore.MVDeltaMerge)
	require.True(t, ok)
	require.NotNil(t, mergePlan.Source)
	require.Equal(t, mvID, mergePlan.MVTableID)
	require.Equal(t, baseID, mergePlan.BaseTableID)
	require.Equal(t, mlogID, mergePlan.MLogTableID)
	require.Equal(t, len(mvTbl.Columns), mergePlan.MVColumnCount)
	require.Equal(t, []int{0}, mergePlan.GroupKeyMVOffsets)
	require.Equal(t, 1, mergePlan.CountStarMVOffset)
	require.Len(t, mergePlan.SourceOutputNames, 5)
	require.Len(t, mergePlan.AggInfos, 2)

	var hasCountStar, hasSum bool
	for _, aggInfo := range mergePlan.AggInfos {
		switch aggInfo.Kind {
		case mvmerge.AggCountStar:
			hasCountStar = true
			require.Equal(t, []int{3}, aggInfo.Dependencies)
		case mvmerge.AggSum:
			hasSum = true
			require.Equal(t, "b", aggInfo.ArgColName)
			require.Equal(t, []int{4}, aggInfo.Dependencies)
		}
	}
	require.True(t, hasCountStar)
	require.True(t, hasSum)
}

func mkTestCol(id int64, name string, offset int, tp byte) *model.ColumnInfo {
	fieldType := types.NewFieldType(tp)
	return &model.ColumnInfo{
		ID:        id,
		Name:      pmodel.NewCIStr(name),
		Offset:    offset,
		State:     model.StatePublic,
		FieldType: *fieldType,
	}
}
