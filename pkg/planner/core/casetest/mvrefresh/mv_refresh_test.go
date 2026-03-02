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

func TestBuildRefreshMVFastPlan(t *testing.T) {
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
	require.Len(t, mergePlan.SourceOutputNames, 8)
	require.Len(t, mergePlan.AggInfos, 3)

	var hasCountStar, hasCountExpr, hasSum bool
	for _, aggInfo := range mergePlan.AggInfos {
		switch aggInfo.Kind {
		case mvmerge.AggCountStar:
			hasCountStar = true
			require.Equal(t, []int{0}, aggInfo.Dependencies)
		case mvmerge.AggCount:
			hasCountExpr = true
			require.Equal(t, "b", aggInfo.ArgColName)
			require.Equal(t, []int{1}, aggInfo.Dependencies)
		case mvmerge.AggSum:
			hasSum = true
			require.Equal(t, "b", aggInfo.ArgColName)
			require.Equal(t, []int{2, 5}, aggInfo.Dependencies)
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
		[]string{"MVDeltaMerge", "N/A", "root", "", "agg_deps:[count(*)@1->[0], count(b)@2->[1], sum(b)@3->[2,5]]"},
		explain.Rows[0],
	)
}

func TestExplainRefreshMVFastPlanTree(t *testing.T) {
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
		{"MVDeltaMerge", "N/A", "root", "", "agg_deps:[count(*)@1->[0]]"},
		{"└─HashJoin", "8000.00", "root", "", "left outer join, equal:[nulleq(test.$mlog$t.a, test.mv_tbl_explain.a)]"},
		{"  ├─HashAgg(Build)", "6400.00", "root", "", "group by:test.$mlog$t.a, funcs:sum_int(Column#11)->Column#6, funcs:firstrow(test.$mlog$t.a)->test.$mlog$t.a"},
		{"  │ └─TableReader", "6400.00", "root", "", "data:HashAgg"},
		{"  │   └─HashAgg", "6400.00", "cop[tikv]", "", "group by:test.$mlog$t.a, funcs:sum_int(test.$mlog$t._mlog$_old_new)->Column#11"},
		{"  │     └─Selection", "8000.00", "cop[tikv]", "", "gt(test.$mlog$t._tidb_commit_ts, 0)"},
		{"  │       └─TableFullScan", "10000.00", "cop[tikv]", "table:$mlog$t", "keep order:false, stats:pseudo"},
		{"  └─TableReader(Probe)", "10000.00", "root", "", "data:TableFullScan"},
		{"    └─TableFullScan", "10000.00", "cop[tikv]", "table:mv", "keep order:false, stats:pseudo"},
	}, explain.Rows)
}

func TestBuildRefreshMVFastSumNotNullNoCountExpr(t *testing.T) {
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
	require.Len(t, mergePlan.SourceOutputNames, 6)
	require.Len(t, mergePlan.AggInfos, 2)

	var hasCountStar, hasSum bool
	for _, aggInfo := range mergePlan.AggInfos {
		switch aggInfo.Kind {
		case mvmerge.AggCountStar:
			hasCountStar = true
			require.Equal(t, []int{0}, aggInfo.Dependencies)
		case mvmerge.AggSum:
			hasSum = true
			require.Equal(t, "b", aggInfo.ArgColName)
			require.Equal(t, []int{1}, aggInfo.Dependencies)
		}
	}
	require.True(t, hasCountStar)
	require.True(t, hasSum)
}

func TestMVMergeBuildResultHandleCols(t *testing.T) {
	type handleCase struct {
		name               string
		baseID             int64
		mlogID             int64
		mvID               int64
		baseCols           []handleColDef
		buildMV            func(baseID, mvID int64) *model.TableInfo
		expectHandleIsInt  bool
		expectHandleIdxs   []int
		expectHandleLayout []string
	}

	cases := []handleCase{
		{
			name:   "tidb_rowid",
			baseID: 1001,
			mlogID: 1002,
			mvID:   1003,
			baseCols: []handleColDef{
				{id: 1, name: "a", tp: mysql.TypeLong},
			},
			buildMV: func(baseID, mvID int64) *model.TableInfo {
				return &model.TableInfo{
					ID:    mvID,
					Name:  pmodel.NewCIStr("mv_tidb_rowid"),
					State: model.StatePublic,
					Columns: []*model.ColumnInfo{
						mkTestCol(1, "x", 0, mysql.TypeLong),
						mkTestCol(2, "cnt", 1, mysql.TypeLonglong),
					},
					MaterializedView: &model.MaterializedViewInfo{
						BaseTableIDs: []int64{baseID},
						SQLContent:   "select a, count(1) from t group by a",
					},
				}
			},
			expectHandleIsInt:  true,
			expectHandleIdxs:   []int{3},
			expectHandleLayout: []string{"__mvmerge_mv_rowid"},
		},
		{
			name:   "int_handle",
			baseID: 2001,
			mlogID: 2002,
			mvID:   2003,
			baseCols: []handleColDef{
				{id: 1, name: "a", tp: mysql.TypeLong},
			},
			buildMV: func(baseID, mvID int64) *model.TableInfo {
				pkCol := mkTestCol(2, "x", 1, mysql.TypeLong)
				pkCol.AddFlag(mysql.PriKeyFlag)
				pkCol.AddFlag(mysql.NotNullFlag)
				return &model.TableInfo{
					ID:         mvID,
					Name:       pmodel.NewCIStr("mv_int_handle"),
					State:      model.StatePublic,
					PKIsHandle: true,
					Columns: []*model.ColumnInfo{
						mkTestCol(1, "cnt", 0, mysql.TypeLonglong),
						pkCol,
					},
					MaterializedView: &model.MaterializedViewInfo{
						BaseTableIDs: []int64{baseID},
						SQLContent:   "select count(1), a from t group by a",
					},
				}
			},
			expectHandleIsInt:  true,
			expectHandleIdxs:   []int{2},
			expectHandleLayout: []string{"x"},
		},
		{
			name:   "common_handle",
			baseID: 3001,
			mlogID: 3002,
			mvID:   3003,
			baseCols: []handleColDef{
				{id: 1, name: "a", tp: mysql.TypeLong},
				{id: 2, name: "b", tp: mysql.TypeLong},
			},
			buildMV: func(baseID, mvID int64) *model.TableInfo {
				return &model.TableInfo{
					ID:             mvID,
					Name:           pmodel.NewCIStr("mv_common_handle"),
					State:          model.StatePublic,
					IsCommonHandle: true,
					Columns: []*model.ColumnInfo{
						mkTestCol(1, "x", 0, mysql.TypeLong),
						mkTestCol(2, "cnt", 1, mysql.TypeLonglong),
						mkTestCol(3, "y", 2, mysql.TypeLong),
					},
					Indices: []*model.IndexInfo{
						{
							ID:      1,
							Name:    pmodel.NewCIStr("PRIMARY"),
							Primary: true,
							Unique:  true,
							State:   model.StatePublic,
							Columns: []*model.IndexColumn{
								{Name: pmodel.NewCIStr("y"), Offset: 2},
								{Name: pmodel.NewCIStr("x"), Offset: 0},
							},
						},
					},
					MaterializedView: &model.MaterializedViewInfo{
						BaseTableIDs: []int64{baseID},
						SQLContent:   "select a, count(1), b from t group by a, b",
					},
				}
			},
			expectHandleIsInt:  false,
			expectHandleIdxs:   []int{3, 1},
			expectHandleLayout: []string{"y", "x"},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			baseTbl, mlogTbl := buildBaseAndMLogForHandleTest(tc.baseID, tc.mlogID, tc.baseCols)
			mvTbl := tc.buildMV(tc.baseID, tc.mvID)
			res, outputNames := buildMVMergeResultForHandleTest(t, baseTbl, mlogTbl, mvTbl)
			require.NotNil(t, res.MVTablePKCols)
			require.Equal(t, tc.expectHandleIsInt, res.MVTablePKCols.IsInt())
			require.Equal(t, len(tc.expectHandleIdxs), res.MVTablePKCols.NumCols())
			for i, idx := range tc.expectHandleIdxs {
				require.Equal(t, idx, res.MVTablePKCols.GetCol(i).Index)
			}
			assertHandleColsMatchMergeSourceLayout(t, res, outputNames, tc.expectHandleLayout)
		})
	}
}

type handleColDef struct {
	id   int64
	name string
	tp   byte
}

func buildBaseAndMLogForHandleTest(baseID, mlogID int64, cols []handleColDef) (*model.TableInfo, *model.TableInfo) {
	baseCols := make([]*model.ColumnInfo, 0, len(cols))
	mlogCols := make([]*model.ColumnInfo, 0, len(cols)+2)
	mlogPayloadCols := make([]pmodel.CIStr, 0, len(cols))
	maxColID := int64(0)
	for i, col := range cols {
		if col.id > maxColID {
			maxColID = col.id
		}
		baseCols = append(baseCols, mkTestCol(col.id, col.name, i, col.tp))
		mlogCols = append(mlogCols, mkTestCol(col.id, col.name, i, col.tp))
		mlogPayloadCols = append(mlogPayloadCols, pmodel.NewCIStr(col.name))
	}

	mlogCols = append(
		mlogCols,
		mkTestCol(maxColID+1, model.MaterializedViewLogDMLTypeColumnName, len(cols), mysql.TypeVarchar),
		mkTestCol(maxColID+2, model.MaterializedViewLogOldNewColumnName, len(cols)+1, mysql.TypeTiny),
	)

	baseTbl := &model.TableInfo{
		ID:      baseID,
		Name:    pmodel.NewCIStr("t"),
		State:   model.StatePublic,
		Columns: baseCols,
		MaterializedViewBase: &model.MaterializedViewBaseInfo{
			MLogID: mlogID,
		},
	}
	mlogTbl := &model.TableInfo{
		ID:      mlogID,
		Name:    pmodel.NewCIStr("$mlog$t"),
		State:   model.StatePublic,
		Columns: mlogCols,
		MaterializedViewLog: &model.MaterializedViewLogInfo{
			BaseTableID: baseID,
			Columns:     mlogPayloadCols,
		},
	}
	return baseTbl, mlogTbl
}

func buildMVMergeResultForHandleTest(
	t *testing.T,
	baseTbl, mlogTbl, mvTbl *model.TableInfo,
) (*mvmerge.BuildResult, types.NameSlice) {
	t.Helper()
	sctx := plannercore.MockContext()
	is := infoschema.MockInfoSchema([]*model.TableInfo{baseTbl, mlogTbl, mvTbl})
	domain.GetDomain(sctx).MockInfoCacheAndLoadInfoSchema(is)

	res, err := mvmerge.BuildForTest(
		sctx.GetPlanCtx(),
		is,
		mvTbl,
		mvmerge.BuildOptions{FromTS: 1},
		nil,
	)
	require.NoError(t, err)

	nodeW := resolve.NewNodeW(res.MergeSourceSelect)
	err = plannercore.Preprocess(
		context.Background(),
		sctx,
		nodeW,
		plannercore.WithPreprocessorReturn(&plannercore.PreprocessorReturn{InfoSchema: is}),
	)
	require.NoError(t, err)
	builder, _ := plannercore.NewPlanBuilder().Init(sctx.GetPlanCtx(), is, hint.NewQBHintHandler(nil))
	p, err := builder.Build(context.Background(), nodeW)
	require.NoError(t, err)
	return res, p.OutputNames()
}

func assertHandleColsMatchMergeSourceLayout(
	t *testing.T,
	res *mvmerge.BuildResult,
	outputNames types.NameSlice,
	expectedCols []string,
) {
	t.Helper()
	require.NotNil(t, res)
	require.NotNil(t, res.MVTablePKCols)
	require.NotNil(t, res.MergeSourceSelect)
	require.NotNil(t, res.MergeSourceSelect.Fields)
	fields := res.MergeSourceSelect.Fields.Fields
	require.Len(t, fields, res.SourceColumnCount)
	require.Len(t, outputNames, res.SourceColumnCount)
	require.Equal(t, len(expectedCols), res.MVTablePKCols.NumCols())
	for i, expectedCol := range expectedCols {
		idx := res.MVTablePKCols.GetCol(i).Index
		require.GreaterOrEqual(t, idx, 0)
		require.Less(t, idx, len(fields))
		require.Equal(t, expectedCol, fields[idx].AsName.O)
		require.Equal(t, expectedCol, outputNames[idx].ColName.O)
	}
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
