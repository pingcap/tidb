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

package mvmerge_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	pmodel "github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/core"
	corebase "github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/resolve"
	"github.com/pingcap/tidb/pkg/planner/mvmerge"
	_ "github.com/pingcap/tidb/pkg/session"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/hint"
	"github.com/stretchr/testify/require"
)

const (
	deltaTableAlias = "delta"
	mvTableAlias    = "mv"

	deltaCntStarName = "__mvmerge_delta_cnt_star"
	removedRowsName  = "__mvmerge_removed_rows"
)

func optimizeForTest(sctx sessionctx.Context, is infoschema.InfoSchema) func(ctx context.Context, sel *ast.SelectStmt) (corebase.PhysicalPlan, types.NameSlice, error) {
	return func(ctx context.Context, sel *ast.SelectStmt) (corebase.PhysicalPlan, types.NameSlice, error) {
		nodeW := resolve.NewNodeW(sel)
		err := core.Preprocess(ctx, sctx, nodeW, core.WithPreprocessorReturn(&core.PreprocessorReturn{InfoSchema: is}))
		if err != nil {
			return nil, nil, err
		}
		builder, _ := core.NewPlanBuilder().Init(sctx.GetPlanCtx(), is, hint.NewQBHintHandler(nil))
		p, err := builder.Build(ctx, nodeW)
		if err != nil {
			return nil, nil, err
		}
		names := p.OutputNames()
		logic, ok := p.(corebase.LogicalPlan)
		if !ok {
			return nil, nil, fmt.Errorf("expected logical plan from select, got %T", p)
		}
		pp, _, err := core.DoOptimize(ctx, sctx.GetPlanCtx(), builder.GetOptFlag(), logic)
		if err != nil {
			return nil, nil, err
		}
		return pp, names, nil
	}
}

func TestBuildCountSum(t *testing.T) {
	sctx := core.MockContext()

	baseID := int64(1)
	mlogID := int64(2)
	mvID := int64(3)

	base := &model.TableInfo{
		ID:    baseID,
		Name:  pmodel.NewCIStr("t"),
		State: model.StatePublic,
		Columns: []*model.ColumnInfo{
			mkCol(1, "a", 0, mysql.TypeLong),
			mkCol(2, "b", 1, mysql.TypeLong),
		},
		MaterializedViewBase: &model.MaterializedViewBaseInfo{MLogID: mlogID},
	}
	mlog := &model.TableInfo{
		ID:    mlogID,
		Name:  pmodel.NewCIStr("$mlog$t"),
		State: model.StatePublic,
		Columns: []*model.ColumnInfo{
			mkCol(1, "a", 0, mysql.TypeLong),
			mkCol(2, "b", 1, mysql.TypeLong),
			mkCol(3, model.MaterializedViewLogDMLTypeColumnName, 2, mysql.TypeVarchar),
			mkCol(4, model.MaterializedViewLogOldNewColumnName, 3, mysql.TypeTiny),
		},
		MaterializedViewLog: &model.MaterializedViewLogInfo{
			BaseTableID: baseID,
			Columns:     []pmodel.CIStr{pmodel.NewCIStr("a"), pmodel.NewCIStr("b")},
		},
	}
	mv := &model.TableInfo{
		ID:    mvID,
		Name:  pmodel.NewCIStr("mv_tbl"),
		State: model.StatePublic,
		Columns: []*model.ColumnInfo{
			mkCol(1, "x", 0, mysql.TypeLong),
			mkCol(2, "cnt", 1, mysql.TypeLonglong),
			mkCol(3, "cnt_b", 2, mysql.TypeLonglong),
			mkCol(4, "s", 3, mysql.TypeLonglong),
		},
		MaterializedView: &model.MaterializedViewInfo{
			BaseTableIDs: []int64{baseID},
			SQLContent:   "select a, count(1), count(b), sum(b) from t group by a",
		},
	}

	is := infoschema.MockInfoSchema([]*model.TableInfo{base, mlog, mv})
	domain.GetDomain(sctx).MockInfoCacheAndLoadInfoSchema(is)

	res, err := mvmerge.Build(
		sctx.GetPlanCtx(),
		is,
		mv,
		mvmerge.BuildOptions{FromTS: 10, ToTS: 20},
		nil,
	)
	require.NoError(t, err)
	plan, outputNames, err := optimizeForTest(sctx, is)(context.Background(), res.MergeSourceSelect)
	require.NoError(t, err)
	require.NotNil(t, plan)
	require.Equal(t, len(mv.Columns), res.MVColumnCount)
	require.Equal(t, 1, res.CountStarMVOffset)

	var hasCountStar, hasCountExpr, hasSum bool
	for _, ai := range res.AggInfos {
		switch ai.Kind {
		case mvmerge.AggCountStar:
			hasCountStar = true
			requireDependencies(t, ai, []int{0})
		case mvmerge.AggSum:
			hasSum = true
			require.Equal(t, "b", ai.ArgColName)
			requireDependencies(t, ai, []int{2, 5})
		case mvmerge.AggCount:
			hasCountExpr = true
			require.Equal(t, "b", ai.ArgColName)
			requireDependencies(t, ai, []int{1})
		}
	}
	require.True(t, hasCountStar)
	require.True(t, hasCountExpr)
	require.True(t, hasSum)

	item, ok := is.TableItemByID(mv.ID)
	require.True(t, ok)
	require.NotEmpty(t, item.DBName.O)
	mvDBName := item.DBName.O
	requireMergePlanOutputNames(t, plan, outputNames, []fieldNameInfo{
		{Pos: 0, Tbl: deltaTableAlias, Col: deltaCntStarName},
		{Pos: 1, Tbl: deltaTableAlias, Col: "__mvmerge_delta_cnt_2"},
		{Pos: 2, Tbl: deltaTableAlias, Col: "__mvmerge_delta_sum_3"},
		{Pos: 3, DB: mvDBName, Tbl: deltaTableAlias, Col: "x", OrigTbl: mlog.Name.O, OrigCol: "a"},
		{Pos: 4, DB: mvDBName, Tbl: mvTableAlias, Col: "cnt", OrigTbl: mv.Name.O, OrigCol: "cnt"},
		{Pos: 5, DB: mvDBName, Tbl: mvTableAlias, Col: "cnt_b", OrigTbl: mv.Name.O, OrigCol: "cnt_b"},
		{Pos: 6, DB: mvDBName, Tbl: mvTableAlias, Col: "s", OrigTbl: mv.Name.O, OrigCol: "s"},
		{Pos: 7, DB: mvDBName, Tbl: mvTableAlias, Col: "__mvmerge_mv_rowid", OrigCol: "_tidb_rowid"},
	})
}

func TestBuildCountExprSumExpr(t *testing.T) {
	sctx := core.MockContext()

	baseID := int64(11)
	mlogID := int64(22)
	mvID := int64(33)

	base := &model.TableInfo{
		ID:    baseID,
		Name:  pmodel.NewCIStr("t"),
		State: model.StatePublic,
		Columns: []*model.ColumnInfo{
			mkCol(1, "a", 0, mysql.TypeLong),
			mkCol(2, "b", 1, mysql.TypeLong),
		},
		MaterializedViewBase: &model.MaterializedViewBaseInfo{MLogID: mlogID},
	}
	mlog := &model.TableInfo{
		ID:    mlogID,
		Name:  pmodel.NewCIStr("$mlog$t"),
		State: model.StatePublic,
		Columns: []*model.ColumnInfo{
			mkCol(1, "a", 0, mysql.TypeLong),
			mkCol(2, "b", 1, mysql.TypeLong),
			mkCol(3, model.MaterializedViewLogDMLTypeColumnName, 2, mysql.TypeVarchar),
			mkCol(4, model.MaterializedViewLogOldNewColumnName, 3, mysql.TypeTiny),
		},
		MaterializedViewLog: &model.MaterializedViewLogInfo{
			BaseTableID: baseID,
			Columns:     []pmodel.CIStr{pmodel.NewCIStr("a"), pmodel.NewCIStr("b")},
		},
	}
	mv := &model.TableInfo{
		ID:    mvID,
		Name:  pmodel.NewCIStr("mv_expr_tbl"),
		State: model.StatePublic,
		Columns: []*model.ColumnInfo{
			mkCol(1, "x", 0, mysql.TypeLong),
			mkCol(2, "cnt_star", 1, mysql.TypeLonglong),
			mkCol(3, "cnt_b", 2, mysql.TypeLonglong),
			mkCol(4, "s_expr", 3, mysql.TypeLonglong),
		},
		MaterializedView: &model.MaterializedViewInfo{
			BaseTableIDs: []int64{baseID},
			SQLContent:   "select a, count(1), count(a+b), sum((a+b)) from t group by a",
		},
	}

	is := infoschema.MockInfoSchema([]*model.TableInfo{base, mlog, mv})
	domain.GetDomain(sctx).MockInfoCacheAndLoadInfoSchema(is)

	res, err := mvmerge.Build(
		sctx.GetPlanCtx(),
		is,
		mv,
		mvmerge.BuildOptions{FromTS: 10, ToTS: 20},
		nil,
	)
	require.NoError(t, err)
	plan, outputNames, err := optimizeForTest(sctx, is)(context.Background(), res.MergeSourceSelect)
	require.NoError(t, err)
	require.NotNil(t, plan)
	require.Equal(t, 1, res.CountStarMVOffset)

	var hasCountStar, hasCount, hasSum bool
	for _, ai := range res.AggInfos {
		switch ai.Kind {
		case mvmerge.AggCountStar:
			hasCountStar = true
			requireDependencies(t, ai, []int{0})
		case mvmerge.AggCount:
			hasCount = true
			require.Empty(t, ai.ArgColName)
			requireDependencies(t, ai, []int{1})
		case mvmerge.AggSum:
			hasSum = true
			require.Empty(t, ai.ArgColName)
			requireDependencies(t, ai, []int{2, 5})
		}
	}
	require.True(t, hasCountStar)
	require.True(t, hasCount)
	require.True(t, hasSum)

	item, ok := is.TableItemByID(mv.ID)
	require.True(t, ok)
	mvDBName := item.DBName.O
	requireMergePlanOutputNames(t, plan, outputNames, []fieldNameInfo{
		{Pos: 0, Tbl: deltaTableAlias, Col: deltaCntStarName},
		{Pos: 1, Tbl: deltaTableAlias, Col: "__mvmerge_delta_cnt_2"},
		{Pos: 2, Tbl: deltaTableAlias, Col: "__mvmerge_delta_sum_3"},
		{Pos: 3, DB: mvDBName, Tbl: deltaTableAlias, Col: "x", OrigTbl: mlog.Name.O, OrigCol: "a"},
		{Pos: 4, DB: mvDBName, Tbl: mvTableAlias, Col: "cnt_star", OrigTbl: mv.Name.O, OrigCol: "cnt_star"},
		{Pos: 5, DB: mvDBName, Tbl: mvTableAlias, Col: "cnt_b", OrigTbl: mv.Name.O, OrigCol: "cnt_b"},
		{Pos: 6, DB: mvDBName, Tbl: mvTableAlias, Col: "s_expr", OrigTbl: mv.Name.O, OrigCol: "s_expr"},
		{Pos: 7, DB: mvDBName, Tbl: mvTableAlias, Col: "__mvmerge_mv_rowid", OrigCol: "_tidb_rowid"},
	})
}

func TestBuildMinMaxHasRemovedGate(t *testing.T) {
	sctx := core.MockContext()

	baseID := int64(10)
	mlogID := int64(20)
	mvID := int64(30)

	base := &model.TableInfo{
		ID:    baseID,
		Name:  pmodel.NewCIStr("t"),
		State: model.StatePublic,
		Columns: []*model.ColumnInfo{
			mkCol(1, "a", 0, mysql.TypeLong),
			mkCol(2, "b", 1, mysql.TypeLong),
		},
		MaterializedViewBase: &model.MaterializedViewBaseInfo{MLogID: mlogID},
	}
	mlog := &model.TableInfo{
		ID:    mlogID,
		Name:  pmodel.NewCIStr("$mlog$t"),
		State: model.StatePublic,
		Columns: []*model.ColumnInfo{
			mkCol(1, "a", 0, mysql.TypeLong),
			mkCol(2, "b", 1, mysql.TypeLong),
			mkCol(3, model.MaterializedViewLogDMLTypeColumnName, 2, mysql.TypeVarchar),
			mkCol(4, model.MaterializedViewLogOldNewColumnName, 3, mysql.TypeTiny),
		},
		MaterializedViewLog: &model.MaterializedViewLogInfo{
			BaseTableID: baseID,
			Columns:     []pmodel.CIStr{pmodel.NewCIStr("a"), pmodel.NewCIStr("b")},
		},
	}
	mv := &model.TableInfo{
		ID:    mvID,
		Name:  pmodel.NewCIStr("mv_minmax_tbl"),
		State: model.StatePublic,
		Columns: []*model.ColumnInfo{
			mkCol(1, "x", 0, mysql.TypeLong),
			mkCol(2, "cnt", 1, mysql.TypeLonglong),
			mkCol(3, "mx", 2, mysql.TypeLong),
			mkCol(4, "mn", 3, mysql.TypeLong),
		},
		MaterializedView: &model.MaterializedViewInfo{
			BaseTableIDs: []int64{baseID},
			SQLContent:   "select a, count(1), max(b), min(b) from t group by a",
		},
	}

	is := infoschema.MockInfoSchema([]*model.TableInfo{base, mlog, mv})
	domain.GetDomain(sctx).MockInfoCacheAndLoadInfoSchema(is)

	res, err := mvmerge.Build(
		sctx.GetPlanCtx(),
		is,
		mv,
		mvmerge.BuildOptions{FromTS: 1, ToTS: 2},
		nil,
	)
	require.NoError(t, err)
	plan, outputNames, err := optimizeForTest(sctx, is)(context.Background(), res.MergeSourceSelect)
	require.NoError(t, err)
	require.NotNil(t, res.RemovedRowCountDelta)
	require.Equal(t, 1, res.CountStarMVOffset)

	var hasMax, hasMin bool
	for _, ai := range res.AggInfos {
		if ai.Kind == mvmerge.AggMax {
			hasMax = true
			requireDependencies(t, ai, []int{1, 3})
		}
		if ai.Kind == mvmerge.AggMin {
			hasMin = true
			requireDependencies(t, ai, []int{2, 3})
		}
		if ai.Kind == mvmerge.AggCountStar {
			requireDependencies(t, ai, []int{0})
		}
	}
	require.True(t, hasMax)
	require.True(t, hasMin)

	item, ok := is.TableItemByID(mv.ID)
	require.True(t, ok)
	require.NotEmpty(t, item.DBName.O)
	mvDBName := item.DBName.O
	requireMergePlanOutputNames(t, plan, outputNames, []fieldNameInfo{
		{Pos: 0, Tbl: deltaTableAlias, Col: deltaCntStarName},
		{Pos: 1, Tbl: deltaTableAlias, Col: "__mvmerge_max_in_added_2"},
		{Pos: 2, Tbl: deltaTableAlias, Col: "__mvmerge_min_in_added_3"},
		{Pos: 3, Tbl: deltaTableAlias, Col: removedRowsName},
		{Pos: 4, DB: mvDBName, Tbl: deltaTableAlias, Col: "x", OrigTbl: mlog.Name.O, OrigCol: "a"},
		{Pos: 5, DB: mvDBName, Tbl: mvTableAlias, Col: "cnt", OrigTbl: mv.Name.O, OrigCol: "cnt"},
		{Pos: 6, DB: mvDBName, Tbl: mvTableAlias, Col: "mx", OrigTbl: mv.Name.O, OrigCol: "mx"},
		{Pos: 7, DB: mvDBName, Tbl: mvTableAlias, Col: "mn", OrigTbl: mv.Name.O, OrigCol: "mn"},
		{Pos: 8, DB: mvDBName, Tbl: mvTableAlias, Col: "__mvmerge_mv_rowid", OrigCol: "_tidb_rowid"},
	})
}

func TestBuildSumWithoutCountExpr(t *testing.T) {
	sctx := core.MockContext()

	baseID := int64(101)
	mlogID := int64(202)
	mvID := int64(303)

	base := &model.TableInfo{
		ID:    baseID,
		Name:  pmodel.NewCIStr("t"),
		State: model.StatePublic,
		Columns: []*model.ColumnInfo{
			mkCol(1, "a", 0, mysql.TypeLong),
			mkCol(2, "b", 1, mysql.TypeLong),
		},
		MaterializedViewBase: &model.MaterializedViewBaseInfo{MLogID: mlogID},
	}
	mlog := &model.TableInfo{
		ID:    mlogID,
		Name:  pmodel.NewCIStr("$mlog$t"),
		State: model.StatePublic,
		Columns: []*model.ColumnInfo{
			mkCol(1, "a", 0, mysql.TypeLong),
			mkCol(2, "b", 1, mysql.TypeLong),
			mkCol(3, model.MaterializedViewLogDMLTypeColumnName, 2, mysql.TypeVarchar),
			mkCol(4, model.MaterializedViewLogOldNewColumnName, 3, mysql.TypeTiny),
		},
		MaterializedViewLog: &model.MaterializedViewLogInfo{
			BaseTableID: baseID,
			Columns:     []pmodel.CIStr{pmodel.NewCIStr("a"), pmodel.NewCIStr("b")},
		},
	}
	mv := &model.TableInfo{
		ID:    mvID,
		Name:  pmodel.NewCIStr("mv_sum_only_tbl"),
		State: model.StatePublic,
		Columns: []*model.ColumnInfo{
			mkCol(1, "x", 0, mysql.TypeLong),
			mkCol(2, "cnt_star", 1, mysql.TypeLonglong),
			mkCol(3, "s", 2, mysql.TypeLonglong),
		},
		MaterializedView: &model.MaterializedViewInfo{
			BaseTableIDs: []int64{baseID},
			SQLContent:   "select a, count(1), sum(b) from t group by a",
		},
	}

	is := infoschema.MockInfoSchema([]*model.TableInfo{base, mlog, mv})
	domain.GetDomain(sctx).MockInfoCacheAndLoadInfoSchema(is)

	_, err := mvmerge.Build(
		sctx.GetPlanCtx(),
		is,
		mv,
		mvmerge.BuildOptions{FromTS: 1, ToTS: 2},
		nil,
	)
	require.ErrorContains(t, err, "requires matching COUNT(expr)")
}

func TestBuildMissingCountStar(t *testing.T) {
	sctx := core.MockContext()

	baseID := int64(111)
	mlogID := int64(222)
	mvID := int64(333)

	base := &model.TableInfo{
		ID:    baseID,
		Name:  pmodel.NewCIStr("t"),
		State: model.StatePublic,
		Columns: []*model.ColumnInfo{
			mkCol(1, "a", 0, mysql.TypeLong),
			mkCol(2, "b", 1, mysql.TypeLong),
		},
		MaterializedViewBase: &model.MaterializedViewBaseInfo{MLogID: mlogID},
	}
	mlog := &model.TableInfo{
		ID:    mlogID,
		Name:  pmodel.NewCIStr("$mlog$t"),
		State: model.StatePublic,
		Columns: []*model.ColumnInfo{
			mkCol(1, "a", 0, mysql.TypeLong),
			mkCol(2, "b", 1, mysql.TypeLong),
			mkCol(3, model.MaterializedViewLogDMLTypeColumnName, 2, mysql.TypeVarchar),
			mkCol(4, model.MaterializedViewLogOldNewColumnName, 3, mysql.TypeTiny),
		},
		MaterializedViewLog: &model.MaterializedViewLogInfo{
			BaseTableID: baseID,
			Columns:     []pmodel.CIStr{pmodel.NewCIStr("a"), pmodel.NewCIStr("b")},
		},
	}
	mv := &model.TableInfo{
		ID:    mvID,
		Name:  pmodel.NewCIStr("mv_no_cnt_star_tbl"),
		State: model.StatePublic,
		Columns: []*model.ColumnInfo{
			mkCol(1, "x", 0, mysql.TypeLong),
			mkCol(2, "cnt_b", 1, mysql.TypeLonglong),
			mkCol(3, "s", 2, mysql.TypeLonglong),
		},
		MaterializedView: &model.MaterializedViewInfo{
			BaseTableIDs: []int64{baseID},
			SQLContent:   "select a, count(b), sum(b) from t group by a",
		},
	}

	is := infoschema.MockInfoSchema([]*model.TableInfo{base, mlog, mv})
	domain.GetDomain(sctx).MockInfoCacheAndLoadInfoSchema(is)

	_, err := mvmerge.Build(
		sctx.GetPlanCtx(),
		is,
		mv,
		mvmerge.BuildOptions{FromTS: 1, ToTS: 2},
		nil,
	)
	require.ErrorContains(t, err, "must include COUNT(*)")
}

func TestBuildMissingOldNew(t *testing.T) {
	sctx := core.MockContext()

	baseID := int64(1000)
	mlogID := int64(2000)
	mvID := int64(3000)

	base := &model.TableInfo{
		ID:    baseID,
		Name:  pmodel.NewCIStr("t"),
		State: model.StatePublic,
		Columns: []*model.ColumnInfo{
			mkCol(1, "a", 0, mysql.TypeLong),
		},
		MaterializedViewBase: &model.MaterializedViewBaseInfo{MLogID: mlogID},
	}
	mlog := &model.TableInfo{
		ID:    mlogID,
		Name:  pmodel.NewCIStr("$mlog$t"),
		State: model.StatePublic,
		Columns: []*model.ColumnInfo{
			mkCol(1, "a", 0, mysql.TypeLong),
			mkCol(3, model.MaterializedViewLogDMLTypeColumnName, 1, mysql.TypeVarchar),
		},
		MaterializedViewLog: &model.MaterializedViewLogInfo{
			BaseTableID: baseID,
			Columns:     []pmodel.CIStr{pmodel.NewCIStr("a")},
		},
	}
	mv := &model.TableInfo{
		ID:    mvID,
		Name:  pmodel.NewCIStr("mv"),
		State: model.StatePublic,
		Columns: []*model.ColumnInfo{
			mkCol(1, "x", 0, mysql.TypeLong),
			mkCol(2, "cnt", 1, mysql.TypeLonglong),
		},
		MaterializedView: &model.MaterializedViewInfo{
			BaseTableIDs: []int64{baseID},
			SQLContent:   "select a, count(1) from t group by a",
		},
	}

	is := infoschema.MockInfoSchema([]*model.TableInfo{base, mlog, mv})
	domain.GetDomain(sctx).MockInfoCacheAndLoadInfoSchema(is)

	_, err := mvmerge.Build(
		sctx.GetPlanCtx(),
		is,
		mv,
		mvmerge.BuildOptions{FromTS: 1, ToTS: 2},
		nil,
	)
	require.ErrorContains(t, err, model.MaterializedViewLogOldNewColumnName)
}

func mkCol(id int64, name string, offset int, tp byte) *model.ColumnInfo {
	ft := types.NewFieldType(tp)
	return &model.ColumnInfo{
		ID:        id,
		Name:      pmodel.NewCIStr(name),
		Offset:    offset,
		State:     model.StatePublic,
		FieldType: *ft,
	}
}

func requireDependencies(t *testing.T, ai mvmerge.AggInfo, expected []int) {
	t.Helper()
	require.Equal(t, expected, ai.Dependencies)
}

type fieldNameInfo struct {
	Pos int

	Hidden bool
	DB     string
	Tbl    string
	Col    string

	OrigTbl string
	OrigCol string
}

func nameSliceInfo(names types.NameSlice) []fieldNameInfo {
	out := make([]fieldNameInfo, 0, len(names))
	for i, n := range names {
		if n == nil {
			out = append(out, fieldNameInfo{Pos: i, Col: "<nil>"})
			continue
		}
		out = append(out, fieldNameInfo{
			Pos:     i,
			Hidden:  n.Hidden,
			DB:      n.DBName.O,
			Tbl:     n.TblName.O,
			Col:     n.ColName.O,
			OrigTbl: n.OrigTblName.O,
			OrigCol: n.OrigColName.O,
		})
	}
	return out
}

func requireMergePlanOutputNames(t *testing.T, plan corebase.PhysicalPlan, outputNames types.NameSlice, expected []fieldNameInfo) {
	t.Helper()

	require.Len(t, outputNames, plan.Schema().Len())
	require.Len(t, expected, len(outputNames))
	require.Equal(t, expected, nameSliceInfo(outputNames))
}
