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

package mvmerge

import (
	"context"
	"testing"

	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/meta/model"
	pmodel "github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/core"
	_ "github.com/pingcap/tidb/pkg/session"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/stretchr/testify/require"
)

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
			mkCol(3, "s", 2, mysql.TypeLonglong),
		},
		MaterializedView: &model.MaterializedViewInfo{
			BaseTableIDs: []int64{baseID},
			SQLContent:   "select a, count(1), sum(b) from t group by a",
		},
	}

	is := infoschema.MockInfoSchema([]*model.TableInfo{base, mlog, mv})
	domain.GetDomain(sctx).MockInfoCacheAndLoadInfoSchema(is)

	res, err := Build(context.Background(), sctx, is, mv, BuildOptions{FromTS: 10, ToTS: 20})
	require.NoError(t, err)
	require.NotNil(t, res.Plan)
	require.Equal(t, len(mv.Columns), res.MVColumnCount)

	require.NotEmpty(t, res.DeltaColumns)
	require.Contains(t, deltaColNames(res.DeltaColumns), deltaCntStarName)
	require.Contains(t, deltaColNames(res.DeltaColumns), "__mvmerge_delta_sum_2")

	// COUNT(*) and SUM(b) should both reference delta columns.
	var hasCount, hasSum bool
	for _, ai := range res.AggInfos {
		switch ai.Kind {
		case AggCountStar:
			hasCount = true
			require.GreaterOrEqual(t, ai.DeltaOffset, res.MVColumnCount)
		case AggSum:
			hasSum = true
			require.Equal(t, "b", ai.ArgColName)
			require.GreaterOrEqual(t, ai.DeltaOffset, res.MVColumnCount)
		}
	}
	require.True(t, hasCount)
	require.True(t, hasSum)

	mvDBName, err := dbNameByTableID(is, mv.ID)
	require.NoError(t, err)
	requireMergePlanOutputNames(t, res, []fieldNameInfo{
		{Pos: 0, Col: "x"},
		{Pos: 1, DB: mvDBName.O, Tbl: mvTableAlias, Col: "cnt", OrigTbl: mv.Name.O, OrigCol: "cnt"},
		{Pos: 2, DB: mvDBName.O, Tbl: mvTableAlias, Col: "s", OrigTbl: mv.Name.O, OrigCol: "s"},
		{Pos: 3, Tbl: deltaTableAlias, Col: deltaCntStarName},
		{Pos: 4, Tbl: deltaTableAlias, Col: "__mvmerge_delta_sum_2"},
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

	res, err := Build(context.Background(), sctx, is, mv, BuildOptions{FromTS: 1, ToTS: 2})
	require.NoError(t, err)
	require.NotNil(t, res.RemovedRowCountDelta)
	require.Contains(t, deltaColNames(res.DeltaColumns), removedRowsName)

	var hasMax, hasMin bool
	for _, ai := range res.AggInfos {
		if ai.Kind == AggMax {
			hasMax = true
			require.True(t, ai.NeedsDetailOnRemoval)
		}
		if ai.Kind == AggMin {
			hasMin = true
			require.True(t, ai.NeedsDetailOnRemoval)
		}
	}
	require.True(t, hasMax)
	require.True(t, hasMin)

	mvDBName, err := dbNameByTableID(is, mv.ID)
	require.NoError(t, err)
	requireMergePlanOutputNames(t, res, []fieldNameInfo{
		{Pos: 0, Col: "x"},
		{Pos: 1, DB: mvDBName.O, Tbl: mvTableAlias, Col: "cnt", OrigTbl: mv.Name.O, OrigCol: "cnt"},
		{Pos: 2, DB: mvDBName.O, Tbl: mvTableAlias, Col: "mx", OrigTbl: mv.Name.O, OrigCol: "mx"},
		{Pos: 3, DB: mvDBName.O, Tbl: mvTableAlias, Col: "mn", OrigTbl: mv.Name.O, OrigCol: "mn"},
		{Pos: 4, Tbl: deltaTableAlias, Col: deltaCntStarName},
		{Pos: 5, Tbl: deltaTableAlias, Col: "__mvmerge_max_in_added_2"},
		{Pos: 6, Tbl: deltaTableAlias, Col: "__mvmerge_min_in_added_3"},
		{Pos: 7, Tbl: deltaTableAlias, Col: removedRowsName},
	})
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

	_, err := Build(context.Background(), sctx, is, mv, BuildOptions{FromTS: 1, ToTS: 2})
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

func deltaColNames(cols []DeltaColumn) []string {
	out := make([]string, 0, len(cols))
	for _, c := range cols {
		out = append(out, c.Name)
	}
	return out
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

func requireMergePlanOutputNames(t *testing.T, res *BuildResult, expected []fieldNameInfo) {
	t.Helper()

	outputNames := res.Plan.OutputNames()
	require.Len(t, outputNames, res.Plan.Schema().Len())
	require.Len(t, outputNames, res.MVColumnCount+len(res.DeltaColumns))
	require.Len(t, expected, len(outputNames))
	require.Equal(t, expected, nameSliceInfo(outputNames))
}
