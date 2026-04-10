// Copyright 2023 PingCAP, Inc.
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

package executor

import (
	"context"
	"errors"
	"reflect"
	"regexp"
	"testing"
	"time"
	"unsafe"

	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/auth"
	pmodel "github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	plannercore "github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/privilege"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/set"
	"github.com/stretchr/testify/require"
)

type mockInfoSchemaWithItems struct {
	infoschema.InfoSchema
	items []infoschema.TableItem
}

func (m *mockInfoSchemaWithItems) IterateAllTableItems(visit func(infoschema.TableItem) bool) {
	for _, item := range m.items {
		if !visit(item) {
			return
		}
	}
}

type tableByNameErrorInfoSchema struct {
	infoschema.InfoSchema
	err error
}

func (m *tableByNameErrorInfoSchema) TableByName(
	_ context.Context,
	_, _ pmodel.CIStr,
) (table.Table, error) {
	return nil, m.err
}

type stubPrivilegeManager struct {
	privilege.Manager
	allow func(db, table string) bool
	calls []string
}

func (m *stubPrivilegeManager) RequestVerification(
	_ []*auth.RoleIdentity,
	db, table, _ string,
	_ mysql.PrivilegeType,
) bool {
	m.calls = append(m.calls, db+"."+table)
	if m.allow == nil {
		return true
	}
	return m.allow(db, table)
}

func setExtractorRegexp(
	t *testing.T,
	ex *plannercore.InfoSchemaTiDBMViewsExtractor,
	col string,
	patterns ...string,
) {
	t.Helper()

	regs := make([]*regexp.Regexp, 0, len(patterns))
	for _, pattern := range patterns {
		regs = append(regs, regexp.MustCompile(pattern))
	}

	field := reflect.ValueOf(&ex.InfoSchemaBaseExtractor).Elem().FieldByName("colsRegexp")
	require.True(t, field.IsValid())
	field = reflect.NewAt(field.Type(), unsafe.Pointer(field.UnsafeAddr())).Elem()
	field.Set(reflect.ValueOf(map[string][]*regexp.Regexp{col: regs}))
}

func newColumnInfo(name string) *model.ColumnInfo {
	return &model.ColumnInfo{Name: pmodel.NewCIStr(name)}
}

func newMViewTableInfo(id int64, name string, updateTS uint64) *model.TableInfo {
	return &model.TableInfo{
		ID:       id,
		Name:     pmodel.NewCIStr(name),
		Comment:  name + "-comment",
		UpdateTS: updateTS,
		State:    model.StatePublic,
		MaterializedView: &model.MaterializedViewInfo{
			SQLContent:       "select 1",
			RefreshMethod:    "FAST",
			RefreshStartWith: "CURRENT_TIMESTAMP",
			RefreshNext:      "CURRENT_TIMESTAMP + INTERVAL 1 HOUR",
		},
	}
}

func newMLogTableInfo(id int64, name string, baseTableID int64) *model.TableInfo {
	return &model.TableInfo{
		ID:    id,
		Name:  pmodel.NewCIStr(name),
		State: model.StatePublic,
		MaterializedViewLog: &model.MaterializedViewLogInfo{
			BaseTableID:    baseTableID,
			Columns:        []pmodel.CIStr{pmodel.NewCIStr("a"), pmodel.NewCIStr("b")},
			PurgeMethod:    "DEFERRED",
			PurgeStartWith: "CURRENT_TIMESTAMP",
			PurgeNext:      "CURRENT_TIMESTAMP + INTERVAL 1 HOUR",
		},
	}
}

func TestSetDataFromCheckConstraints(t *testing.T) {
	tblInfos := []*model.TableInfo{
		{
			ID:    1,
			Name:  pmodel.NewCIStr("t1"),
			State: model.StatePublic,
		},
		{
			ID:   2,
			Name: pmodel.NewCIStr("t2"),
			Columns: []*model.ColumnInfo{
				{
					Name:      pmodel.NewCIStr("id"),
					FieldType: *types.NewFieldType(mysql.TypeLonglong),
					State:     model.StatePublic,
				},
			},
			Constraints: []*model.ConstraintInfo{
				{
					Name:       pmodel.NewCIStr("t2_c1"),
					Table:      pmodel.NewCIStr("t2"),
					ExprString: "id<10",
					State:      model.StatePublic,
				},
			},
			State: model.StatePublic,
		},
		{
			ID:   3,
			Name: pmodel.NewCIStr("t3"),
			Columns: []*model.ColumnInfo{
				{
					Name:      pmodel.NewCIStr("id"),
					FieldType: *types.NewFieldType(mysql.TypeLonglong),
					State:     model.StatePublic,
				},
			},
			Constraints: []*model.ConstraintInfo{
				{
					Name:       pmodel.NewCIStr("t3_c1"),
					Table:      pmodel.NewCIStr("t3"),
					ExprString: "id<10",
					State:      model.StateDeleteOnly,
				},
			},
			State: model.StatePublic,
		},
	}
	mockIs := infoschema.MockInfoSchema(tblInfos)
	mt := memtableRetriever{is: mockIs, extractor: &plannercore.InfoSchemaCheckConstraintsExtractor{}}
	sctx := defaultCtx()
	err := mt.setDataFromCheckConstraints(context.Background(), sctx)
	require.NoError(t, err)

	require.Equal(t, 1, len(mt.rows))    // 1 row
	require.Equal(t, 4, len(mt.rows[0])) // 4 columns
	require.Equal(t, types.NewStringDatum("def"), mt.rows[0][0])
	require.Equal(t, types.NewStringDatum("test"), mt.rows[0][1])
	require.Equal(t, types.NewStringDatum("t2_c1"), mt.rows[0][2])
	require.Equal(t, types.NewStringDatum("(id<10)"), mt.rows[0][3])
}

func TestSetDataFromTiDBCheckConstraints(t *testing.T) {
	mt := memtableRetriever{}
	sctx := defaultCtx()
	tblInfos := []*model.TableInfo{
		{
			ID:    1,
			Name:  pmodel.NewCIStr("t1"),
			State: model.StatePublic,
		},
		{
			ID:   2,
			Name: pmodel.NewCIStr("t2"),
			Columns: []*model.ColumnInfo{
				{
					Name:      pmodel.NewCIStr("id"),
					FieldType: *types.NewFieldType(mysql.TypeLonglong),
					State:     model.StatePublic,
				},
			},
			Constraints: []*model.ConstraintInfo{
				{
					Name:       pmodel.NewCIStr("t2_c1"),
					Table:      pmodel.NewCIStr("t2"),
					ExprString: "id<10",
					State:      model.StatePublic,
				},
			},
			State: model.StatePublic,
		},
		{
			ID:   3,
			Name: pmodel.NewCIStr("t3"),
			Columns: []*model.ColumnInfo{
				{
					Name:      pmodel.NewCIStr("id"),
					FieldType: *types.NewFieldType(mysql.TypeLonglong),
					State:     model.StatePublic,
				},
			},
			Constraints: []*model.ConstraintInfo{
				{
					Name:       pmodel.NewCIStr("t3_c1"),
					Table:      pmodel.NewCIStr("t3"),
					ExprString: "id<10",
					State:      model.StateDeleteOnly,
				},
			},
			State: model.StatePublic,
		},
	}
	mockIs := infoschema.MockInfoSchema(tblInfos)
	mt.is = mockIs
	mt.extractor = &plannercore.InfoSchemaTiDBCheckConstraintsExtractor{}
	err := mt.setDataFromTiDBCheckConstraints(context.Background(), sctx)
	require.NoError(t, err)

	require.Equal(t, 1, len(mt.rows))    // 1 row
	require.Equal(t, 6, len(mt.rows[0])) // 6 columns
	require.Equal(t, types.NewStringDatum("def"), mt.rows[0][0])
	require.Equal(t, types.NewStringDatum("test"), mt.rows[0][1])
	require.Equal(t, types.NewStringDatum("t2_c1"), mt.rows[0][2])
	require.Equal(t, types.NewStringDatum("(id<10)"), mt.rows[0][3])
	require.Equal(t, types.NewStringDatum("t2"), mt.rows[0][4])
	require.Equal(t, types.NewIntDatum(2), mt.rows[0][5])
}

func TestSetDataFromKeywords(t *testing.T) {
	mt := memtableRetriever{}
	err := mt.setDataFromKeywords()
	require.NoError(t, err)
	require.Equal(t, types.NewStringDatum("ADD"), mt.rows[0][0]) // Keyword: ADD
	require.Equal(t, types.NewIntDatum(1), mt.rows[0][1])        // Reserved: true(1)
}

func TestSetDataFromTiDBMViews(t *testing.T) {
	t.Run("wrong extractor type", func(t *testing.T) {
		mt := memtableRetriever{
			extractor: &plannercore.InfoSchemaTablesExtractor{},
		}

		err := mt.setDataFromTiDBMViews(context.Background(), defaultCtx())
		require.ErrorContains(t, err, "wrong extractor type")
	})

	t.Run("skip request", func(t *testing.T) {
		ex := plannercore.NewInfoSchemaTiDBMViewsExtractor()
		ex.SkipRequest = true
		mt := memtableRetriever{extractor: ex}

		err := mt.setDataFromTiDBMViews(context.Background(), defaultCtx())
		require.NoError(t, err)
		require.Nil(t, mt.rows)
	})

	t.Run("returns list schemas error", func(t *testing.T) {
		ex := plannercore.NewInfoSchemaTiDBMViewsExtractor()
		ex.ColPredicates = map[string]set.StringSet{
			plannercore.MViewName: set.NewStringSet("mv_err"),
		}
		mt := memtableRetriever{
			is: &tableByNameErrorInfoSchema{
				InfoSchema: infoschema.MockInfoSchema(nil),
				err:        errors.New("boom"),
			},
			extractor: ex,
			columns: []*model.ColumnInfo{
				newColumnInfo("mview_sql_content"),
			},
		}

		err := mt.setDataFromTiDBMViews(context.Background(), defaultCtx())
		require.ErrorContains(t, err, "boom")
	})

	t.Run("columns are not eligible", func(t *testing.T) {
		updateTS := uint64(time.Date(2025, 1, 2, 3, 4, 5, 0, time.UTC).UnixMilli()) << 18
		ex := plannercore.NewInfoSchemaTiDBMViewsExtractor()
		mt := memtableRetriever{
			is: infoschema.MockInfoSchema([]*model.TableInfo{
				{
					ID:    1,
					Name:  pmodel.NewCIStr("base"),
					State: model.StatePublic,
				},
				newMViewTableInfo(2, "mv_deny", updateTS),
				newMViewTableInfo(3, "mv_keep", updateTS),
			}),
			extractor: ex,
			columns: []*model.ColumnInfo{
				newColumnInfo("mview_name"),
				newColumnInfo("mview_sql_content"),
			},
		}

		sctx := defaultCtx()
		sctx.GetSessionVars().TimeZone = nil
		pm := &stubPrivilegeManager{
			allow: func(db, table string) bool {
				return table != "mv_deny"
			},
		}
		privilege.BindPrivilegeManager(sctx, pm)

		err := mt.setDataFromTiDBMViews(context.Background(), sctx)
		require.NoError(t, err)
		require.ElementsMatch(t, []string{"test.mv_deny", "test.mv_keep"}, pm.calls)
		require.Len(t, mt.rows, 1)

		row := mt.rows[0]
		require.Equal(t, types.NewStringDatum(infoschema.CatalogVal), row[0])
		require.Equal(t, types.NewStringDatum("test"), row[1])
		require.Equal(t, types.NewIntDatum(3), row[2])
		require.Equal(t, types.NewStringDatum("mv_keep"), row[3])
		require.Equal(t, types.NewStringDatum("select 1"), row[4])
		require.Equal(t, types.NewStringDatum("mv_keep-comment"), row[5])
		expectedTime := types.NewTime(
			types.FromGoTime(model.TSConvert2Time(updateTS).In(time.Local)),
			mysql.TypeDatetime,
			types.DefaultFsp,
		)
		require.Equal(t, expectedTime, row[6].GetMysqlTime())
		require.Equal(t, types.NewStringDatum("FAST"), row[7])
		require.Equal(t, types.NewStringDatum("CURRENT_TIMESTAMP"), row[8])
		require.Equal(t, types.NewStringDatum("CURRENT_TIMESTAMP + INTERVAL 1 HOUR"), row[9])
	})

	t.Run("predicates are not eligible", func(t *testing.T) {
		loc := time.FixedZone("UTC+8", 8*3600)
		updateTS := uint64(time.Date(2025, 6, 7, 8, 9, 10, 0, time.UTC).UnixMilli()) << 18
		ex := plannercore.NewInfoSchemaTiDBMViewsExtractor()
		ex.ColPredicates = map[string]set.StringSet{
			"refresh_next": set.NewStringSet("ignored"),
		}
		mt := memtableRetriever{
			is: infoschema.MockInfoSchema([]*model.TableInfo{
				newMViewTableInfo(1, "mv_keep", updateTS),
			}),
			extractor: ex,
			columns: []*model.ColumnInfo{
				newColumnInfo("mview_name"),
			},
		}

		sctx := defaultCtx()
		sctx.GetSessionVars().TimeZone = loc

		err := mt.setDataFromTiDBMViews(context.Background(), sctx)
		require.NoError(t, err)
		require.Len(t, mt.rows, 1)
		require.Equal(
			t,
			types.NewTime(types.FromGoTime(model.TSConvert2Time(updateTS).In(loc)), mysql.TypeDatetime, types.DefaultFsp),
			mt.rows[0][6].GetMysqlTime(),
		)
	})
}

func TestSetDataFromTiDBMLogs(t *testing.T) {
	t.Run("wrong extractor type", func(t *testing.T) {
		mt := memtableRetriever{
			extractor: &plannercore.InfoSchemaTablesExtractor{},
		}

		err := mt.setDataFromTiDBMLogs(context.Background(), defaultCtx())
		require.ErrorContains(t, err, "wrong extractor type")
	})

	t.Run("skip request", func(t *testing.T) {
		ex := plannercore.NewInfoSchemaTiDBMLogsExtractor()
		ex.SkipRequest = true
		mt := memtableRetriever{extractor: ex}

		err := mt.setDataFromTiDBMLogs(context.Background(), defaultCtx())
		require.NoError(t, err)
		require.Nil(t, mt.rows)
	})

	t.Run("returns list schemas error", func(t *testing.T) {
		ex := plannercore.NewInfoSchemaTiDBMLogsExtractor()
		ex.ColPredicates = map[string]set.StringSet{
			plannercore.MLogName: set.NewStringSet("$mlog$err"),
		}
		mt := memtableRetriever{
			is: &tableByNameErrorInfoSchema{
				InfoSchema: infoschema.MockInfoSchema(nil),
				err:        errors.New("boom"),
			},
			extractor: ex,
			columns: []*model.ColumnInfo{
				newColumnInfo("mlog_columns"),
			},
		}

		err := mt.setDataFromTiDBMLogs(context.Background(), defaultCtx())
		require.ErrorContains(t, err, "boom")
	})

	t.Run("columns are eligible", func(t *testing.T) {
		mt := memtableRetriever{
			is: infoschema.MockInfoSchema([]*model.TableInfo{
				{ID: 1, Name: pmodel.NewCIStr("base"), State: model.StatePublic},
				newMLogTableInfo(2, "$mlog$deny", 1),
				newMLogTableInfo(3, "$mlog$keep", 1),
			}),
			extractor: plannercore.NewInfoSchemaTiDBMLogsExtractor(),
			columns: []*model.ColumnInfo{
				newColumnInfo("mlog_name"),
				newColumnInfo("base_table_name"),
				newColumnInfo("mlog_columns"),
			},
		}

		sctx := defaultCtx()
		pm := &stubPrivilegeManager{
			allow: func(db, table string) bool {
				return table != "$mlog$deny"
			},
		}
		privilege.BindPrivilegeManager(sctx, pm)

		err := mt.setDataFromTiDBMLogs(context.Background(), sctx)
		require.NoError(t, err)
		require.ElementsMatch(t, []string{"test.$mlog$deny", "test.$mlog$keep"}, pm.calls)
		require.Len(t, mt.rows, 1)

		row := mt.rows[0]
		require.Equal(t, types.NewStringDatum(infoschema.CatalogVal), row[0])
		require.Equal(t, types.NewStringDatum("test"), row[1])
		require.Equal(t, types.NewIntDatum(3), row[2])
		require.Equal(t, types.NewStringDatum("$mlog$keep"), row[3])
		require.Equal(t, types.NewStringDatum("a,b"), row[4])
		require.Equal(t, types.NewStringDatum(infoschema.CatalogVal), row[5])
		require.Equal(t, types.NewStringDatum("test"), row[6])
		require.Equal(t, types.NewStringDatum("1"), row[7])
		require.Equal(t, types.NewStringDatum("base"), row[8])
		require.Equal(t, types.NewStringDatum("DEFERRED"), row[9])
		require.Equal(t, types.NewStringDatum("CURRENT_TIMESTAMP"), row[10])
		require.Equal(t, types.NewStringDatum("CURRENT_TIMESTAMP + INTERVAL 1 HOUR"), row[11])
	})

	t.Run("uses base predicates only", func(t *testing.T) {
		ex := plannercore.NewInfoSchemaTiDBMLogsExtractor()
		ex.ColPredicates = map[string]set.StringSet{
			plannercore.BaseTableName: set.NewStringSet("base_keep"),
		}
		mt := memtableRetriever{
			is: infoschema.MockInfoSchema([]*model.TableInfo{
				{ID: 1, Name: pmodel.NewCIStr("base_drop"), State: model.StatePublic},
				{ID: 2, Name: pmodel.NewCIStr("base_keep"), State: model.StatePublic},
				newMLogTableInfo(3, "$mlog$drop", 1),
				newMLogTableInfo(4, "$mlog$keep", 2),
			}),
			extractor: ex,
			columns: []*model.ColumnInfo{
				newColumnInfo("mlog_name"),
			},
		}

		err := mt.setDataFromTiDBMLogs(context.Background(), defaultCtx())
		require.NoError(t, err)
		require.Len(t, mt.rows, 1)
		require.Equal(t, types.NewStringDatum("$mlog$keep"), mt.rows[0][3])
	})

	t.Run("uses mlog predicates only", func(t *testing.T) {
		ex := plannercore.NewInfoSchemaTiDBMLogsExtractor()
		ex.ColPredicates = map[string]set.StringSet{
			plannercore.MLogName: set.NewStringSet("$mlog$keep"),
		}
		mt := memtableRetriever{
			is: infoschema.MockInfoSchema([]*model.TableInfo{
				{ID: 1, Name: pmodel.NewCIStr("base_drop"), State: model.StatePublic},
				{ID: 2, Name: pmodel.NewCIStr("base_keep"), State: model.StatePublic},
				newMLogTableInfo(3, "$mlog$drop", 1),
				newMLogTableInfo(4, "$mlog$keep", 2),
			}),
			extractor: ex,
			columns: []*model.ColumnInfo{
				newColumnInfo("mlog_name"),
			},
		}

		err := mt.setDataFromTiDBMLogs(context.Background(), defaultCtx())
		require.NoError(t, err)
		require.Len(t, mt.rows, 1)
		require.Equal(t, types.NewStringDatum("$mlog$keep"), mt.rows[0][3])
		require.Equal(t, types.NewStringDatum("base_keep"), mt.rows[0][8])
	})

	t.Run("uses mlog and base predicates together", func(t *testing.T) {
		ex := plannercore.NewInfoSchemaTiDBMLogsExtractor()
		ex.ColPredicates = map[string]set.StringSet{
			plannercore.MLogID:        set.NewStringSet("3", "4"),
			plannercore.BaseTableName: set.NewStringSet("base_keep"),
		}
		mt := memtableRetriever{
			is: infoschema.MockInfoSchema([]*model.TableInfo{
				{ID: 1, Name: pmodel.NewCIStr("base_drop"), State: model.StatePublic},
				{ID: 2, Name: pmodel.NewCIStr("base_keep"), State: model.StatePublic},
				newMLogTableInfo(3, "$mlog$keep1", 1),
				newMLogTableInfo(4, "$mlog$keep2", 2),
			}),
			extractor: ex,
			columns: []*model.ColumnInfo{
				newColumnInfo("mlog_name"),
			},
		}

		err := mt.setDataFromTiDBMLogs(context.Background(), defaultCtx())
		require.NoError(t, err)
		require.Len(t, mt.rows, 1)
		require.Equal(t, types.NewStringDatum("$mlog$keep2"), mt.rows[0][3])
		require.Equal(t, types.NewStringDatum("base_keep"), mt.rows[0][8])
	})

	t.Run("predicates are not eligible", func(t *testing.T) {
		ex := plannercore.NewInfoSchemaTiDBMLogsExtractor()
		ex.ColPredicates = map[string]set.StringSet{
			"purge_next": set.NewStringSet("ignored"),
		}
		mt := memtableRetriever{
			is: infoschema.MockInfoSchema([]*model.TableInfo{
				{ID: 1, Name: pmodel.NewCIStr("base"), State: model.StatePublic},
				newMLogTableInfo(2, "$mlog$keep", 1),
			}),
			extractor: ex,
			columns: []*model.ColumnInfo{
				newColumnInfo("mlog_name"),
			},
		}

		err := mt.setDataFromTiDBMLogs(context.Background(), defaultCtx())
		require.NoError(t, err)
		require.Len(t, mt.rows, 1)
		require.Equal(t, types.NewStringDatum("CURRENT_TIMESTAMP + INTERVAL 1 HOUR"), mt.rows[0][11])
	})
}
