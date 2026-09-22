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

package ddl

import (
	"context"
	"testing"

	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/store/mockstore"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/stretchr/testify/require"
)

func TestInitCreateMaterializedViewBuildSessionAppliesDefinitionDivPrecisionIncrement(t *testing.T) {
	sessCtx := mock.NewContext()
	sessVars := sessCtx.GetSessionVars()
	sessVars.DivPrecisionIncrement = 2
	sessVars.CurrentDB = "before"
	job := &model.Job{
		ReorgMeta: &model.DDLReorgMeta{
			Location: &model.TimeZoneLocation{Name: "UTC"},
		},
	}
	mviewTableInfo := &model.TableInfo{
		MaterializedView: &model.MaterializedViewInfo{DefinitionDivPrecisionIncrement: 9},
	}

	restore, err := initCreateMaterializedViewBuildSession(sessCtx, job, mviewTableInfo, "test")
	require.NoError(t, err)
	require.Equal(t, 9, sessVars.DivPrecisionIncrement)
	require.Equal(t, "test", sessVars.CurrentDB)

	restore()
	require.Equal(t, 2, sessVars.DivPrecisionIncrement)
	require.Equal(t, "before", sessVars.CurrentDB)
}

func TestUpdateMaterializedViewBaseInfoOnCreateMissingBaseTable(t *testing.T) {
	store, err := mockstore.NewMockStore()
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	err = kv.RunInNewTxn(context.Background(), store, true, func(_ context.Context, txn kv.Transaction) error {
		metaMut := meta.NewMutator(txn)
		require.NoError(t, metaMut.CreateDatabase(&model.DBInfo{ID: 1, Name: ast.NewCIStr("test")}))

		job := &model.Job{SchemaID: 1, State: model.JobStateRunning}
		createdTable := &model.TableInfo{
			ID:               2,
			MaterializedView: &model.MaterializedViewInfo{BaseTableIDs: []int64{3}},
		}
		_, err := updateMaterializedViewBaseInfoOnCreate(&jobContext{metaMut: metaMut}, job, createdTable)
		require.Error(t, err)
		require.True(t, infoschema.ErrTableNotExists.Equal(err))
		require.Equal(t, model.JobStateCancelled, job.State)
		return nil
	})
	require.NoError(t, err)
}

func TestUpdateMaterializedViewBaseInfoOnDropPropagatesGetTableError(t *testing.T) {
	store, err := mockstore.NewMockStore()
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	err = kv.RunInNewTxn(context.Background(), store, true, func(_ context.Context, txn kv.Transaction) error {
		metaMut := meta.NewMutator(txn)
		job := &model.Job{SchemaID: 1}
		droppingTable := &model.TableInfo{
			ID: 2,
			MaterializedView: &model.MaterializedViewInfo{
				BaseTableIDs: []int64{3},
			},
		}

		_, err := updateMaterializedViewBaseInfoOnDrop(&jobContext{metaMut: metaMut}, job, droppingTable)
		require.Error(t, err)
		require.True(t, meta.ErrDBNotExists.Equal(err))
		return nil
	})
	require.NoError(t, err)
}

func TestUpdateMaterializedViewBaseInfoOnDropWithoutBaseTables(t *testing.T) {
	droppingTable := &model.TableInfo{
		ID:               2,
		MaterializedView: &model.MaterializedViewInfo{},
	}

	extraInfos, err := updateMaterializedViewBaseInfoOnDrop(&jobContext{}, &model.Job{TableID: droppingTable.ID}, droppingTable)
	require.NoError(t, err)
	require.Empty(t, extraInfos)
}

func TestBuildDropTableInvolvingSchemaInfo(t *testing.T) {
	base1 := &model.TableInfo{ID: 1, Name: ast.NewCIStr("base1")}
	base2 := &model.TableInfo{ID: 2, Name: ast.NewCIStr("base2")}
	mv := &model.TableInfo{
		ID:   3,
		Name: ast.NewCIStr("mv"),
		MaterializedView: &model.MaterializedViewInfo{
			BaseTableIDs: []int64{base1.ID, base2.ID},
		},
	}
	mlog := &model.TableInfo{
		ID:   4,
		Name: ast.NewCIStr("$mlog$base1"),
		MaterializedViewLog: &model.MaterializedViewLogInfo{
			BaseTableID: base1.ID,
		},
	}
	base1.MaterializedViewBase = &model.MaterializedViewBaseInfo{MLogID: mlog.ID}
	mlog.MaterializedViewLog.DependentMViewIDs = []int64{mv.ID}
	is := infoschema.MockInfoSchema([]*model.TableInfo{base1, base2, mv, mlog})

	involving, err := buildDropTableInvolvingSchemaInfo(context.Background(), is, "test", mv)
	require.NoError(t, err)
	require.Equal(t, []model.InvolvingSchemaInfo{
		{Database: "test", Table: "mv"},
		{Database: "test", Table: "base1"},
		{Database: "test", Table: "$mlog$base1"},
		{Database: "test", Table: "base2"},
	}, involving)

	mlog.MaterializedViewLog.DependentMViewIDs = nil
	involving, err = buildDropTableInvolvingSchemaInfo(context.Background(), is, "test", mv)
	require.NoError(t, err)
	require.Equal(t, []model.InvolvingSchemaInfo{
		{Database: "test", Table: "mv"},
		{Database: "test", Table: "base1"},
		{Database: "test", Table: "base2"},
	}, involving)

	base1.MaterializedViewBase.MLogID = 99
	_, err = buildDropTableInvolvingSchemaInfo(context.Background(), is, "test", mv)
	require.Error(t, err)
	require.True(t, infoschema.ErrTableNotExists.Equal(err))
	base1.MaterializedViewBase.MLogID = mlog.ID

	// A missing base table must not prevent dropping an orphaned MView.
	is = infoschema.MockInfoSchema([]*model.TableInfo{base1, mv, mlog})
	involving, err = buildDropTableInvolvingSchemaInfo(context.Background(), is, "test", mv)
	require.NoError(t, err)
	require.Equal(t, []model.InvolvingSchemaInfo{
		{Database: "test", Table: "mv"},
		{Database: "test", Table: "base1"},
	}, involving)

	involving, err = buildDropTableInvolvingSchemaInfo(context.Background(), is, "test", mlog)
	require.NoError(t, err)
	require.Equal(t, []model.InvolvingSchemaInfo{
		{Database: "test", Table: "$mlog$base1"},
		{Database: "test", Table: "base1"},
	}, involving)
}

func TestUpdateMaterializedViewBaseInfoOnDropMLogDependency(t *testing.T) {
	runCase := func(t *testing.T, dependent bool, baseExists bool, mlogExists bool, wrongBaseTableID bool) {
		store, err := mockstore.NewMockStore()
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, store.Close()) })

		err = kv.RunInNewTxn(context.Background(), store, true, func(_ context.Context, txn kv.Transaction) error {
			metaMut := meta.NewMutator(txn)
			require.NoError(t, metaMut.CreateDatabase(&model.DBInfo{ID: 1, Name: ast.NewCIStr("test")}))

			base := &model.TableInfo{
				ID:   1,
				Name: ast.NewCIStr("base"),
				MaterializedViewBase: &model.MaterializedViewBaseInfo{
					MLogID:   2,
					MViewIDs: []int64{3},
				},
			}
			mlog := &model.TableInfo{
				ID:   2,
				Name: ast.NewCIStr("$mlog$base"),
				MaterializedViewLog: &model.MaterializedViewLogInfo{
					BaseTableID: 1,
				},
			}
			if dependent {
				mlog.MaterializedViewLog.DependentMViewIDs = []int64{3}
			}
			if wrongBaseTableID {
				mlog.MaterializedViewLog.BaseTableID = 4
			}
			if baseExists {
				require.NoError(t, metaMut.CreateTableOrView(1, base))
			}
			if mlogExists {
				require.NoError(t, metaMut.CreateTableOrView(1, mlog))
			}

			droppingTable := &model.TableInfo{
				ID: 3,
				MaterializedView: &model.MaterializedViewInfo{
					BaseTableIDs: []int64{1},
				},
			}
			extraInfos, err := updateMaterializedViewBaseInfoOnDrop(
				&jobContext{metaMut: metaMut},
				&model.Job{SchemaID: 1, TableID: droppingTable.ID},
				droppingTable,
			)
			if !baseExists {
				require.NoError(t, err)
				require.Empty(t, extraInfos)
				return nil
			}
			if !dependent {
				require.NoError(t, err)
				require.Len(t, extraInfos, 1)
				return nil
			}
			if !mlogExists || wrongBaseTableID {
				require.NoError(t, err)
				require.Len(t, extraInfos, 1)
				require.Empty(t, extraInfos[0].tblInfo.MaterializedViewBase.MViewIDs)
				return nil
			}
			require.NoError(t, err)
			require.Len(t, extraInfos, 2)
			require.Empty(t, extraInfos[1].tblInfo.MaterializedViewLog.DependentMViewIDs)
			return nil
		})
		require.NoError(t, err)
	}

	t.Run("dependent mlog is updated", func(t *testing.T) {
		runCase(t, true, true, true, false)
	})
	t.Run("unrelated mlog is not updated", func(t *testing.T) {
		runCase(t, false, true, true, false)
	})
	t.Run("missing base table is skipped", func(t *testing.T) {
		runCase(t, true, false, true, false)
	})
	t.Run("missing mlog does not retry a started drop", func(t *testing.T) {
		runCase(t, true, true, false, false)
	})
	t.Run("mlog with wrong base table does not retry a started drop", func(t *testing.T) {
		runCase(t, true, true, true, true)
	})
}
