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
