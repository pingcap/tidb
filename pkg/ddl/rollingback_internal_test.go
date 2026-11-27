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

	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/store/mockstore"
	"github.com/pingcap/tidb/pkg/util/dbterror"
	"github.com/stretchr/testify/require"
)

func TestCancelQueuedAddIndexKeepsExistingIndex(t *testing.T) {
	for _, version := range []model.JobVersion{model.JobVersion1, model.JobVersion2} {
		for _, started := range []bool{false, true} {
			store, err := mockstore.NewMockStore()
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, store.Close()) })
			job := &model.Job{
				ID: 10, Version: version, Type: model.ActionAddIndex,
				SchemaID: 1, TableID: 2, State: model.JobStateCancelling,
			}
			index := &model.IndexInfo{ID: 3, Name: ast.NewCIStr("idx"), State: model.StatePublic}
			if started {
				job.SchemaState = model.StateWriteOnly
				index.State = model.StateWriteOnly
			}
			job.FillArgs(&model.ModifyIndexArgs{IndexArgs: []*model.IndexArg{{IndexName: index.Name}}})
			_, err = job.Encode(true)
			require.NoError(t, err)
			err = kv.RunInNewTxn(context.Background(), store, true, func(_ context.Context, txn kv.Transaction) error {
				m := meta.NewMutator(txn)
				require.NoError(t, m.CreateDatabase(&model.DBInfo{ID: 1, Name: ast.NewCIStr("test")}))
				require.NoError(t, m.CreateTableOrView(1, &model.TableInfo{
					ID: 2, Name: ast.NewCIStr("t"), State: model.StatePublic, Indices: []*model.IndexInfo{index},
				}))
				jobCtx := &jobContext{metaMut: m, schemaVersionManager: newSchemaVersionManager(store)}
				defer jobCtx.unlockSchemaVersion(jobCtx, job.ID)
				_, cancelErr := convertNotReorgAddIdxJob2RollbackJob(jobCtx, job, dbterror.ErrCancelledDDLJob)
				require.True(t, dbterror.ErrCancelledDDLJob.Equal(cancelErr), "%v", cancelErr)
				updated, getErr := m.GetTable(1, 2)
				require.NoError(t, getErr)
				require.Len(t, updated.Indices, 1)
				require.Equal(t, index.ID, updated.Indices[0].ID)
				if started {
					require.Equal(t, model.StateDeleteOnly, updated.Indices[0].State)
					require.Equal(t, model.JobStateRollingback, job.State)
				} else {
					// A queued job has not created an index. The public index belongs
					// to an earlier job and must not become a rollback target.
					require.Equal(t, model.StatePublic, updated.Indices[0].State)
					require.Equal(t, model.JobStateCancelled, job.State)
					require.Equal(t, model.StateNone, job.SchemaState)
				}
				return nil
			})
			require.NoError(t, err)
		}
	}
}

func TestRollingbackCreateMaterializedViewCannotCancelKeepsState(t *testing.T) {
	job := &model.Job{
		ID:          42,
		Version:     model.JobVersion2,
		Type:        model.ActionCreateMaterializedView,
		State:       model.JobStateCancelling,
		SchemaState: model.StatePublic,
	}
	job.FillArgs(&model.CreateMaterializedViewArgs{TableInfo: &model.TableInfo{ID: 1}})

	_, err := rollingbackCreateMaterializedView(nil, job)
	require.True(t, dbterror.ErrCannotCancelDDLJob.Equal(err))
	require.Equal(t, model.JobStateCancelling, job.State)
}
