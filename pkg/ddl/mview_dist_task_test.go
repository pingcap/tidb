// Copyright 2019 PingCAP, Inc.
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
	"encoding/json"
	goerrors "errors"
	"testing"

	"github.com/pingcap/tidb/pkg/ddl/session"
	"github.com/pingcap/tidb/pkg/dxf/framework/proto"
	"github.com/pingcap/tidb/pkg/dxf/framework/storage"
	"github.com/pingcap/tidb/pkg/dxf/importinto/taskinfo"
	"github.com/pingcap/tidb/pkg/executor/importer"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/stretchr/testify/require"
)

func TestMViewBuildReadTS(t *testing.T) {
	data, err := json.Marshal(&taskinfo.TaskMeta{
		JobID: 42,
		Plan:  importer.Plan{QuerySource: &importer.QuerySource{ReadTS: 1234}},
	})
	require.NoError(t, err)
	readTS, err := mviewBuildReadTS(data)
	require.NoError(t, err)
	require.EqualValues(t, 1234, readTS)

	for _, meta := range []*taskinfo.TaskMeta{
		{Plan: importer.Plan{QuerySource: &importer.QuerySource{ReadTS: 1234}}},
		{JobID: 42},
		{JobID: 42, Plan: importer.Plan{QuerySource: &importer.QuerySource{}}},
	} {
		data, err := json.Marshal(meta)
		require.NoError(t, err)
		_, err = mviewBuildReadTS(data)
		require.ErrorContains(t, err, "no read timestamp")
	}
	_, err = mviewBuildReadTS([]byte("{"))
	require.Error(t, err)
}

// NewMViewBuildWorkerForTest creates only the resources needed by the MV task lifecycle.
func NewMViewBuildWorkerForTest(ctx context.Context, store kv.Storage, pool util.SessionPool) *worker {
	w := &worker{workCtx: ctx, sessPool: session.NewSessionPool(pool), ddlCtx: &ddlCtx{ctx: ctx, store: store}}
	w.reorgCtx.reorgCtxMap = make(map[int64]*reorgCtx)
	return w
}

// StopMViewBuildForTest runs the production rollback task cleanup.
func (w *worker) StopMViewBuildForTest(ctx context.Context, job *model.Job) error {
	return w.stopMViewDistTask(ctx, job)
}

// ExecuteMViewBuildForTest reconnects to a task and returns its build snapshot.
func (w *worker) ExecuteMViewBuildForTest(ctx context.Context, job *model.Job, tbl *model.TableInfo) (uint64, error) {
	rc := w.newReorgCtx(job.ID, 0)
	defer w.removeReorgCtx(job.ID)
	err := w.executeMViewDistTask(ctx, job, tbl)
	return rc.getSnapshotVer(), err
}

// SubmitMViewBuildForTest exercises submission with an already prepared task.
func (w *worker) SubmitMViewBuildForTest(ctx context.Context, mgr *storage.TaskManager, key string, spec *proto.Task) (*proto.Task, error) {
	return w.submitMViewDistTask(ctx, mgr, key, spec)
}

// IsRetryableMViewBuildErrorForTest checks the classification used by the DDL boundary.
func IsRetryableMViewBuildErrorForTest(err error) bool {
	var retryable *retryableMViewBuildError
	return goerrors.As(err, &retryable)
}

func TestMViewBuildErrorState(t *testing.T) {
	cause := goerrors.New("build operation failed")
	for _, tc := range []struct {
		name  string
		err   error
		state model.JobState
	}{
		{"retryable infrastructure failure", &retryableMViewBuildError{cause}, model.JobStateRunning},
		{"preparation or terminal failure", cause, model.JobStateRollingback},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			w := &worker{ddlCtx: &ddlCtx{ctx: ctx, reorgCtx: reorgContexts{reorgCtxMap: make(map[int64]*reorgCtx)}}}
			job := &model.Job{ID: 1, Version: model.JobVersion2, Type: model.ActionCreateMaterializedView,
				State: model.JobStateRunning, SchemaState: model.StateWriteReorganization}
			job.FillArgs(&model.CreateMaterializedViewArgs{TableInfo: &model.TableInfo{ID: 2,
				MaterializedView: &model.MaterializedViewInfo{BaseTableIDs: []int64{3}}}})
			// Deliver the completed worker result through the real DDL state handler.
			w.newReorgCtx(job.ID, 0).doneCh <- reorgFnResult{err: tc.err}
			_, err := w.onCreateMaterializedView(&jobContext{stepCtx: ctx, oldDDLCtx: w.ddlCtx}, job)
			require.ErrorIs(t, err, cause)
			require.Equal(t, tc.state, job.State)
		})
	}
}
