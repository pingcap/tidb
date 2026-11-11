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

package importinto

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/pingcap/tidb/pkg/config/kerneltype"
	"github.com/pingcap/tidb/pkg/dxf/framework/mock"
	"github.com/pingcap/tidb/pkg/dxf/framework/proto"
	"github.com/pingcap/tidb/pkg/dxf/framework/storage"
	"github.com/pingcap/tidb/pkg/executor/importer"
	importcore "github.com/pingcap/tidb/pkg/importinto"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/store/mockstore"
	utilmock "github.com/pingcap/tidb/pkg/util/mock"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

func TestMViewBuildDoesNotAccessImportJob(t *testing.T) {
	if kerneltype.IsClassic() {
		t.Skip("MV distributed builds require NextGen")
	}
	ctx := context.Background()
	store, err := mockstore.NewMockStore()
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	ctrl := gomock.NewController(t)
	manager := mock.NewMockTaskManager(ctrl)
	meta := importcore.TaskMeta{
		DDLJobID: 42,
		Plan: importer.Plan{
			DBID:                  1,
			TableInfo:             &model.TableInfo{ID: 42},
			CloudStorageURI:       t.TempDir(),
			QuerySource:           &importer.QuerySource{ReadTS: 1234},
			ThreadCnt:             1,
			MaxNodeCnt:            1,
			DisableTiKVImportMode: true,
		},
	}
	data, err := json.Marshal(meta)
	require.NoError(t, err)
	task := &proto.Task{
		TaskBase: proto.TaskBase{ID: 42, Type: proto.MViewBuild, Step: proto.StepInit,
			Keyspace: store.GetKeyspace(), RequiredSlots: 1, MaxNodeCount: 1},
		Meta: data,
	}
	sch := NewMViewBuildScheduler(ctx, task, newSchedulerParamForTest(t, manager, store, nil)).(*mviewBuildScheduler)
	require.NoError(t, sch.Init())
	t.Cleanup(sch.Close)
	require.Same(t, sch, sch.BaseScheduler.Extension)
	require.ErrorContains(t, sch.OnPrepare(ctx, nil, task), "prepared by the DDL owner")
	// No task-manager access is expected while planning the query source. Any
	// attempt to check, start or transition an IMPORT job fails the strict mock.
	metas, err := sch.OnNextSubtasksBatch(ctx, nil, task, nil, proto.ImportStepQuery)
	require.NoError(t, err)
	require.Len(t, metas, 1)
	// Stats failures are warning-only. The mock SQL executor returns an error;
	// calling IMPORT FinishJob afterward would therefore fail this test.
	manager.EXPECT().WithNewTxn(gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, fn func(sessionctx.Context) error) error {
			se := utilmock.NewContext()
			se.Store = store
			defer se.RollbackTxn(ctx)
			return fn(se)
		})
	require.NoError(t, sch.OnDone(ctx, nil, task))
	for _, err := range []error{nil, storage.ErrTaskNotFound} {
		task.State = proto.TaskStateReverting
		task.Error = err
		require.NoError(t, sch.OnDone(ctx, nil, task))
	}
}

func TestImportQueryRequiresRunner(t *testing.T) {
	// A missing constructor dependency must fail before accessing the runtime.
	step := &importStepExecutor{}
	err := step.readQuery(context.Background(), make(chan importer.QueryChunk))
	require.ErrorContains(t, err, "query runner is not configured")
}
