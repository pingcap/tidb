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

package export

import (
	"context"
	"errors"
	"testing"

	"github.com/pingcap/kvproto/pkg/keyspacepb"
	"github.com/pingcap/tidb/pkg/config/kerneltype"
	"github.com/pingcap/tidb/pkg/dxf/framework/proto"
	"github.com/pingcap/tidb/pkg/keyspace"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	drivererr "github.com/pingcap/tidb/pkg/store/driver/error"
	"github.com/pingcap/tidb/pkg/store/mockstore"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestGetNextStep(t *testing.T) {
	s := &exportScheduler{}
	// StepInit -> Dump -> Schema -> Done.
	require.Equal(t, proto.ExportStepDump, s.GetNextStep(&proto.TaskBase{Step: proto.StepInit}))
	require.Equal(t, proto.ExportStepSchema, s.GetNextStep(&proto.TaskBase{Step: proto.ExportStepDump}))
	require.Equal(t, proto.StepDone, s.GetNextStep(&proto.TaskBase{Step: proto.ExportStepSchema}))
	require.Equal(t, proto.StepDone, s.GetNextStep(&proto.TaskBase{Step: proto.StepDone}))
}

func TestTaskKey(t *testing.T) {
	want := "export/100/42"
	if kerneltype.IsNextGen() {
		want = keyspace.GetKeyspaceNameBySettings() + "/" + want
	}
	require.Equal(t, want, TaskKey(100, 42))
	wantExplicit := "export/100/42"
	if kerneltype.IsNextGen() {
		wantExplicit = "ks1/" + wantExplicit
		require.NotEqual(t, TaskKeyInKeyspace("ks1", 100, 42), TaskKeyInKeyspace("ks2", 100, 42))
	}
	require.Equal(t, wantExplicit, TaskKeyInKeyspace("ks1", 100, 42))
}

func TestSnapshotTableInfosUsesTaskStore(t *testing.T) {
	store, err := mockstore.NewMockStore(mockstore.WithCurrentKeyspaceMeta(&keyspacepb.KeyspaceMeta{
		Keyspace: &keyspacepb.KeyspaceMeta_Id{Id: 1},
		Name:     "user-keyspace",
	}))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	require.Equal(t, "user-keyspace", store.GetKeyspace())
	dbInfo := &model.DBInfo{ID: 1, Name: ast.NewCIStr("db"), State: model.StatePublic}
	tableInfo := &model.TableInfo{ID: 2, DBID: dbInfo.ID, Name: ast.NewCIStr("tbl"), State: model.StatePublic}
	nonPublicTable := &model.TableInfo{ID: 3, DBID: dbInfo.ID, Name: ast.NewCIStr("hidden"), State: model.StateWriteOnly}
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnDDL)
	require.NoError(t, kv.RunInNewTxn(ctx, store, true, func(_ context.Context, txn kv.Transaction) error {
		mutator := meta.NewMutator(txn)
		if err := mutator.CreateDatabase(dbInfo); err != nil {
			return err
		}
		if err := mutator.CreateTableOrView(dbInfo.ID, tableInfo); err != nil {
			return err
		}
		return mutator.CreateTableOrView(dbInfo.ID, nonPublicTable)
	}))
	version, err := store.CurrentVersion(kv.GlobalTxnScope)
	require.NoError(t, err)
	s := &exportScheduler{
		store: store,
		taskMeta: &TaskMeta{
			SnapshotTS: version.Ver,
			DBs:        []DBSpec{{DBID: dbInfo.ID, DBName: "stale-name", TableIDs: []int64{tableInfo.ID}}},
		},
	}

	got, err := snapshotTableInfos(s.store, s.taskMeta)
	require.NoError(t, err)
	require.Equal(t, tableInfo, got[tableInfo.ID])
	require.Equal(t, dbInfo.Name.O, s.taskMeta.DBs[0].DBName)

	s.taskMeta.DBs[0].TableIDs = []int64{nonPublicTable.ID}
	_, err = snapshotTableInfos(s.store, s.taskMeta)
	require.ErrorContains(t, err, "is not public")
}

func TestIsRetryableErr(t *testing.T) {
	s := &exportScheduler{}
	require.False(t, s.IsRetryableErr(nil))
	require.True(t, s.IsRetryableErr(drivererr.ErrRegionUnavailable))
	require.True(t, s.IsRetryableErr(status.Error(codes.Unavailable, "temporarily unavailable")))
	require.False(t, s.IsRetryableErr(status.Error(codes.PermissionDenied, "invalid credentials")))
	require.False(t, s.IsRetryableErr(status.Error(codes.NotFound, "missing object")))
	require.False(t, s.IsRetryableErr(status.Error(codes.Unknown, "unknown planning failure")))
	require.False(t, s.IsRetryableErr(errors.New("permanent planning error")))
}
