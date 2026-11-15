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
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/disttask/framework/proto"
	"github.com/pingcap/tidb/pkg/disttask/framework/scheduler"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/lightning/backend"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/meta/model"
	pmodel "github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	fieldtypes "github.com/pingcap/tidb/pkg/parser/types"
	"github.com/stretchr/testify/require"
)

func TestBuildSPFreshBuildRoutePlanPersistsBuildLevelRouting(t *testing.T) {
	withSPFreshRemoteBackendConfig(t, "127.0.0.1:1234", "http://worker-1:19000")
	store := createMockStore(t)
	t.Cleanup(func() {
		require.NoError(t, store.Close())
	})
	tblInfo := newSPFreshBackfillTableInfo()
	job := newSPFreshBackfillJob(t, tblInfo.ID)
	job.RealStartTS = 12345

	routePlan, err := buildSPFreshBuildRoutePlan(store, job, tblInfo, []*model.IndexInfo{tblInfo.Indices[0]})
	require.NoError(t, err)
	keyspaceID := uint32(store.GetCodec().GetKeyspaceID())
	require.Equal(t, fmt.Sprintf("%d-%d-%d-%d", keyspaceID, job.ID, tblInfo.ID, tblInfo.Indices[0].ID), routePlan.IndexBuildID())
	require.Equal(t, keyspaceID, routePlan.KeyspaceID)
	require.Equal(t, job.ID, routePlan.JobID)
	require.Equal(t, tblInfo.ID, routePlan.PhysicalTableID)
	require.Equal(t, tblInfo.Indices[0].ID, routePlan.IndexID)
	require.Equal(t, job.RealStartTS, routePlan.BaseSnapshotTS)
	require.Equal(t, []string{"http://127.0.0.1:1234", "http://worker-1:19000"}, routePlan.OwnerEndpoints)
}

func TestCalculateRegionBatch(t *testing.T) {
	for _, tc := range []struct {
		name          string
		regions       int
		nodes         int
		useLocalDisk  bool
		wantBatchSize int
	}{
		{name: "remote", regions: 100, nodes: 8, wantBatchSize: 13},
		{name: "remoteMoreNodes", regions: 2, nodes: 8, wantBatchSize: 1},
		{name: "remoteOnePerNode", regions: 8, nodes: 8, wantBatchSize: 1},
		{name: "localSmallTable", regions: 100, nodes: 8, useLocalDisk: true, wantBatchSize: 100},
		{name: "localMoreNodes", regions: 2, nodes: 8, useLocalDisk: true, wantBatchSize: 2},
		{name: "localCappedNodes", regions: 1000, nodes: 8, useLocalDisk: true, wantBatchSize: 334},
		{name: "localTwoNodes", regions: 1000, nodes: 2, useLocalDisk: true, wantBatchSize: 500},
		{name: "localMinimumBatch", regions: 200, nodes: 3, useLocalDisk: true, wantBatchSize: 100},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.wantBatchSize, calculateRegionBatch(tc.regions, tc.nodes, tc.useLocalDisk))
		})
	}
}

func TestSPFreshRoutePlanDrivesRemoteSortAfterConfigCleared(t *testing.T) {
	restore := config.RestoreFunc()
	t.Cleanup(restore)
	config.UpdateGlobal(func(conf *config.Config) {
		conf.TiKVAPIServiceAddrs = nil
	})

	tblInfo := newSPFreshBackfillTableInfo()
	job := newSPFreshBackfillJob(t, tblInfo.ID)
	job.RealStartTS = 12345
	indexInfo := tblInfo.Indices[0]
	routePlan := &backend.SPFreshBuildRoutePlan{
		KeyspaceID:      1,
		JobID:           job.ID,
		PhysicalTableID: tblInfo.ID,
		IndexID:         indexInfo.ID,
		BaseSnapshotTS:  job.RealStartTS,
		OwnerEndpoints:  []string{"http://worker-0:19000"},
	}
	taskMeta := &BackfillTaskMeta{
		Job:                   *job,
		SPFreshBuildRoutePlan: routePlan,
	}

	taskMetaBytes, err := json.Marshal(taskMeta)
	require.NoError(t, err)
	sch := &LitBackfillScheduler{
		BaseScheduler: scheduler.NewBaseScheduler(context.Background(), &proto.Task{
			TaskBase: proto.TaskBase{
				ID:   1,
				Type: proto.Backfill,
			},
			Meta: taskMetaBytes,
		}, scheduler.Param{}),
	}
	require.NoError(t, sch.Init())
	require.Equal(t, proto.BackfillStepWriteAndIngest, sch.GetNextStep(&proto.TaskBase{Step: proto.BackfillStepReadIndex}))
}

func TestSPFreshRoutePlanRejectsCloudStorageURI(t *testing.T) {
	tblInfo := newSPFreshBackfillTableInfo()
	job := newSPFreshBackfillJob(t, tblInfo.ID)
	job.RealStartTS = 12345
	indexInfo := tblInfo.Indices[0]
	routePlan := &backend.SPFreshBuildRoutePlan{
		KeyspaceID:      1,
		JobID:           job.ID,
		PhysicalTableID: tblInfo.ID,
		IndexID:         indexInfo.ID,
		BaseSnapshotTS:  job.RealStartTS,
		OwnerEndpoints:  []string{"http://worker-0:19000"},
	}
	taskMeta := &BackfillTaskMeta{
		Job:                   *job,
		CloudStorageURI:       "s3://global-sort-bucket/tmp",
		SPFreshBuildRoutePlan: routePlan,
	}

	taskMetaBytes, err := json.Marshal(taskMeta)
	require.NoError(t, err)
	sch := &LitBackfillScheduler{
		BaseScheduler: scheduler.NewBaseScheduler(context.Background(), &proto.Task{
			TaskBase: proto.TaskBase{
				ID:   1,
				Type: proto.Backfill,
			},
			Meta: taskMetaBytes,
		}, scheduler.Param{}),
	}
	require.ErrorContains(t, sch.Init(), "cannot use cloud storage and SPFresh build routing together")

	exec := &backfillDistExecutor{}
	require.ErrorContains(t, exec.refreshTaskMeta(&proto.Task{Meta: taskMetaBytes}),
		"cannot use cloud storage and SPFresh build routing together")
}

func TestBackfillExecutorRejectsInvalidSPFreshRoutePlan(t *testing.T) {
	taskMetaBytes, err := json.Marshal(&BackfillTaskMeta{
		SPFreshBuildRoutePlan: &backend.SPFreshBuildRoutePlan{},
	})
	require.NoError(t, err)

	exec := &backfillDistExecutor{}
	require.ErrorContains(t, exec.refreshTaskMeta(&proto.Task{Meta: taskMetaBytes}),
		"invalid SPFresh build route plan")
}

func TestBackfillSchedulerInitializesSPFreshBuildBeforePlanningReadSubtasks(t *testing.T) {
	called := false
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
		http.Error(w, "stop after initialization", http.StatusBadRequest)
	}))
	t.Cleanup(server.Close)

	store := createMockStore(t)
	t.Cleanup(func() {
		require.NoError(t, store.Close())
	})
	tblInfo := newSPFreshBackfillTableInfo()
	job := newSPFreshBackfillJob(t, tblInfo.ID)
	require.NoError(t, kv.RunInNewTxn(context.Background(), store, false, func(_ context.Context, txn kv.Transaction) error {
		metaMut := meta.NewMutator(txn)
		if err := metaMut.CreateDatabase(&model.DBInfo{ID: job.SchemaID, Name: pmodel.NewCIStr("test")}); err != nil {
			return err
		}
		return metaMut.CreateTableOrView(job.SchemaID, tblInfo)
	}))
	routePlan := &backend.SPFreshBuildRoutePlan{
		KeyspaceID:      uint32(store.GetCodec().GetKeyspaceID()),
		JobID:           job.ID,
		PhysicalTableID: tblInfo.ID,
		IndexID:         tblInfo.Indices[0].ID,
		BaseSnapshotTS:  12345,
		OwnerEndpoints:  []string{server.URL},
	}
	taskMetaBytes, err := json.Marshal(&BackfillTaskMeta{
		Job:                   *job,
		SPFreshBuildRoutePlan: routePlan,
	})
	require.NoError(t, err)

	sch := &LitBackfillScheduler{d: &ddl{ddlCtx: &ddlCtx{store: store}}}
	_, err = sch.OnNextSubtasksBatch(
		context.Background(),
		nil,
		&proto.Task{TaskBase: proto.TaskBase{Type: proto.Backfill}, Meta: taskMetaBytes},
		nil,
		proto.BackfillStepReadIndex,
	)
	require.ErrorContains(t, err, "stop after initialization")
	require.True(t, called)
}

func TestBackfillSchedulerOnDoneCleansSPFreshBuildShardsAfterTaskDone(t *testing.T) {
	store := createMockStore(t)
	t.Cleanup(func() {
		require.NoError(t, store.Close())
	})
	clusterID, err := resolveSPFreshClusterID(context.Background(), store)
	require.NoError(t, err)

	type cleanupCall struct {
		workerURL string
		clusterID uint64
		prefix    string
	}
	var (
		mu    sync.Mutex
		calls []cleanupCall
	)
	newWorker := func() *httptest.Server {
		var server *httptest.Server
		server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			require.Equal(t, http.MethodDelete, r.Method)
			require.Equal(t, "/spfresh/build", r.URL.Path)
			require.Equal(t, "1-2-3-4-endpoint-", r.URL.Query().Get("task_id"))
			require.Equal(t, "1-2-3-4-endpoint-", r.URL.Query().Get("task_id_prefix"))
			mu.Lock()
			defer mu.Unlock()
			calls = append(calls, cleanupCall{
				workerURL: server.URL,
				clusterID: clusterID,
				prefix:    r.URL.Query().Get("task_id_prefix"),
			})
			w.WriteHeader(http.StatusOK)
		}))
		return server
	}
	first := newWorker()
	defer first.Close()
	second := newWorker()
	defer second.Close()

	taskMetaBytes, err := json.Marshal(&BackfillTaskMeta{
		Job: model.Job{ID: 2},
		SPFreshBuildRoutePlan: &backend.SPFreshBuildRoutePlan{
			KeyspaceID:      1,
			JobID:           2,
			PhysicalTableID: 3,
			IndexID:         4,
			BaseSnapshotTS:  5,
			OwnerEndpoints:  []string{first.URL, second.URL},
		},
	})
	require.NoError(t, err)

	sch := &LitBackfillScheduler{d: &ddl{ddlCtx: &ddlCtx{store: store}}}
	err = sch.OnDone(context.Background(), nil, &proto.Task{
		TaskBase: proto.TaskBase{ID: 99, Type: proto.Backfill},
		Meta:     taskMetaBytes,
	})
	require.NoError(t, err)

	mu.Lock()
	defer mu.Unlock()
	require.ElementsMatch(t, []cleanupCall{
		{workerURL: first.URL, clusterID: clusterID, prefix: "1-2-3-4-endpoint-"},
		{workerURL: second.URL, clusterID: clusterID, prefix: "1-2-3-4-endpoint-"},
	}, calls)
}

func withSPFreshRemoteBackendConfig(t *testing.T, addrs ...string) {
	t.Helper()
	origCfg := config.GetGlobalConfig()
	newCfg := *origCfg
	newCfg.TiKVAPIServiceAddrs = append([]string(nil), addrs...)
	config.StoreGlobalConfig(&newCfg)
	t.Cleanup(func() {
		config.StoreGlobalConfig(origCfg)
	})
}

func newSPFreshBackfillJob(t *testing.T, tableID int64) *model.Job {
	defaultSQLMode, err := mysql.GetSQLMode(mysql.DefaultSQLMode)
	require.NoError(t, err)
	return &model.Job{
		ID:       time.Now().UnixNano(),
		SchemaID: 1,
		TableID:  tableID,
		ReorgMeta: &model.DDLReorgMeta{
			SQLMode:     defaultSQLMode,
			Location:    &model.TimeZoneLocation{Name: time.UTC.String(), Offset: 0},
			ReorgTp:     model.ReorgTypeIngest,
			IsDistReorg: true,
		},
	}
}

func newPlainBackfillTableInfo() *model.TableInfo {
	return &model.TableInfo{
		ID:    101,
		Name:  pmodel.NewCIStr("t_plain"),
		State: model.StatePublic,
		Columns: []*model.ColumnInfo{
			{
				ID:        1,
				Name:      pmodel.NewCIStr("id"),
				Offset:    0,
				State:     model.StatePublic,
				FieldType: *fieldtypes.NewFieldType(mysql.TypeLonglong),
			},
		},
		Indices: []*model.IndexInfo{
			{
				ID:    21,
				Name:  pmodel.NewCIStr("idx_id"),
				Table: pmodel.NewCIStr("t_plain"),
				State: model.StatePublic,
				Tp:    pmodel.IndexTypeBtree,
				Columns: []*model.IndexColumn{
					{Name: pmodel.NewCIStr("id"), Offset: 0, Length: fieldtypes.UnspecifiedLength},
				},
			},
		},
	}
}

func newSPFreshBackfillTableInfo() *model.TableInfo {
	vecType := fieldtypes.NewFieldType(mysql.TypeTiDBVectorFloat32)
	vecType.SetFlen(3)
	runtimeConfig := model.DefaultSPFreshVectorRuntimeConfig()
	return &model.TableInfo{
		ID:    102,
		Name:  pmodel.NewCIStr("t_spfresh"),
		State: model.StatePublic,
		Columns: []*model.ColumnInfo{
			{
				ID:        1,
				Name:      pmodel.NewCIStr("id"),
				Offset:    0,
				State:     model.StatePublic,
				FieldType: *fieldtypes.NewFieldType(mysql.TypeLonglong),
			},
			{
				ID:        2,
				Name:      pmodel.NewCIStr("vec"),
				Offset:    1,
				State:     model.StatePublic,
				FieldType: *vecType,
			},
		},
		Indices: []*model.IndexInfo{
			{
				ID:    11,
				Name:  pmodel.NewCIStr("idx_vec"),
				Table: pmodel.NewCIStr("t_spfresh"),
				State: model.StatePublic,
				Tp:    pmodel.IndexTypeSPFresh,
				Columns: []*model.IndexColumn{
					{Name: pmodel.NewCIStr("vec"), Offset: 1, Length: fieldtypes.UnspecifiedLength},
				},
				VectorInfo: &model.VectorIndexInfo{
					Kind:           model.VectorIndexKindSPFresh,
					Dimension:      3,
					DistanceMetric: model.DistanceMetricCosine,
					RuntimeConfig:  &runtimeConfig,
				},
			},
		},
	}
}
