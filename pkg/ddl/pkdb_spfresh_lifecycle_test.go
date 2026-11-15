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
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/lightning/backend"
	"github.com/pingcap/tidb/pkg/lightning/backend/remote"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/meta/model"
	pmodel "github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/store/mockstore"
	"github.com/pingcap/tidb/pkg/util/dbterror"
	"github.com/pingcap/tidb/pkg/util/tikvapi"
	"github.com/pingcap/tipb/go-tipb"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestSPFreshRuntimeConfigTerminalErrorClassification(t *testing.T) {
	tests := []struct {
		name       string
		statusCode int
		terminal   bool
	}{
		{name: "bad request", statusCode: http.StatusBadRequest, terminal: true},
		{name: "not found", statusCode: http.StatusNotFound, terminal: true},
		{name: "conflict", statusCode: http.StatusConflict, terminal: true},
		{name: "request timeout", statusCode: http.StatusRequestTimeout},
		{name: "too many requests", statusCode: http.StatusTooManyRequests},
		{name: "server error", statusCode: http.StatusInternalServerError},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := newSPFreshLifecycleEndpointError(
				"runtime config update",
				"http://worker",
				&remote.SPFreshControlRequestError{StatusCode: test.statusCode},
			)
			require.Equal(t, test.terminal, isSPFreshRuntimeConfigTerminalError(err))
		})
	}
}

func TestSPFreshRuntimeConfigAggregatedErrorClassification(t *testing.T) {
	terminal := newSPFreshLifecycleEndpointError(
		"runtime config update",
		"http://terminal-worker",
		&remote.SPFreshControlRequestError{StatusCode: http.StatusNotFound},
	)
	retryable := newSPFreshLifecycleEndpointError(
		"runtime config update",
		"http://retryable-worker",
		&remote.SPFreshControlRequestError{StatusCode: http.StatusServiceUnavailable},
	)
	tests := []struct {
		name           string
		errs           []error
		terminal       bool
		mayHaveApplied bool
	}{
		{name: "all terminal before CAS", errs: []error{terminal}, terminal: true},
		{name: "partial success", errs: []error{terminal, nil}, terminal: true, mayHaveApplied: true},
		{name: "ambiguous retryable failure", errs: []error{retryable}, mayHaveApplied: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			aggregated := aggregateSPFreshLifecycleErrors("runtime config update", test.errs)
			updateErr := classifySPFreshRuntimeConfigUpdateError(aggregated)
			require.Equal(t, test.mayHaveApplied, mayHaveAppliedSPFreshRuntimeConfig(updateErr))
			require.Equal(t, test.terminal, isSPFreshRuntimeConfigTerminalError(updateErr))
			var endpointErr spfreshLifecycleEndpointError
			require.ErrorAs(t, updateErr, &endpointErr)
		})
	}
}

func TestTeardownSPFreshIndexesCarriesIdentity(t *testing.T) {
	origResolve := resolveSPFreshWorkerTargets
	t.Cleanup(func() {
		resolveSPFreshWorkerTargets = origResolve
	})

	type call struct {
		workerURL  string
		clusterID  uint64
		keyspaceID uint32
		tableID    int64
		indexID    int64
	}
	var (
		mu    sync.Mutex
		calls []call
	)
	var worker *httptest.Server
	worker = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, http.MethodPost, r.Method)
		require.Equal(t, "/spfresh/teardown", r.URL.Path)
		require.Equal(t, "7", r.URL.Query().Get("cluster_id"))
		require.Equal(t, "application/x-protobuf", r.Header.Get("Content-Type"))

		body, err := io.ReadAll(r.Body)
		require.NoError(t, err)
		var req tipb.SPFreshTeardownRequest
		require.NoError(t, proto.Unmarshal(body, &req))

		mu.Lock()
		defer mu.Unlock()
		calls = append(calls, call{
			workerURL:  worker.URL,
			clusterID:  7,
			keyspaceID: req.GetKeyspaceId(),
			tableID:    req.GetTableId(),
			indexID:    req.GetIndexId(),
		})
		w.WriteHeader(http.StatusOK)
	}))
	defer worker.Close()

	resolveSPFreshWorkerTargets = func(context.Context, kv.Storage) (spfreshWorkerTargets, error) {
		return spfreshWorkerTargets{
			clusterID:   7,
			keyspaceID:  2,
			workerAddrs: []string{worker.URL},
		}, nil
	}

	err := teardownSPFreshIndexesByID(
		context.Background(),
		nil,
		101,
		[]int64{11, 13},
	)
	require.NoError(t, err)

	mu.Lock()
	defer mu.Unlock()
	require.Equal(t, []call{
		{workerURL: worker.URL, clusterID: 7, keyspaceID: 2, tableID: 101, indexID: 11},
		{workerURL: worker.URL, clusterID: 7, keyspaceID: 2, tableID: 101, indexID: 13},
	}, calls)
}

func TestTeardownSPFreshIndexesFansOutAndKeepsWorkerIndexesSerial(t *testing.T) {
	origResolve := resolveSPFreshWorkerTargets
	t.Cleanup(func() {
		resolveSPFreshWorkerTargets = origResolve
	})

	type workerState struct {
		active int32
		mu     sync.Mutex
		index  []int64
	}
	newWorker := func(state *workerState) *httptest.Server {
		return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			require.Equal(t, "/spfresh/teardown", r.URL.Path)
			if got := atomic.AddInt32(&state.active, 1); got != 1 {
				t.Errorf("concurrent teardown RPCs on one worker: active=%d", got)
			}
			defer atomic.AddInt32(&state.active, -1)

			body, err := io.ReadAll(r.Body)
			require.NoError(t, err)
			var req tipb.SPFreshTeardownRequest
			require.NoError(t, proto.Unmarshal(body, &req))
			time.Sleep(10 * time.Millisecond)

			state.mu.Lock()
			state.index = append(state.index, req.GetIndexId())
			state.mu.Unlock()
			w.WriteHeader(http.StatusOK)
		}))
	}

	var firstState, secondState workerState
	first := newWorker(&firstState)
	defer first.Close()
	second := newWorker(&secondState)
	defer second.Close()

	resolveSPFreshWorkerTargets = func(context.Context, kv.Storage) (spfreshWorkerTargets, error) {
		return spfreshWorkerTargets{
			clusterID:   7,
			keyspaceID:  2,
			workerAddrs: []string{first.URL, second.URL},
		}, nil
	}

	require.NoError(t, teardownSPFreshIndexesByID(context.Background(), nil, 101, []int64{11, 13}))
	require.Equal(t, []int64{11, 13}, firstState.index)
	require.Equal(t, []int64{11, 13}, secondState.index)
}

func TestTeardownSPFreshIndexesAggregatesPartialFailure(t *testing.T) {
	origResolve := resolveSPFreshWorkerTargets
	t.Cleanup(func() {
		resolveSPFreshWorkerTargets = origResolve
	})

	var okRequests atomic.Int32
	okWorker := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		okRequests.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer okWorker.Close()
	badWorker := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "bad worker", http.StatusBadRequest)
	}))
	defer badWorker.Close()

	resolveSPFreshWorkerTargets = func(context.Context, kv.Storage) (spfreshWorkerTargets, error) {
		return spfreshWorkerTargets{
			clusterID:   7,
			keyspaceID:  2,
			workerAddrs: []string{okWorker.URL, badWorker.URL},
		}, nil
	}

	err := teardownSPFreshIndexesByID(context.Background(), nil, 101, []int64{11})
	require.Error(t, err)
	require.Contains(t, err.Error(), "spfresh teardown failed on 1 endpoint(s)")
	require.Contains(t, err.Error(), badWorker.URL)
	require.Contains(t, err.Error(), "http 400")
	require.Contains(t, err.Error(), "bad worker")
	require.Equal(t, int32(1), okRequests.Load())
}

func TestCleanupSPFreshBuildTasksByRoutePlanUsesPersistedEndpointsAndPrefix(t *testing.T) {
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
			require.Equal(t, "77", r.URL.Query().Get("cluster_id"))
			require.Equal(t, "1-2-3-4-endpoint-", r.URL.Query().Get("task_id"))
			require.Equal(t, "1-2-3-4-endpoint-", r.URL.Query().Get("task_id_prefix"))
			mu.Lock()
			defer mu.Unlock()
			calls = append(calls, cleanupCall{
				workerURL: server.URL,
				clusterID: 77,
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

	routePlan := &backend.SPFreshBuildRoutePlan{
		KeyspaceID:      1,
		JobID:           2,
		PhysicalTableID: 3,
		IndexID:         4,
		BaseSnapshotTS:  5,
		OwnerEndpoints:  []string{first.URL, second.URL},
	}
	cleanupSPFreshBuildTasksByRoutePlan(context.Background(), first.Client(), 77, routePlan, zap.NewNop())

	mu.Lock()
	defer mu.Unlock()
	require.ElementsMatch(t, []cleanupCall{
		{workerURL: first.URL, clusterID: 77, prefix: "1-2-3-4-endpoint-"},
		{workerURL: second.URL, clusterID: 77, prefix: "1-2-3-4-endpoint-"},
	}, calls)
}

func TestBootstrapSPFreshReadyIndexesUsesSingleOwner(t *testing.T) {
	origResolve := resolveSPFreshWorkerTargets
	t.Cleanup(func() {
		resolveSPFreshWorkerTargets = origResolve
	})

	var firstRequests atomic.Int32
	first := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "/spfresh/bootstrap", r.URL.Path)
		firstRequests.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer first.Close()
	var secondRequests atomic.Int32
	second := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "/spfresh/bootstrap", r.URL.Path)
		secondRequests.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer second.Close()

	endpoints := []string{first.URL, second.URL}
	resolveSPFreshWorkerTargets = func(context.Context, kv.Storage) (spfreshWorkerTargets, error) {
		return spfreshWorkerTargets{
			clusterID:   7,
			keyspaceID:  2,
			workerAddrs: endpoints,
		}, nil
	}

	tbInfo := newSPFreshBootstrapTestTableInfo()
	indexIDs := []int64{11, 13}
	store := createMockStore(t)
	t.Cleanup(func() {
		require.NoError(t, store.Close())
	})
	expectedOwner, err := tikvapi.SelectRendezvousEndpoint(
		spfreshBootstrapTaskID(2, tbInfo.ID, indexIDs),
		endpoints,
	)
	require.NoError(t, err)

	err = bootstrapSPFreshReadyIndexes(context.Background(), store, expectedOwner, tbInfo, indexIDs, 12345)
	require.NoError(t, err)
	if expectedOwner == first.URL {
		require.Equal(t, int32(2), firstRequests.Load())
		require.Equal(t, int32(0), secondRequests.Load())
	} else {
		require.Equal(t, int32(0), firstRequests.Load())
		require.Equal(t, int32(2), secondRequests.Load())
	}
}

func TestBootstrapSPFreshReadyIndexesUsesProvidedOwnerEachCall(t *testing.T) {
	origResolve := resolveSPFreshWorkerTargets
	t.Cleanup(func() {
		resolveSPFreshWorkerTargets = origResolve
	})

	var firstRequests atomic.Int32
	first := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		firstRequests.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer first.Close()
	var secondRequests atomic.Int32
	second := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		secondRequests.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer second.Close()

	var resolveCalls atomic.Int32
	resolveSPFreshWorkerTargets = func(context.Context, kv.Storage) (spfreshWorkerTargets, error) {
		if resolveCalls.Add(1) == 1 {
			return spfreshWorkerTargets{clusterID: 7, keyspaceID: 2, workerAddrs: []string{first.URL}}, nil
		}
		return spfreshWorkerTargets{clusterID: 7, keyspaceID: 2, workerAddrs: []string{second.URL}}, nil
	}

	tbInfo := newSPFreshBootstrapTestTableInfo()
	store := createMockStore(t)
	t.Cleanup(func() {
		require.NoError(t, store.Close())
	})
	require.NoError(t, bootstrapSPFreshReadyIndexes(context.Background(), store, first.URL, tbInfo, []int64{11}, 12345))
	require.NoError(t, bootstrapSPFreshReadyIndexes(context.Background(), store, second.URL, tbInfo, []int64{11}, 12345))
	require.Equal(t, int32(1), firstRequests.Load())
	require.Equal(t, int32(1), secondRequests.Load())
	require.Equal(t, int32(2), resolveCalls.Load())
}

func TestBootstrapSPFreshIndexesForTruncatedTableBootstrapsNewOnly(t *testing.T) {
	newTblInfo := newSPFreshBootstrapTestTableInfo()
	newTblInfo.ID = 202
	newTblInfo.UpdateTS = 1
	job := &model.Job{
		ID:          88,
		RealStartTS: 556677,
		ReorgMeta:   &model.DDLReorgMeta{},
	}

	origBootstrap := bootstrapSPFreshReadyIndexes
	origCfg := config.GetGlobalConfig()
	newCfg := *origCfg
	newCfg.TiKVAPIServiceAddrs = []string{"127.0.0.1:20180"}
	config.StoreGlobalConfig(&newCfg)
	t.Cleanup(func() {
		bootstrapSPFreshReadyIndexes = origBootstrap
		config.StoreGlobalConfig(origCfg)
	})

	type step struct {
		kind           string
		workerAddr     string
		indexIDs       []int64
		baseSnapshotTS uint64
	}
	var steps []step
	bootstrapSPFreshReadyIndexes = func(_ context.Context, _ kv.Storage, workerAddr string, _ *model.TableInfo, indexIDs []int64, baseSnapshotTS uint64) error {
		steps = append(steps, step{kind: "bootstrap", workerAddr: workerAddr, indexIDs: indexIDs, baseSnapshotTS: baseSnapshotTS})
		return nil
	}

	store, err := mockstore.NewMockStore()
	require.NoError(t, err)
	defer store.Close()
	err = bootstrapSPFreshIndexesForTruncatedTable(context.Background(), store, newTblInfo, job)
	require.NoError(t, err)
	// A retry retains the original owner and snapshot even when the caller's
	// timestamp and configured endpoint change.
	job.RealStartTS++
	newCfg.TiKVAPIServiceAddrs = []string{"127.0.0.1:20181"}
	config.StoreGlobalConfig(&newCfg)
	err = bootstrapSPFreshIndexesForTruncatedTable(context.Background(), store, newTblInfo, job)
	require.NoError(t, err)
	require.Equal(t, []step{
		{kind: "bootstrap", workerAddr: "http://127.0.0.1:20180", indexIDs: []int64{11, 13}, baseSnapshotTS: 556677},
		{kind: "bootstrap", workerAddr: "http://127.0.0.1:20180", indexIDs: []int64{11, 13}, baseSnapshotTS: 556677},
	}, steps)
}

func TestCleanupTruncatedTableOldSPFreshIndexesUsesNewIndexIDsAndOldTableID(t *testing.T) {
	newTblInfo := newSPFreshBootstrapTestTableInfo()
	newTblInfo.ID = 202
	newTblInfo.UpdateTS = 1
	origResolve := resolveSPFreshWorkerTargets
	t.Cleanup(func() {
		resolveSPFreshWorkerTargets = origResolve
	})

	type step struct {
		kind    string
		tableID int64
		indexID int64
	}
	var steps []step
	worker := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, http.MethodPost, r.Method)
		require.Equal(t, "/spfresh/teardown", r.URL.Path)
		body, err := io.ReadAll(r.Body)
		require.NoError(t, err)
		var req tipb.SPFreshTeardownRequest
		require.NoError(t, proto.Unmarshal(body, &req))
		steps = append(steps, step{kind: "teardown", tableID: req.GetTableId(), indexID: req.GetIndexId()})
		w.WriteHeader(http.StatusOK)
	}))
	defer worker.Close()

	resolveSPFreshWorkerTargets = func(context.Context, kv.Storage) (spfreshWorkerTargets, error) {
		return spfreshWorkerTargets{
			clusterID:   7,
			keyspaceID:  2,
			workerAddrs: []string{worker.URL},
		}, nil
	}

	err := cleanupTruncatedTableOldSPFreshIndexes(context.Background(), nil, 101, newTblInfo)
	require.NoError(t, err)
	require.Equal(t, []step{
		{kind: "teardown", tableID: 101, indexID: 11},
		{kind: "teardown", tableID: 101, indexID: 13},
	}, steps)
}

func TestOnTruncateTableCleanupOldSPFreshIndexesFinishesAfterSchemaBarrier(t *testing.T) {
	store := createMockStore(t)
	t.Cleanup(func() {
		require.NoError(t, store.Close())
	})

	dbInfo := &model.DBInfo{ID: 1, Name: pmodel.NewCIStr("test_spfresh_truncate_cleanup")}
	newTblInfo := newSPFreshBootstrapTestTableInfo()
	newTblInfo.ID = 202
	require.NoError(t, kv.RunInNewTxn(context.Background(), store, false, func(_ context.Context, txn kv.Transaction) error {
		metaMut := meta.NewMutator(txn)
		require.NoError(t, metaMut.CreateDatabase(dbInfo))
		require.NoError(t, metaMut.CreateTableOrView(dbInfo.ID, newTblInfo))
		return nil
	}))

	origResolve := resolveSPFreshWorkerTargets
	t.Cleanup(func() {
		resolveSPFreshWorkerTargets = origResolve
	})

	type step struct {
		tableID int64
		indexID int64
	}
	var steps []step
	workerServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, http.MethodPost, r.Method)
		require.Equal(t, "/spfresh/teardown", r.URL.Path)
		body, err := io.ReadAll(r.Body)
		require.NoError(t, err)
		var req tipb.SPFreshTeardownRequest
		require.NoError(t, proto.Unmarshal(body, &req))
		steps = append(steps, step{tableID: req.GetTableId(), indexID: req.GetIndexId()})
		w.WriteHeader(http.StatusOK)
	}))
	defer workerServer.Close()

	resolveSPFreshWorkerTargets = func(context.Context, kv.Storage) (spfreshWorkerTargets, error) {
		return spfreshWorkerTargets{
			clusterID:   7,
			keyspaceID:  2,
			workerAddrs: []string{workerServer.URL},
		}, nil
	}

	job := &model.Job{
		Version:     model.JobVersion2,
		ID:          88,
		SchemaID:    dbInfo.ID,
		TableID:     101,
		Type:        model.ActionTruncateTable,
		State:       model.JobStateRunning,
		SchemaState: model.StateDeleteReorganization,
		BinlogInfo: &model.HistoryInfo{
			SchemaVersion: 123,
			TableInfo:     newTblInfo,
		},
	}
	args := &model.TruncateTableArgs{
		NewTableID:      newTblInfo.ID,
		OldPartitionIDs: []int64{1011, 1013},
	}

	require.NoError(t, kv.RunInNewTxn(context.Background(), store, false, func(_ context.Context, txn kv.Transaction) error {
		jobCtx := &jobContext{
			metaMut: meta.NewMutator(txn),
			store:   store,
			stepCtx: context.Background(),
		}
		_, err := (&worker{}).onTruncateTableCleanupOldSPFreshIndexes(jobCtx, job, args)
		return err
	}))

	require.Equal(t, []step{
		{tableID: 101, indexID: 11},
		{tableID: 101, indexID: 13},
	}, steps)
	require.Equal(t, model.JobStateDone, job.State)
	require.Equal(t, model.StatePublic, job.SchemaState)
	require.Equal(t, int64(123), job.BinlogInfo.SchemaVersion)
	require.Equal(t, newTblInfo.ID, job.BinlogInfo.TableInfo.ID)
	finishedArgs, err := model.GetFinishedTruncateTableArgs(job)
	require.NoError(t, err)
	require.Equal(t, []int64{1011, 1013}, finishedArgs.OldPartitionIDs)
}

func TestTeardownSPFreshIndexesForTableUsesSPFreshIndexIDs(t *testing.T) {
	origResolve := resolveSPFreshWorkerTargets
	t.Cleanup(func() {
		resolveSPFreshWorkerTargets = origResolve
	})

	type call struct {
		tableID int64
		indexID int64
	}
	var calls []call
	worker := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, http.MethodPost, r.Method)
		require.Equal(t, "/spfresh/teardown", r.URL.Path)
		body, err := io.ReadAll(r.Body)
		require.NoError(t, err)
		var req tipb.SPFreshTeardownRequest
		require.NoError(t, proto.Unmarshal(body, &req))
		calls = append(calls, call{
			tableID: req.GetTableId(),
			indexID: req.GetIndexId(),
		})
		w.WriteHeader(http.StatusOK)
	}))
	defer worker.Close()

	resolveSPFreshWorkerTargets = func(context.Context, kv.Storage) (spfreshWorkerTargets, error) {
		return spfreshWorkerTargets{
			clusterID:   7,
			keyspaceID:  2,
			workerAddrs: []string{worker.URL},
		}, nil
	}

	tbInfo := newSPFreshBootstrapTestTableInfo()
	require.NoError(t, teardownSPFreshIndexesForTable(context.Background(), nil, tbInfo))
	require.Equal(t, []call{
		{tableID: 101, indexID: 11},
		{tableID: 101, indexID: 13},
	}, calls)
}

func TestRejectRecoverSPFreshVectorIndexes(t *testing.T) {
	tbInfo := newSPFreshBootstrapTestTableInfo()

	err := rejectRecoverSPFreshVectorIndexes(tbInfo)
	require.Error(t, err)
	require.True(t, dbterror.ErrUnsupportedIndexType.Equal(err))
	require.Contains(t, err.Error(), "recover/flashback")

	withoutSPFresh := tbInfo.Clone()
	withoutSPFresh.Indices = withoutSPFresh.Indices[1:2]
	require.NoError(t, rejectRecoverSPFreshVectorIndexes(nil, withoutSPFresh))
}

func TestRejectRecoverSPFreshVectorIndexesFromRecoverInfos(t *testing.T) {
	err := rejectRecoverSPFreshVectorIndexesFromRecoverInfos([]*model.RecoverTableInfo{
		nil,
		{TableInfo: &model.TableInfo{}},
		{TableInfo: newSPFreshBootstrapTestTableInfo()},
	})
	require.Error(t, err)
	require.True(t, dbterror.ErrUnsupportedIndexType.Equal(err))
}
