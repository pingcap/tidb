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

package ddl

import (
	"context"
	"encoding/json"
	goerrors "errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/kvproto/pkg/keyspacepb"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/ddl/ingest"
	"github.com/pingcap/tidb/pkg/disttask/framework/proto"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/lightning/backend"
	"github.com/pingcap/tidb/pkg/lightning/backend/local"
	"github.com/pingcap/tidb/pkg/lightning/backend/remote"
	"github.com/pingcap/tidb/pkg/meta/model"
	pmodel "github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/resourcemanager/pool/workerpool"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/table/tables"
	"github.com/pingcap/tidb/pkg/util/tikvapi"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/tikv"
)

type keyspaceTestStorage interface {
	kv.Storage
	tikv.Storage
}

type keyspaceTestStore struct {
	keyspaceTestStorage
	codec tikv.Codec
}

func (s *keyspaceTestStore) GetCodec() tikv.Codec {
	return s.codec
}

type recordingBackendCtx struct {
	registerCalls  int
	ingestCalls    int
	closeCalls     int
	lastUnregister ingest.UnregisterOpt
	flushCalls     int
	watermarks     []bool
	flushFlushed   *bool
	flushImported  bool
	advanceErr     error
	finishErr      error
}

func (b *recordingBackendCtx) Register(_ []int64, _ []bool, _ table.Table) ([]ingest.Engine, error) {
	b.registerCalls++
	return nil, nil
}

func (b *recordingBackendCtx) FinishAndUnregisterEngines(opt ingest.UnregisterOpt) error {
	b.lastUnregister = opt
	return b.finishErr
}

func (b *recordingBackendCtx) IngestIfQuotaExceeded(context.Context, int, int) error {
	return nil
}

func (b *recordingBackendCtx) Ingest(context.Context) error {
	b.ingestCalls++
	return nil
}

func (b *recordingBackendCtx) Flush(_ context.Context) (bool, bool, error) {
	b.flushCalls++
	flushed := true
	if b.flushFlushed != nil {
		flushed = *b.flushFlushed
	}
	return flushed, b.flushImported, nil
}

func (*recordingBackendCtx) NextStartKey() kv.Key { return nil }

func (*recordingBackendCtx) TotalKeyCount() int { return 0 }

func (*recordingBackendCtx) AddChunk(int, kv.Key) {}

func (*recordingBackendCtx) UpdateChunk(int, int, bool) {}

func (*recordingBackendCtx) FinishChunk(int, int) {}

func (b *recordingBackendCtx) AdvanceWatermark(imported bool) error {
	b.watermarks = append(b.watermarks, imported)
	return b.advanceErr
}

func (*recordingBackendCtx) GetImportTS() uint64 { return 0 }

func (*recordingBackendCtx) GetLocalBackend() *local.Backend { return nil }

func (*recordingBackendCtx) CollectRemoteDuplicateRows(int64, table.Table) error { return nil }

func (*recordingBackendCtx) GetDiskUsage() uint64 { return 0 }

func (b *recordingBackendCtx) Close() { b.closeCalls++ }

func TestIndexWriteResultSinkAdvancesWatermarkAfterFlushOnly(t *testing.T) {
	backendCtx := &recordingBackendCtx{}
	wctx := workerpool.NewContext(context.Background())
	defer wctx.Cancel()

	sink := newIndexWriteResultSink(wctx, backendCtx, nil, nil, &EmptyRowCntListener{})
	require.NoError(t, sink.flush())
	require.Equal(t, 1, backendCtx.flushCalls)
	require.Equal(t, []bool{false}, backendCtx.watermarks)
}

func TestIndexWriteResultSinkAdvancesWatermarkAfterFlush(t *testing.T) {
	backendCtx := &recordingBackendCtx{flushImported: true}
	wctx := workerpool.NewContext(context.Background())
	defer wctx.Cancel()

	sink := newIndexWriteResultSink(wctx, backendCtx, nil, nil, &EmptyRowCntListener{})
	require.NoError(t, sink.flush())
	require.Equal(t, 1, backendCtx.flushCalls)
	require.Equal(t, []bool{true}, backendCtx.watermarks)
}

func TestIndexWriteResultSinkSkipsWatermarkWhenFlushNoops(t *testing.T) {
	flushed := false
	backendCtx := &recordingBackendCtx{flushFlushed: &flushed}
	wctx := workerpool.NewContext(context.Background())
	defer wctx.Cancel()

	sink := newIndexWriteResultSink(wctx, backendCtx, nil, nil, &EmptyRowCntListener{})
	require.NoError(t, sink.flush())
	require.Equal(t, 1, backendCtx.flushCalls)
	require.Empty(t, backendCtx.watermarks)
}

func TestIndexWriteResultSinkPropagatesAdvanceWatermarkError(t *testing.T) {
	backendCtx := &recordingBackendCtx{
		flushImported: true,
		advanceErr:    goerrors.New("advance watermark failed"),
	}
	wctx := workerpool.NewContext(context.Background())
	defer wctx.Cancel()

	sink := newIndexWriteResultSink(wctx, backendCtx, nil, nil, &EmptyRowCntListener{})
	err := sink.flush()
	require.ErrorContains(t, err, "advance watermark failed")
	require.Equal(t, 1, backendCtx.flushCalls)
	require.Equal(t, []bool{true}, backendCtx.watermarks)
}

func TestReadIndexRemoteSortFinishedRecordsDataState(t *testing.T) {
	sm := &BackfillSubTaskMeta{}
	meta, err := sm.Marshal()
	require.NoError(t, err)
	subtask := &proto.Subtask{
		SubtaskBase: proto.SubtaskBase{TaskID: 11},
		Meta:        meta,
	}
	backendCtx := &summaryBackendCtx{
		hasData: true,
	}
	exec := &readIndexStepExecutor{
		indexes: []*model.IndexInfo{{
			ID: 4,
			Tp: pmodel.IndexTypeSPFresh,
			VectorInfo: &model.VectorIndexInfo{
				Kind: model.VectorIndexKindSPFresh,
			},
		}},
	}

	require.NoError(t, exec.finishRemoteSortReadIndex(subtask, sm, backendCtx))
	decoded, err := decodeBackfillSubTaskMeta(context.Background(), "", subtask.Meta)
	require.NoError(t, err)
	require.NotNil(t, decoded.SPFreshBuildHasData)
	require.True(t, *decoded.SPFreshBuildHasData)
}

func TestReadIndexRemoteSortFinishFailureDoesNotRecordDataState(t *testing.T) {
	sm := &BackfillSubTaskMeta{}
	meta, err := sm.Marshal()
	require.NoError(t, err)
	subtask := &proto.Subtask{Meta: meta}
	backendCtx := &summaryBackendCtx{
		hasData: true,
	}
	backendCtx.finishErr = goerrors.New("flush failed")
	exec := &readIndexStepExecutor{
		indexes: []*model.IndexInfo{{
			ID: 4,
			Tp: pmodel.IndexTypeSPFresh,
			VectorInfo: &model.VectorIndexInfo{
				Kind: model.VectorIndexKindSPFresh,
			},
		}},
	}

	err = exec.finishRemoteSortReadIndex(subtask, sm, backendCtx)
	require.ErrorContains(t, err, "flush failed")
	require.Equal(t, meta, subtask.Meta)
	decoded, err := decodeBackfillSubTaskMeta(context.Background(), "", subtask.Meta)
	require.NoError(t, err)
	require.Nil(t, decoded.SPFreshBuildHasData)
}

type summaryBackendCtx struct {
	recordingBackendCtx
	hasData bool
}

func spfreshBuildDataState(hasData bool) *bool {
	return &hasData
}

func (b *summaryBackendCtx) FinishSPFreshBuild() (bool, error) {
	if err := b.FinishAndUnregisterEngines(ingest.OptCloseEngines); err != nil {
		return false, err
	}
	return b.hasData, nil
}

func TestRemoteIngestExecutorCoordinatesRoutedSPFreshReadResult(t *testing.T) {
	restore := config.RestoreFunc()
	t.Cleanup(restore)

	var (
		mu           sync.Mutex
		sealed       []string
		sealAttempts map[string]int
		deleted      []string
		commitBody   map[string]any
		commitCalls  int
	)
	sealAttempts = make(map[string]int)
	var server *httptest.Server
	server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.NotEmpty(t, r.URL.Query().Get("cluster_id"))
		taskID := r.URL.Query().Get("task_id")
		switch {
		case r.URL.Path == "/spfresh/build" && r.Method == http.MethodPost && r.URL.Query().Get("seal") == "true":
			var identity remote.SPFreshBuildSealRequest
			require.NoError(t, json.NewDecoder(r.Body).Decode(&identity))
			require.Equal(t, remote.SPFreshBuildSealRequest{
				BuildID:    fmt.Sprintf("%d-2-3-4", ^uint32(0)),
				KeyspaceID: ^uint32(0), TableID: 3, IndexID: 4, BaseSnapshotTS: 5,
			}, identity)

			mu.Lock()
			sealAttempts[taskID]++
			attempt := sealAttempts[taskID]
			if attempt > 1 {
				sealed = append(sealed, taskID)
			}
			mu.Unlock()
			if attempt == 1 {
				w.WriteHeader(http.StatusAccepted)
				_, _ = w.Write([]byte(`{"task_id":"` + taskID + `","status":"retrying","retry-reason":"deadlock","retry-attempt":1,"next-retry-at-ms":0}`))
				return
			}
			_, _ = w.Write([]byte(`{"task_id":"` + taskID + `","status":"sealed","finished":true}`))
		case r.URL.Path == "/spfresh/build/commit" && r.Method == http.MethodPost:
			mu.Lock()
			commitCalls++
			mu.Unlock()
			require.Empty(t, taskID)
			require.Empty(t, r.URL.Query().Get("commit"))
			require.NoError(t, json.NewDecoder(r.Body).Decode(&commitBody))
			w.WriteHeader(http.StatusNoContent)
		case r.URL.Path == "/spfresh/build" && r.Method == http.MethodDelete:
			mu.Lock()
			deleted = append(deleted, taskID)
			mu.Unlock()
		default:
			http.Error(w, "unexpected request", http.StatusBadRequest)
		}
	}))
	t.Cleanup(server.Close)
	config.UpdateGlobal(func(conf *config.Config) {
		conf.TiKVAPIServiceAddrs = []string{server.URL}
	})

	indexBuildID := fmt.Sprintf("%d-2-3-4", ^uint32(0))
	err := runSPFreshCoordinatorTestSubtask(t, server, true)
	require.NoError(t, err)

	require.Equal(t, []string{indexBuildID + "-endpoint-1"}, sealed)
	require.Equal(t, map[string]int{indexBuildID + "-endpoint-1": 2}, sealAttempts)
	require.Equal(t, 1, commitCalls)
	require.Equal(t, indexBuildID, commitBody["build_id"])
	require.Equal(t, float64(^uint32(0)), commitBody["keyspace_id"])
	require.Equal(t, float64(3), commitBody["table_id"])
	require.Equal(t, float64(4), commitBody["index_id"])
	require.NotContains(t, commitBody, "build_attempt_id")
	require.NotContains(t, commitBody, "base_snapshot_ts")
	require.Empty(t, deleted)
}

func TestRemoteIngestExecutorUsesPersistedRoutePlanEndpointsAfterConfigChange(t *testing.T) {
	restore := config.RestoreFunc()
	t.Cleanup(restore)

	var configEndpointCalls int
	configEndpoint := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		configEndpointCalls++
		http.Error(w, "current config endpoint should not be used", http.StatusInternalServerError)
	}))
	t.Cleanup(configEndpoint.Close)

	var commitCalls int
	routeEndpoint := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/spfresh/build" && r.Method == http.MethodPost && r.URL.Query().Get("seal") == "true":
			_, _ = w.Write([]byte(`{"status":"sealed"}`))
		case r.URL.Path == "/spfresh/build/commit" && r.Method == http.MethodPost:
			commitCalls++
			require.Empty(t, r.URL.Query().Get("task_id"))
			require.Empty(t, r.URL.Query().Get("commit"))
			w.WriteHeader(http.StatusNoContent)
		case r.URL.Path == "/spfresh/build" && r.Method == http.MethodDelete:
			w.WriteHeader(http.StatusOK)
		default:
			http.Error(w, "unexpected request", http.StatusBadRequest)
		}
	}))
	t.Cleanup(routeEndpoint.Close)
	config.UpdateGlobal(func(conf *config.Config) {
		conf.TiKVAPIServiceAddrs = []string{configEndpoint.URL}
	})

	store := createMockStore(t)
	t.Cleanup(func() {
		require.NoError(t, store.Close())
	})
	keyspaceID := uint32(store.GetCodec().GetKeyspaceID())
	tblInfo := newSPFreshBackfillTableInfo()
	indexID := tblInfo.Indices[0].ID
	routePlan := &backend.SPFreshBuildRoutePlan{
		KeyspaceID:      keyspaceID,
		JobID:           2,
		PhysicalTableID: tblInfo.ID,
		IndexID:         indexID,
		BaseSnapshotTS:  5,
		OwnerEndpoints:  []string{routeEndpoint.URL},
	}
	readMeta := &BackfillSubTaskMeta{SPFreshBuildHasData: spfreshBuildDataState(true)}
	metaBytes, err := readMeta.Marshal()
	require.NoError(t, err)
	exec, err := newRemoteIngestExecutor(
		tables.MockTableFromMeta(tblInfo).(table.PhysicalTable),
		store,
		routePlan,
		routeEndpoint.Client(),
	)
	require.NoError(t, err)

	require.NoError(t, exec.RunSubtask(context.Background(), &proto.Subtask{Meta: metaBytes}))
	require.Equal(t, 1, commitCalls)
	require.Zero(t, configEndpointCalls)
}

func TestRemoteIngestExecutorSealFailureBlocksCommit(t *testing.T) {
	restore := config.RestoreFunc()
	t.Cleanup(restore)

	var commitCalls int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/spfresh/build/commit" && r.Method == http.MethodPost {
			commitCalls++
		}
		http.Error(w, "seal failed", http.StatusBadRequest)
	}))
	t.Cleanup(server.Close)
	config.UpdateGlobal(func(conf *config.Config) {
		conf.TiKVAPIServiceAddrs = []string{server.URL}
	})

	err := runSPFreshCoordinatorTestSubtask(t, server, true)
	require.ErrorContains(t, err, "seal failed")
	require.Zero(t, commitCalls)
}

func TestRemoteIngestExecutorCommitFailureDoesNotCleanup(t *testing.T) {
	restore := config.RestoreFunc()
	t.Cleanup(restore)

	var deleteCalls int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/spfresh/build" && r.Method == http.MethodPost && r.URL.Query().Get("seal") == "true":
			_, _ = w.Write([]byte(`{"status":"sealed"}`))
		case r.URL.Path == "/spfresh/build/commit" && r.Method == http.MethodPost:
			http.Error(w, "commit failed", http.StatusBadRequest)
		case r.URL.Path == "/spfresh/build" && r.Method == http.MethodDelete:
			deleteCalls++
		default:
			http.Error(w, "unexpected request", http.StatusBadRequest)
		}
	}))
	t.Cleanup(server.Close)
	config.UpdateGlobal(func(conf *config.Config) {
		conf.TiKVAPIServiceAddrs = []string{server.URL}
	})

	err := runSPFreshCoordinatorTestSubtask(t, server, true)
	require.ErrorContains(t, err, "commit failed")
	require.Zero(t, deleteCalls)
}

func TestRemoteIngestExecutorCommitFallbacksAfterRetryableFailure(t *testing.T) {
	restore := config.RestoreFunc()
	t.Cleanup(restore)

	indexBuildID := fmt.Sprintf("%d-2-3-4", ^uint32(0))
	var failEndpoint string
	var commitEndpoints []string
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		taskID := r.URL.Query().Get("task_id")
		switch {
		case r.URL.Path == "/spfresh/build" && r.Method == http.MethodPost && r.URL.Query().Get("seal") == "true":
			_, _ = w.Write([]byte(fmt.Sprintf(`{"task_id":"%s","status":"sealed","finished":true}`, taskID)))
		case r.URL.Path == "/spfresh/build/commit" && r.Method == http.MethodPost:
			endpoint := fmt.Sprintf("http://%s", r.Host)
			commitEndpoints = append(commitEndpoints, endpoint)
			require.Empty(t, taskID)
			require.Empty(t, r.URL.Query().Get("commit"))
			if endpoint == failEndpoint {
				http.Error(w, "commit worker unavailable", http.StatusServiceUnavailable)
				return
			}
			w.WriteHeader(http.StatusNoContent)
		case r.Method == http.MethodDelete:
			w.WriteHeader(http.StatusOK)
		default:
			http.Error(w, "unexpected request", http.StatusBadRequest)
		}
	})
	first := httptest.NewServer(handler)
	t.Cleanup(first.Close)
	second := httptest.NewServer(handler)
	t.Cleanup(second.Close)

	endpoints := []string{first.URL, second.URL}
	order, err := spfreshCommitEndpointOrder(indexBuildID, endpoints)
	require.NoError(t, err)
	selected, err := tikvapi.SelectRendezvousEndpoint(indexBuildID, endpoints)
	require.NoError(t, err)
	require.Equal(t, selected, order[0])
	require.Len(t, order, 2)
	failEndpoint = order[0]
	config.UpdateGlobal(func(conf *config.Config) {
		conf.TiKVAPIServiceAddrs = endpoints
	})

	err = runSPFreshCoordinatorTestSubtaskWithTargets(t, first.Client(), spfreshWorkerTargets{
		clusterID:   77,
		workerAddrs: endpoints,
	}, true)
	require.NoError(t, err)
	require.Equal(t, order, commitEndpoints)
}

func TestRemoteIngestExecutorCommitDoesNotFallbackOnBusinessOrProtocolError(t *testing.T) {
	restore := config.RestoreFunc()
	t.Cleanup(restore)

	cases := []struct {
		name      string
		writeFail func(http.ResponseWriter, string)
		errText   string
	}{
		{
			name: "http_4xx",
			writeFail: func(w http.ResponseWriter, _ string) {
				http.Error(w, "commit rejected", http.StatusBadRequest)
			},
			errText: "commit rejected",
		},
		{
			name: "http_200_protocol_error",
			writeFail: func(w http.ResponseWriter, _ string) {
				_, _ = w.Write([]byte(`{"status":"running"}`))
			},
			errText: `{"status":"running"}`,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			indexBuildID := fmt.Sprintf("%d-2-3-4", ^uint32(0))
			var failEndpoint string
			var commitEndpoints []string
			handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				taskID := r.URL.Query().Get("task_id")
				switch {
				case r.URL.Path == "/spfresh/build" && r.Method == http.MethodPost && r.URL.Query().Get("seal") == "true":
					_, _ = w.Write([]byte(fmt.Sprintf(`{"task_id":"%s","status":"sealed","finished":true}`, taskID)))
				case r.URL.Path == "/spfresh/build/commit" && r.Method == http.MethodPost:
					endpoint := fmt.Sprintf("http://%s", r.Host)
					commitEndpoints = append(commitEndpoints, endpoint)
					require.Empty(t, taskID)
					require.Empty(t, r.URL.Query().Get("commit"))
					if endpoint == failEndpoint {
						tc.writeFail(w, taskID)
						return
					}
					w.WriteHeader(http.StatusNoContent)
				default:
					http.Error(w, "unexpected request", http.StatusBadRequest)
				}
			})
			first := httptest.NewServer(handler)
			t.Cleanup(first.Close)
			second := httptest.NewServer(handler)
			t.Cleanup(second.Close)

			endpoints := []string{first.URL, second.URL}
			order, err := spfreshCommitEndpointOrder(indexBuildID, endpoints)
			require.NoError(t, err)
			require.Len(t, order, 2)
			failEndpoint = order[0]
			err = runSPFreshCoordinatorTestSubtaskWithTargets(t, first.Client(), spfreshWorkerTargets{
				clusterID:   77,
				workerAddrs: endpoints,
			}, true)
			require.ErrorContains(t, err, tc.errText)
			require.Equal(t, []string{order[0]}, commitEndpoints)
		})
	}
}

func TestRemoteIngestExecutorCommitDoesNotFallbackAfterContextCancel(t *testing.T) {
	okWorker := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Errorf("canceled commit should not reach worker: %s %s", r.Method, r.URL.String())
		http.Error(w, "unexpected request", http.StatusInternalServerError)
	}))
	t.Cleanup(okWorker.Close)
	closedEndpoint := closedHTTPURL(t)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := (&remoteIngestExecutor{httpClient: okWorker.Client()}).commitSPFreshBuildReady(ctx, spfreshWorkerTargets{
		clusterID:   77,
		workerAddrs: []string{closedEndpoint, okWorker.URL},
	}, remote.SPFreshBuildCommitRequest{
		BuildID:    "1-2-3-4",
		KeyspaceID: 1,
		TableID:    3,
		IndexID:    4,
	})
	require.ErrorIs(t, err, context.Canceled)
}

func TestRemoteIngestExecutorEmptyRoutedBuildBootstrapsReadyIndex(t *testing.T) {
	restore := config.RestoreFunc()
	t.Cleanup(restore)
	const keyspaceID = uint32(271828)

	var (
		bootstrapWorker  string
		bootstrapIndexes []int64
		bootstrapTS      uint64
		bootstrapCalls   int
	)
	var server *httptest.Server
	server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/spfresh/build" && r.Method == http.MethodPost && r.URL.Query().Get("seal") == "true":
			t.Errorf("empty routed SPFresh build should not seal uninitialized shard %q", r.URL.Query().Get("task_id"))
			http.Error(w, "unexpected seal", http.StatusInternalServerError)
		case r.URL.Path == "/spfresh/build/commit" && r.Method == http.MethodPost:
			t.Error("empty routed SPFresh build should bootstrap Ready metadata directly")
			http.Error(w, "unexpected commit", http.StatusInternalServerError)
		case r.URL.Path == "/spfresh/bootstrap" && r.Method == http.MethodPost:
			bootstrapCalls++
			bootstrapWorker = server.URL
			var request struct {
				KeyspaceID     uint32 `json:"keyspace_id"`
				IndexID        int64  `json:"index_id"`
				BaseSnapshotTS uint64 `json:"base_snapshot_ts"`
			}
			require.NoError(t, json.NewDecoder(r.Body).Decode(&request))
			if request.KeyspaceID != keyspaceID {
				http.Error(w, "unexpected keyspace", http.StatusBadRequest)
				return
			}
			bootstrapIndexes = append(bootstrapIndexes, request.IndexID)
			bootstrapTS = request.BaseSnapshotTS
			w.WriteHeader(http.StatusOK)
		default:
			http.Error(w, "unexpected request", http.StatusBadRequest)
		}
	}))
	t.Cleanup(server.Close)
	config.UpdateGlobal(func(conf *config.Config) {
		conf.TiKVAPIServiceAddrs = []string{server.URL}
	})

	baseStore := createMockStore(t)
	t.Cleanup(func() {
		require.NoError(t, baseStore.Close())
	})
	codec, err := tikv.NewCodecV2(tikv.ModeTxn, &keyspacepb.KeyspaceMeta{Id: keyspaceID})
	require.NoError(t, err)
	store := &keyspaceTestStore{
		keyspaceTestStorage: baseStore.(keyspaceTestStorage),
		codec:               codec,
	}
	tblInfo := newSPFreshBackfillTableInfo()
	routePlan := &backend.SPFreshBuildRoutePlan{
		KeyspaceID:      keyspaceID,
		JobID:           2,
		PhysicalTableID: tblInfo.ID,
		IndexID:         11,
		BaseSnapshotTS:  5,
		OwnerEndpoints:  []string{server.URL},
	}
	readMeta := &BackfillSubTaskMeta{SPFreshBuildHasData: spfreshBuildDataState(false)}
	metaBytes, err := readMeta.Marshal()
	require.NoError(t, err)

	exec, err := newRemoteIngestExecutor(
		tables.MockTableFromMeta(tblInfo).(table.PhysicalTable),
		store,
		routePlan,
		server.Client(),
	)
	require.NoError(t, err)
	config.UpdateGlobal(func(conf *config.Config) {
		conf.TiKVAPIServiceAddrs = nil
	})

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	require.NoError(t, exec.RunSubtask(ctx, &proto.Subtask{Meta: metaBytes}))
	require.Equal(t, 1, bootstrapCalls)
	require.Equal(t, []int64{routePlan.IndexID}, bootstrapIndexes)
	require.Equal(t, routePlan.BaseSnapshotTS, bootstrapTS)

	expectedWorker, err := selectSPFreshBootstrapOwner(spfreshWorkerTargets{
		keyspaceID:  keyspaceID,
		workerAddrs: routePlan.OwnerEndpoints,
	}, tblInfo, []int64{routePlan.IndexID})
	require.NoError(t, err)
	require.Equal(t, expectedWorker, bootstrapWorker)
}

func TestGenerateRemoteIngestPlanAggregatesSPFreshDataState(t *testing.T) {
	firstRead := &BackfillSubTaskMeta{SPFreshBuildHasData: spfreshBuildDataState(false)}
	firstMetaBytes, err := firstRead.Marshal()
	require.NoError(t, err)
	secondRead := &BackfillSubTaskMeta{
		SPFreshBuildHasData: spfreshBuildDataState(true),
	}
	secondMetaBytes, err := secondRead.Marshal()
	require.NoError(t, err)
	handle := &remoteIngestPlanTaskHandle{
		metas: map[proto.Step][][]byte{
			proto.BackfillStepReadIndex: {firstMetaBytes, secondMetaBytes},
		},
	}

	metas, err := generateRemoteIngestPlan(context.Background(), handle, &proto.Task{
		TaskBase: proto.TaskBase{ID: 11},
	})
	require.NoError(t, err)
	require.Len(t, metas, 1)
	decoded, err := decodeBackfillSubTaskMeta(context.Background(), "", metas[0])
	require.NoError(t, err)
	require.NotNil(t, decoded.SPFreshBuildHasData)
	require.True(t, *decoded.SPFreshBuildHasData)
}

func TestGenerateRemoteIngestPlanRejectsMissingSPFreshDataState(t *testing.T) {
	metaBytes, err := (&BackfillSubTaskMeta{}).Marshal()
	require.NoError(t, err)
	handle := &remoteIngestPlanTaskHandle{
		metas: map[proto.Step][][]byte{
			proto.BackfillStepReadIndex: {metaBytes},
		},
	}

	_, err = generateRemoteIngestPlan(context.Background(), handle, &proto.Task{
		TaskBase: proto.TaskBase{ID: 11},
	})
	require.ErrorContains(t, err, "missing build data state")
}

func TestGenerateRemoteIngestPlanRejectsMissingTaskHandle(t *testing.T) {
	_, err := generateRemoteIngestPlan(context.Background(), nil, &proto.Task{
		TaskBase: proto.TaskBase{ID: 11},
	})
	require.ErrorContains(t, err, "requires task handle")
}

func runSPFreshCoordinatorTestSubtask(
	t *testing.T,
	server *httptest.Server,
	hasData bool,
) error {
	t.Helper()
	return runSPFreshCoordinatorTestSubtaskWithTargets(t, server.Client(), spfreshWorkerTargets{
		clusterID:   77,
		workerAddrs: []string{server.URL},
	}, hasData)
}

func runSPFreshCoordinatorTestSubtaskWithTargets(
	t *testing.T,
	httpClient *http.Client,
	targets spfreshWorkerTargets,
	hasData bool,
) error {
	t.Helper()
	store := createMockStore(t)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	readMeta := &BackfillSubTaskMeta{SPFreshBuildHasData: &hasData}
	metaBytes, err := readMeta.Marshal()
	require.NoError(t, err)
	routePlan := &backend.SPFreshBuildRoutePlan{
		KeyspaceID:      uint32(store.GetCodec().GetKeyspaceID()),
		JobID:           2,
		PhysicalTableID: 3,
		IndexID:         4,
		BaseSnapshotTS:  5,
		OwnerEndpoints:  targets.workerAddrs,
	}
	exec, err := newRemoteIngestExecutor(
		nil,
		store,
		routePlan,
		httpClient,
	)
	require.NoError(t, err)
	return exec.RunSubtask(context.Background(), &proto.Subtask{Meta: metaBytes})
}

func closedHTTPURL(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := listener.Addr().String()
	require.NoError(t, listener.Close())
	return fmt.Sprintf("http://%s", addr)
}

type remoteIngestPlanTaskHandle struct {
	metas map[proto.Step][][]byte
}

func (h *remoteIngestPlanTaskHandle) GetPreviousSubtaskMetas(_ int64, step proto.Step) ([][]byte, error) {
	return h.metas[step], nil
}

func (*remoteIngestPlanTaskHandle) WithNewSession(func(se sessionctx.Context) error) error {
	return nil
}

func (*remoteIngestPlanTaskHandle) WithNewTxn(context.Context, func(se sessionctx.Context) error) error {
	return nil
}
