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

package remote

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/google/uuid"
	"github.com/pingcap/tidb/pkg/lightning/backend"
	lkv "github.com/pingcap/tidb/pkg/lightning/backend/kv"
	"github.com/pingcap/tidb/pkg/lightning/common"
	"github.com/pingcap/tidb/pkg/lightning/log"
	"github.com/pingcap/tidb/pkg/meta/model"
	pmodel "github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/codec"
	"github.com/pingcap/tidb/pkg/util/dbterror"
	"github.com/stretchr/testify/require"
)

func TestAppendSPFreshBuildRecordEncoding(t *testing.T) {
	w := &writer{
		e: &engine{taskType: taskTypeSPFreshBuild},
	}

	err := w.AppendSPFreshBuildRecord(context.Background(), []byte("h1"), []byte{1, 2, 3}, []byte{4, 5})
	require.NoError(t, err)
	require.Equal(t, []byte{
		2, 0, 'h', '1', 1, 2, 3, 2, 0, 0, 0, 4, 5,
	}, w.buf)
}

func TestAppendSPFreshBuildRecordRejectsOversizedRecord(t *testing.T) {
	w := &writer{e: &engine{taskType: taskTypeSPFreshBuild}}

	err := w.AppendSPFreshBuildRecord(context.Background(), []byte("h1"), make([]byte, maxSPFreshBuildChunkBytes), nil)

	require.ErrorContains(t, err, "exceeds max chunk size")
	require.Empty(t, w.buf)
}

func TestAppendRowsRejectsSPFreshBuildEngine(t *testing.T) {
	w := &writer{e: &engine{taskType: taskTypeSPFreshBuild}}
	rows := lkv.MakeRowsFromKvPairs([]common.KvPair{{Key: []byte("h1"), Val: []byte{1}}})

	err := w.AppendRows(context.Background(), nil, rows)

	require.ErrorContains(t, err, "SPFresh build engines require AppendSPFreshBuildRecord")
	require.Empty(t, w.buf)
}

func TestSPFreshBuildPutChunkUsesCommonProgressRetry(t *testing.T) {
	var gotChunks []uint64
	var gotBodies []string
	client := &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		require.Equal(t, http.MethodPut, req.Method)
		require.Equal(t, "/spfresh/build", req.URL.Path)
		require.Equal(t, "7", req.URL.Query().Get("cluster_id"))
		require.Equal(t, "task-1", req.URL.Query().Get("task_id"))
		require.Equal(t, "11", req.URL.Query().Get("writer_id"))

		chunkID, err := strconv.ParseUint(req.URL.Query().Get("chunk_id"), 10, 64)
		require.NoError(t, err)
		body, err := io.ReadAll(req.Body)
		require.NoError(t, err)
		gotChunks = append(gotChunks, chunkID)
		gotBodies = append(gotBodies, string(body))

		var respBody string
		switch len(gotChunks) {
		case 1:
			require.Equal(t, uint64(2), chunkID)
			respBody = `{"handled-chunk-id":0,"flushed-chunk-id":0}`
		case 2:
			require.Equal(t, uint64(1), chunkID)
			respBody = `{"handled-chunk-id":1,"flushed-chunk-id":0}`
		case 3:
			require.Equal(t, uint64(2), chunkID)
			respBody = `{"handled-chunk-id":2,"flushed-chunk-id":2}`
		default:
			t.Fatalf("unexpected request %d for chunk %d", len(gotChunks), chunkID)
		}
		return jsonResponse(req, respBody), nil
	})}

	cache, err := newChunkCache("task-1", 11, "", true)
	require.NoError(t, err)
	require.NoError(t, cache.put(1, []byte("first")))
	c := newTestChunkSender(client, cache, taskTypeSPFreshBuild)

	err = c.putChunkToRemote(context.Background(), &chunk{id: 2, data: []byte("second")})
	require.NoError(t, err)
	require.Equal(t, []uint64{2, 1, 2}, gotChunks)
	require.Equal(t, []string{"second", "first", "second"}, gotBodies)
	require.Equal(t, uint64(0), c.getFlushedChunkID())
	require.Equal(t, uint64(2), c.loadState().HandledChunkID)
	_, err = cache.get(1)
	require.NoError(t, err)
	_, err = cache.get(2)
	require.NoError(t, err)
}

func TestSPFreshBuildPutChunkWaitsForAcceptedTransientRecovery(t *testing.T) {
	var gotChunks []uint64
	var gotBodies []string
	client := &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		if req.Method == http.MethodGet {
			return jsonResponse(req, `{"task-id":"task-1","status":"running"}`), nil
		}
		require.Equal(t, http.MethodPut, req.Method)
		require.Equal(t, "/spfresh/build", req.URL.Path)
		chunkID, err := strconv.ParseUint(req.URL.Query().Get("chunk_id"), 10, 64)
		require.NoError(t, err)
		body, err := io.ReadAll(req.Body)
		require.NoError(t, err)
		gotChunks = append(gotChunks, chunkID)
		gotBodies = append(gotBodies, string(body))

		if len(gotChunks) == 1 {
			resp := jsonResponse(req, `{
				"handled-chunk-id":0,
				"flushed-chunk-id":0,
				"error":"transient recovery in progress",
				"status":"retrying",
				"retry-reason":"deadlock",
				"retry-attempt":2,
				"next-retry-at-ms":0
			}`)
			resp.StatusCode = http.StatusAccepted
			resp.Status = http.StatusText(http.StatusAccepted)
			return resp, nil
		}
		return jsonResponse(req, `{
			"handled-chunk-id":1,
			"flushed-chunk-id":0,
			"status":"running"
		}`), nil
	})}

	cache, err := newChunkCache("task-1", 11, "", true)
	require.NoError(t, err)
	c := newTestChunkSender(client, cache, taskTypeSPFreshBuild)
	require.NoError(t, c.putChunkToRemote(context.Background(), &chunk{id: 1, data: []byte("chunk")}))
	require.Equal(t, []uint64{1, 1}, gotChunks)
	require.Equal(t, []string{"chunk", "chunk"}, gotBodies)
	require.Equal(t, uint64(1), c.loadState().HandledChunkID)
	_, err = cache.get(1)
	require.NoError(t, err)
}

func TestSPFreshBuildPutChunkRejectsInvalidAcceptedResponse(t *testing.T) {
	for _, body := range []string{"", "{", "null", "{}", `{"status":"unknown"}`} {
		t.Run(body, func(t *testing.T) {
			requests := 0
			client := &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
				requests++
				require.Equal(t, http.MethodPut, req.Method)
				resp := jsonResponse(req, body)
				resp.StatusCode = http.StatusAccepted
				return resp, nil
			})}
			cache, err := newChunkCache("task-1", 11, "", true)
			require.NoError(t, err)
			c := newTestChunkSender(client, cache, taskTypeSPFreshBuild)
			require.Error(t, c.putChunkToRemote(context.Background(), &chunk{id: 1, data: []byte("chunk")}))
			require.Equal(t, 1, requests)
			require.Zero(t, c.loadState().HandledChunkID)
			data, err := cache.get(1)
			require.NoError(t, err)
			require.Equal(t, "chunk", string(data))
		})
	}
}

func TestSPFreshBuildFlushUsesCommonFlushedChunkIDsForRecovery(t *testing.T) {
	var gotOps []string
	client := &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		require.Equal(t, "/spfresh/build", req.URL.Path)
		require.Equal(t, "7", req.URL.Query().Get("cluster_id"))
		require.Equal(t, "task-1", req.URL.Query().Get("task_id"))
		require.Equal(t, "11", req.URL.Query().Get("writer_id"))

		var respBody string
		switch req.Method {
		case http.MethodPost:
			require.Equal(t, "true", req.URL.Query().Get("flush"))
			require.Equal(t, "2", req.URL.Query().Get("target_chunk_id"))
			gotOps = append(gotOps, "flush")
			if len(gotOps) == 1 {
				respBody = `{"flushed-chunk-ids":{"11":0}}`
			} else {
				respBody = `{"flushed-chunk-ids":{"11":2}}`
			}
		case http.MethodPut:
			chunkID, err := strconv.ParseUint(req.URL.Query().Get("chunk_id"), 10, 64)
			require.NoError(t, err)
			body, err := io.ReadAll(req.Body)
			require.NoError(t, err)
			require.NotEmpty(t, body)
			gotOps = append(gotOps, fmt.Sprintf("put-%d", chunkID))
			respBody = fmt.Sprintf(
				`{"handled-chunk-id":%d,"flushed-chunk-id":%d}`,
				chunkID,
				chunkID,
			)
		default:
			t.Fatalf("method=%s, want POST or PUT", req.Method)
		}
		return jsonResponse(req, respBody), nil
	})}

	cache, err := newChunkCache("task-1", 11, "", true)
	require.NoError(t, err)
	require.NoError(t, cache.put(1, []byte("first")))
	require.NoError(t, cache.put(2, []byte("second")))
	c := newTestChunkSender(client, cache, taskTypeSPFreshBuild)
	c.nextChunkID.Store(2)

	err = c.sendFlushToRemote(context.Background())
	require.NoError(t, err)
	require.Equal(t, []string{"flush", "put-1", "put-2", "flush"}, gotOps)
	require.Equal(t, uint64(2), c.getFlushedChunkID())
	require.Equal(t, uint64(2), c.loadState().HandledChunkID)
}

func TestSPFreshBuildFlushWaitsForAcceptedTransientRecovery(t *testing.T) {
	var gotOps []string
	client := &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		require.Equal(t, http.MethodPost, req.Method)
		require.Equal(t, "/spfresh/build", req.URL.Path)
		require.Equal(t, "true", req.URL.Query().Get("flush"))
		require.Equal(t, "2", req.URL.Query().Get("target_chunk_id"))
		gotOps = append(gotOps, "flush")
		if len(gotOps) == 1 {
			resp := jsonResponse(req, `{
				"flushed-chunk-ids":{"11":0},
				"error":"transient recovery in progress",
				"status":"retrying",
				"retry-reason":"deadlock",
				"retry-attempt":3,
				"next-retry-at-ms":0
			}`)
			resp.StatusCode = http.StatusAccepted
			resp.Status = http.StatusText(http.StatusAccepted)
			return resp, nil
		}
		return jsonResponse(req, `{"flushed-chunk-ids":{"11":2}}`), nil
	})}

	cache, err := newChunkCache("task-1", 11, "", true)
	require.NoError(t, err)
	c := newTestChunkSender(client, cache, taskTypeSPFreshBuild)
	c.nextChunkID.Store(2)

	require.NoError(t, c.sendFlushToRemote(context.Background()))
	require.Equal(t, []string{"flush", "flush"}, gotOps)
	require.Equal(t, uint64(2), c.getFlushedChunkID())
}

func TestSPFreshBuildFlushRetriesMalformedAcceptedResponse(t *testing.T) {
	var targetChunkIDs []string
	client := &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		targetChunkIDs = append(targetChunkIDs, req.URL.Query().Get("target_chunk_id"))
		if len(targetChunkIDs) == 1 {
			resp := jsonResponse(req, "not-json")
			resp.StatusCode = http.StatusAccepted
			resp.Status = http.StatusText(http.StatusAccepted)
			return resp, nil
		}
		return jsonResponse(req, `{"flushed-chunk-ids":{"11":1}}`), nil
	})}

	cache, err := newChunkCache("task-1", 11, "", true)
	require.NoError(t, err)
	c := newTestChunkSender(client, cache, taskTypeSPFreshBuild)
	c.nextChunkID.Store(1)

	require.NoError(t, c.sendFlushToRemote(context.Background()))
	require.Equal(t, []string{"1", "1"}, targetChunkIDs)
}

func TestSPFreshBuildRoutesWriterSlotToOneEndpointShard(t *testing.T) {
	ctx := context.Background()
	var senderID uint64 = 1000
	var mu sync.Mutex
	var requests []string
	putBodies := make(map[string]string)

	handler := func(w http.ResponseWriter, req *http.Request) {
		if req.URL.Path != "/spfresh/build" {
			http.Error(w, "bad path", http.StatusNotFound)
			return
		}
		taskID := req.URL.Query().Get("task_id")
		writerID := req.URL.Query().Get("writer_id")
		chunkID := req.URL.Query().Get("chunk_id")
		body, err := io.ReadAll(req.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		mu.Lock()
		requestKey := req.Method + " " + taskID + " writer=" + writerID + " chunk=" + chunkID + " flush=" + req.URL.Query().Get("flush") + " target=" + req.URL.Query().Get("target_chunk_id")
		requests = append(requests, requestKey)
		if req.Method == http.MethodPut {
			putBodies[requestKey] = string(body)
		}
		mu.Unlock()

		switch req.Method {
		case http.MethodPost:
			if req.URL.Query().Get("flush") == "true" {
				_, _ = w.Write([]byte(fmt.Sprintf(`{"flushed-chunk-ids":{"%s":%s}}`, writerID, req.URL.Query().Get("target_chunk_id"))))
				return
			}
			_, _ = w.Write([]byte(`{}`))
		case http.MethodPut:
			_, _ = w.Write([]byte(fmt.Sprintf(`{"handled-chunk-id":%s,"flushed-chunk-id":0}`, chunkID)))
		default:
			http.Error(w, "bad method", http.StatusMethodNotAllowed)
		}
	}
	first := httptest.NewServer(http.HandlerFunc(handler))
	defer first.Close()
	second := httptest.NewServer(http.HandlerFunc(handler))
	defer second.Close()

	b := &Backend{
		logger:          log.L(),
		httpClient:      http.DefaultClient,
		chunkCacheInMem: true,
		allocateSenderIDFn: func(context.Context) (uint64, error) {
			senderID++
			return senderID, nil
		},
	}
	e := newTestRoutedSPFreshBuildEngine(t, b, []string{first.URL, second.URL})
	engineUUID := uuid.New()
	b.engines.Store(engineUUID, e)
	w, err := b.LocalWriter(ctx, &backend.LocalWriterConfig{SPFreshWriterSlot: 1}, engineUUID)
	require.NoError(t, err)
	spfreshWriter := w.(*writer)
	defer func() {
		require.NoError(t, spfreshWriter.closeChunkSenders())
	}()

	spfreshWriter.buf = append(spfreshWriter.buf, "first-complete-chunk"...)
	require.NoError(t, spfreshWriter.addChunk(ctx))
	spfreshWriter.buf = append(spfreshWriter.buf, "second-complete-chunk"...)
	require.NoError(t, spfreshWriter.addChunk(ctx))

	require.NoError(t, spfreshWriter.flush(ctx))
	require.True(t, e.spfreshBuildRouting.HasData())

	mu.Lock()
	defer mu.Unlock()
	require.Equal(t, []string{
		"PUT 1-2-42-7-endpoint-1 writer=1001 chunk=1 flush= target=",
		"PUT 1-2-42-7-endpoint-1 writer=1001 chunk=2 flush= target=",
		"POST 1-2-42-7-endpoint-1 writer=1001 chunk= flush=true target=2",
	}, requests)
	require.Equal(t, "first-complete-chunk", putBodies["PUT 1-2-42-7-endpoint-1 writer=1001 chunk=1 flush= target="])
	require.Equal(t, "second-complete-chunk", putBodies["PUT 1-2-42-7-endpoint-1 writer=1001 chunk=2 flush= target="])
}

func TestInitializeSPFreshBuildSendsManifestToEveryShard(t *testing.T) {
	ctx := context.Background()
	var mu sync.Mutex
	initRequests := make(map[string]spfreshBuildInitRequest)

	handler := func(w http.ResponseWriter, req *http.Request) {
		if req.URL.Path != "/spfresh/build" {
			http.Error(w, "bad path", http.StatusNotFound)
			return
		}
		if req.Method != http.MethodPost || req.URL.Query().Get("flush") == "true" {
			http.Error(w, "bad method", http.StatusMethodNotAllowed)
			return
		}
		taskID := req.URL.Query().Get("task_id")
		var body spfreshBuildInitRequest
		if err := json.NewDecoder(req.Body).Decode(&body); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		mu.Lock()
		initRequests[taskID] = body
		mu.Unlock()
		_, _ = w.Write([]byte(`{}`))
	}
	const shardCount = 10
	endpoints := make([]string, 0, shardCount)
	for range shardCount {
		server := httptest.NewServer(http.HandlerFunc(handler))
		t.Cleanup(server.Close)
		endpoints = append(endpoints, server.URL)
	}

	routePlan := &backend.SPFreshBuildRoutePlan{
		KeyspaceID:      1,
		JobID:           2,
		PhysicalTableID: 42,
		IndexID:         7,
		BaseSnapshotTS:  12345,
		OwnerEndpoints:  endpoints,
	}
	runtimeConfig := model.DefaultSPFreshVectorRuntimeConfig()
	tableInfo := newSPFreshBuildTestTableInfo(42, 7, 2, &model.VectorIndexInfo{
		Kind:           model.VectorIndexKindSPFresh,
		Dimension:      3,
		DistanceMetric: model.DistanceMetricL2,
		RuntimeConfig:  &runtimeConfig,
	}).Core

	require.NoError(t, InitializeSPFreshBuild(ctx, http.DefaultClient, 9, tableInfo, routePlan))

	wantShardIDs := []string{
		"1-2-42-7-endpoint-1",
		"1-2-42-7-endpoint-10",
		"1-2-42-7-endpoint-2",
		"1-2-42-7-endpoint-3",
		"1-2-42-7-endpoint-4",
		"1-2-42-7-endpoint-5",
		"1-2-42-7-endpoint-6",
		"1-2-42-7-endpoint-7",
		"1-2-42-7-endpoint-8",
		"1-2-42-7-endpoint-9",
	}
	mu.Lock()
	defer mu.Unlock()
	require.Len(t, initRequests, shardCount)
	for _, shardID := range wantShardIDs {
		req, ok := initRequests[shardID]
		require.True(t, ok, "missing init for %s", shardID)
		require.Equal(t, "1-2-42-7", req.BuildID)
		require.Equal(t, wantShardIDs, req.ShardIDs)
		require.Equal(t, uint32(1), req.KeyspaceID)
		require.Equal(t, int64(42), req.TableID)
		require.Equal(t, int64(7), req.IndexID)
		require.Equal(t, uint64(12345), req.BaseSnapshotTS)
	}
}

func TestSPFreshBuildInitReturnsDownstreamError(t *testing.T) {
	const downstreamErr = "codec v1 nullspaceid is rejected"
	routePlan, tableInfo, client := newTestSPFreshBuildInit(t, http.StatusBadRequest, downstreamErr)
	err := InitializeSPFreshBuild(context.Background(), client, 1, tableInfo, routePlan)
	require.True(t, dbterror.ErrIngestFailed.Equal(err))
	require.Contains(t, err.Error(), downstreamErr)
}

func TestSPFreshBuildInitTaskExistsSucceeds(t *testing.T) {
	routePlan, tableInfo, client := newTestSPFreshBuildInit(t, http.StatusConflict, taskExitsMsg)
	require.NoError(t, InitializeSPFreshBuild(context.Background(), client, 1, tableInfo, routePlan))
}

func TestSPFreshBuildInitDoesNotFollowRedirectOrRewriteOwner(t *testing.T) {
	withNoOwnerBoundRetryDelay(t)

	redirectRequests := 0
	redirectServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		redirectRequests++
		w.WriteHeader(http.StatusOK)
	}))
	defer redirectServer.Close()

	ownerRequests := 0
	ownerServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ownerRequests++
		if r.Method != http.MethodPost {
			http.Error(w, "bad method", http.StatusMethodNotAllowed)
			return
		}
		if r.URL.Path != "/spfresh/build" {
			http.Error(w, "bad path", http.StatusNotFound)
			return
		}
		http.Redirect(w, r, redirectServer.URL+"/spfresh/build", http.StatusFound)
	}))
	defer ownerServer.Close()

	routePlan, tableInfo, _ := newTestSPFreshBuildInit(t, http.StatusOK, "")
	routePlan.OwnerEndpoints = []string{ownerServer.URL}

	err := InitializeSPFreshBuild(context.Background(), ownerServer.Client(), 1, tableInfo, routePlan)
	require.Error(t, err)
	require.Equal(t, 1, ownerRequests)
	require.Equal(t, 0, redirectRequests)
}

func TestOpenEngineRequiresSPFreshBuildRouting(t *testing.T) {
	b := &Backend{}
	err := b.OpenEngine(context.Background(), &backend.EngineConfig{
		TaskID:   1,
		EngineID: 7,
		TableInfo: newSPFreshBuildTestTableInfo(42, 7, 42, &model.VectorIndexInfo{
			Kind:           model.VectorIndexKindSPFresh,
			Dimension:      3,
			DistanceMetric: model.DistanceMetricL2,
		}),
	}, uuid.New())
	require.ErrorContains(t, err, "SPFresh build routing is not set")
}

func newTestSPFreshBuildInit(
	t *testing.T,
	statusCode int,
	body string,
) (*backend.SPFreshBuildRoutePlan, *model.TableInfo, *http.Client) {
	t.Helper()
	client := &http.Client{
		Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			require.Equal(t, http.MethodPost, req.Method)
			require.Equal(t, "/spfresh/build", req.URL.Path)
			return &http.Response{
				StatusCode: statusCode,
				Status:     http.StatusText(statusCode),
				Body:       io.NopCloser(strings.NewReader(body)),
				Header:     make(http.Header),
				Request:    req,
			}, nil
		}),
	}
	vectorIndex := &model.VectorIndexInfo{
		Kind:           model.VectorIndexKindSPFresh,
		Dimension:      3,
		DistanceMetric: model.DistanceMetricL2,
	}
	runtimeConfig := model.DefaultSPFreshVectorRuntimeConfig()
	vectorIndex.RuntimeConfig = &runtimeConfig
	tableInfo := newSPFreshBuildTestTableInfo(42, 7, 42, vectorIndex).Core
	routePlan := &backend.SPFreshBuildRoutePlan{
		KeyspaceID:      0,
		JobID:           2,
		PhysicalTableID: 42,
		IndexID:         7,
		BaseSnapshotTS:  12345,
		OwnerEndpoints:  []string{"http://spfresh-build.test"},
	}
	return routePlan, tableInfo, client
}

func TestSPFreshStoredColumnsToAPIDerivesOriginDefaults(t *testing.T) {
	notNullType := types.NewFieldType(mysql.TypeLonglong)
	notNullType.AddFlag(mysql.NotNullFlag | mysql.NoDefaultValueFlag)
	tblInfo := &model.TableInfo{
		Columns: []*model.ColumnInfo{
			{ID: 1, Name: pmodel.NewCIStr("missing"), FieldType: *notNullType},
			{ID: 2, Name: pmodel.NewCIStr("nullable"), FieldType: *types.NewFieldType(mysql.TypeLonglong)},
			{ID: 3, Name: pmodel.NewCIStr("defaulted"), FieldType: *types.NewFieldType(mysql.TypeLonglong), OriginDefaultValue: int64(42)},
		},
	}
	idxInfo := &model.IndexInfo{
		Tp: pmodel.IndexTypeSPFresh,
		VectorInfo: &model.VectorIndexInfo{
			Kind:            model.VectorIndexKindSPFresh,
			StoredColumnIDs: []int64{1, 2, 3},
		},
	}

	columns, err := spfreshStoredColumnsToAPI(tblInfo, idxInfo)
	require.NoError(t, err)
	require.Len(t, columns, 3)
	require.Nil(t, columns[0].OriginDefaultValue)
	require.Equal(t, &spfreshJSONBytes{codec.NilFlag}, columns[1].OriginDefaultValue)
	require.NotNil(t, columns[2].OriginDefaultValue)
	remaining, defaultValue, err := codec.DecodeOne(*columns[2].OriginDefaultValue)
	require.NoError(t, err)
	require.Empty(t, remaining)
	require.Equal(t, int64(42), defaultValue.GetInt64())
}

func newTestRoutedSPFreshBuildEngine(
	t *testing.T,
	b *Backend,
	endpoints []string,
) *engine {
	t.Helper()

	routePlan := &backend.SPFreshBuildRoutePlan{
		KeyspaceID:      1,
		JobID:           2,
		PhysicalTableID: 42,
		IndexID:         7,
		BaseSnapshotTS:  12345,
		OwnerEndpoints:  endpoints,
	}
	routing, err := backend.NewSPFreshBuildRoutingConfig(routePlan)
	require.NoError(t, err)
	runtimeConfig := model.DefaultSPFreshVectorRuntimeConfig()
	return &engine{
		ctx:                 context.Background(),
		logger:              log.L(),
		engineID:            7,
		taskType:            taskTypeSPFreshBuild,
		clusterID:           9,
		httpClient:          b.httpClient,
		spfreshBuildRouting: routing,
		tbl: newSPFreshBuildTestTableInfo(42, 7, 2, &model.VectorIndexInfo{
			Kind:           model.VectorIndexKindSPFresh,
			Dimension:      3,
			DistanceMetric: model.DistanceMetricL2,
			RuntimeConfig:  &runtimeConfig,
		}),
	}
}
