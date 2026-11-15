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

package executor_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/config"
	ingesttestutil "github.com/pingcap/tidb/pkg/ddl/ingest/testutil"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/executor"
	pmodel "github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

type statusBatchRequest struct {
	KeyspaceID uint32             `json:"keyspace_id"`
	Indexes    []statusBatchIndex `json:"indexes"`
}

type statusBatchIndex struct {
	TableID int64 `json:"table_id"`
	IndexID int64 `json:"index_id"`
}

type statusBatchResponse struct {
	Statuses []statusBatchRow `json:"statuses"`
}

type statusBatchRow struct {
	KeyspaceID         uint32  `json:"keyspace_id"`
	TableID            int64   `json:"table_id"`
	IndexID            int64   `json:"index_id"`
	Dimensions         uint64  `json:"dimensions"`
	DistanceMetric     string  `json:"distance_metric"`
	AppliedBaseTS      uint64  `json:"applied_base_ts"`
	LagSeconds         *uint64 `json:"lag_seconds"`
	OwnerLeaseExpireTS *uint64 `json:"owner_lease_expire_ts"`
}

type capturedStatusRequest struct {
	clusterID string
	payload   statusBatchRequest
}

func setupSPFreshStatusTestKit(t *testing.T, workerURL string) (*testkit.TestKit, *domain.Domain) {
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/ddl/mockSPFreshBuildTransport", "return(true)"))
	t.Cleanup(func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/ddl/mockSPFreshBuildTransport"))
	})
	t.Helper()
	executor.ResetSPFreshVectorSearchForTest()

	restore := config.RestoreFunc()
	t.Cleanup(restore)
	config.UpdateGlobal(func(conf *config.Config) {
		if workerURL == "" {
			conf.TiKVAPIServiceAddrs = nil
		} else {
			conf.TiKVAPIServiceAddrs = []string{workerURL}
		}
	})
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/ddl/MockBootstrapEmptySPFreshIndexes", `return(true)`))
	t.Cleanup(func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/ddl/MockBootstrapEmptySPFreshIndexes"))
	})
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/ddl/MockBootstrapInlineSPFreshIndexes", `return(true)`))
	t.Cleanup(func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/ddl/MockBootstrapInlineSPFreshIndexes"))
	})

	store, dom := testkit.CreateMockStoreAndDomain(t)
	t.Cleanup(ingesttestutil.InjectMockBackendCtx(t, store))

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	return tk, dom
}

func mustGetSPFreshIndexRef(t *testing.T, dom *domain.Domain, tableName, indexName string) (int64, int64) {
	t.Helper()
	tbl, err := dom.InfoSchema().TableByName(
		context.Background(),
		pmodel.NewCIStr("test"),
		pmodel.NewCIStr(tableName),
	)
	require.NoError(t, err)
	for _, idx := range tbl.Meta().Indices {
		if idx.Name.L == indexName {
			return tbl.Meta().ID, idx.ID
		}
	}
	t.Fatalf("index %s not found on table %s", indexName, tableName)
	return 0, 0
}

func ptr[T any](v T) *T {
	return &v
}

func handleSPFreshStatusTestBootstrap(t *testing.T, w http.ResponseWriter, r *http.Request) bool {
	t.Helper()
	if r.URL.Path == "/spfresh/build" && r.Method == http.MethodDelete {
		w.WriteHeader(http.StatusNoContent)
		return true
	}
	if r.URL.Path == "/spfresh/build-lifecycle" {
		_, err := io.Copy(io.Discard, r.Body)
		require.NoError(t, err)
		w.WriteHeader(http.StatusNoContent)
		return true
	}
	if r.URL.Path != "/spfresh/bootstrap" {
		return false
	}
	require.Equal(t, http.MethodPost, r.Method)
	require.NotEmpty(t, r.URL.Query().Get("cluster_id"))
	_, err := io.Copy(io.Discard, r.Body)
	require.NoError(t, err)
	require.NoError(t, r.Body.Close())
	w.WriteHeader(http.StatusOK)
	return true
}

func TestSPFreshIndexStatusTableSchema(t *testing.T) {
	tk, _ := setupSPFreshStatusTestKit(t, "")

	tk.MustQuery(`
		select column_name, data_type
		from information_schema.columns
		where table_schema = 'information_schema'
		  and table_name = 'TIDB_SPFRESH_INDEX_STATUS'
		order by ordinal_position`,
	).Check(testkit.Rows(
		"TABLE_SCHEMA varchar",
		"TABLE_NAME varchar",
		"INDEX_NAME varchar",
		"KEYSPACE_ID bigint",
		"TABLE_ID bigint",
		"INDEX_ID bigint",
		"DIMENSIONS bigint",
		"DISTANCE_METRIC varchar",
		"APPLIED_BASE_TS bigint",
		"LAG_SECONDS bigint",
		"INDEX_STATE varchar",
		"IS_READY tinyint",
		"OWNER_LEASE_EXPIRE_TS bigint",
		"OBSERVED_AT timestamp",
	))
}

func TestSPFreshIndexStatusTableSuccessAndPruning(t *testing.T) {
	var (
		mu       sync.Mutex
		requests []capturedStatusRequest
	)

	statuses := make(map[[2]int64]statusBatchRow)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if handleSPFreshStatusTestBootstrap(t, w, r) {
			return
		}
		require.Equal(t, "/spfresh/status/batch", r.URL.Path)
		body, err := io.ReadAll(r.Body)
		require.NoError(t, err)
		require.NoError(t, r.Body.Close())

		var req statusBatchRequest
		require.NoError(t, json.Unmarshal(body, &req))

		mu.Lock()
		requests = append(requests, capturedStatusRequest{
			clusterID: r.URL.Query().Get("cluster_id"),
			payload:   req,
		})
		mu.Unlock()

		resp := statusBatchResponse{
			Statuses: make([]statusBatchRow, 0, len(req.Indexes)),
		}
		for _, index := range req.Indexes {
			if status, ok := statuses[[2]int64{index.TableID, index.IndexID}]; ok {
				resp.Statuses = append(resp.Statuses, status)
			}
		}
		w.Header().Set("Content-Type", "application/json")
		require.NoError(t, json.NewEncoder(w).Encode(resp))
	}))
	t.Cleanup(srv.Close)

	tk, dom := setupSPFreshStatusTestKit(t, srv.URL)
	tk.MustExec("create table t_spfresh_status_a(id int primary key, vec vector(3), vec2 vector(3))")
	tk.MustExec("create vector index idx_ready on t_spfresh_status_a ((vec_l2_distance(vec))) using spfresh")
	tk.MustExec("create vector index idx_without_checkpoint on t_spfresh_status_a ((vec_cosine_distance(vec2))) using spfresh")
	tk.MustExec("create table t_spfresh_status_b(id int primary key, vec vector(3))")
	tk.MustExec("create vector index idx_without_checkpoint on t_spfresh_status_b ((vec_l2_distance(vec))) using spfresh")

	readyTableID, readyIndexID := mustGetSPFreshIndexRef(t, dom, "t_spfresh_status_a", "idx_ready")
	clusterID := strconv.FormatUint(dom.GetPDClient().GetClusterID(context.Background()), 10)
	keyspaceID := uint32(tk.Session().GetStore().GetCodec().GetKeyspaceID())

	statuses[[2]int64{readyTableID, readyIndexID}] = statusBatchRow{
		KeyspaceID:         keyspaceID,
		TableID:            readyTableID,
		IndexID:            readyIndexID,
		Dimensions:         3,
		DistanceMetric:     "l2",
		AppliedBaseTS:      101,
		LagSeconds:         ptr[uint64](3),
		OwnerLeaseExpireTS: ptr[uint64](999),
	}

	tk.MustQuery(`
		select table_name, index_name, dimensions, distance_metric, index_state, is_ready,
		       ifnull(cast(applied_base_ts as char), 'NULL'),
		       ifnull(cast(lag_seconds as char), 'NULL'),
		       ifnull(cast(owner_lease_expire_ts as char), 'NULL'),
		       observed_at is not null
		from information_schema.tidb_spfresh_index_status
		where table_schema = 'test'
		order by table_name, index_name`,
	).Check(testkit.Rows(
		"t_spfresh_status_a idx_ready 3 l2 READY 1 101 3 999 1",
		"t_spfresh_status_a idx_without_checkpoint 3 cosine UNKNOWN 0 NULL NULL NULL 1",
		"t_spfresh_status_b idx_without_checkpoint 3 l2 UNKNOWN 0 NULL NULL NULL 1",
	))

	mu.Lock()
	seen := append([]capturedStatusRequest(nil), requests...)
	requests = nil
	mu.Unlock()
	require.NotEmpty(t, seen)
	for _, req := range seen {
		require.Equal(t, clusterID, req.clusterID)
		require.Equal(t, keyspaceID, req.payload.KeyspaceID)
		require.Len(t, req.payload.Indexes, 3)
	}

	tk.MustQuery(`
		select index_state
		from information_schema.tidb_spfresh_index_status
		where table_schema = 'test'
		  and table_name = 't_spfresh_status_a'
		  and index_name = 'idx_ready'`,
	).Check(testkit.Rows("READY"))

	mu.Lock()
	defer mu.Unlock()
	require.NotEmpty(t, requests)
	for _, req := range requests {
		require.Equal(t, clusterID, req.clusterID)
		require.Equal(t, keyspaceID, req.payload.KeyspaceID)
		require.Equal(t, []statusBatchIndex{{TableID: readyTableID, IndexID: readyIndexID}}, req.payload.Indexes)
	}
}

func TestSPFreshIndexStatusTableUnknownOnWorkerFailure(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if handleSPFreshStatusTestBootstrap(t, w, r) {
			return
		}
		require.Equal(t, "/spfresh/status/batch", r.URL.Path)
		http.Error(w, "boom", http.StatusServiceUnavailable)
	}))
	t.Cleanup(srv.Close)

	tk, dom := setupSPFreshStatusTestKit(t, srv.URL)
	tk.MustExec("create table t_spfresh_status_unknown(id int primary key, vec vector(3))")
	tk.MustExec("create vector index idx_unknown on t_spfresh_status_unknown ((vec_l2_distance(vec))) using spfresh")
	_, _ = mustGetSPFreshIndexRef(t, dom, "t_spfresh_status_unknown", "idx_unknown")

	tk.MustQuery(`
		select table_name, index_name, dimensions, distance_metric, index_state, is_ready,
		       applied_base_ts is null, lag_seconds is null, owner_lease_expire_ts is null, observed_at is not null
		from information_schema.tidb_spfresh_index_status
		where table_schema = 'test'
		  and table_name = 't_spfresh_status_unknown'`,
	).Check(testkit.Rows(
		"t_spfresh_status_unknown idx_unknown 3 l2 UNKNOWN 0 1 1 1 1",
	))
}

func TestSPFreshIndexStatusTableFallbackOnFirstWorkerFailure(t *testing.T) {
	cases := []struct {
		name         string
		tableName    string
		indexName    string
		firstHandler func(t *testing.T, w http.ResponseWriter, r *http.Request)
	}{
		{
			name:      "retryable_http_status",
			tableName: "t_spfresh_status_fallback",
			indexName: "idx_fallback",
			firstHandler: func(t *testing.T, w http.ResponseWriter, r *http.Request) {
				http.Error(w, "unavailable", http.StatusServiceUnavailable)
			},
		},
		{
			name:      "body_read_failure",
			tableName: "t_spfresh_status_body_read_failure",
			indexName: "idx_body_read_failure",
			firstHandler: func(t *testing.T, w http.ResponseWriter, r *http.Request) {
				hijacker, ok := w.(http.Hijacker)
				require.True(t, ok)
				conn, _, err := hijacker.Hijack()
				require.NoError(t, err)
				_, _ = conn.Write([]byte("HTTP/1.1 200 OK\r\nContent-Length: 64\r\n\r\n{\"statuses\":"))
				require.NoError(t, conn.Close())
			},
		},
		{
			name:      "invalid_json",
			tableName: "t_spfresh_status_invalid_json",
			indexName: "idx_invalid_json",
			firstHandler: func(t *testing.T, w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusOK)
				_, _ = w.Write([]byte(`{"statuses":`))
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var firstHits atomic.Int32
			first := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if handleSPFreshStatusTestBootstrap(t, w, r) {
					return
				}
				require.Equal(t, "/spfresh/status/batch", r.URL.Path)
				firstHits.Add(1)
				tc.firstHandler(t, w, r)
			}))
			t.Cleanup(first.Close)

			var secondHits atomic.Int32
			second := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if handleSPFreshStatusTestBootstrap(t, w, r) {
					return
				}
				require.Equal(t, "/spfresh/status/batch", r.URL.Path)
				secondHits.Add(1)
				writeSPFreshStatusReadyResponse(t, w, r)
			}))
			t.Cleanup(second.Close)

			tk, _ := setupSPFreshStatusTestKit(t, first.URL)
			tk.MustExec(fmt.Sprintf("create table %s(id int primary key, vec vector(3))", tc.tableName))
			tk.MustExec(fmt.Sprintf("create vector index %s on %s ((vec_l2_distance(vec))) using spfresh", tc.indexName, tc.tableName))
			config.UpdateGlobal(func(conf *config.Config) {
				conf.TiKVAPIServiceAddrs = []string{first.URL, second.URL}
			})

			tk.MustQuery(fmt.Sprintf(`
				select index_state, is_ready
				from information_schema.tidb_spfresh_index_status
				where table_schema = 'test'
				  and table_name = '%s'`, tc.tableName),
			).Check(testkit.Rows("READY 1"))
			require.Positive(t, firstHits.Load())
			require.Positive(t, secondHits.Load())
		})
	}
}

func writeSPFreshStatusReadyResponse(t *testing.T, w http.ResponseWriter, r *http.Request) {
	var req statusBatchRequest
	body, err := io.ReadAll(r.Body)
	require.NoError(t, err)
	require.NoError(t, r.Body.Close())
	require.NoError(t, json.Unmarshal(body, &req))
	require.Len(t, req.Indexes, 1)

	w.Header().Set("Content-Type", "application/json")
	require.NoError(t, json.NewEncoder(w).Encode(statusBatchResponse{
		Statuses: []statusBatchRow{{
			KeyspaceID:     req.KeyspaceID,
			TableID:        req.Indexes[0].TableID,
			IndexID:        req.Indexes[0].IndexID,
			Dimensions:     3,
			DistanceMetric: "l2",
			AppliedBaseTS:  101,
			LagSeconds:     ptr[uint64](0),
		}},
	}))
}

func TestSPFreshIndexStatusTableUnknownWhenAllWorkersReturnInvalidJSON(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if handleSPFreshStatusTestBootstrap(t, w, r) {
			return
		}
		require.Equal(t, "/spfresh/status/batch", r.URL.Path)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"statuses":`))
	}))
	t.Cleanup(srv.Close)

	tk, _ := setupSPFreshStatusTestKit(t, srv.URL)
	tk.MustExec("create table t_spfresh_status_all_invalid_json(id int primary key, vec vector(3))")
	tk.MustExec("create vector index idx_all_invalid_json on t_spfresh_status_all_invalid_json ((vec_l2_distance(vec))) using spfresh")

	tk.MustQuery(`
		select index_state, is_ready
		from information_schema.tidb_spfresh_index_status
		where table_schema = 'test'
		  and table_name = 't_spfresh_status_all_invalid_json'`,
	).Check(testkit.Rows("UNKNOWN 0"))
}

func TestSPFreshIndexStatusTableBusinessErrorReturnsDirectly(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if handleSPFreshStatusTestBootstrap(t, w, r) {
			return
		}
		require.Equal(t, "/spfresh/status/batch", r.URL.Path)
		http.Error(w, "missing index", http.StatusNotFound)
	}))
	t.Cleanup(srv.Close)

	tk, _ := setupSPFreshStatusTestKit(t, srv.URL)
	tk.MustExec("create table t_spfresh_status_business_error(id int primary key, vec vector(3))")
	tk.MustExec("create vector index idx_business_error on t_spfresh_status_business_error ((vec_l2_distance(vec))) using spfresh")

	err := tk.QueryToErr(`
		select index_state
		from information_schema.tidb_spfresh_index_status
		where table_schema = 'test'
		  and table_name = 't_spfresh_status_business_error'`)
	require.Error(t, err)
	require.Contains(t, err.Error(), "http 404")
	require.Contains(t, err.Error(), "missing index")
}

func TestSPFreshIndexStatusTableInvalidEndpointConfigReturnsError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if handleSPFreshStatusTestBootstrap(t, w, r) {
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	t.Cleanup(srv.Close)

	tk, _ := setupSPFreshStatusTestKit(t, srv.URL)
	tk.MustExec("create table t_spfresh_status_invalid_config(id int primary key, vec vector(3))")
	tk.MustExec("create vector index idx_invalid_config on t_spfresh_status_invalid_config ((vec_l2_distance(vec))) using spfresh")
	config.UpdateGlobal(func(conf *config.Config) {
		conf.TiKVAPIServiceAddrs = []string{"http://127.0.0.1:1234/path"}
	})

	err := tk.QueryToErr(`
		select index_state
		from information_schema.tidb_spfresh_index_status
		where table_schema = 'test'
		  and table_name = 't_spfresh_status_invalid_config'`)
	require.Error(t, err)
	require.Contains(t, err.Error(), "path is not allowed")
}

func TestSPFreshIndexStatusTableUnknownOnMissingWorkerRow(t *testing.T) {
	var (
		mu       sync.Mutex
		requests []capturedStatusRequest
	)

	statuses := make(map[[2]int64]statusBatchRow)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if handleSPFreshStatusTestBootstrap(t, w, r) {
			return
		}
		require.Equal(t, "/spfresh/status/batch", r.URL.Path)
		body, err := io.ReadAll(r.Body)
		require.NoError(t, err)
		require.NoError(t, r.Body.Close())

		var req statusBatchRequest
		require.NoError(t, json.Unmarshal(body, &req))

		mu.Lock()
		requests = append(requests, capturedStatusRequest{
			clusterID: r.URL.Query().Get("cluster_id"),
			payload:   req,
		})
		mu.Unlock()

		resp := statusBatchResponse{
			Statuses: make([]statusBatchRow, 0, len(req.Indexes)),
		}
		for _, index := range req.Indexes {
			if status, ok := statuses[[2]int64{index.TableID, index.IndexID}]; ok {
				resp.Statuses = append(resp.Statuses, status)
			}
		}
		w.Header().Set("Content-Type", "application/json")
		require.NoError(t, json.NewEncoder(w).Encode(resp))
	}))
	t.Cleanup(srv.Close)

	tk, dom := setupSPFreshStatusTestKit(t, srv.URL)
	tk.MustExec("create table t_spfresh_status_missing(id int primary key, vec vector(3), vec2 vector(3))")
	tk.MustExec("create vector index idx_ready on t_spfresh_status_missing ((vec_l2_distance(vec))) using spfresh")
	tk.MustExec("create vector index idx_missing on t_spfresh_status_missing ((vec_cosine_distance(vec2))) using spfresh")

	readyTableID, readyIndexID := mustGetSPFreshIndexRef(t, dom, "t_spfresh_status_missing", "idx_ready")
	missingTableID, missingIndexID := mustGetSPFreshIndexRef(t, dom, "t_spfresh_status_missing", "idx_missing")
	clusterID := strconv.FormatUint(dom.GetPDClient().GetClusterID(context.Background()), 10)
	keyspaceID := uint32(tk.Session().GetStore().GetCodec().GetKeyspaceID())

	statuses[[2]int64{readyTableID, readyIndexID}] = statusBatchRow{
		KeyspaceID:         keyspaceID,
		TableID:            readyTableID,
		IndexID:            readyIndexID,
		Dimensions:         3,
		DistanceMetric:     "l2",
		AppliedBaseTS:      101,
		LagSeconds:         ptr[uint64](3),
		OwnerLeaseExpireTS: ptr[uint64](999),
	}

	tk.MustQuery(`
		select index_name, index_state, is_ready,
		       ifnull(cast(applied_base_ts as char), 'NULL'),
		       ifnull(cast(lag_seconds as char), 'NULL'),
		       ifnull(cast(owner_lease_expire_ts as char), 'NULL')
		from information_schema.tidb_spfresh_index_status
		where table_schema = 'test'
		  and table_name = 't_spfresh_status_missing'
		order by index_name`,
	).Check(testkit.Rows(
		"idx_missing UNKNOWN 0 NULL NULL NULL",
		"idx_ready READY 1 101 3 999",
	))

	mu.Lock()
	defer mu.Unlock()
	require.NotEmpty(t, requests)
	for _, req := range requests {
		require.Equal(t, clusterID, req.clusterID)
		require.Equal(t, keyspaceID, req.payload.KeyspaceID)
		require.ElementsMatch(t, []statusBatchIndex{
			{TableID: readyTableID, IndexID: readyIndexID},
			{TableID: missingTableID, IndexID: missingIndexID},
		}, req.payload.Indexes)
	}
}

func TestSPFreshIndexStatusTableUnknownOnMismatchedWorkerKeyspace(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if handleSPFreshStatusTestBootstrap(t, w, r) {
			return
		}
		require.Equal(t, "/spfresh/status/batch", r.URL.Path)

		var req statusBatchRequest
		body, err := io.ReadAll(r.Body)
		require.NoError(t, err)
		require.NoError(t, r.Body.Close())
		require.NoError(t, json.Unmarshal(body, &req))
		require.Len(t, req.Indexes, 1)

		w.Header().Set("Content-Type", "application/json")
		require.NoError(t, json.NewEncoder(w).Encode(statusBatchResponse{
			Statuses: []statusBatchRow{{
				KeyspaceID:         req.KeyspaceID + 1,
				TableID:            req.Indexes[0].TableID,
				IndexID:            req.Indexes[0].IndexID,
				Dimensions:         3,
				DistanceMetric:     "l2",
				AppliedBaseTS:      101,
				LagSeconds:         ptr[uint64](3),
				OwnerLeaseExpireTS: ptr[uint64](999),
			}},
		}))
	}))
	t.Cleanup(srv.Close)

	tk, _ := setupSPFreshStatusTestKit(t, srv.URL)
	tk.MustExec("create table t_spfresh_status_wrong_keyspace(id int primary key, vec vector(3))")
	tk.MustExec("create vector index idx_wrong_keyspace on t_spfresh_status_wrong_keyspace ((vec_l2_distance(vec))) using spfresh")

	tk.MustQuery(`
		select table_name, index_name, dimensions, distance_metric, index_state, is_ready,
		       applied_base_ts is null, lag_seconds is null, owner_lease_expire_ts is null, observed_at is not null
		from information_schema.tidb_spfresh_index_status
		where table_schema = 'test'
		  and table_name = 't_spfresh_status_wrong_keyspace'`,
	).Check(testkit.Rows(
		"t_spfresh_status_wrong_keyspace idx_wrong_keyspace 3 l2 UNKNOWN 0 1 1 1 1",
	))
}

func TestSPFreshIndexStatusTableUnknownOnMismatchedWorkerConfig(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if handleSPFreshStatusTestBootstrap(t, w, r) {
			return
		}
		require.Equal(t, "/spfresh/status/batch", r.URL.Path)

		var req statusBatchRequest
		body, err := io.ReadAll(r.Body)
		require.NoError(t, err)
		require.NoError(t, r.Body.Close())
		require.NoError(t, json.Unmarshal(body, &req))
		require.Len(t, req.Indexes, 1)

		w.Header().Set("Content-Type", "application/json")
		require.NoError(t, json.NewEncoder(w).Encode(statusBatchResponse{
			Statuses: []statusBatchRow{{
				KeyspaceID:         req.KeyspaceID,
				TableID:            req.Indexes[0].TableID,
				IndexID:            req.Indexes[0].IndexID,
				Dimensions:         4,
				DistanceMetric:     "cosine",
				AppliedBaseTS:      101,
				LagSeconds:         ptr[uint64](3),
				OwnerLeaseExpireTS: ptr[uint64](999),
			}},
		}))
	}))
	t.Cleanup(srv.Close)

	tk, _ := setupSPFreshStatusTestKit(t, srv.URL)
	tk.MustExec("create table t_spfresh_status_wrong_config(id int primary key, vec vector(3))")
	tk.MustExec("create vector index idx_wrong_config on t_spfresh_status_wrong_config ((vec_l2_distance(vec))) using spfresh")

	tk.MustQuery(`
		select table_name, index_name, dimensions, distance_metric, index_state, is_ready,
		       applied_base_ts is null, lag_seconds is null, owner_lease_expire_ts is null, observed_at is not null
		from information_schema.tidb_spfresh_index_status
		where table_schema = 'test'
		  and table_name = 't_spfresh_status_wrong_config'`,
	).Check(testkit.Rows(
		"t_spfresh_status_wrong_config idx_wrong_config 3 l2 UNKNOWN 0 1 1 1 1",
	))
}

func TestSPFreshIndexStatusTableUnknownOnIncompleteWorkerCheckpoint(t *testing.T) {
	tests := []struct {
		name          string
		appliedBaseTS uint64
		lagSeconds    *uint64
	}{
		{name: "zero applied base ts", lagSeconds: ptr[uint64](3)},
		{name: "missing lag seconds", appliedBaseTS: 101},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if handleSPFreshStatusTestBootstrap(t, w, r) {
					return
				}
				require.Equal(t, "/spfresh/status/batch", r.URL.Path)

				var req statusBatchRequest
				body, err := io.ReadAll(r.Body)
				require.NoError(t, err)
				require.NoError(t, r.Body.Close())
				require.NoError(t, json.Unmarshal(body, &req))
				require.Len(t, req.Indexes, 1)

				w.Header().Set("Content-Type", "application/json")
				require.NoError(t, json.NewEncoder(w).Encode(statusBatchResponse{
					Statuses: []statusBatchRow{{
						KeyspaceID:     req.KeyspaceID,
						TableID:        req.Indexes[0].TableID,
						IndexID:        req.Indexes[0].IndexID,
						Dimensions:     3,
						DistanceMetric: "l2",
						AppliedBaseTS:  tt.appliedBaseTS,
						LagSeconds:     tt.lagSeconds,
					}},
				}))
			}))
			t.Cleanup(srv.Close)

			tk, _ := setupSPFreshStatusTestKit(t, srv.URL)
			tk.MustExec("create table t_spfresh_status_incomplete(id int primary key, vec vector(3))")
			tk.MustExec("create vector index idx_incomplete on t_spfresh_status_incomplete ((vec_l2_distance(vec))) using spfresh")

			tk.MustQuery(`
				select index_state, is_ready,
				       applied_base_ts is null, lag_seconds is null, owner_lease_expire_ts is null
				from information_schema.tidb_spfresh_index_status
				where table_schema = 'test'
				  and table_name = 't_spfresh_status_incomplete'`,
			).Check(testkit.Rows("UNKNOWN 0 1 1 1"))
		})
	}
}

func TestSPFreshIndexStatusTableUnknownOnDuplicateWorkerRows(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if handleSPFreshStatusTestBootstrap(t, w, r) {
			return
		}
		require.Equal(t, "/spfresh/status/batch", r.URL.Path)

		var req statusBatchRequest
		body, err := io.ReadAll(r.Body)
		require.NoError(t, err)
		require.NoError(t, r.Body.Close())
		require.NoError(t, json.Unmarshal(body, &req))
		require.Len(t, req.Indexes, 1)

		w.Header().Set("Content-Type", "application/json")
		require.NoError(t, json.NewEncoder(w).Encode(statusBatchResponse{
			Statuses: []statusBatchRow{
				{
					KeyspaceID:         req.KeyspaceID,
					TableID:            req.Indexes[0].TableID,
					IndexID:            req.Indexes[0].IndexID,
					Dimensions:         3,
					DistanceMetric:     "l2",
					AppliedBaseTS:      101,
					LagSeconds:         ptr[uint64](3),
					OwnerLeaseExpireTS: ptr[uint64](999),
				},
				{
					KeyspaceID:         req.KeyspaceID,
					TableID:            req.Indexes[0].TableID,
					IndexID:            req.Indexes[0].IndexID,
					Dimensions:         3,
					DistanceMetric:     "l2",
					AppliedBaseTS:      202,
					LagSeconds:         ptr[uint64](5),
					OwnerLeaseExpireTS: ptr[uint64](1001),
				},
			},
		}))
	}))
	t.Cleanup(srv.Close)

	tk, _ := setupSPFreshStatusTestKit(t, srv.URL)
	tk.MustExec("create table t_spfresh_status_duplicate(id int primary key, vec vector(3))")
	tk.MustExec("create vector index idx_duplicate on t_spfresh_status_duplicate ((vec_l2_distance(vec))) using spfresh")

	tk.MustQuery(`
		select table_name, index_name, dimensions, distance_metric, index_state, is_ready,
		       applied_base_ts is null, lag_seconds is null, owner_lease_expire_ts is null, observed_at is not null
		from information_schema.tidb_spfresh_index_status
		where table_schema = 'test'
		  and table_name = 't_spfresh_status_duplicate'`,
	).Check(testkit.Rows(
		"t_spfresh_status_duplicate idx_duplicate 3 l2 UNKNOWN 0 1 1 1 1",
	))
}
