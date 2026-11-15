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
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/meta/model"
	pmodel "github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	fieldtypes "github.com/pingcap/tidb/pkg/parser/types"
	"github.com/pingcap/tidb/pkg/types"
)

func TestBootstrapReadySPFreshIndexesUsesDedicatedBootstrapEndpoint(t *testing.T) {
	reqCh := make(chan spfreshIndexRequest, 2)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Fatalf("method=%q, want %q", r.Method, http.MethodPost)
		}
		if r.URL.Path != spfreshBootstrapPath {
			t.Fatalf("path=%q, want %q", r.URL.Path, spfreshBootstrapPath)
		}
		if got := r.URL.Query().Get("cluster_id"); got != "7" {
			t.Fatalf("cluster_id=%q, want %q", got, "7")
		}
		if got := r.URL.Query().Get("task_id"); got != "" {
			t.Fatalf("task_id=%q, want empty", got)
		}

		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Fatalf("read body: %v", err)
		}
		var raw map[string]json.RawMessage
		if err := json.Unmarshal(body, &raw); err != nil {
			t.Fatalf("unmarshal raw body: %v", err)
		}
		for _, field := range []string{"build_id", "shard_id", "shard_ids"} {
			if _, ok := raw[field]; ok {
				t.Fatalf("bootstrap body contains build-only field %q: %s", field, body)
			}
		}
		if options, ok := raw["index_options"]; ok {
			var staticOptions map[string]json.RawMessage
			if err := json.Unmarshal(options, &staticOptions); err != nil {
				t.Errorf("unmarshal index options: %v", err)
			}
			if _, ok := staticOptions["build_shard"]; ok {
				t.Errorf("bootstrap body contains coordinator-only build_shard: %s", body)
			}
		}
		var req spfreshIndexRequest
		if err := json.Unmarshal(body, &req); err != nil {
			t.Fatalf("unmarshal body: %v", err)
		}
		reqCh <- req
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := BootstrapReadySPFreshIndexes(
		ctx,
		server.Client(),
		server.URL,
		7,
		2,
		newSPFreshBootstrapTestTableInfo(),
		[]int64{11, 13},
		12345,
	); err != nil {
		t.Fatalf("BootstrapReadySPFreshIndexes: %v", err)
	}

	first := waitBootstrapRequest(ctx, t, reqCh)
	if first.KeyspaceID != 2 || first.TableID != 101 || first.IndexID != 11 {
		t.Fatalf("first request=%+v", first)
	}
	if first.VectorColumnID != 2 || first.BaseSnapshotTS != 12345 || first.DistanceMetric != "cosine" {
		t.Fatalf("first request=%+v", first)
	}
	wantIndexOptions := &spfreshStaticIndexOptions{
		RotAlgorithm:     "matrix",
		MinPartitionSize: 32,
		MaxPartitionSize: 256,
		WriteBeamSize:    24,
	}
	if first.IndexOptions == nil || *first.IndexOptions != *wantIndexOptions {
		t.Fatalf("first index_options=%+v, want %+v", first.IndexOptions, wantIndexOptions)
	}
	if first.RuntimeConfig != model.DefaultSPFreshVectorRuntimeConfig() {
		t.Fatalf("first runtime_config=%+v", first.RuntimeConfig)
	}
	if first.HandleColumn == nil || first.HandleColumn.ColumnID != int64(model.ExtraHandleID) {
		t.Fatalf("first handle_column=%+v, want _tidb_rowid", first.HandleColumn)
	}
	firstHandleType := decodeSPFreshStoredColumnFieldType(t, first.HandleColumn.FieldType)
	if firstHandleType.GetTp() != int32(mysql.TypeLonglong) {
		t.Fatalf("first handle_column.field_type=%+v, want BIGINT", firstHandleType)
	}

	second := waitBootstrapRequest(ctx, t, reqCh)
	if second.KeyspaceID != 2 || second.TableID != 101 || second.IndexID != 13 {
		t.Fatalf("second request=%+v", second)
	}
	if second.VectorColumnID != 3 || second.BaseSnapshotTS != 12345 || second.DistanceMetric != "l2" {
		t.Fatalf("second request=%+v", second)
	}
	if second.IndexOptions != nil {
		t.Fatalf("second index_options=%+v, want nil", second.IndexOptions)
	}
	if len(second.StoredColumns) != 1 {
		t.Fatalf("second stored_columns=%+v, want one column", second.StoredColumns)
	}
	fieldType := decodeSPFreshStoredColumnFieldType(t, second.StoredColumns[0].FieldType)
	if second.StoredColumns[0].ColumnID != 1 || fieldType.GetTp() != int32(mysql.TypeLonglong) || fieldType.GetFlag()&uint32(mysql.UnsignedFlag) == 0 {
		t.Fatalf("second stored_columns=%+v, want unsigned bigint column_id=1", second.StoredColumns)
	}
}

func TestBootstrapReadySPFreshIndexesRetriesRetryableStatuses(t *testing.T) {
	retryableStatuses := []int{
		http.StatusRequestTimeout,
		http.StatusTooManyRequests,
		http.StatusInternalServerError,
		http.StatusBadGateway,
		http.StatusServiceUnavailable,
		http.StatusGatewayTimeout,
	}
	for _, status := range retryableStatuses {
		t.Run(strconv.Itoa(status), func(t *testing.T) {
			withNoLongOwnerBoundRetryDelay(t)

			attempts := 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				attempts++
				if attempts == 1 {
					http.Error(w, "retry later", status)
					return
				}
				w.WriteHeader(http.StatusOK)
			}))
			defer server.Close()

			err := BootstrapReadySPFreshIndexes(
				context.Background(),
				server.Client(),
				server.URL,
				7,
				2,
				newSPFreshBootstrapTestTableInfo(),
				[]int64{11},
				12345,
			)
			if err != nil {
				t.Fatalf("BootstrapReadySPFreshIndexes: %v", err)
			}
			if attempts != 2 {
				t.Fatalf("attempts=%d, want 2", attempts)
			}
		})
	}
}

func TestBootstrapReadySPFreshIndexesUsesLongRetryBudget(t *testing.T) {
	withNoLongOwnerBoundRetryDelay(t)

	attempts := 0
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts++
		if attempts == 4 {
			cancel()
		}
		http.Error(w, "worker down", http.StatusServiceUnavailable)
	}))
	defer server.Close()

	err := BootstrapReadySPFreshIndexes(
		ctx,
		server.Client(),
		server.URL,
		7,
		2,
		newSPFreshBootstrapTestTableInfo(),
		[]int64{11},
		12345,
	)
	if err == nil {
		t.Fatal("BootstrapReadySPFreshIndexes succeeded, want error")
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("BootstrapReadySPFreshIndexes error=%T %[1]v, want context.Canceled", err)
	}
	if attempts < 4 {
		t.Fatalf("attempts=%d, want at least 4", attempts)
	}
}

func TestBootstrapReadySPFreshIndexesDoesNotRetryBusinessStatuses(t *testing.T) {
	withNoLongOwnerBoundRetryDelay(t)

	attempts := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts++
		http.Error(w, "bad lifecycle request", http.StatusConflict)
	}))
	defer server.Close()

	err := BootstrapReadySPFreshIndexes(
		context.Background(),
		server.Client(),
		server.URL,
		7,
		2,
		newSPFreshBootstrapTestTableInfo(),
		[]int64{11},
		12345,
	)
	if err == nil {
		t.Fatal("BootstrapReadySPFreshIndexes succeeded, want error")
	}
	if attempts != 1 {
		t.Fatalf("attempts=%d, want 1", attempts)
	}
}

func TestBootstrapReadySPFreshIndexesDoesNotFollowRedirect(t *testing.T) {
	redirectRequests := 0
	redirectServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		redirectRequests++
		w.WriteHeader(http.StatusOK)
	}))
	defer redirectServer.Close()

	ownerRequests := 0
	ownerServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ownerRequests++
		http.Redirect(w, r, redirectServer.URL+spfreshBootstrapPath, http.StatusFound)
	}))
	defer ownerServer.Close()

	err := BootstrapReadySPFreshIndexes(
		context.Background(),
		ownerServer.Client(),
		ownerServer.URL,
		7,
		2,
		newSPFreshBootstrapTestTableInfo(),
		[]int64{11},
		12345,
	)
	if err == nil {
		t.Fatal("BootstrapReadySPFreshIndexes succeeded, want redirect response error")
	}
	if ownerRequests != 1 {
		t.Fatalf("ownerRequests=%d, want 1", ownerRequests)
	}
	if redirectRequests != 0 {
		t.Fatalf("redirectRequests=%d, want 0", redirectRequests)
	}
}

func waitBootstrapRequest(
	ctx context.Context,
	t *testing.T,
	reqCh <-chan spfreshIndexRequest,
) spfreshIndexRequest {
	t.Helper()
	select {
	case req := <-reqCh:
		return req
	case <-ctx.Done():
		t.Fatalf("timed out waiting for request: %v", ctx.Err())
		return spfreshIndexRequest{}
	}
}

func newSPFreshBootstrapTestTableInfo() *model.TableInfo {
	runtimeConfig := model.DefaultSPFreshVectorRuntimeConfig()
	vecType1 := fieldtypes.NewFieldType(mysql.TypeTiDBVectorFloat32)
	vecType1.SetFlen(3)
	vecType2 := fieldtypes.NewFieldType(mysql.TypeTiDBVectorFloat32)
	vecType2.SetFlen(4)

	return &model.TableInfo{
		ID:   101,
		Name: pmodel.NewCIStr("t"),
		Columns: []*model.ColumnInfo{
			{
				ID:     1,
				Name:   pmodel.NewCIStr("id"),
				Offset: 0,
				State:  model.StatePublic,
				FieldType: func() types.FieldType {
					ft := fieldtypes.NewFieldType(mysql.TypeLonglong)
					ft.AddFlag(mysql.UnsignedFlag)
					return *ft
				}(),
			},
			{
				ID:        2,
				Name:      pmodel.NewCIStr("vec1"),
				Offset:    1,
				State:     model.StatePublic,
				FieldType: *vecType1,
			},
			{
				ID:        3,
				Name:      pmodel.NewCIStr("vec2"),
				Offset:    2,
				State:     model.StatePublic,
				FieldType: *vecType2,
			},
		},
		Indices: []*model.IndexInfo{
			{
				ID:    11,
				Name:  pmodel.NewCIStr("idx_sp_1"),
				Table: pmodel.NewCIStr("t"),
				State: model.StatePublic,
				Tp:    pmodel.IndexTypeSPFresh,
				Columns: []*model.IndexColumn{
					{Name: pmodel.NewCIStr("vec1"), Offset: 1, Length: fieldtypes.UnspecifiedLength},
				},
				VectorInfo: &model.VectorIndexInfo{
					Kind:           model.VectorIndexKindSPFresh,
					Dimension:      3,
					DistanceMetric: model.DistanceMetricCosine,
					IndexOptions: &model.SPFreshVectorIndexOptions{
						RotAlgorithm:     "matrix",
						MinPartitionSize: 32,
						MaxPartitionSize: 256,
						BuildShard:       4,
						WriteBeamSize:    24,
					},
					RuntimeConfig: &runtimeConfig,
				},
			},
			{
				ID:    13,
				Name:  pmodel.NewCIStr("idx_sp_2"),
				Table: pmodel.NewCIStr("t"),
				State: model.StatePublic,
				Tp:    pmodel.IndexTypeSPFresh,
				Columns: []*model.IndexColumn{
					{Name: pmodel.NewCIStr("vec2"), Offset: 2, Length: fieldtypes.UnspecifiedLength},
				},
				VectorInfo: &model.VectorIndexInfo{
					Kind:            model.VectorIndexKindSPFresh,
					Dimension:       4,
					DistanceMetric:  model.DistanceMetricL2,
					RuntimeConfig:   &runtimeConfig,
					StoredColumnIDs: []int64{1},
				},
			},
		},
	}
}
