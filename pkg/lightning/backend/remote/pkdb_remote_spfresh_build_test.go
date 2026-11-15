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
	"strings"
	"testing"
	"time"

	"github.com/golang/protobuf/proto"
	pingcaperrors "github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/lightning/checkpoints"
	"github.com/pingcap/tidb/pkg/lightning/common"
	"github.com/pingcap/tidb/pkg/meta/model"
	pmodel "github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/dbterror"
	"github.com/pingcap/tipb/go-tipb"
)

func decodeSPFreshStoredColumnFieldType(t *testing.T, data []byte) *tipb.FieldType {
	t.Helper()

	var fieldType tipb.FieldType
	if err := proto.Unmarshal(data, &fieldType); err != nil {
		t.Fatalf("unmarshal stored column field_type: %v", err)
	}
	return &fieldType
}

func newSPFreshBuildTestTableInfo(tableID, indexID, vectorColumnID int64, vectorIndex *model.VectorIndexInfo) *checkpoints.TidbTableInfo {
	return &checkpoints.TidbTableInfo{
		ID: tableID,
		Core: &model.TableInfo{
			ID: tableID,
			Columns: []*model.ColumnInfo{
				{
					ID:        vectorColumnID,
					Name:      pmodel.NewCIStr("vec"),
					Offset:    0,
					FieldType: *types.NewFieldType(mysql.TypeTiDBVectorFloat32),
				},
			},
			Indices: []*model.IndexInfo{
				{
					ID: indexID,
					Tp: pmodel.IndexTypeSPFresh,
					Columns: []*model.IndexColumn{
						{Name: pmodel.NewCIStr("vec"), Offset: 0},
					},
					VectorInfo: vectorIndex,
				},
			},
		},
	}
}

func TestSealSPFreshBuildShardSendsSealAndAcceptsSealed(t *testing.T) {
	client := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		requireSPFreshBuildRequest(t, r)
		if r.Method != http.MethodPost {
			t.Fatalf("method=%q, want POST", r.Method)
		}
		if got := r.URL.Query().Get("seal"); got != "true" {
			t.Fatalf("seal=%q, want true", got)
		}
		return jsonResponse(r, `{"task_id":"task-1","status":"sealed","finished":true}`), nil
	})}

	if err := SealSPFreshBuildShard(context.Background(), client, "http://spfresh-build.test", 7, "task-1", testSPFreshBuildSealRequest()); err != nil {
		t.Fatalf("SealSPFreshBuildShard: %v", err)
	}
}

func TestSealSPFreshBuildShardPollsAcceptedRecovery(t *testing.T) {
	attempts := 0
	client := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		requireSPFreshBuildRequest(t, r)
		if r.Method != http.MethodPost {
			t.Fatalf("method=%q, want POST", r.Method)
		}
		if got := r.URL.Query().Get("seal"); got != "true" {
			t.Fatalf("seal=%q, want true", got)
		}
		attempts++
		if attempts == 1 {
			resp := jsonResponse(r, `{
				"task-id":"task-1",
				"status":"retrying",
				"retry-reason":"deadlock",
				"retry-attempt":2,
				"next-retry-at-ms":0
			}`)
			resp.StatusCode = http.StatusAccepted
			resp.Status = http.StatusText(http.StatusAccepted)
			return resp, nil
		}
		return jsonResponse(r, `{"task-id":"task-1","status":"sealed","finished":true}`), nil
	})}

	err := SealSPFreshBuildShard(
		context.Background(),
		client,
		"http://spfresh-build.test",
		7,
		"task-1",
		testSPFreshBuildSealRequest(),
	)
	if err != nil {
		t.Fatalf("SealSPFreshBuildShard: %v", err)
	}
	if attempts != 2 {
		t.Fatalf("seal attempts=%d, want 2", attempts)
	}
}

func TestSealSPFreshBuildShardAcceptedRecoveryHonorsContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	client := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		cancel()
		resp := jsonResponse(r, `{
			"task-id":"task-1",
			"status":"retrying",
			"retry-reason":"deadlock",
			"retry-attempt":2,
			"next-retry-at-ms":0
		}`)
		resp.StatusCode = http.StatusAccepted
		resp.Status = http.StatusText(http.StatusAccepted)
		return resp, nil
	})}

	err := SealSPFreshBuildShard(ctx, client, "http://spfresh-build.test", 7, "task-1", testSPFreshBuildSealRequest())
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("SealSPFreshBuildShard error=%v, want context.Canceled", err)
	}
}

func TestSealSPFreshBuildShardPollsMalformedAcceptedRecovery(t *testing.T) {
	attempts := 0
	client := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		requireSPFreshBuildRequest(t, r)
		attempts++
		if attempts == 1 {
			resp := jsonResponse(r, "not-json")
			resp.StatusCode = http.StatusAccepted
			resp.Status = http.StatusText(http.StatusAccepted)
			return resp, nil
		}
		return jsonResponse(r, `{"status":"sealed"}`), nil
	})}

	err := SealSPFreshBuildShard(
		context.Background(),
		client,
		"http://spfresh-build.test",
		7,
		"task-1",
		testSPFreshBuildSealRequest(),
	)
	if err != nil {
		t.Fatalf("SealSPFreshBuildShard: %v", err)
	}
	if attempts != 2 {
		t.Fatalf("seal attempts=%d, want 2", attempts)
	}
}

func TestSPFreshBuildRetryDelayIsBounded(t *testing.T) {
	if got := spfreshBuildRetryDelay(0); got != 100*time.Millisecond {
		t.Fatalf("retry delay at attempt 0=%s, want 100ms", got)
	}
	if got := spfreshBuildRetryDelay(1); got != 200*time.Millisecond {
		t.Fatalf("retry delay at attempt 1=%s, want 200ms", got)
	}
	if got := spfreshBuildRetryDelay(4); got != 1600*time.Millisecond {
		t.Fatalf("retry delay at attempt 4=%s, want 1.6s", got)
	}
	if got := spfreshBuildRetryDelay(5); got != spfreshBuildRetryMaxDelay {
		t.Fatalf("retry delay at attempt 5=%s, want %s", got, spfreshBuildRetryMaxDelay)
	}
	if got := spfreshBuildRetryDelay(100); got != spfreshBuildRetryMaxDelay {
		t.Fatalf("retry delay at attempt 100=%s, want %s", got, spfreshBuildRetryMaxDelay)
	}
}

func TestWaitForSPFreshBuildRetryLimitsMissingMetadata(t *testing.T) {
	count, err := waitForSPFreshBuildRetry(
		context.Background(),
		nil,
		maxConsecutiveSPFreshRecoveryMetadataMisses,
	)
	if err == nil {
		t.Fatal("waitForSPFreshBuildRetry() error=nil, want missing metadata error")
	}
	if count != maxConsecutiveSPFreshRecoveryMetadataMisses {
		t.Fatalf("missing metadata count=%d, want %d", count, maxConsecutiveSPFreshRecoveryMetadataMisses)
	}
}

func TestSealSPFreshBuildShardRejectsEmptyResponse(t *testing.T) {
	client := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		return jsonResponse(r, ""), nil
	})}

	err := SealSPFreshBuildShard(context.Background(), client, "http://spfresh-build.test", 7, "task-1", testSPFreshBuildSealRequest())
	if err == nil {
		t.Fatal("SealSPFreshBuildShard(empty response) = nil, want error")
	}
}

func TestSealSPFreshBuildShardRejectsOversizedResponse(t *testing.T) {
	client := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		return jsonResponse(r, strings.Repeat("x", maxSPFreshControlBodyBytes+1)), nil
	})}

	err := SealSPFreshBuildShard(context.Background(), client, "http://spfresh-build.test", 7, "task-1", testSPFreshBuildSealRequest())
	if err == nil || !strings.Contains(err.Error(), "response exceeds") {
		t.Fatalf("SealSPFreshBuildShard(oversized response) error=%v, want response exceeds", err)
	}
}

func TestCommitSPFreshBuildReadySendsBody(t *testing.T) {
	var got SPFreshBuildCommitRequest
	client := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		requireSPFreshBuildCommitRequest(t, r)
		if r.Method != http.MethodPost {
			t.Fatalf("method=%q, want POST", r.Method)
		}
		if err := json.NewDecoder(r.Body).Decode(&got); err != nil {
			t.Fatalf("decode body: %v", err)
		}
		return spfreshBuildResponse(r, http.StatusNoContent, ""), nil
	})}

	want := SPFreshBuildCommitRequest{
		BuildID:    "build-1",
		KeyspaceID: 2,
		TableID:    3,
		IndexID:    4,
	}
	if err := CommitSPFreshBuildReady(context.Background(), client, "http://spfresh-build.test", 7, want); err != nil {
		t.Fatalf("CommitSPFreshBuildReady: %v", err)
	}
	if got != want {
		t.Fatalf("commit body=%+v, want %+v", got, want)
	}
}

func TestIsRetryableSPFreshBuildCommitError(t *testing.T) {
	if !IsRetryableSPFreshBuildCommitError(&SPFreshControlRequestError{
		Operation: "commit", StatusCode: http.StatusInternalServerError,
	}) {
		t.Fatal("IsRetryableSPFreshBuildCommitError(http 500) = false, want true")
	}
	if IsRetryableSPFreshBuildCommitError(&SPFreshControlRequestError{
		Operation: "commit", StatusCode: http.StatusBadRequest,
	}) {
		t.Fatal("IsRetryableSPFreshBuildCommitError(http 400) = true, want false")
	}

	if !IsRetryableSPFreshBuildCommitError(&SPFreshControlSendError{
		Method: http.MethodPost,
		URL:    "http://spfresh-build.test/spfresh/build/commit",
		Err:    errors.New("connection reset"),
	}) {
		t.Fatal("IsRetryableSPFreshBuildCommitError(send error) = false, want true")
	}
	if IsRetryableSPFreshBuildCommitError(context.Canceled) {
		t.Fatal("IsRetryableSPFreshBuildCommitError(context.Canceled) = true, want false")
	}
}

func TestSPFreshControlRequestErrorWithBodyIsRetryable(t *testing.T) {
	controlErr := &SPFreshControlRequestError{
		Operation:  "seal",
		Status:     http.StatusText(http.StatusAccepted),
		StatusCode: http.StatusAccepted,
		Body:       "transient recovery in progress",
		cause:      dbterror.ErrIngestFailed.FastGenByArgs("transient recovery in progress"),
	}
	wrappedErr := pingcaperrors.WithStack(controlErr)

	if !controlErr.Retryable() {
		t.Fatal("SPFreshControlRequestError.Retryable() = false, want true")
	}
	if !common.IsRetryableError(wrappedErr) {
		t.Fatal("common.IsRetryableError(wrapped SPFreshControlRequestError) = false, want true")
	}

	controlErr.StatusCode = http.StatusBadRequest
	if common.IsRetryableError(wrappedErr) {
		t.Fatal("common.IsRetryableError(http 400 SPFreshControlRequestError) = true, want false")
	}
}

func TestDeleteSPFreshBuildTasksByPrefixSendsPrefixDelete(t *testing.T) {
	const taskIDPrefix = "1-2-3-4-endpoint-"
	client := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		if r.URL.Path != "/spfresh/build" {
			t.Fatalf("path=%q, want %q", r.URL.Path, "/spfresh/build")
		}
		if r.Method != http.MethodDelete {
			t.Fatalf("method=%q, want DELETE", r.Method)
		}
		if got := r.URL.Query().Get("cluster_id"); got != "7" {
			t.Fatalf("cluster_id=%q, want %q", got, "7")
		}
		if got := r.URL.Query().Get("task_id_prefix"); got != taskIDPrefix {
			t.Fatalf("task_id_prefix=%q, want %q", got, taskIDPrefix)
		}
		return jsonResponse(r, ""), nil
	})}

	if err := DeleteSPFreshBuildTasksByPrefix(context.Background(), client, "http://spfresh-build.test", 7, taskIDPrefix); err != nil {
		t.Fatalf("DeleteSPFreshBuildTasksByPrefix: %v", err)
	}
}

func TestSealSPFreshBuildShardUsesLongRetryBudget(t *testing.T) {
	withNoLongOwnerBoundRetryDelay(t)

	attempts := 0
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	client := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		if err := r.Context().Err(); err != nil {
			return nil, err
		}
		requireSPFreshBuildRequest(t, r)
		attempts++
		if attempts == 4 {
			cancel()
		}
		return spfreshBuildResponse(r, http.StatusServiceUnavailable, "retry later"), nil
	})}

	err := SealSPFreshBuildShard(ctx, client, "http://spfresh-build.test", 7, "task-1", testSPFreshBuildSealRequest())
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("SealSPFreshBuildShard error=%T %[1]v, want context.Canceled", err)
	}
	if attempts < 4 {
		t.Fatalf("attempts=%d, want at least 4", attempts)
	}
}

func TestSealSPFreshBuildShardRejectsMissingShard(t *testing.T) {
	client := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		requireSPFreshBuildRequest(t, r)
		return spfreshBuildResponse(r, http.StatusNotFound, "task not found"), nil
	})}

	if err := SealSPFreshBuildShard(context.Background(), client, "http://spfresh-build.test", 7, "task-1", testSPFreshBuildSealRequest()); err == nil {
		t.Fatal("SealSPFreshBuildShard succeeded, want missing shard error")
	}
}

func requireSPFreshBuildRequest(t *testing.T, r *http.Request) {
	t.Helper()
	if r.URL.Path != "/spfresh/build" {
		t.Fatalf("path=%q, want %q", r.URL.Path, "/spfresh/build")
	}
	if got := r.URL.Query().Get("cluster_id"); got != "7" {
		t.Fatalf("cluster_id=%q, want %q", got, "7")
	}
	if got := r.URL.Query().Get("task_id"); got != "task-1" {
		t.Fatalf("task_id=%q, want %q", got, "task-1")
	}
}

func requireSPFreshBuildCommitRequest(t *testing.T, r *http.Request) {
	t.Helper()
	if r.URL.Path != "/spfresh/build/commit" {
		t.Fatalf("path=%q, want %q", r.URL.Path, "/spfresh/build/commit")
	}
	if got := r.URL.Query().Get("cluster_id"); got != "7" {
		t.Fatalf("cluster_id=%q, want %q", got, "7")
	}
	if got := r.URL.Query().Get("task_id"); got != "" {
		t.Fatalf("task_id=%q, want empty", got)
	}
	if got := r.URL.Query().Get("commit"); got != "" {
		t.Fatalf("commit=%q, want empty", got)
	}
}

func spfreshBuildResponse(req *http.Request, statusCode int, body string) *http.Response {
	return &http.Response{
		StatusCode: statusCode,
		Status:     http.StatusText(statusCode),
		Body:       io.NopCloser(strings.NewReader(body)),
		Header:     make(http.Header),
		Request:    req,
	}
}
