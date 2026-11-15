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

package executor

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/executor/internal/exec"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/pingcap/tipb/go-tipb"
	"github.com/stretchr/testify/require"
)

func TestSPFreshVectorSearchRejectsOversizedRequest(t *testing.T) {
	req := &tipb.SPFreshSearchRequest{TopK: 1, QueryVectorF32Le: make([]byte, maxVectorSearchRequestBytes)}

	_, err := (&SPFreshVectorSearchExec{}).vectorSearch(context.Background(), 1, req)

	require.ErrorContains(t, err, "request exceeds")
}

func TestSPFreshSearchTimeoutMS(t *testing.T) {
	now := time.Unix(100, 0)
	cases := []struct {
		name     string
		deadline time.Duration
		want     uint64
	}{
		{name: "expired", deadline: -time.Millisecond, want: 0},
		{name: "sub_millisecond", deadline: time.Nanosecond, want: 1},
		{name: "round_up", deadline: 1500 * time.Microsecond, want: 2},
		{name: "exact_millisecond", deadline: 1500 * time.Millisecond, want: 1500},
		{name: "cap", deadline: maxSPFreshSearchTimeout + time.Second, want: 60_000},
	}

	require.Equal(t, uint64(20_000), spfreshSearchTimeoutMSAt(context.Background(), now))
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithDeadline(context.Background(), now.Add(tc.deadline))
			defer cancel()
			require.Equal(t, tc.want, spfreshSearchTimeoutMSAt(ctx, now))
		})
	}
}

func TestSPFreshVectorSearchSetsRequestBudgetAndDefaultTimeout(t *testing.T) {
	ResetSPFreshVectorSearchForTest()

	var gotTimeout atomic.Value
	server := newSPFreshSearchTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		gotTimeout.Store(r.URL.Query().Get("timeout_ms"))
		writeSPFreshSearchProto(w, &tipb.SPFreshSearchResponse{})
	})
	t.Cleanup(server.Close)

	restore := config.RestoreFunc()
	t.Cleanup(restore)
	config.UpdateGlobal(func(conf *config.Config) {
		conf.TiKVAPIServiceAddrs = []string{server.URL}
	})

	req := &tipb.SPFreshSearchRequest{TopK: 1}
	_, err := (&SPFreshVectorSearchExec{}).vectorSearch(context.Background(), 1, req)
	require.NoError(t, err)
	require.Equal(t, uint64(maxVectorSearchResponseBytes), req.GetMaxResponseBytes())
	require.Zero(t, req.GetOversampleFactor())
	require.Equal(t, "20000", gotTimeout.Load())
}

func TestSPFreshVectorSearchDefaultTimeoutBoundsHTTPRequest(t *testing.T) {
	ResetSPFreshVectorSearchForTest()

	oldDefaultTimeout := defaultSPFreshSearchTimeout
	oldResponseGrace := spfreshSearchResponseGrace
	defaultSPFreshSearchTimeout = 20 * time.Millisecond
	spfreshSearchResponseGrace = 10 * time.Millisecond
	t.Cleanup(func() {
		defaultSPFreshSearchTimeout = oldDefaultTimeout
		spfreshSearchResponseGrace = oldResponseGrace
	})

	var hits atomic.Int32
	servers := make([]*httptest.Server, 2)
	for i := range servers {
		servers[i] = newSPFreshSearchTestServer(t, func(w http.ResponseWriter, r *http.Request) {
			hits.Add(1)
			select {
			case <-time.After(250 * time.Millisecond):
				writeSPFreshSearchProto(w, &tipb.SPFreshSearchResponse{})
			case <-r.Context().Done():
			}
		})
		t.Cleanup(servers[i].Close)
	}

	restore := config.RestoreFunc()
	t.Cleanup(restore)
	config.UpdateGlobal(func(conf *config.Config) {
		conf.TiKVAPIServiceAddrs = []string{servers[0].URL, servers[1].URL}
	})

	start := time.Now()
	_, err := (&SPFreshVectorSearchExec{}).vectorSearch(context.Background(), 1, &tipb.SPFreshSearchRequest{TopK: 1})
	elapsed := time.Since(start)

	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Less(t, elapsed, 200*time.Millisecond)
	require.Equal(t, int32(1), hits.Load())

	states, stateErr := spfreshRouter.configuredEndpointStates()
	require.NoError(t, stateErr)
	require.Len(t, states, 2)
	for _, state := range states {
		require.Zero(t, state.failures.Load())
	}
}

func TestSPFreshVectorSearchDoesNotSendExpiredContext(t *testing.T) {
	ResetSPFreshVectorSearchForTest()

	var hits atomic.Int32
	server := newSPFreshSearchTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		writeSPFreshSearchProto(w, &tipb.SPFreshSearchResponse{})
	})
	t.Cleanup(server.Close)

	restore := config.RestoreFunc()
	t.Cleanup(restore)
	config.UpdateGlobal(func(conf *config.Config) {
		conf.TiKVAPIServiceAddrs = []string{server.URL}
	})

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := (&SPFreshVectorSearchExec{}).vectorSearch(ctx, 1, &tipb.SPFreshSearchRequest{TopK: 1})
	require.ErrorIs(t, err, context.Canceled)
	require.Zero(t, hits.Load())
}

func TestSPFreshVectorSearchRejectsCandidateBudgetBeforeSending(t *testing.T) {
	_, err := (&SPFreshVectorSearchExec{}).vectorSearch(context.Background(), 1, &tipb.SPFreshSearchRequest{
		TopK:             5121,
		OversampleFactor: 2,
	})
	require.ErrorContains(t, err, "rough candidate target exceeds 10240")
}

func TestSPFreshVectorSearchRejectsTopKBeforeSending(t *testing.T) {
	_, err := (&SPFreshVectorSearchExec{}).vectorSearch(context.Background(), 1, &tipb.SPFreshSearchRequest{
		TopK: model.SPFreshMaxTopK + 1,
	})
	require.ErrorContains(t, err, "top_k must be in [1, 8192]")
}

func TestSPFreshVectorSearchRejectsNilRow(t *testing.T) {
	e := &SPFreshVectorSearchExec{
		BaseExecutor: exec.NewBaseExecutor(mock.NewContext(), expression.NewSchema(), 0),
	}

	_, err := e.decodeSPFreshSearchRows([]*tipb.SPFreshSearchRow{nil})

	require.ErrorContains(t, err, "spfresh /spfresh/search returned nil row")
}

func TestSPFreshVectorSearchMalformedResponseMarksEndpointFailure(t *testing.T) {
	ResetSPFreshVectorSearchForTest()
	server := newSPFreshSearchTestServer(t, func(w http.ResponseWriter, _ *http.Request) {
		respBytes, err := proto.Marshal(&tipb.SPFreshSearchResponse{})
		require.NoError(t, err)
		w.Header().Set("Content-Type", "application/x-protobuf")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(respBytes)
	})
	t.Cleanup(server.Close)

	cfg := config.GetGlobalConfig()
	oldEndpoints := append([]string(nil), cfg.TiKVAPIServiceAddrs...)
	cfg.TiKVAPIServiceAddrs = []string{server.URL}
	t.Cleanup(func() { cfg.TiKVAPIServiceAddrs = oldEndpoints })
	defer ResetSPFreshVectorSearchForTest()

	_, err := (&SPFreshVectorSearchExec{}).vectorSearch(context.Background(), 1, &tipb.SPFreshSearchRequest{TopK: 1})
	require.ErrorContains(t, err, "unset result")

	states, stateErr := spfreshRouter.configuredEndpointStates()
	require.NoError(t, stateErr)
	require.Len(t, states, 1)
	require.False(t, states[0].healthy(time.Now()))
}

func TestSPFreshFilterWarningsBoundsHugeOmittedCount(t *testing.T) {
	ctx := mock.NewContext()
	stmtCtx := ctx.GetSessionVars().StmtCtx
	stmtCtx.AddOmittedWarnings(uint64(^uint16(0) - 2))
	e := &SPFreshVectorSearchExec{
		BaseExecutor: exec.NewBaseExecutor(ctx, expression.NewSchema(), 0),
	}

	warnings, omitted, err := e.spfreshFilterWarnings(&tipb.SPFreshSearchResult{WarningCount: ^uint64(0)})

	require.NoError(t, err)
	require.Empty(t, warnings)
	require.Equal(t, uint64(2), omitted)
}

func TestSPFreshSearchEndpointOrderStartsWithP2CAndAppendsHealthyConfigured(t *testing.T) {
	ResetSPFreshVectorSearchForTest()

	restore := config.RestoreFunc()
	t.Cleanup(restore)
	config.UpdateGlobal(func(conf *config.Config) {
		conf.TiKVAPIServiceAddrs = []string{"worker-0:19000", "worker-1:19000", "worker-2:19000"}
	})

	states, err := spfreshRouter.configuredEndpointStates()
	require.NoError(t, err)
	require.Len(t, states, 3)
	states[1].markFailure(time.Now())

	order, err := spfreshRouter.searchEndpointOrder()
	require.NoError(t, err)
	require.Equal(t, []*spfreshEndpointState{states[0], states[2]}, order)
}

func TestSPFreshSearchEndpointOrderRotatesFromLowerInflightSecondCandidate(t *testing.T) {
	ResetSPFreshVectorSearchForTest()

	restore := config.RestoreFunc()
	t.Cleanup(restore)
	config.UpdateGlobal(func(conf *config.Config) {
		conf.TiKVAPIServiceAddrs = []string{"worker-0:19000", "worker-1:19000", "worker-2:19000"}
	})

	states, err := spfreshRouter.configuredEndpointStates()
	require.NoError(t, err)
	require.Len(t, states, 3)

	salt := uint64(0)
	spfreshRouter.searchID.Store(salt)
	firstIdx := int(salt % uint64(len(states)))
	secondIdx := int(splitmix64(salt+0x9e3779b97f4a7c15) % uint64(len(states)-1))
	if secondIdx >= firstIdx {
		secondIdx++
	}
	states[firstIdx].searchInflight.Store(10)

	order, err := spfreshRouter.searchEndpointOrder()
	require.NoError(t, err)
	require.Equal(t, []*spfreshEndpointState{
		states[secondIdx],
		states[(secondIdx+1)%len(states)],
		states[(secondIdx+2)%len(states)],
	}, order)
}

func TestSPFreshVectorSearchFallbackAfterSelectedEndpointReadRetryableStatus(t *testing.T) {
	cases := []struct {
		name       string
		statusCode int
	}{
		{name: "request_timeout", statusCode: http.StatusRequestTimeout},
		{name: "too_many_requests", statusCode: http.StatusTooManyRequests},
		{name: "internal", statusCode: http.StatusInternalServerError},
		{name: "bad_gateway", statusCode: http.StatusBadGateway},
		{name: "unavailable", statusCode: http.StatusServiceUnavailable},
		{name: "gateway_timeout", statusCode: http.StatusGatewayTimeout},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ResetSPFreshVectorSearchForTest()

			var badHits atomic.Int32
			bad := newSPFreshSearchTestServer(t, func(w http.ResponseWriter, r *http.Request) {
				badHits.Add(1)
				http.Error(w, "retryable", tc.statusCode)
			})
			t.Cleanup(bad.Close)

			var goodHits atomic.Int32
			good := newSPFreshSearchTestServer(t, func(w http.ResponseWriter, r *http.Request) {
				goodHits.Add(1)
				writeSPFreshSearchProto(w, &tipb.SPFreshSearchResponse{})
			})
			t.Cleanup(good.Close)

			restore := config.RestoreFunc()
			t.Cleanup(restore)
			config.UpdateGlobal(func(conf *config.Config) {
				conf.TiKVAPIServiceAddrs = []string{bad.URL, good.URL}
			})

			states, err := spfreshRouter.configuredEndpointStates()
			require.NoError(t, err)
			require.Len(t, states, 2)

			_, err = (&SPFreshVectorSearchExec{}).vectorSearch(context.Background(), 1, &tipb.SPFreshSearchRequest{TopK: 1})
			require.NoError(t, err)
			require.Equal(t, int32(1), badHits.Load())
			require.Equal(t, int32(1), goodHits.Load())
			require.Zero(t, states[0].searchInflight.Load())
			require.Zero(t, states[1].searchInflight.Load())
			require.Positive(t, states[0].failures.Load())
			require.Zero(t, states[1].failures.Load())
		})
	}
}

func TestSPFreshVectorSearchNoFallbackOnStructuredCorruption(t *testing.T) {
	ResetSPFreshVectorSearchForTest()

	var corruptedHits atomic.Int32
	corrupted := newSPFreshSearchTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		corruptedHits.Add(1)
		writeSPFreshSearchProto(w, spfreshErrorResponseForTest(&tipb.Error{
			Code: int32(tipb.SPFreshErrorCode_SPFreshIndexCorruption),
			Msg:  "persist NeedsRebuild failed",
		}))
	})
	t.Cleanup(corrupted.Close)

	var fallbackHits atomic.Int32
	fallback := newSPFreshSearchTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		fallbackHits.Add(1)
		writeSPFreshSearchProto(w, &tipb.SPFreshSearchResponse{})
	})
	t.Cleanup(fallback.Close)

	restore := config.RestoreFunc()
	t.Cleanup(restore)
	config.UpdateGlobal(func(conf *config.Config) {
		conf.TiKVAPIServiceAddrs = []string{corrupted.URL, fallback.URL}
	})

	states, err := spfreshRouter.configuredEndpointStates()
	require.NoError(t, err)
	require.Len(t, states, 2)

	_, err = (&SPFreshVectorSearchExec{}).vectorSearch(context.Background(), 1, &tipb.SPFreshSearchRequest{TopK: 1})
	require.ErrorContains(t, err, "persist NeedsRebuild failed")
	require.Equal(t, int32(1), corruptedHits.Load())
	require.Equal(t, int32(0), fallbackHits.Load())
	require.Zero(t, states[0].failures.Load())
	require.True(t, states[0].healthy(time.Now()))
}

func TestSPFreshVectorSearchNoFallbackOnResponseTooLarge(t *testing.T) {
	ResetSPFreshVectorSearchForTest()

	var rejectedHits atomic.Int32
	rejected := newSPFreshSearchTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		rejectedHits.Add(1)
		writeSPFreshSearchProto(w, spfreshErrorResponseForTest(&tipb.Error{
			Code: int32(tipb.SPFreshErrorCode_SPFreshResponseTooLarge),
			Msg:  "encoded response exceeds 67108864 bytes",
		}))
	})
	t.Cleanup(rejected.Close)

	var fallbackHits atomic.Int32
	fallback := newSPFreshSearchTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		fallbackHits.Add(1)
		writeSPFreshSearchProto(w, &tipb.SPFreshSearchResponse{})
	})
	t.Cleanup(fallback.Close)

	restore := config.RestoreFunc()
	t.Cleanup(restore)
	config.UpdateGlobal(func(conf *config.Config) {
		conf.TiKVAPIServiceAddrs = []string{rejected.URL, fallback.URL}
	})

	states, err := spfreshRouter.configuredEndpointStates()
	require.NoError(t, err)
	require.Len(t, states, 2)

	_, err = (&SPFreshVectorSearchExec{}).vectorSearch(context.Background(), 1, &tipb.SPFreshSearchRequest{TopK: 1})
	require.ErrorContains(t, err, "SPFresh vector search response exceeds limit")
	tidbErr, ok := errors.Cause(err).(*terror.Error)
	require.True(t, ok)
	require.Equal(t, uint16(tipb.SPFreshErrorCode_SPFreshResponseTooLarge), terror.ToSQLError(tidbErr).Code)
	require.Equal(t, int32(1), rejectedHits.Load())
	require.Equal(t, int32(0), fallbackHits.Load())
	require.Zero(t, states[0].failures.Load())
	require.True(t, states[0].healthy(time.Now()))
}

func TestSPFreshVectorSearchNoFallbackOnSQLResponseError(t *testing.T) {
	ResetSPFreshVectorSearchForTest()

	var rejectedHits atomic.Int32
	rejected := newSPFreshSearchTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		rejectedHits.Add(1)
		writeSPFreshSearchProto(w, spfreshErrorResponseForTest(&tipb.Error{
			Code: int32(mysql.ErrWarnAllowedPacketOverflowed),
			Msg:  "result exceeds max_allowed_packet",
		}))
	})
	t.Cleanup(rejected.Close)

	var fallbackHits atomic.Int32
	fallback := newSPFreshSearchTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		fallbackHits.Add(1)
		writeSPFreshSearchProto(w, &tipb.SPFreshSearchResponse{})
	})
	t.Cleanup(fallback.Close)

	restore := config.RestoreFunc()
	t.Cleanup(restore)
	config.UpdateGlobal(func(conf *config.Config) {
		conf.TiKVAPIServiceAddrs = []string{rejected.URL, fallback.URL}
	})

	states, err := spfreshRouter.configuredEndpointStates()
	require.NoError(t, err)
	require.Len(t, states, 2)

	_, err = (&SPFreshVectorSearchExec{}).vectorSearch(context.Background(), 1, &tipb.SPFreshSearchRequest{TopK: 1})
	require.ErrorContains(t, err, "result exceeds max_allowed_packet")
	tidbErr, ok := errors.Cause(err).(*terror.Error)
	require.True(t, ok)
	require.Equal(t, uint16(mysql.ErrWarnAllowedPacketOverflowed), terror.ToSQLError(tidbErr).Code)
	require.Equal(t, int32(1), rejectedHits.Load())
	require.Equal(t, int32(0), fallbackHits.Load())
	require.Zero(t, states[0].failures.Load())
	require.True(t, states[0].healthy(time.Now()))
}

func TestSPFreshVectorSearchNonOKProtobufCorruptionUsesHTTPStatus(t *testing.T) {
	ResetSPFreshVectorSearchForTest()

	var rejectedHits atomic.Int32
	rejected := newSPFreshSearchTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		rejectedHits.Add(1)
		respBytes, err := proto.Marshal(spfreshErrorResponseForTest(&tipb.Error{
			Code: int32(tipb.SPFreshErrorCode_SPFreshIndexCorruption),
			Msg:  "corruption detail",
		}))
		require.NoError(t, err)
		w.Header().Set("Content-Type", "application/x-protobuf")
		w.WriteHeader(http.StatusConflict)
		_, _ = w.Write(respBytes)
	})
	t.Cleanup(rejected.Close)

	var fallbackHits atomic.Int32
	fallback := newSPFreshSearchTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		fallbackHits.Add(1)
		writeSPFreshSearchProto(w, &tipb.SPFreshSearchResponse{})
	})
	t.Cleanup(fallback.Close)

	restore := config.RestoreFunc()
	t.Cleanup(restore)
	config.UpdateGlobal(func(conf *config.Config) {
		conf.TiKVAPIServiceAddrs = []string{rejected.URL, fallback.URL}
	})

	_, err := (&SPFreshVectorSearchExec{}).vectorSearch(context.Background(), 1, &tipb.SPFreshSearchRequest{TopK: 1})
	require.ErrorContains(t, err, "index not ready or mismatch")
	_, isStructured := errors.Cause(err).(*terror.Error)
	require.False(t, isStructured)
	require.Equal(t, int32(1), rejectedHits.Load())
	require.Equal(t, int32(0), fallbackHits.Load())
}

func TestSPFreshVectorSearchRetryableStatusIgnoresProtobufCorruption(t *testing.T) {
	ResetSPFreshVectorSearchForTest()

	var badHits atomic.Int32
	bad := newSPFreshSearchTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		badHits.Add(1)
		respBytes, err := proto.Marshal(spfreshErrorResponseForTest(&tipb.Error{
			Code: int32(tipb.SPFreshErrorCode_SPFreshIndexCorruption),
			Msg:  "corruption detail",
		}))
		require.NoError(t, err)
		w.Header().Set("Content-Type", "application/x-protobuf")
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write(respBytes)
	})
	t.Cleanup(bad.Close)

	var goodHits atomic.Int32
	good := newSPFreshSearchTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		goodHits.Add(1)
		writeSPFreshSearchProto(w, &tipb.SPFreshSearchResponse{})
	})
	t.Cleanup(good.Close)

	restore := config.RestoreFunc()
	t.Cleanup(restore)
	config.UpdateGlobal(func(conf *config.Config) {
		conf.TiKVAPIServiceAddrs = []string{bad.URL, good.URL}
	})

	_, err := (&SPFreshVectorSearchExec{}).vectorSearch(context.Background(), 1, &tipb.SPFreshSearchRequest{TopK: 1})
	require.NoError(t, err)
	require.Equal(t, int32(1), badHits.Load())
	require.Equal(t, int32(1), goodHits.Load())
}

func TestSPFreshVectorSearchFallbackAfterSelectedEndpointSendError(t *testing.T) {
	ResetSPFreshVectorSearchForTest()

	var badHits atomic.Int32
	bad := newSPFreshSearchTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		badHits.Add(1)
	})

	var goodHits atomic.Int32
	good := newSPFreshSearchTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		goodHits.Add(1)
		writeSPFreshSearchProto(w, &tipb.SPFreshSearchResponse{})
	})
	t.Cleanup(good.Close)
	bad.Close()

	restore := config.RestoreFunc()
	t.Cleanup(restore)
	config.UpdateGlobal(func(conf *config.Config) {
		conf.TiKVAPIServiceAddrs = []string{bad.URL, good.URL}
	})

	states, err := spfreshRouter.configuredEndpointStates()
	require.NoError(t, err)
	require.Len(t, states, 2)

	_, err = (&SPFreshVectorSearchExec{}).vectorSearch(context.Background(), 1, &tipb.SPFreshSearchRequest{TopK: 1})
	require.NoError(t, err)
	require.Equal(t, int32(0), badHits.Load())
	require.Equal(t, int32(1), goodHits.Load())
	require.Zero(t, states[0].searchInflight.Load())
	require.Zero(t, states[1].searchInflight.Load())
	require.Positive(t, states[0].failures.Load())
}

func TestSPFreshVectorSearchFallbackAfterSelectedEndpointBodyReadError(t *testing.T) {
	ResetSPFreshVectorSearchForTest()

	var badHits atomic.Int32
	bad := newSPFreshSearchTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		badHits.Add(1)
		hijacker, ok := w.(http.Hijacker)
		require.True(t, ok)
		conn, _, err := hijacker.Hijack()
		require.NoError(t, err)
		_, _ = conn.Write([]byte("HTTP/1.1 200 OK\r\nContent-Length: 64\r\n\r\ntruncated"))
		require.NoError(t, conn.Close())
	})
	t.Cleanup(bad.Close)

	var goodHits atomic.Int32
	good := newSPFreshSearchTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		goodHits.Add(1)
		writeSPFreshSearchProto(w, &tipb.SPFreshSearchResponse{})
	})
	t.Cleanup(good.Close)

	restore := config.RestoreFunc()
	t.Cleanup(restore)
	config.UpdateGlobal(func(conf *config.Config) {
		conf.TiKVAPIServiceAddrs = []string{bad.URL, good.URL}
	})

	states, err := spfreshRouter.configuredEndpointStates()
	require.NoError(t, err)
	require.Len(t, states, 2)

	_, err = (&SPFreshVectorSearchExec{}).vectorSearch(context.Background(), 1, &tipb.SPFreshSearchRequest{TopK: 1})
	require.NoError(t, err)
	require.Equal(t, int32(1), badHits.Load())
	require.Equal(t, int32(1), goodHits.Load())
	require.Zero(t, states[0].searchInflight.Load())
	require.Zero(t, states[1].searchInflight.Load())
	require.Positive(t, states[0].failures.Load())
}

func TestSPFreshVectorSearchNoFallbackOnNonRetryableStatus(t *testing.T) {
	cases := []struct {
		name       string
		statusCode int
	}{
		{name: "bad_request", statusCode: http.StatusBadRequest},
		{name: "not_found", statusCode: http.StatusNotFound},
		{name: "conflict", statusCode: http.StatusConflict},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ResetSPFreshVectorSearchForTest()

			var badHits atomic.Int32
			bad := newSPFreshSearchTestServer(t, func(w http.ResponseWriter, r *http.Request) {
				badHits.Add(1)
				http.Error(w, "worker error", tc.statusCode)
			})
			t.Cleanup(bad.Close)

			var goodHits atomic.Int32
			good := newSPFreshSearchTestServer(t, func(w http.ResponseWriter, r *http.Request) {
				goodHits.Add(1)
				writeSPFreshSearchProto(w, &tipb.SPFreshSearchResponse{})
			})
			t.Cleanup(good.Close)

			restore := config.RestoreFunc()
			t.Cleanup(restore)
			config.UpdateGlobal(func(conf *config.Config) {
				conf.TiKVAPIServiceAddrs = []string{bad.URL, good.URL}
			})

			states, err := spfreshRouter.configuredEndpointStates()
			require.NoError(t, err)
			require.Len(t, states, 2)

			_, err = (&SPFreshVectorSearchExec{}).vectorSearch(context.Background(), 1, &tipb.SPFreshSearchRequest{TopK: 1})
			require.Error(t, err)
			require.Equal(t, int32(1), badHits.Load())
			require.Equal(t, int32(0), goodHits.Load())
			require.Zero(t, states[0].searchInflight.Load())
			require.Zero(t, states[1].searchInflight.Load())
			require.Zero(t, states[0].failures.Load())
		})
	}
}

func TestSPFreshVectorSearchNoFallbackAfterOKDecodeError(t *testing.T) {
	ResetSPFreshVectorSearchForTest()

	var badHits atomic.Int32
	bad := newSPFreshSearchTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		badHits.Add(1)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("not protobuf"))
	})
	t.Cleanup(bad.Close)

	var goodHits atomic.Int32
	good := newSPFreshSearchTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		goodHits.Add(1)
		writeSPFreshSearchProto(w, &tipb.SPFreshSearchResponse{})
	})
	t.Cleanup(good.Close)

	restore := config.RestoreFunc()
	t.Cleanup(restore)
	config.UpdateGlobal(func(conf *config.Config) {
		conf.TiKVAPIServiceAddrs = []string{bad.URL, good.URL}
	})

	states, err := spfreshRouter.configuredEndpointStates()
	require.NoError(t, err)
	require.Len(t, states, 2)

	_, err = (&SPFreshVectorSearchExec{}).vectorSearch(context.Background(), 1, &tipb.SPFreshSearchRequest{TopK: 1})
	require.Error(t, err)
	require.Equal(t, int32(1), badHits.Load())
	require.Equal(t, int32(0), goodHits.Load())
	require.Zero(t, states[0].searchInflight.Load())
	require.Zero(t, states[1].searchInflight.Load())
	require.Positive(t, states[0].failures.Load())
}

func TestSPFreshEndpointRouterReconcilesByCanonicalEndpoint(t *testing.T) {
	ResetSPFreshVectorSearchForTest()

	restore := config.RestoreFunc()
	t.Cleanup(restore)
	config.UpdateGlobal(func(conf *config.Config) {
		conf.TiKVAPIServiceAddrs = []string{"HTTP://Worker-0:19000", "worker-1:19000"}
	})
	states, err := spfreshRouter.configuredEndpointStates()
	require.NoError(t, err)
	require.Len(t, states, 2)
	states[0].searchInflight.Store(7)

	config.UpdateGlobal(func(conf *config.Config) {
		conf.TiKVAPIServiceAddrs = []string{"worker-2:19000", "http://worker-0:19000/"}
	})
	states, err = spfreshRouter.configuredEndpointStates()
	require.NoError(t, err)
	require.Len(t, states, 2)
	require.Equal(t, "http://worker-2:19000", states[0].endpoint)
	require.Equal(t, "http://worker-0:19000", states[1].endpoint)
	require.Equal(t, int64(7), states[1].searchInflight.Load())
}

func TestSPFreshEndpointRouterRefreshesEquivalentConfiguredSnapshot(t *testing.T) {
	ResetSPFreshVectorSearchForTest()

	restore := config.RestoreFunc()
	t.Cleanup(restore)
	config.UpdateGlobal(func(conf *config.Config) {
		conf.TiKVAPIServiceAddrs = []string{"HTTP://Worker-0:19000"}
	})
	states, err := spfreshRouter.configuredEndpointStates()
	require.NoError(t, err)
	require.Len(t, states, 1)

	config.UpdateGlobal(func(conf *config.Config) {
		conf.TiKVAPIServiceAddrs = []string{"http://worker-0:19000/"}
	})
	nextStates, err := spfreshRouter.configuredEndpointStates()
	require.NoError(t, err)
	require.Len(t, nextStates, 1)
	require.Same(t, states[0], nextStates[0])

	snapshot := spfreshRouter.snapshot.Load()
	require.NotNil(t, snapshot)
	require.Equal(t, []string{"http://worker-0:19000/"}, snapshot.configured)
}

func TestSPFreshEndpointFailureCooldownExponentialAndReset(t *testing.T) {
	state := &spfreshEndpointState{endpoint: "http://worker-0:19000"}
	now := time.Unix(100, 0)

	state.markFailure(now)
	first := time.Duration(state.cooldownUntil.Load() - now.UnixNano())
	require.GreaterOrEqual(t, first, spfreshEndpointMinFailureCooldown)
	require.LessOrEqual(t, first, 1200*time.Millisecond)
	require.False(t, state.healthy(now.Add(first-time.Nanosecond)))

	state.markFailure(now)
	second := time.Duration(state.cooldownUntil.Load() - now.UnixNano())
	require.Greater(t, second, first)
	require.LessOrEqual(t, second, 2400*time.Millisecond)

	for i := 0; i < 8; i++ {
		state.markFailure(now)
	}
	capped := time.Duration(state.cooldownUntil.Load() - now.UnixNano())
	require.LessOrEqual(t, capped, spfreshEndpointMaxFailureCooldown)

	state.markSuccess()
	require.Zero(t, state.failures.Load())
	require.True(t, state.healthy(now))
}

func TestSPFreshVectorSearchInflightDecrement(t *testing.T) {
	cases := []struct {
		name            string
		handler         func(http.ResponseWriter, *http.Request)
		cancel          bool
		wantErr         bool
		wantErrContains string
	}{
		{
			name: "success",
			handler: func(w http.ResponseWriter, r *http.Request) {
				writeSPFreshSearchProto(w, &tipb.SPFreshSearchResponse{})
			},
		},
		{
			name: "http_error",
			handler: func(w http.ResponseWriter, r *http.Request) {
				http.Error(w, "unavailable", http.StatusServiceUnavailable)
			},
			wantErr: true,
		},
		{
			name: "decode_error",
			handler: func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusOK)
				_, _ = w.Write([]byte("not protobuf"))
			},
			wantErr: true,
		},
		{
			name: "response_too_large",
			handler: func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusOK)
				_, _ = io.CopyN(w, zeroReader{}, maxVectorSearchResponseBytes+1)
			},
			wantErr:         true,
			wantErrContains: "response exceeds 67108864 bytes",
		},
		{
			name: "protobuf_error_response_too_large",
			handler: func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/x-protobuf")
				w.WriteHeader(http.StatusInternalServerError)
				_, _ = io.CopyN(w, zeroReader{}, maxSPFreshErrorBodyBytes+1)
			},
			wantErr:         true,
			wantErrContains: "error response exceeds 4096 bytes",
		},
		{
			name: "cancel",
			handler: func(w http.ResponseWriter, r *http.Request) {
				<-r.Context().Done()
			},
			cancel:  true,
			wantErr: true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ResetSPFreshVectorSearchForTest()

			srv := newSPFreshSearchTestServer(t, func(w http.ResponseWriter, r *http.Request) {
				tc.handler(w, r)
			})
			t.Cleanup(srv.Close)

			restore := config.RestoreFunc()
			t.Cleanup(restore)
			config.UpdateGlobal(func(conf *config.Config) {
				conf.TiKVAPIServiceAddrs = []string{srv.URL}
			})

			states, err := spfreshRouter.configuredEndpointStates()
			require.NoError(t, err)
			require.Len(t, states, 1)

			ctx := context.Background()
			if tc.cancel {
				cancelCtx, cancel := context.WithCancel(ctx)
				cancel()
				ctx = cancelCtx
			}
			_, err = (&SPFreshVectorSearchExec{}).vectorSearch(ctx, 1, &tipb.SPFreshSearchRequest{TopK: 1})
			if tc.wantErr {
				require.Error(t, err)
				if tc.wantErrContains != "" {
					require.ErrorContains(t, err, tc.wantErrContains)
				}
			} else {
				require.NoError(t, err)
			}
			require.Zero(t, states[0].searchInflight.Load())
		})
	}
}

type zeroReader struct{}

func (zeroReader) Read(p []byte) (int, error) {
	clear(p)
	return len(p), nil
}

func newSPFreshSearchTestServer(t *testing.T, handler func(http.ResponseWriter, *http.Request)) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != spfreshVectorSearchPath {
			http.Error(w, "bad path", http.StatusNotFound)
			return
		}
		if _, err := io.Copy(io.Discard, r.Body); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if err := r.Body.Close(); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		handler(w, r)
	}))
}

func writeSPFreshSearchProto(w http.ResponseWriter, resp *tipb.SPFreshSearchResponse) {
	if resp.GetResult() == nil {
		resp = &tipb.SPFreshSearchResponse{
			Result: &tipb.SPFreshSearchResponse_Success{Success: &tipb.SPFreshSearchResult{Stats: &tipb.SPFreshSearchStats{}}},
		}
	}
	respBytes, err := proto.Marshal(resp)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/x-protobuf")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(respBytes)
}

func spfreshErrorResponseForTest(err *tipb.Error) *tipb.SPFreshSearchResponse {
	return &tipb.SPFreshSearchResponse{
		Result: &tipb.SPFreshSearchResponse_Error{Error: err},
	}
}
