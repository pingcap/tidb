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
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/pingcap/tidb/pkg/lightning/backend"
	"github.com/pingcap/tidb/pkg/lightning/checkpoints"
	"github.com/pingcap/tidb/pkg/lightning/log"
	"github.com/stretchr/testify/require"
)

func TestLoadDataInitFollowsRedirectAndRewritesOwner(t *testing.T) {
	withNoOwnerBoundRetryDelay(t)

	redirectRequests := 0
	redirectServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		redirectRequests++
		if r.Method != http.MethodPost {
			http.Error(w, "bad method", http.StatusMethodNotAllowed)
			return
		}
		if r.URL.Path != "/load_data" {
			http.Error(w, "bad path", http.StatusNotFound)
			return
		}
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
		if r.URL.Path != "/load_data" {
			http.Error(w, "bad path", http.StatusNotFound)
			return
		}
		http.Redirect(w, r, redirectServer.URL+"/load_data", http.StatusFound)
	}))
	defer ownerServer.Close()

	b := &Backend{
		logger:     log.L(),
		httpClient: ownerServer.Client(),
	}
	e := &engine{
		logger:         log.L(),
		loadDataTaskID: "task-1",
		addr:           ownerServer.URL,
		clusterID:      7,
		ts:             11,
	}

	err := b.loadDataInit(context.Background(), e, 1024)
	require.NoError(t, err)
	require.Equal(t, 1, ownerRequests)
	require.Equal(t, 1, redirectRequests)
	require.Equal(t, redirectServer.URL, e.addr)
}

func TestCheckLoadDataTaskFollowsRedirect(t *testing.T) {
	withNoOwnerBoundRetryDelay(t)

	redirectRequests := 0
	redirectServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		redirectRequests++
		if r.Method != http.MethodGet {
			http.Error(w, "bad method", http.StatusMethodNotAllowed)
			return
		}
		if r.URL.Path != "/load_data" {
			http.Error(w, "bad path", http.StatusNotFound)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer redirectServer.Close()

	ownerRequests := 0
	ownerServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ownerRequests++
		if r.Method != http.MethodGet {
			http.Error(w, "bad method", http.StatusMethodNotAllowed)
			return
		}
		if r.URL.Path != "/load_data" {
			http.Error(w, "bad path", http.StatusNotFound)
			return
		}
		http.Redirect(w, r, redirectServer.URL+"/load_data", http.StatusFound)
	}))
	defer ownerServer.Close()

	b := &Backend{
		logger:     log.L(),
		httpClient: ownerServer.Client(),
	}
	exist, err := b.checkLoadDataTaskOnWorker(context.Background(), "task-1", 7, ownerServer.URL)
	require.NoError(t, err)
	require.True(t, exist)
	require.Equal(t, 1, ownerRequests)
	require.Equal(t, 1, redirectRequests)
}

func TestOwnerBoundRequestRetriesRetryableStatuses(t *testing.T) {
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
			withNoShortOwnerBoundRetryDelay(t)

			attempts := 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				attempts++
				if attempts == 1 {
					w.WriteHeader(status)
					return
				}
				w.WriteHeader(http.StatusOK)
				_, _ = w.Write([]byte("ok"))
			}))
			defer server.Close()

			resp, err := sendShortOwnerBoundRequestWithRetry(context.Background(), server.Client(), http.MethodGet, server.URL, nil, nil)
			require.NoError(t, err)
			_ = resp.Body.Close()
			require.Equal(t, 2, attempts)
		})
	}
}

func TestOwnerBoundRequestDoesNotRetryBusinessStatuses(t *testing.T) {
	businessStatuses := []int{
		http.StatusBadRequest,
		http.StatusNotFound,
		http.StatusConflict,
		http.StatusFound,
	}
	for _, status := range businessStatuses {
		t.Run(strconv.Itoa(status), func(t *testing.T) {
			withNoOwnerBoundRetryDelay(t)

			attempts := 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				attempts++
				w.WriteHeader(status)
			}))
			defer server.Close()

			resp, err := sendShortOwnerBoundRequestWithRetry(context.Background(), server.Client(), http.MethodGet, server.URL, nil, nil)
			require.NoError(t, err)
			_ = resp.Body.Close()
			require.Equal(t, status, resp.StatusCode)
			require.Equal(t, 1, attempts)
		})
	}
}

func TestShortOwnerBoundRequestStopsAfterThreeAttempts(t *testing.T) {
	withNoShortOwnerBoundRetryDelay(t)

	attempts := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts++
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer server.Close()

	resp, err := sendShortOwnerBoundRequestWithRetry(context.Background(), server.Client(), http.MethodGet, server.URL, nil, nil)
	require.Error(t, err)
	require.Nil(t, resp)
	require.Equal(t, 3, attempts)
}

func TestLongOwnerBoundRequestContinuesAfterThreeAttempts(t *testing.T) {
	withNoLongOwnerBoundRetryDelay(t)

	attempts := 0
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts++
		if attempts == 4 {
			cancel()
		}
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer server.Close()

	resp, err := sendLongOwnerBoundRequestWithRetry(ctx, server.Client(), http.MethodGet, server.URL, nil, nil)
	require.ErrorIs(t, err, context.Canceled)
	require.Nil(t, resp)
	require.GreaterOrEqual(t, attempts, 4)
}

func TestOpenEngineRequiresRemoteTaskOwnerBeforeInit(t *testing.T) {
	b := &Backend{}
	err := b.OpenEngine(context.Background(), &backend.EngineConfig{
		TaskID: 1,
		TableInfo: &checkpoints.TidbTableInfo{
			ID: 11,
		},
		EngineID: 101,
	}, uuid.New())
	require.ErrorContains(t, err, "remote task owner is not assigned")
}

func withNoOwnerBoundRetryDelay(t *testing.T) {
	t.Helper()
	withNoShortOwnerBoundRetryDelay(t)
	withNoLongOwnerBoundRetryDelay(t)
}

func withNoShortOwnerBoundRetryDelay(t *testing.T) {
	t.Helper()
	oldDelay := shortOwnerBoundRetryDelay
	shortOwnerBoundRetryDelay = func(int) time.Duration {
		return 0
	}
	t.Cleanup(func() {
		shortOwnerBoundRetryDelay = oldDelay
	})
}

func withNoLongOwnerBoundRetryDelay(t *testing.T) {
	t.Helper()
	oldDelay := longOwnerBoundRetryDelay
	longOwnerBoundRetryDelay = func(int) time.Duration {
		return 0
	}
	t.Cleanup(func() {
		longOwnerBoundRetryDelay = oldDelay
	})
}

func newTestChunkSender(client *http.Client, cache *chunkCache, taskType taskType) *chunkSender {
	c := &chunkSender{
		id:          11,
		e:           &engine{taskType: taskType, addr: "http://remote.test", clusterID: 7, loadDataTaskID: "task-1", logger: log.L()},
		httpClient:  client,
		chunksCache: cache,
	}
	c.state.Store(&chunksState{})
	return c
}

func jsonResponse(req *http.Request, body string) *http.Response {
	return &http.Response{
		StatusCode: http.StatusOK,
		Status:     http.StatusText(http.StatusOK),
		Body:       io.NopCloser(strings.NewReader(body)),
		Header:     make(http.Header),
		Request:    req,
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}
