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

package tiflashcompute

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestTopoFetcherCancellation(t *testing.T) {
	for _, tc := range []struct {
		name  string
		fetch func(context.Context, string) ([]string, error)
	}{
		{
			name: "mock",
			fetch: func(ctx context.Context, addr string) ([]string, error) {
				return NewMockAutoScalerFetcher(addr).FetchAndGetTopo(ctx)
			},
		},
		{
			name: "aws",
			fetch: func(ctx context.Context, addr string) ([]string, error) {
				return NewAWSAutoScalerFetcher(addr, "test-cluster", false).FetchAndGetTopo(ctx)
			},
		},
		{
			name: "aws_fixed_pool",
			fetch: func(ctx context.Context, addr string) ([]string, error) {
				return NewAWSAutoScalerFetcher(addr, "test-cluster", true).FetchAndGetTopo(ctx)
			},
		},
		{
			name: "aws_recovery",
			fetch: func(ctx context.Context, addr string) ([]string, error) {
				return NewAWSAutoScalerFetcher(addr, "test-cluster", false).RecoveryAndGetTopo(ctx, RecoveryTypeMemLimit, 1)
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Run("deadline", func(t *testing.T) {
				server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
					t.Error("expired topology request reached the autoscaler")
				}))
				defer server.Close()
				ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
				defer cancel()
				topo, err := tc.fetch(ctx, strings.TrimPrefix(server.URL, "http://"))
				require.ErrorIs(t, err, context.DeadlineExceeded)
				require.Empty(t, topo)
			})

			for _, phase := range []string{"headers", "body"} {
				t.Run(phase, func(t *testing.T) {
					started := make(chan struct{})
					release := make(chan struct{})
					server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						if phase == "body" {
							w.WriteHeader(http.StatusOK)
							w.(http.Flusher).Flush()
						}
						close(started)
						select {
						case <-r.Context().Done():
						case <-release:
						}
					}))
					defer server.Close()
					defer close(release)

					ctx, cancel := context.WithCancel(context.Background())
					defer cancel()
					result := make(chan error, 1)
					go func() {
						_, err := tc.fetch(ctx, strings.TrimPrefix(server.URL, "http://"))
						result <- err
					}()

					select {
					case <-started:
					case <-time.After(5 * time.Second):
						t.Fatal("topology request did not reach the autoscaler")
					}
					cancel()
					select {
					case err := <-result:
						require.ErrorIs(t, err, context.Canceled)
					case <-time.After(5 * time.Second):
						t.Fatal("topology request did not stop after cancellation")
					}
				})
			}
		})
	}
}

func TestTopoFetcherHTTPResponse(t *testing.T) {
	for _, tc := range []struct {
		name      string
		status    int
		body      string
		expectErr bool
	}{
		{name: "success", status: http.StatusOK, body: `{"topology":["compute:3930"],"timestamp":"1"}`},
		{name: "http_error", status: http.StatusServiceUnavailable, body: "unavailable", expectErr: true},
		{name: "invalid_json", status: http.StatusOK, body: "invalid", expectErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(tc.status)
				_, _ = w.Write([]byte(tc.body))
			}))
			defer server.Close()
			fetcher := NewAWSAutoScalerFetcher(strings.TrimPrefix(server.URL, "http://"), "test-cluster", false)
			topo, err := fetcher.FetchAndGetTopo(context.Background())
			if tc.expectErr {
				require.True(t, errTopoFetcher.Equal(err))
				require.Empty(t, topo)
			} else {
				require.NoError(t, err)
				require.Equal(t, []string{"compute:3930"}, topo)
			}
		})
	}
}
