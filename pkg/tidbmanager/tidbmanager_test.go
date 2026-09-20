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

package tidbmanager

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFree(t *testing.T) {
	var gotPath string
	var gotQuery url.Values
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		gotQuery = r.URL.Query()
		require.Equal(t, http.MethodPut, r.Method)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	cli := NewClient(server.Listener.Addr().String(), nil, "pod-1", "10.0.0.1", "ns-1")
	require.NoError(t, cli.Free(context.Background(), "idle restart"))
	require.Equal(t, freeReqPath, gotPath)
	require.Equal(t, "pod-1", gotQuery.Get("pod_name"))
	require.Equal(t, "10.0.0.1", gotQuery.Get("pod_ip"))
	require.Equal(t, "ns-1", gotQuery.Get("ns"))
	require.Equal(t, "idle restart", gotQuery.Get("normal_restart_log"))
}

func TestFreeReturnsErrorOnNonOK(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte("not ready"))
	}))
	defer server.Close()

	cli := NewClient(server.Listener.Addr().String(), nil, "pod-1", "10.0.0.1", "ns-1")
	err := cli.Free(context.Background(), "idle restart")
	require.ErrorContains(t, err, "503 Service Unavailable")
	require.ErrorContains(t, err, "not ready")
}

var errReadFailed = errors.New("read failed")

type errorRoundTripper struct{}

func (errorRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	return &http.Response{
		StatusCode: http.StatusServiceUnavailable,
		Status:     "503 Service Unavailable",
		Body:       errReader{},
		Request:    req,
	}, nil
}

type errReader struct{}

func (errReader) Read(_ []byte) (int, error) {
	return 0, errReadFailed
}

func (errReader) Close() error {
	return nil
}

func TestFreeReturnsBodyReadError(t *testing.T) {
	cli := NewClient("manager.example.com", nil, "pod-1", "10.0.0.1", "ns-1").(*client)
	cli.httpClient.Transport = errorRoundTripper{}

	err := cli.Free(context.Background(), "idle restart")
	require.ErrorContains(t, err, "503 Service Unavailable")
	require.ErrorContains(t, err, "read body failed: read failed")
	require.ErrorIs(t, err, errReadFailed)
}
