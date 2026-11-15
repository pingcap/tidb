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
	stderrors "errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSPFreshBulkBuildingSendsOneExactRequest(t *testing.T) {
	requests := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		require.Equal(t, http.MethodPost, r.Method)
		require.Equal(t, spfreshBulkBuildingPath, r.URL.Path)
		require.Empty(t, r.URL.RawQuery)
		require.Equal(t, "application/json", r.Header.Get("Content-Type"))
		var request spfreshBulkBuildingRequest
		require.NoError(t, json.NewDecoder(r.Body).Decode(&request))
		require.Equal(t, spfreshBulkBuildingRequest{
			BuildID:          "1-2-3-4",
			KeyspaceID:       1,
			TableID:          3,
			IndexID:          4,
			SourceSnapshotTS: 123,
		}, request)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	require.NoError(t, InitSPFreshBulkBuilding(
		context.Background(), server.Client(), server.URL,
		"1-2-3-4", 1, 3, 4, 123,
	))
	require.Equal(t, 1, requests)
}

func TestSPFreshBulkBuildingDoesNotRetryOrFollowRedirect(t *testing.T) {
	redirectRequests := 0
	redirect := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		redirectRequests++
		w.WriteHeader(http.StatusOK)
	}))
	defer redirect.Close()

	requests := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		http.Redirect(w, r, redirect.URL+spfreshBulkBuildingPath, http.StatusTemporaryRedirect)
	}))
	defer server.Close()

	err := InitSPFreshBulkBuilding(
		context.Background(), server.Client(), server.URL,
		"1-2-3-4", 1, 3, 4, 123,
	)
	require.Error(t, err)
	var controlErr *SPFreshControlRequestError
	require.True(t, stderrors.As(err, &controlErr))
	require.Equal(t, http.StatusTemporaryRedirect, controlErr.StatusCode)
	require.Equal(t, 1, requests)
	require.Equal(t, 0, redirectRequests)
}
