// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package httputil

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestGetJSON(t *testing.T) {
	type TestPayload struct {
		Username string `json:"username"`
		Password string `json:"password"`
	}
	request := TestPayload{
		Username: "lightning",
		Password: "lightning-ctl",
	}

	ctx := context.Background()
	// Mock success response
	handle := func(res http.ResponseWriter, _ *http.Request) {
		res.WriteHeader(http.StatusOK)
		err := json.NewEncoder(res).Encode(request)
		require.NoError(t, err)
	}
	testServer := httptest.NewServer(http.HandlerFunc(func(res http.ResponseWriter, req *http.Request) {
		handle(res, req)
	}))
	defer testServer.Close()

	client := &http.Client{Timeout: time.Second}

	response := TestPayload{}
	err := GetJSON(ctx, client, "http://localhost:1", &response)
	require.Error(t, err)
	err = GetJSON(ctx, client, testServer.URL, &response)
	require.NoError(t, err)
	require.Equal(t, request, response)

	// Mock `StatusNoContent` response
	handle = func(res http.ResponseWriter, _ *http.Request) {
		res.WriteHeader(http.StatusNoContent)
	}
	err = GetJSON(ctx, client, testServer.URL, &response)
	require.Error(t, err)
	require.Regexp(t, ".*http status code != 200.*", err.Error())
}

func TestGetText(t *testing.T) {
	// Mock success response
	handle := func(res http.ResponseWriter, _ *http.Request) {
		res.WriteHeader(http.StatusOK)
		_, err := res.Write([]byte("test-content"))
		require.NoError(t, err)
	}
	testServer := httptest.NewServer(http.HandlerFunc(func(res http.ResponseWriter, req *http.Request) {
		handle(res, req)
	}))
	defer testServer.Close()

	client := &http.Client{}
	text, err := GetText(client, testServer.URL+"/test")
	require.NoError(t, err)
	require.Equal(t, "test-content", text)
}
