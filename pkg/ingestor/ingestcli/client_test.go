// Copyright 2025 PingCAP, Inc.
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

package ingestcli

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/pingcap/kvproto/pkg/import_sstpb"
	"github.com/stretchr/testify/require"
)

func TestWriteClientWriteChunk(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		require.NoError(t, err)
		expected := []byte{
			0x00, 0x03, 'k', 'e', 'y',
			0x00, 0x00, 0x00, 0x05, 'v', 'a', 'l', 'u', 'e',
		}
		require.Equal(t, expected, body)
		w.WriteHeader(http.StatusOK)
		_, err = w.Write([]byte(`{"sst_file": "mock_sst_file.sst"}`))
		require.NoError(t, err)
	}))
	defer server.Close()

	client := newWriteClient(server.URL, 12345, 1, server.Client())
	client.commitTS = 67890
	err := client.init(context.Background())
	require.NoError(t, err)

	req := &WriteRequest{
		Pairs: []*import_sstpb.Pair{
			{Key: []byte("key"), Value: []byte("value")},
		},
	}
	err = client.WriteChunk(req)
	require.NoError(t, err)

	resp, err := client.Close()
	require.NoError(t, err)
	require.NotNil(t, resp)
	require.Equal(t, "mock_sst_file.sst", resp.SSTFile)
}
