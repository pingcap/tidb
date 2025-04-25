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

	"github.com/pingcap/kvproto/pkg/errorpb"
	"github.com/pingcap/kvproto/pkg/import_sstpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/tidb/br/pkg/restore/split"
	"github.com/stretchr/testify/require"
)

func TestWriteClientWriteChunk(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		require.NoError(t, err)
		expected := []byte("\x03\x00key\x05\x00\x00\x00value")
		require.Equal(t, expected, body)
		w.WriteHeader(http.StatusOK)
		_, err = w.Write([]byte(`{"sst_file": "mock_sst_file.sst"}`))
		require.NoError(t, err)
	}))
	defer server.Close()

	client := newWriteClient(server.URL, 12345, server.Client(), 67890)
	client.commitTS = 67890
	defer client.Close()
	err := client.init(context.Background())
	require.NoError(t, err)

	req := &WriteRequest{
		Pairs: []*import_sstpb.Pair{
			{Key: []byte("key"), Value: []byte("value")},
		},
	}
	err = client.Write(req)
	require.NoError(t, err)

	resp, err := client.Recv()
	require.NoError(t, err)
	require.NotNil(t, resp)
	require.Equal(t, "mock_sst_file.sst", resp.nextGenSSTMeta)
}

func TestClientWriteServerError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("internal server error"))
	}))
	defer server.Close()

	client := newWriteClient(server.URL, 12345, server.Client(), 67890)
	err := client.init(context.Background())
	require.NoError(t, err)

	req := &WriteRequest{Pairs: []*import_sstpb.Pair{{Key: []byte("key"), Value: []byte("value")}}}
	err = client.Write(req)
	require.NoError(t, err) // Error only return when pipeWriter is closed?

	_, err = client.Recv()
	require.Error(t, err)
	require.Contains(t, err.Error(), "internal server error")
}

func TestClientIngest(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "/ingest_s3", r.URL.Path)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	client := NewClient(server.URL, 12345, server.Client(), &storeClient{addr: server.URL})
	req := &IngestRequest{
		WriteResp: &WriteResponse{
			nextGenSSTMeta: &nextGenSSTMeta{
				Id: 1,
			},
		},
		Region: &split.RegionInfo{Region: &metapb.Region{Id: 1, RegionEpoch: &metapb.RegionEpoch{Version: 1}}},
	}
	err := client.Ingest(context.Background(), req)
	require.NoError(t, err)
}

func TestClientIngestError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		pbErr := &errorpb.Error{Message: "test error"}
		data, err := pbErr.Marshal()
		require.NoError(t, err)
		w.Write(data)
	}))
	defer server.Close()

	client := NewClient(server.URL, 12345, server.Client(), &storeClient{addr: server.URL})
	req := &IngestRequest{
		WriteResp: &WriteResponse{
			nextGenSSTMeta: &nextGenSSTMeta{
				Id: 1,
			},
		},
		Region: &split.RegionInfo{Region: &metapb.Region{Id: 1, RegionEpoch: &metapb.RegionEpoch{Version: 1}}},
	}
	err := client.Ingest(context.Background(), req)
	require.Error(t, err)
	require.Contains(t, err.Error(), "test error")
}

type storeClient struct {
	split.SplitClient
	addr string
}

func (sc *storeClient) GetStore(_ context.Context, _ uint64) (*metapb.Store, error) {
	return &metapb.Store{
		Address: sc.addr,
	}, nil
}
