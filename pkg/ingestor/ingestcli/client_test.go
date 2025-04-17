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

func TestWriteClientWriteWithPairs(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "PUT", r.Method)
		require.Contains(t, r.URL.String(), "/write_sst")
		require.Equal(t, "12345", r.URL.Query().Get("cluster_id"))
		require.Equal(t, "task1", r.URL.Query().Get("task_id"))
		require.Equal(t, "1", r.URL.Query().Get("chunk_id"))

		body, err := io.ReadAll(r.Body)
		require.NoError(t, err)
		expected := []byte{
			0x00, 0x03, 'k', 'e', 'y',
			0x00, 0x00, 0x00, 0x05, 'v', 'a', 'l', 'u', 'e',
		}
		require.Equal(t, expected, body)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	client := newWriteClient(server.URL, 12345, "task1", server.Client())
	req := &WriteRequest{
		ChunkID: 1,
		Pairs: []*import_sstpb.Pair{
			{Key: []byte("key"), Value: []byte("value")},
		},
	}
	err := client.Write(context.Background(), req)
	require.NoError(t, err)
}

func TestWriteClientCloseAndRecv(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "POST", r.Method)
		require.Contains(t, r.URL.String(), "/write_sst")
		require.Equal(t, "12345", r.URL.Query().Get("cluster_id"))
		require.Equal(t, "task1", r.URL.Query().Get("task_id"))
		require.Equal(t, "true", r.URL.Query().Get("build"))
		require.Equal(t, "zstd", r.URL.Query().Get("compression"))

		w.WriteHeader(http.StatusOK)
		_, err := w.Write([]byte(`{"file_name": "mock_sst_file.sst"}`))
		require.NoError(t, err)
	}))
	defer server.Close()
	client := newWriteClient(server.URL, 12345, "task1", server.Client())
	resp, err := client.CloseAndRecv(context.Background())
	require.NoError(t, err)
	require.NotNil(t, resp)
	require.Equal(t, "mock_sst_file.sst", resp.SSTFile)
}
