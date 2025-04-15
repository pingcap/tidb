package ingestcli

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/pingcap/tidb/dumpling/context"
)

var _ WriteClient = &writeClient{}

type writeClient struct {
	tikvWorkerURL string
	clusterID     uint64
	httpClient    *http.Client
}

func newWriteClient(tikvWorkerURL string, clusterID uint64, httpClient *http.Client) *writeClient {
	return &writeClient{
		tikvWorkerURL: tikvWorkerURL,
		clusterID:     clusterID,
		httpClient:    httpClient,
	}
}

func (w *writeClient) Write(ctx context.Context, in *WriteRequest) error {
	url := fmt.Sprintf("%s/write_sst?cluster_id=%d&task_id=%s&chunk_id=%d",
		w.tikvWorkerURL, w.clusterID, in.TaskID, in.ChunkID)

	// TODO: pass retry counter metrics.
	_, err := sendRequest(ctx, w.httpClient, "PUT", url, in.Data, nil)
	if err != nil {
		return err
	}
	return nil
}

func (w *writeClient) Close(ctx context.Context) (*WriteResponse, error) {
	return nil, nil
}

var _ Client = &client{}

type client struct {
	tikvWorkerURL string
	clusterID     uint64
	httpClient    *http.Client
}

func (c *client) WriteClient(ctx context.Context) (WriteClient, error) {
	return newWriteClient(c.tikvWorkerURL, c.clusterID, c.httpClient), nil
}

type FileNameResult struct {
	FileName string `json:"file_name"`
}

func (c *client) Ingest(ctx context.Context, in *IngestRequest) (*IngestResponse, error) {
	url := fmt.Sprintf("%s/write_sst?cluster_id=%d&task_id=%s&build=true&compression=zstd",
		c.tikvWorkerURL, c.clusterID, in.TaskID)

	// TODO: pass retry counter metrics.
	data, err := sendRequest(ctx, c.httpClient, "POST", url, nil, nil)
	if err != nil {
		return nil, err
	}

	result := new(FileNameResult)
	err = json.Unmarshal(data, result)
	if err != nil {
		return nil, err
	}

	return &IngestResponse{FileName: result.FileName}, nil
}
