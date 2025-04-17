package ingestcli

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"net/http"
)

var _ WriteClient = &writeClient{}

type writeClient struct {
	tikvWorkerURL string
	clusterID     uint64
	taskID        string
	httpClient    *http.Client
}

func newWriteClient(tikvWorkerURL string, clusterID uint64, taskID string, httpClient *http.Client) *writeClient {
	return &writeClient{
		tikvWorkerURL: tikvWorkerURL,
		clusterID:     clusterID,
		taskID:        taskID,
		httpClient:    httpClient,
	}
}

func (w *writeClient) Write(ctx context.Context, in *WriteRequest) error {
	url := fmt.Sprintf("%s/write_sst?cluster_id=%d&task_id=%s&chunk_id=%d",
		w.tikvWorkerURL, w.clusterID, w.taskID, in.ChunkID)

	// TODO: pass retry counter metrics.
	var buf bytes.Buffer
	for _, pair := range in.Pairs {
		keyLen := uint16(len(pair.Key))
		if err := binary.Write(&buf, binary.BigEndian, keyLen); err != nil {
			return err
		}
		buf.Write(pair.Key)
		valLen := uint32(len(pair.Value))
		if err := binary.Write(&buf, binary.BigEndian, valLen); err != nil {
			return err
		}
		buf.Write(pair.Value)
	}

	data := buf.Bytes()
	_, err := sendRequest(ctx, w.httpClient, "PUT", url, data, nil)
	return err
}

func (w *writeClient) CloseAndRecv(ctx context.Context) (*WriteResponse, error) {
	url := fmt.Sprintf("%s/write_sst?cluster_id=%d&task_id=%s&build=true&compression=zstd",
		w.tikvWorkerURL, w.clusterID, w.taskID)

	// TODO: pass retry counter metrics.
	data, err := sendRequest(ctx, w.httpClient, "POST", url, nil, nil)
	if err != nil {
		return nil, err
	}

	result := new(FileNameResult)
	err = json.Unmarshal(data, result)
	if err != nil {
		return nil, err
	}

	return &WriteResponse{SSTFile: result.FileName}, nil
}

var _ Client = &client{}

type client struct {
	tikvWorkerURL string
	clusterID     uint64
	taskID        string
	httpClient    *http.Client
}

func (c *client) WriteClient(ctx context.Context) (WriteClient, error) {
	return newWriteClient(c.tikvWorkerURL, c.clusterID, c.taskID, c.httpClient), nil
}

type FileNameResult struct {
	FileName string `json:"file_name"`
}

func (c *client) Ingest(ctx context.Context, in *IngestRequest) error {
	return nil
}
