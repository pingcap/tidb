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
	taskID        int64
	httpClient    *http.Client

	chunkID int // maintained internally.
}

func newWriteClient(tikvWorkerURL string, clusterID uint64, taskID int64, httpClient *http.Client) *writeClient {
	return &writeClient{
		tikvWorkerURL: tikvWorkerURL,
		clusterID:     clusterID,
		taskID:        taskID,
		httpClient:    httpClient,
		chunkID:       1, // start from 1.
	}
}

func (w *writeClient) Write(ctx context.Context, in *WriteRequest) error {
	defer func() {
		w.chunkID++
	}()
	url := fmt.Sprintf("%s/write_sst?cluster_id=%d&task_id=%d&chunk_id=%d",
		w.tikvWorkerURL, w.clusterID, w.taskID, w.chunkID)

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
	url := fmt.Sprintf("%s/write_sst?cluster_id=%d&task_id=%d&build=true&compression=zstd",
		w.tikvWorkerURL, w.clusterID, w.taskID)

	// TODO: pass retry counter metrics.
	data, err := sendRequest(ctx, w.httpClient, "POST", url, nil, nil)
	if err != nil {
		return nil, err
	}

	result := new(fileNameResult)
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
	httpClient    *http.Client

	currentTaskID int64
}

// NewClient creates a new Client instance.
func NewClient(tikvWorkerURL string, clusterID uint64, httpClient *http.Client) Client {
	return &client{
		tikvWorkerURL: tikvWorkerURL,
		clusterID:     clusterID,
		httpClient:    httpClient,
	}
}

func (c *client) WriteClient(ctx context.Context) (WriteClient, error) {
	c.currentTaskID++
	return newWriteClient(c.tikvWorkerURL, c.clusterID, c.currentTaskID, c.httpClient), nil
}

type fileNameResult struct {
	FileName string `json:"file_name"`
}

func (c *client) Ingest(ctx context.Context, in *IngestRequest) error {
	// TODO(tangenta): implement when the tikv worker API is ready.
	return nil
}
