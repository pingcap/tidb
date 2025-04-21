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
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/util/intest"
	"golang.org/x/sync/errgroup"
)

var _ WriteClient = &writeClient{}

type writeClient struct {
	tikvWorkerURL string
	clusterID     uint64
	taskID        int64
	httpClient    *http.Client
	commitTS      uint64

	eg          errgroup.Group
	inited      bool
	errCh       chan error
	writer      *io.PipeWriter
	reader      *io.PipeReader
	sstFilePath string
}

// newWriteClient creates a writeClient. It is caller's responsibility to call Close() when it meets an error.
func newWriteClient(tikvWorkerURL string, clusterID uint64, taskID int64, httpClient *http.Client) *writeClient {
	return &writeClient{
		tikvWorkerURL: tikvWorkerURL,
		clusterID:     clusterID,
		taskID:        taskID,
		httpClient:    httpClient,
	}
}

func (w *writeClient) init(ctx context.Context) error {
	intest.Assert(!w.inited)
	defer func() {
		w.inited = true
	}()
	pr, pw := io.Pipe()
	url := fmt.Sprintf("%s/write_sst?cluster_id=%d&commit_ts=%d",
		w.tikvWorkerURL, w.clusterID, w.commitTS)
	req, err := http.NewRequestWithContext(ctx, "POST", url, pr)
	if err != nil {
		return errors.Trace(err)
	}
	req.Header.Set("Content-Type", "application/octet-stream")
	w.errCh = make(chan error, 1)
	w.startChunkedHTTPRequest(req)
	w.reader = pr
	w.writer = pw
	return nil
}

func (w *writeClient) startChunkedHTTPRequest(req *http.Request) {
	w.eg.Go(func() error {
		defer close(w.errCh)
		resp, err := w.httpClient.Do(req)
		if err != nil {
			w.errCh <- err
			return errors.Trace(err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			body, _ := io.ReadAll(resp.Body)
			return fmt.Errorf("failed to send chunked request: %s", string(body))
		}
		data, err := io.ReadAll(resp.Body)
		if err != nil {
			w.errCh <- err
			return errors.Trace(err)
		}
		result := new(sstFileResult)
		err = json.Unmarshal(data, result)
		if err != nil {
			w.errCh <- err
			return errors.Trace(err)
		}
		w.sstFilePath = result.SSTFile
		return nil
	})
}

func (w *writeClient) checkErrBeforeWrite() error {
	select {
	case err := <-w.errCh:
		return err
	default:
		return nil
	}
}

func (w *writeClient) WriteChunk(req *WriteRequest) error {
	err := w.checkErrBeforeWrite()
	if err != nil {
		return err
	}
	for _, pair := range req.Pairs {
		keyLen := uint16(len(pair.Key))
		if err := binary.Write(w.writer, binary.BigEndian, keyLen); err != nil {
			return errors.Trace(err)
		}
		if _, err := w.writer.Write(pair.Key); err != nil {
			return errors.Trace(err)
		}
		valLen := uint32(len(pair.Value))
		if err := binary.Write(w.writer, binary.BigEndian, valLen); err != nil {
			return errors.Trace(err)
		}
		if _, err := w.writer.Write(pair.Value); err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

func (w *writeClient) Close() (*WriteResponse, error) {
	if err := w.writer.Close(); err != nil {
		return nil, err
	}
	err := w.eg.Wait()
	if err != nil {
		return nil, err
	}
	if err := w.reader.Close(); err != nil {
		return nil, err
	}
	return &WriteResponse{SSTFile: w.sstFilePath}, nil
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
	cli := newWriteClient(c.tikvWorkerURL, c.clusterID, c.currentTaskID, c.httpClient)
	cli.init(ctx)
	return cli, nil
}

type sstFileResult struct {
	SSTFile string `json:"sst_file"`
}

func (c *client) Ingest(ctx context.Context, in *IngestRequest) error {
	// TODO(tangenta): implement when the tikv worker API is ready.
	return nil
}
