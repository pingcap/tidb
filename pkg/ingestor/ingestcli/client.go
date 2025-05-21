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
	goerrors "errors"
	"fmt"
	"io"
	"net/http"

	"github.com/gogo/protobuf/proto"
	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/errorpb"
	"github.com/pingcap/tidb/br/pkg/restore/split"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/redact"
	"go.uber.org/atomic"
	"go.uber.org/zap"
)

type jsonByteSlice []byte

// MarshalJSON implements the json.Marshaler interface.
// nextgen TiKV is using Vector<u8> to store the keys, when marshalling to json,
// it's a json array, while in golang, it will be a base64 encoded string.
func (s jsonByteSlice) MarshalJSON() ([]byte, error) {
	if s == nil {
		return []byte("null"), nil
	}
	tmp := make([]int, 0, len(s))
	for _, b := range s {
		tmp = append(tmp, int(b))
	}
	return json.Marshal(tmp)
}

type nextGenResp struct {
	SstMeta nextGenSSTMeta `json:"sst_meta"`
}

type nextGenSSTMeta struct {
	ID         int64         `json:"id"`
	Smallest   jsonByteSlice `json:"smallest"`
	Biggest    jsonByteSlice `json:"biggest"`
	MetaOffset int           `json:"meta-offset"`
	CommitTs   int           `json:"commit-ts"`
}

func (m *nextGenSSTMeta) String() string {
	return fmt.Sprintf("{ID: %d, Smallest: %s, Biggest: %s, CommitTs: %d}",
		m.ID, redact.Key(m.Smallest), redact.Key(m.Biggest), m.CommitTs)
}

var _ WriteClient = &writeClient{}

type writeClient struct {
	tikvWorkerURL string
	clusterID     uint64
	httpClient    *http.Client
	commitTS      uint64

	wg         util.WaitGroupWrapper
	sendReqErr atomic.Error
	writer     *io.PipeWriter
	reader     *io.PipeReader
	sstMeta    *nextGenSSTMeta
}

// newWriteClient creates a writeClient.
func newWriteClient(
	tikvWorkerURL string,
	clusterID uint64,
	httpClient *http.Client,
	commitTS uint64,
) *writeClient {
	return &writeClient{
		tikvWorkerURL: tikvWorkerURL,
		clusterID:     clusterID,
		commitTS:      commitTS,
		httpClient:    httpClient,
	}
}

func (w *writeClient) init(ctx context.Context) error {
	pr, pw := io.Pipe()
	url := fmt.Sprintf("%s/write_sst?cluster_id=%d&commit_ts=%d",
		w.tikvWorkerURL, w.clusterID, w.commitTS)
	req, err := http.NewRequestWithContext(ctx, "PUT", url, pr)
	if err != nil {
		return errors.Trace(err)
	}
	req.Header.Set("Content-Type", "application/octet-stream")
	w.startChunkedHTTPRequest(req)
	w.reader = pr // PipeReader will be closed by the httpClient.Do automatically
	w.writer = pw
	return nil
}

func (w *writeClient) startChunkedHTTPRequest(req *http.Request) {
	w.wg.RunWithLog(func() {
		resp, err := w.httpClient.Do(req)
		if err != nil {
			w.sendReqErr.Store(errors.Trace(err))
			return
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			body, err1 := io.ReadAll(resp.Body)
			if err1 != nil {
				w.sendReqErr.Store(errors.Annotate(err1, "failed to readAll response"))
			} else {
				w.sendReqErr.Store(errors.Errorf("failed to send chunked request: %s", string(body)))
			}
			return
		}
		data, err := io.ReadAll(resp.Body)
		if err != nil {
			w.sendReqErr.Store(errors.Trace(err))
			return
		}
		res := &nextGenResp{}
		if err = json.Unmarshal(data, res); err != nil {
			w.sendReqErr.Store(errors.Trace(err))
			return
		}
		w.sstMeta = &res.SstMeta
	})
}

func (w *writeClient) cause(err error) error {
	if goerrors.Is(err, io.ErrClosedPipe) {
		// we close the writer only on Recv or Close, else this error is caused by
		// closed Reader, i.e. the request failed. We need to wait the async routine
		// to finish setting sendReqErr to return the correct error.
		w.wg.Wait()
	}
	if reqErr := w.sendReqErr.Load(); reqErr != nil {
		return errors.Trace(reqErr)
	}
	return errors.Trace(err)
}

func (w *writeClient) Write(req *WriteRequest) (err error) {
	var buf bytes.Buffer
	for _, pair := range req.Pairs {
		keyLen := uint16(len(pair.Key))
		if err := binary.Write(&buf, binary.LittleEndian, keyLen); err != nil {
			return errors.Trace(err)
		}
		if _, err := buf.Write(pair.Key); err != nil {
			return errors.Trace(err)
		}
		valLen := uint32(len(pair.Value))
		if err := binary.Write(&buf, binary.LittleEndian, valLen); err != nil {
			return errors.Trace(err)
		}
		if _, err := buf.Write(pair.Value); err != nil {
			return errors.Trace(err)
		}
	}
	if _, err := w.writer.Write(buf.Bytes()); err != nil {
		return w.cause(err)
	}
	return nil
}

func (w *writeClient) Recv() (*WriteResponse, error) {
	if err := w.writer.Close(); err != nil {
		return nil, errors.Trace(err)
	}
	w.wg.Wait()
	return &WriteResponse{nextGenSSTMeta: w.sstMeta}, w.cause(nil)
}

func (w *writeClient) Close() {
	//nolint: errcheck
	_ = w.writer.Close()
	w.wg.Wait()
}

var _ Client = &client{}

type client struct {
	tikvWorkerURL string
	clusterID     uint64
	httpClient    *http.Client
	splitCli      split.SplitClient
}

// NewClient creates a new Client instance.
func NewClient(tikvWorkerURL string, clusterID uint64, httpClient *http.Client, splitCli split.SplitClient) Client {
	return &client{
		tikvWorkerURL: tikvWorkerURL,
		clusterID:     clusterID,
		httpClient:    httpClient,
		splitCli:      splitCli,
	}
}

func (c *client) WriteClient(ctx context.Context, commitTS uint64) (WriteClient, error) {
	cli := newWriteClient(c.tikvWorkerURL, c.clusterID, c.httpClient, commitTS)
	err := cli.init(ctx)
	return cli, err
}

func (c *client) Ingest(ctx context.Context, in *IngestRequest) error {
	ri := in.Region.Region
	store, err := c.splitCli.GetStore(ctx, in.Region.Leader.GetStoreId())
	if err != nil {
		return errors.Trace(err)
	}
	url := fmt.Sprintf("http://%s/ingest_s3?cluster_id=%d&region_id=%d&epoch_version=%d",
		store.GetStatusAddress(), c.clusterID, ri.Id, ri.RegionEpoch.Version)

	sstMeta := in.WriteResp.nextGenSSTMeta
	logutil.BgLogger().Debug("calling ingest", in.Region.ToZapFields(), zap.Stringer("sstMeta", sstMeta))
	data, err := json.Marshal(sstMeta)
	if err != nil {
		return errors.Trace(err)
	}
	bodyRd := bytes.NewReader(data)
	req, err := http.NewRequestWithContext(ctx, "POST", url, bodyRd)
	if err != nil {
		return errors.Trace(err)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return errors.Trace(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, err1 := io.ReadAll(resp.Body)
		if err1 != nil {
			return errors.Annotate(err1, "failed to readAll response")
		}
		var pbErr errorpb.Error
		if err := proto.Unmarshal(body, &pbErr); err != nil {
			return errors.Annotatef(err, "failed to unmarshal error(%s)", string(body))
		}
		// we annotate the SST ID to help diagnose.
		pbErr.Message = fmt.Sprintf("%s(ingest SST ID %d)", pbErr.Message, sstMeta.ID)
		return &PBError{Err: &pbErr}
	}
	return nil
}

// PBError is a implementation of error.
type PBError struct {
	Err *errorpb.Error
}

// Error implements the error.
func (re *PBError) Error() string {
	return re.Err.GetMessage()
}
