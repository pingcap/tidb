// Copyright 2025 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package remote

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/lightning/backend"
	"github.com/pingcap/tidb/pkg/lightning/log"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
)

var (
	// ErrTaskNotFound is returned when the task is not found in remote.
	ErrTaskNotFound = errors.New("task not found in remote")
	// ErrInitTask is returned when failed to init loadData task.
	ErrInitTask = errors.New("failed to init loadData task")
	// ErrGetTaskState is returned when failed to get task state.
	ErrGetTaskState = errors.New("failed to get task state")
	// ErrChunkSenderLoopQuited is returned when chunk sender loop quited.
	ErrChunkSenderLoopQuited = errors.New("chunk sender loop quited")
	// ErrUnexpectedChunkID is returned when met unexpected chunk id.
	ErrUnexpectedChunkID = errors.New("unexpected chunk id")
	// ErrTaskCanceled is returned when task is canceled.
	ErrTaskCanceled = errors.New("task is canceled")
	// ErrFlushRemoteWorker is returned when failed to flush remote worker.
	ErrFlushRemoteWorker = errors.New("failed to flush remote worker")
	// ErrChunkCacheClosed is returned when chunk cache is closed.
	ErrChunkCacheClosed = errors.New("chunk cache is closed")
	// ErrOverlap is returned when region has overlap data.
	ErrOverlap = errors.New("region has overlap data")
	// ErrDuplicateKey is returned when met duplicate key.
	ErrDuplicateKey = errors.New("too many duplicate key")
)

const (
	maxDuplicateBatchSize = 4 << 20
	taskExitsMsg          = "task exists"
	taskExceedMaxSizeMsg  = "exceeds max data size"

	sleepDuration = 3 * time.Second
	retryCount    = 6000 // 3s * 6000 = 5h

	updateFlushedChunkDuration = 10 * time.Second
)

func genLoadDataTaskID(keyspaceID uint32, cfg *backend.EngineConfig) string {
	return fmt.Sprintf("%d-%d-%d-%d", keyspaceID, cfg.TaskID, cfg.TableInfo.ID, cfg.EngineID)
}

func retryableHTTPStatusCode(statusCode int) bool {
	return statusCode == http.StatusServiceUnavailable || // 503
		statusCode == http.StatusInternalServerError || // 500
		statusCode == http.StatusRequestTimeout // 408
}

// readResponseBodyForLog reads the response body and returns it as a byte slice.
// It properly handles the closing of the response body using defer.
func readResponseBodyForLog(ctx context.Context, resp *http.Response) []byte {
	if resp.Body == nil {
		return nil
	}

	logger := log.FromContext(ctx)
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		logger.Warn("failed to read response body", zap.Error(err))
		return nil
	}
	return body
}

func sendRequestWithRetry(ctx context.Context, httpClient *http.Client,
	method, url string, data []byte, retryCounter prometheus.Counter) (*http.Response, error) {
	var lastErr error
	for retry := 0; retry < retryCount; retry++ {
		req, err := http.NewRequestWithContext(ctx, method, url, bytes.NewReader(data))
		if err != nil {
			return nil, err
		}

		resp, err := httpClient.Do(req)
		if err != nil {
			lastErr = err
		} else if retryableHTTPStatusCode(resp.StatusCode) {
			lastErr = errors.Errorf("http request failed with status code: %d", resp.StatusCode)
		} else {
			// Success case - return immediately
			return resp, nil
		}

		// Log the failure
		statusCode := 0
		var body []byte
		if resp != nil {
			statusCode = resp.StatusCode
			body = readResponseBodyForLog(ctx, resp)
		}
		log.FromContext(ctx).Warn("failed to send http request", zap.Error(lastErr),
			zap.Int("status code", statusCode), zap.ByteString("response body", body),
			zap.String("method", method), zap.String("url", url))

		// Check if context is done or sleep before retry
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(sleepDuration):
			if retryCounter != nil {
				retryCounter.Inc()
			}
		}
	}

	// Exceeded max retry count - lastErr is guaranteed to be non-nil now
	return nil, errors.Wrap(lastErr, "exceeded max retry count")
}

func sendRequest(ctx context.Context, httpClient *http.Client,
	method, url string, data []byte, retryCounter prometheus.Counter) ([]byte, error) {

	resp, err := sendRequestWithRetry(ctx, httpClient, method, url, data, retryCounter)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, errors.Errorf("failed to send request to remote worker, url: %s, status code: %s", url, resp.Status)
	}
	return io.ReadAll(resp.Body)
}
