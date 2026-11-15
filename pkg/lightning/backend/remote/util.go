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
	cryptorand "crypto/rand"
	"encoding/binary"
	"io"
	"math/rand"
	"net/http"
	"sync"
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
	maxDuplicateBatchSize      = 4 << 20
	maxResponseBodyLogBytes    = 4 << 10
	maxSPFreshControlBodyBytes = 4 << 10
	taskExitsMsg               = "task exists"
	taskExceedMaxSizeMsg       = "exceeds max data size"

	sleepDuration = 3 * time.Second
	retryCount    = 6000 // 3s * 6000 = 5h

	updateFlushedChunkDuration = 10 * time.Second
)

var (
	shortOwnerBoundRetryDelay = jitterOwnerBoundRetryDelay
	longOwnerBoundRetryDelay  = func(int) time.Duration {
		return sleepDuration
	}
)

type ownerBoundRequestSender func(
	context.Context,
	*http.Client,
	string,
	string,
	[]byte,
	map[string]string,
	prometheus.Counter,
) (*http.Response, error)

var ownerBoundRetryJitter = struct {
	sync.Mutex
	rng *rand.Rand
}{
	rng: rand.New(rand.NewSource(newOwnerBoundRetryJitterSeed())),
}

func genLoadDataTaskID(keyspaceID uint32, cfg *backend.EngineConfig) string {
	return backend.RemoteTaskID(keyspaceID, cfg)
}

func retryableOwnerBoundHTTPStatusCode(statusCode int) bool {
	switch statusCode {
	case http.StatusRequestTimeout, // 408
		http.StatusTooManyRequests,     // 429
		http.StatusInternalServerError, // 500
		http.StatusBadGateway,          // 502
		http.StatusServiceUnavailable,  // 503
		http.StatusGatewayTimeout:      // 504
		return true
	default:
		return false
	}
}

// readResponseBodyForLog reads the response body and returns it as a byte slice.
// It properly handles the closing of the response body using defer.
func readResponseBodyForLog(ctx context.Context, resp *http.Response) []byte {
	if resp.Body == nil {
		return nil
	}

	logger := log.FromContext(ctx)
	defer resp.Body.Close()

	body, err := io.ReadAll(io.LimitReader(resp.Body, maxResponseBodyLogBytes))
	if err != nil {
		logger.Warn("failed to read response body", zap.Error(err))
		return nil
	}
	return body
}

func jitterOwnerBoundRetryDelay(attempt int) time.Duration {
	backoffs := [...]time.Duration{
		100 * time.Millisecond,
		300 * time.Millisecond,
		time.Second,
	}
	if attempt < 0 {
		attempt = 0
	}
	if attempt >= len(backoffs) {
		attempt = len(backoffs) - 1
	}
	base := backoffs[attempt]
	jitter := base / 5
	if jitter <= 0 {
		return base
	}
	ownerBoundRetryJitter.Lock()
	n := ownerBoundRetryJitter.rng.Int63n(int64(2*jitter) + 1)
	ownerBoundRetryJitter.Unlock()
	return base - jitter + time.Duration(n)
}

func newOwnerBoundRetryJitterSeed() int64 {
	var seedBytes [8]byte
	if _, err := cryptorand.Read(seedBytes[:]); err == nil {
		return int64(binary.LittleEndian.Uint64(seedBytes[:]))
	}
	return time.Now().UnixNano()
}

func sendOwnerBoundRequestWithRetryInternal(
	ctx context.Context,
	httpClient *http.Client,
	method, url string,
	data []byte,
	headers map[string]string,
	retryCounter prometheus.Counter,
	maxAttempts int,
	retryDelay func(int) time.Duration,
) (*http.Response, error) {
	if httpClient == nil {
		httpClient = http.DefaultClient
	}

	client := *httpClient
	client.CheckRedirect = func(_ *http.Request, _ []*http.Request) error {
		return http.ErrUseLastResponse
	}
	httpClient = &client

	var lastErr error
	for attempt := 0; attempt < maxAttempts; attempt++ {
		req, err := http.NewRequestWithContext(ctx, method, url, bytes.NewReader(data))
		if err != nil {
			return nil, err
		}
		for k, v := range headers {
			req.Header.Set(k, v)
		}

		resp, err := httpClient.Do(req)
		if err == nil && !retryableOwnerBoundHTTPStatusCode(resp.StatusCode) {
			return resp, nil
		}
		if err != nil {
			if ctxErr := ctx.Err(); ctxErr != nil {
				return nil, ctxErr
			}
			lastErr = err
		} else {
			lastErr = errors.Errorf("http request failed with status code: %d", resp.StatusCode)
		}

		statusCode := 0
		var body []byte
		if resp != nil {
			statusCode = resp.StatusCode
			body = readResponseBodyForLog(ctx, resp)
		}
		log.FromContext(ctx).Warn("failed to send owner-bound http request",
			zap.Error(lastErr),
			zap.Int("status code", statusCode),
			zap.ByteString("response body", body),
			zap.String("method", method),
			zap.String("url", url),
			zap.Int("attempt", attempt+1))

		if attempt == maxAttempts-1 {
			break
		}
		if retryCounter != nil {
			retryCounter.Inc()
		}
		delay := retryDelay(attempt)
		if delay > 0 {
			timer := time.NewTimer(delay)
			select {
			case <-ctx.Done():
				if !timer.Stop() {
					select {
					case <-timer.C:
					default:
					}
				}
				return nil, ctx.Err()
			case <-timer.C:
			}
		}
	}

	return nil, errors.Wrap(lastErr, "exceeded owner-bound remote request retry attempts")
}

func sendShortOwnerBoundRequestWithRetry(ctx context.Context, httpClient *http.Client,
	method, url string, data []byte, retryCounter prometheus.Counter) (*http.Response, error) {
	const maxAttempts = 3
	return sendOwnerBoundRequestWithRetryInternal(
		ctx,
		httpClient,
		method,
		url,
		data,
		nil,
		retryCounter,
		maxAttempts,
		shortOwnerBoundRetryDelay,
	)
}

func sendShortOwnerBoundRequestWithRetryWithHeaders(
	ctx context.Context,
	httpClient *http.Client,
	method, url string,
	data []byte,
	headers map[string]string,
	retryCounter prometheus.Counter,
) (*http.Response, error) {
	const maxAttempts = 3
	return sendOwnerBoundRequestWithRetryInternal(
		ctx,
		httpClient,
		method,
		url,
		data,
		headers,
		retryCounter,
		maxAttempts,
		shortOwnerBoundRetryDelay,
	)
}

func sendLongOwnerBoundRequestWithRetry(ctx context.Context, httpClient *http.Client,
	method, url string, data []byte, retryCounter prometheus.Counter) (*http.Response, error) {
	return sendOwnerBoundRequestWithRetryInternal(
		ctx,
		httpClient,
		method,
		url,
		data,
		nil,
		retryCounter,
		retryCount,
		longOwnerBoundRetryDelay,
	)
}

func sendLongOwnerBoundRequestWithRetryWithHeaders(
	ctx context.Context,
	httpClient *http.Client,
	method, url string,
	data []byte,
	headers map[string]string,
	retryCounter prometheus.Counter,
) (*http.Response, error) {
	return sendOwnerBoundRequestWithRetryInternal(
		ctx,
		httpClient,
		method,
		url,
		data,
		headers,
		retryCounter,
		retryCount,
		longOwnerBoundRetryDelay,
	)
}

func sendLongOwnerBoundRequest(ctx context.Context, httpClient *http.Client,
	method, url string, data []byte, retryCounter prometheus.Counter) ([]byte, error) {
	resp, err := sendLongOwnerBoundRequestWithRetry(ctx, httpClient, method, url, data, retryCounter)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, errors.Errorf("failed to send request to remote worker, url: %s, status code: %s", url, resp.Status)
	}
	return io.ReadAll(resp.Body)
}

func sendLongOwnerBoundRequestAllowAccepted(
	ctx context.Context,
	httpClient *http.Client,
	method, url string,
	data []byte,
	retryCounter prometheus.Counter,
) ([]byte, int, error) {
	resp, err := sendLongOwnerBoundRequestWithRetry(ctx, httpClient, method, url, data, retryCounter)
	if err != nil {
		return nil, 0, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusAccepted {
		return nil, resp.StatusCode, errors.Errorf("failed to send request to remote worker, url: %s, status code: %s", url, resp.Status)
	}
	body, err := io.ReadAll(resp.Body)
	return body, resp.StatusCode, err
}
