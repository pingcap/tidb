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
	"io"
	"net/http"
	"time"

	"github.com/pingcap/tidb/pkg/lightning/log"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
)

var (
	sleepDuration = 3 * time.Second
	retryCount    = 6000 // 3s * 6000 = 5h
)

func retryableHTTPStatusCode(statusCode int) bool {
	return statusCode == http.StatusServiceUnavailable || // 503
		statusCode == http.StatusInternalServerError || // 500
		statusCode == http.StatusRequestTimeout // 408
}

func sendRequestWithRetry(ctx context.Context, httpClient *http.Client,
	method, url string, data []byte, retryCounter prometheus.Counter) (*http.Response, error) {
	var (
		req  *http.Request
		resp *http.Response
		err  error
	)
	for retry := 0; retry < retryCount; retry++ {
		body := bytes.NewReader(data)
		req, err = http.NewRequestWithContext(ctx, method, url, body)
		if err != nil {
			return nil, err
		}
		resp, err = httpClient.Do(req)
		if err != nil || retryableHTTPStatusCode(resp.StatusCode) {
			statusCode := 0
			if resp != nil {
				statusCode = resp.StatusCode
			}
			log.FromContext(ctx).Warn("failed to send http request", zap.Error(err), zap.Int("status code", statusCode))
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(sleepDuration):
				if retryCounter != nil {
					retryCounter.Inc()
				}
				continue
			}
		}
		break
	}
	return resp, err
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
