// Copyright 2026 PingCAP, Inc.
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

package remote

import (
	"bytes"
	"context"
	"encoding/json"
	stdErrors "errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/lightning/log"
	"go.uber.org/zap"
)

// SPFreshControlSendError wraps failures that happen while sending an SPFresh
// lifecycle request before any HTTP response is available.
type SPFreshControlSendError struct {
	Method string
	URL    string
	Err    error
}

func (e *SPFreshControlSendError) Error() string {
	return fmt.Sprintf("failed to send SPFresh build control request %s %s: %v", e.Method, e.URL, e.Err)
}

func (e *SPFreshControlSendError) Unwrap() error {
	return e.Err
}

func sendSPFreshBuildSealRequest(
	ctx context.Context,
	httpClient *http.Client,
	workerURL string,
	clusterID uint64,
	buildShardID string,
	sealRequest SPFreshBuildSealRequest,
) ([]byte, error) {
	if httpClient == nil {
		return nil, errors.New("spfresh build seal requires http client")
	}
	if buildShardID == "" {
		return nil, errors.New("spfresh build seal requires task ID")
	}
	addr := strings.TrimSuffix(workerURL, "/")
	if addr == "" {
		return nil, errors.New("spfresh build seal requires worker URL")
	}

	requestBody, err := json.Marshal(sealRequest)
	if err != nil {
		return nil, err
	}
	values := url.Values{}
	values.Set("cluster_id", fmt.Sprintf("%d", clusterID))
	values.Set("task_id", buildShardID)
	values.Set("seal", "true")
	requestURL := fmt.Sprintf("%s/spfresh/build?%s", addr, values.Encode())
	missingMetadataCount := 0
	for {
		resp, err := sendLongOwnerBoundRequestWithRetryWithHeaders(ctx, httpClient, http.MethodPost, requestURL, requestBody, map[string]string{"Content-Type": "application/json"}, nil)
		if err != nil {
			return nil, err
		}
		body, statusCode, err := readSPFreshBuildControlResponseAllowAccepted(resp, "seal")
		if err != nil {
			return nil, err
		}
		if statusCode != http.StatusAccepted {
			return body, nil
		}

		// A 202 from seal means that the worker accepted the barrier but is
		// still recovering its transaction. Do not return this as a retryable
		// disttask error: keep the asynchronous barrier in this coordinator and
		// poll the same seal request instead of restarting the whole subtask.
		var state spfreshRecoveryState
		if err := json.Unmarshal(body, &state); err != nil && len(body) != 0 {
			// The HTTP status is authoritative for an accepted barrier. Keep
			// polling with bounded fallback backoff when the optional recovery
			// payload is malformed instead of turning asynchronous recovery
			// terminal.
			log.FromContext(ctx).Warn("SPFresh build seal accepted with invalid recovery metadata",
				zap.String("taskID", buildShardID),
				zap.Error(err))
		}
		log.FromContext(ctx).Debug("SPFresh build seal accepted while remote task recovers",
			zap.String("taskID", buildShardID),
			zap.String("status", state.Status),
			zap.String("retryReason", state.RetryReason),
			zap.Uint64("retryAttempt", state.RetryAttempt))
		missingMetadataCount, err = waitForSPFreshBuildRetry(
			ctx,
			state.NextRetryAtMs,
			missingMetadataCount,
		)
		if err != nil {
			return nil, err
		}
	}
}

func sendSPFreshBuildDeleteRequest(
	ctx context.Context,
	httpClient *http.Client,
	workerURL string,
	clusterID uint64,
	taskIDPrefix string,
) ([]byte, error) {
	if httpClient == nil {
		return nil, errors.New("spfresh build delete_by_prefix requires http client")
	}
	if taskIDPrefix == "" {
		return nil, errors.New("spfresh build delete_by_prefix requires task ID")
	}
	addr := strings.TrimSuffix(workerURL, "/")
	if addr == "" {
		return nil, errors.New("spfresh build delete_by_prefix requires worker URL")
	}

	values := url.Values{}
	values.Set("cluster_id", fmt.Sprintf("%d", clusterID))
	values.Set("task_id", taskIDPrefix)
	values.Set("task_id_prefix", taskIDPrefix)
	requestURL := fmt.Sprintf("%s/spfresh/build?%s", addr, values.Encode())
	resp, err := sendShortOwnerBoundRequestWithRetry(ctx, httpClient, http.MethodDelete, requestURL, nil, nil)
	if err != nil {
		return nil, err
	}
	return readSPFreshBuildControlResponse(resp, "delete_by_prefix")
}

func readSPFreshBuildControlResponse(resp *http.Response, operation string) ([]byte, error) {
	body, statusCode, err := readSPFreshBuildControlResponseAllowAccepted(resp, operation)
	if err != nil {
		return nil, err
	}
	if statusCode != http.StatusOK {
		return nil, errors.WithStack(&SPFreshControlRequestError{
			Operation:  operation,
			Status:     resp.Status,
			StatusCode: statusCode,
			Body:       strings.TrimSpace(string(body)),
		})
	}
	return body, nil
}

func readSPFreshBuildControlResponseAllowAccepted(resp *http.Response, operation string) ([]byte, int, error) {
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxSPFreshControlBodyBytes+1))
	if err != nil {
		return nil, 0, err
	}
	if len(body) > maxSPFreshControlBodyBytes {
		return nil, 0, errors.Errorf("SPFresh build %s response exceeds %d bytes", operation, maxSPFreshControlBodyBytes)
	}
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusAccepted {
		return nil, resp.StatusCode, errors.WithStack(&SPFreshControlRequestError{
			Operation:  operation,
			Status:     resp.Status,
			StatusCode: resp.StatusCode,
			Body:       strings.TrimSpace(string(body)),
		})
	}
	return body, resp.StatusCode, nil
}

func sendOneShotSPFreshBuildCommitRequest(
	ctx context.Context,
	httpClient *http.Client,
	workerURL string,
	clusterID uint64,
	body []byte,
) error {
	addr := strings.TrimSuffix(workerURL, "/")
	if addr == "" {
		return errors.New("spfresh build commit requires worker URL")
	}

	requestURL := fmt.Sprintf("%s/spfresh/build/commit?cluster_id=%d", addr, clusterID)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, requestURL, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	client := *httpClient
	client.CheckRedirect = func(_ *http.Request, _ []*http.Request) error {
		return http.ErrUseLastResponse
	}
	resp, err := client.Do(req)
	if err != nil {
		return &SPFreshControlSendError{
			Method: http.MethodPost,
			URL:    requestURL,
			Err:    err,
		}
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, maxSPFreshControlBodyBytes))
		trimmed := strings.TrimSpace(string(msg))
		return errors.WithStack(&SPFreshControlRequestError{
			Operation:  "commit",
			Status:     resp.Status,
			StatusCode: resp.StatusCode,
			Body:       trimmed,
		})
	}
	return nil
}

func validateSPFreshBuildSealedResponse(body []byte, operation string) error {
	var states struct {
		Status string `json:"status"`
		Error  string `json:"error"`
	}
	if err := json.Unmarshal(body, &states); err != nil {
		return err
	}
	if states.Error != "" {
		return errors.Errorf("SPFresh build %s failed: %s", operation, states.Error)
	}
	if strings.EqualFold(states.Status, "sealed") {
		return nil
	}
	return errors.Errorf("SPFresh build %s returned status %q, want sealed", operation, states.Status)
}

// IsRetryableSPFreshBuildCommitError reports whether a routed commit may be
// retried on another configured TiKV API service endpoint.
func IsRetryableSPFreshBuildCommitError(err error) bool {
	if stdErrors.Is(err, context.Canceled) || stdErrors.Is(err, context.DeadlineExceeded) {
		return false
	}
	var sendErr *SPFreshControlSendError
	if stdErrors.As(err, &sendErr) {
		return true
	}
	var controlErr *SPFreshControlRequestError
	if stdErrors.As(err, &controlErr) {
		return retryableOwnerBoundHTTPStatusCode(controlErr.StatusCode)
	}
	return false
}
