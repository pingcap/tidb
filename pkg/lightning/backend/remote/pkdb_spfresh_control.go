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
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/util/dbterror"
)

func postSPFreshControlRequest(
	ctx context.Context,
	httpClient *http.Client,
	workerURL string,
	clusterID uint64,
	path string,
	body []byte,
	headers map[string]string,
	operation string,
) error {
	return postSPFreshControlRequestWithSender(
		ctx, httpClient, workerURL, clusterID, path, body, headers, operation,
		sendLongOwnerBoundRequestWithRetryWithHeaders,
	)
}

func postShortSPFreshControlRequest(
	ctx context.Context,
	httpClient *http.Client,
	workerURL string,
	clusterID uint64,
	path string,
	body []byte,
	headers map[string]string,
	operation string,
) error {
	return postSPFreshControlRequestWithSender(
		ctx, httpClient, workerURL, clusterID, path, body, headers, operation,
		sendShortOwnerBoundRequestWithRetryWithHeaders,
	)
}

func postSPFreshControlRequestWithSender(
	ctx context.Context,
	httpClient *http.Client,
	workerURL string,
	clusterID uint64,
	path string,
	body []byte,
	headers map[string]string,
	operation string,
	sendRequest ownerBoundRequestSender,
) error {
	if httpClient == nil {
		return errors.Errorf("spfresh %s requires http client", operation)
	}
	addr := strings.TrimSuffix(workerURL, "/")
	if addr == "" {
		return errors.Errorf("spfresh %s requires worker URL", operation)
	}

	url := fmt.Sprintf("%s%s?cluster_id=%d", addr, path, clusterID)
	resp, err := sendRequest(ctx, httpClient, http.MethodPost, url, body, headers, nil)
	if err != nil {
		return err
	}
	if resp.StatusCode == http.StatusOK {
		_ = resp.Body.Close()
		return nil
	}
	return spfreshControlRequestError(resp, operation)
}

// SPFreshControlRequestError describes a non-OK SPFresh lifecycle response.
type SPFreshControlRequestError struct {
	Operation  string
	Status     string
	StatusCode int
	Body       string
	cause      error
}

func (e *SPFreshControlRequestError) Error() string {
	if e.cause != nil {
		return e.cause.Error()
	}
	if e.Body != "" {
		return dbterror.ErrIngestFailed.FastGenByArgs(e.Body).Error()
	}
	return fmt.Sprintf("failed to %s SPFresh index, status: %s", e.Operation, e.Status)
}

func (e *SPFreshControlRequestError) Unwrap() error {
	return e.cause
}

// Cause returns the underlying TiDB error for pingcap/errors compatibility.
func (e *SPFreshControlRequestError) Cause() error {
	return e.cause
}

// Retryable reports whether the remote worker explicitly asked the caller to
// retry this control request. In particular, HTTP 202 means an SPFresh
// flush/seal barrier was accepted while transient recovery is still running;
// it must not be treated as a terminal DDL failure.
func (e *SPFreshControlRequestError) Retryable() bool {
	return e.StatusCode == http.StatusAccepted || retryableOwnerBoundHTTPStatusCode(e.StatusCode)
}

func spfreshControlRequestError(resp *http.Response, operation string) error {
	msg, _ := io.ReadAll(io.LimitReader(resp.Body, maxSPFreshControlBodyBytes))
	_ = resp.Body.Close()
	trimmed := strings.TrimSpace(string(msg))
	var cause error
	if trimmed != "" {
		cause = dbterror.ErrIngestFailed.FastGenByArgs(trimmed)
	}
	return errors.WithStack(&SPFreshControlRequestError{
		Operation:  operation,
		Status:     resp.Status,
		StatusCode: resp.StatusCode,
		Body:       trimmed,
		cause:      cause,
	})
}
