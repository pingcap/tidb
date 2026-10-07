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

package advertisedstatus

import (
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/domain/serverinfo"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
)

const (
	endpointCheckTimeout      = 5 * time.Second
	endpointResponseBodyLimit = 1 << 20
	endpointWarningMessage    = "failed to verify advertised status endpoint identity"
)

type endpointCheckReason string

const (
	endpointRequestFailed    endpointCheckReason = "request-failed"
	endpointUnexpectedStatus endpointCheckReason = "unexpected-status"
	endpointInvalidResponse  endpointCheckReason = "invalid-response"
	endpointMissingIdentity  endpointCheckReason = "missing-identity"
	endpointIdentityMismatch endpointCheckReason = "identity-mismatch"
)

// Options contains the server-owned inputs needed for the advertised status endpoint check.
type Options struct {
	StatusListener   net.Listener
	AdvertiseAddress string
	LocalID          string
	ReportStatus     bool
}

type endpointCheckInput struct {
	endpoint string
	localID  string
}

type endpointCheckResult struct {
	err      error
	remoteID string
	reason   endpointCheckReason
	status   string
}

type testReporterKey struct{}

// Start schedules one warning-only advertised status endpoint check when all prerequisites are available.
func Start(ctx context.Context, options Options) {
	if !options.ReportStatus || options.StatusListener == nil || options.AdvertiseAddress == "" || options.LocalID == "" {
		return
	}

	effectivePort := options.StatusListener.Addr().(*net.TCPAddr).Port
	endpoint := (&url.URL{
		Scheme: util.InternalHTTPSchema(),
		Host:   net.JoinHostPort(options.AdvertiseAddress, strconv.Itoa(effectivePort)),
		Path:   "/info",
	}).String()
	input := endpointCheckInput{
		endpoint: endpoint,
		localID:  options.LocalID,
	}

	reporter := logEndpointCheckWarning
	if testReporter, ok := ctx.Value(testReporterKey{}).(func(
		endpointCheckInput,
		endpointCheckResult,
	)); ok {
		reporter = testReporter
	}

	client := newEndpointHTTPClient()
	go util.WithRecovery(func() {
		defer client.CloseIdleConnections()
		result := checkEndpoint(ctx, client, input.endpoint, input.localID)
		// Cancellation means this Server.Run invocation is ending, not that the endpoint failed verification.
		if ctx.Err() != nil || result.reason == "" {
			return
		}
		reporter(input, result)
	}, nil)
}

func newEndpointHTTPClient() *http.Client {
	var baseTransport *http.Transport
	if internalTransport := util.InternalHTTPClient().Transport; internalTransport == nil {
		baseTransport = http.DefaultTransport.(*http.Transport)
	} else {
		baseTransport = internalTransport.(*http.Transport)
	}
	directTransport := baseTransport.Clone()
	// Do not let a forward proxy or redirect make a different endpoint pass the identity check.
	directTransport.Proxy = nil
	return &http.Client{
		Transport: directTransport,
		Timeout:   endpointCheckTimeout,
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
}

func checkEndpoint(
	ctx context.Context,
	client *http.Client,
	endpoint string,
	expectedID string,
) endpointCheckResult {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return endpointCheckResult{reason: endpointRequestFailed, err: err}
	}
	response, err := client.Do(request)
	if err != nil {
		return endpointCheckResult{reason: endpointRequestFailed, err: err}
	}
	defer response.Body.Close()

	result := endpointCheckResult{status: response.Status}
	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		result.reason = endpointUnexpectedStatus
		return result
	}

	body, err := io.ReadAll(io.LimitReader(response.Body, endpointResponseBodyLimit+1))
	if err != nil {
		result.reason = endpointRequestFailed
		result.err = err
		return result
	}
	if len(body) > endpointResponseBodyLimit {
		result.reason = endpointInvalidResponse
		result.err = errors.Errorf("response body exceeds %d-byte limit", endpointResponseBodyLimit)
		return result
	}
	// Reuse the type embedded in the /info response so the ddl_id field stays aligned with the handler.
	var responseInfo serverinfo.StaticInfo
	if err := json.Unmarshal(body, &responseInfo); err != nil {
		result.reason = endpointInvalidResponse
		result.err = err
		return result
	}
	if responseInfo.ID == "" {
		result.reason = endpointMissingIdentity
		result.err = errors.New("response does not contain ddl_id")
		return result
	}
	result.remoteID = responseInfo.ID
	if responseInfo.ID != expectedID {
		result.reason = endpointIdentityMismatch
	}
	return result
}

func endpointWarningAction(reason endpointCheckReason) string {
	switch reason {
	case endpointRequestFailed:
		return "check DNS, network, TLS, and whether this TiDB instance can complete a request to the advertised status endpoint"
	case endpointUnexpectedStatus,
		endpointInvalidResponse,
		endpointMissingIdentity:
		return "check that advertise-address and status-port serve a valid TiDB /info response"
	case endpointIdentityMismatch:
		return "check that advertise-address and status-port route directly to this TiDB instance and that no TiDB exists outside the intended topology"
	default:
		return "inspect the error and advertised status endpoint"
	}
}

func logEndpointCheckWarning(
	input endpointCheckInput,
	result endpointCheckResult,
) {
	fields := make([]zap.Field, 0, 8)
	fields = append(fields, zap.String("advertised-status-endpoint", input.endpoint))
	fields = append(fields,
		zap.String("local-tidb-id", input.localID),
		zap.String("reason", string(result.reason)),
		zap.String("action", endpointWarningAction(result.reason)),
	)
	if result.remoteID != "" {
		fields = append(fields, zap.String("remote-tidb-id", result.remoteID))
	}
	if result.status != "" {
		fields = append(fields, zap.String("http-status", result.status))
	}
	if result.err != nil {
		fields = append(fields, zap.Error(result.err))
	}
	logutil.BgLogger().Warn(endpointWarningMessage, fields...)
}
