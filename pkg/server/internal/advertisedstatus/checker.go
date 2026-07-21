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
	advertisedStatusEndpointCheckTimeout      = 5 * time.Second
	advertisedStatusEndpointResponseBodyLimit = 1 << 20
	advertisedStatusEndpointWarningMessage    = "failed to verify advertised status endpoint identity"
)

type advertisedStatusEndpointCheckReason string

const (
	advertisedStatusEndpointInputSetupFailed  advertisedStatusEndpointCheckReason = "input-setup-failed"
	advertisedStatusEndpointClientSetupFailed advertisedStatusEndpointCheckReason = "client-setup-failed"
	advertisedStatusEndpointRequestFailed     advertisedStatusEndpointCheckReason = "request-failed"
	advertisedStatusEndpointUnexpectedStatus  advertisedStatusEndpointCheckReason = "unexpected-status"
	advertisedStatusEndpointInvalidResponse   advertisedStatusEndpointCheckReason = "invalid-response"
	advertisedStatusEndpointMissingIdentity   advertisedStatusEndpointCheckReason = "missing-identity"
	advertisedStatusEndpointIdentityMismatch  advertisedStatusEndpointCheckReason = "identity-mismatch"
)

// Options contains the server-owned inputs needed for the advertised status endpoint check.
type Options struct {
	StatusListener   net.Listener
	BaseHTTPClient   *http.Client
	AdvertiseAddress string
	LocalID          string
	Scheme           string
	ReportStatus     bool
}

type advertisedStatusEndpointCheckInput struct {
	endpoint         string
	advertiseAddress string
	localID          string
}

type advertisedStatusEndpointCheckResult struct {
	err      error
	remoteID string
	reason   advertisedStatusEndpointCheckReason
	status   string
}

type advertisedStatusEndpointCheckFunc func(
	context.Context,
	*http.Client,
	string,
	string,
) advertisedStatusEndpointCheckResult

type advertisedStatusEndpointWarningReporter func(
	advertisedStatusEndpointCheckInput,
	advertisedStatusEndpointCheckResult,
)

// Start schedules one warning-only advertised status endpoint check when all prerequisites are available.
func Start(ctx context.Context, options Options) {
	start(ctx, options, logAdvertisedStatusEndpointCheckWarning)
}

func start(ctx context.Context, options Options, reporter advertisedStatusEndpointWarningReporter) {
	if !options.ReportStatus || options.StatusListener == nil || options.AdvertiseAddress == "" || options.LocalID == "" {
		return
	}

	input, err := newAdvertisedStatusEndpointCheckInput(
		options.StatusListener,
		options.AdvertiseAddress,
		options.LocalID,
		options.Scheme,
	)
	if err != nil {
		reporter(input, advertisedStatusEndpointCheckResult{
			reason: advertisedStatusEndpointInputSetupFailed,
			err:    err,
		})
		return
	}

	client, err := newAdvertisedStatusEndpointHTTPClient(options.BaseHTTPClient, advertisedStatusEndpointCheckTimeout)
	if err != nil {
		reporter(input, advertisedStatusEndpointCheckResult{
			reason: advertisedStatusEndpointClientSetupFailed,
			err:    err,
		})
		return
	}

	scheduleAdvertisedStatusEndpointCheck(
		ctx,
		input,
		client,
		checkAdvertisedStatusEndpoint,
		reporter,
	)
}

func buildAdvertisedStatusEndpointURL(scheme, advertiseAddress string, effectivePort int) (string, error) {
	if scheme != "http" && scheme != "https" {
		return "", errors.Errorf("unsupported advertised status endpoint scheme %q", scheme)
	}
	if advertiseAddress == "" {
		return "", errors.New("advertise address is empty")
	}
	if effectivePort <= 0 || effectivePort > 65535 {
		return "", errors.Errorf("invalid effective status port %d", effectivePort)
	}
	endpoint := &url.URL{
		Scheme: scheme,
		Host:   net.JoinHostPort(advertiseAddress, strconv.Itoa(effectivePort)),
		Path:   "/info",
	}
	return endpoint.String(), nil
}

func newAdvertisedStatusEndpointCheckInput(
	statusListener net.Listener,
	advertiseAddress string,
	localID string,
	scheme string,
) (advertisedStatusEndpointCheckInput, error) {
	input := advertisedStatusEndpointCheckInput{
		advertiseAddress: advertiseAddress,
		localID:          localID,
	}
	listenerAddr := statusListener.Addr()
	if listenerAddr == nil {
		return input, errors.New("status listener address is nil")
	}
	_, portString, err := net.SplitHostPort(listenerAddr.String())
	if err != nil {
		return input, errors.Annotate(err, "parse status listener address")
	}
	effectivePort, err := strconv.Atoi(portString)
	if err != nil {
		return input, errors.Annotate(err, "parse effective status port")
	}
	input.endpoint, err = buildAdvertisedStatusEndpointURL(scheme, advertiseAddress, effectivePort)
	if err != nil {
		return input, err
	}
	return input, nil
}

func newAdvertisedStatusEndpointHTTPClient(baseClient *http.Client, timeout time.Duration) (*http.Client, error) {
	var baseTransport *http.Transport
	if baseClient == nil || baseClient.Transport == nil {
		var ok bool
		baseTransport, ok = http.DefaultTransport.(*http.Transport)
		if !ok {
			return nil, errors.New("default HTTP transport is not cloneable")
		}
	} else {
		var ok bool
		baseTransport, ok = baseClient.Transport.(*http.Transport)
		if !ok {
			return nil, errors.Errorf("internal HTTP transport has unsupported type %T", baseClient.Transport)
		}
	}
	directTransport := baseTransport.Clone()
	// Do not let a forward proxy or redirect make a different endpoint pass the identity check.
	directTransport.Proxy = nil
	return &http.Client{
		Transport: directTransport,
		Timeout:   timeout,
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}, nil
}

func checkAdvertisedStatusEndpoint(
	ctx context.Context,
	client *http.Client,
	endpoint string,
	expectedID string,
) advertisedStatusEndpointCheckResult {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return advertisedStatusEndpointCheckResult{reason: advertisedStatusEndpointRequestFailed, err: err}
	}
	response, err := client.Do(request)
	if err != nil {
		return advertisedStatusEndpointCheckResult{reason: advertisedStatusEndpointRequestFailed, err: err}
	}
	defer response.Body.Close()

	result := advertisedStatusEndpointCheckResult{status: response.Status}
	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		result.reason = advertisedStatusEndpointUnexpectedStatus
		return result
	}

	body, err := io.ReadAll(io.LimitReader(response.Body, advertisedStatusEndpointResponseBodyLimit+1))
	if err != nil {
		result.reason = advertisedStatusEndpointRequestFailed
		result.err = err
		return result
	}
	if len(body) > advertisedStatusEndpointResponseBodyLimit {
		result.reason = advertisedStatusEndpointInvalidResponse
		result.err = errors.Errorf("response body exceeds %d-byte limit", advertisedStatusEndpointResponseBodyLimit)
		return result
	}
	// Reuse the type embedded in the /info response so the ddl_id field stays aligned with the handler.
	var responseInfo serverinfo.StaticInfo
	if err := json.Unmarshal(body, &responseInfo); err != nil {
		result.reason = advertisedStatusEndpointInvalidResponse
		result.err = err
		return result
	}
	if responseInfo.ID == "" {
		result.reason = advertisedStatusEndpointMissingIdentity
		result.err = errors.New("response does not contain ddl_id")
		return result
	}
	result.remoteID = responseInfo.ID
	if responseInfo.ID != expectedID {
		result.reason = advertisedStatusEndpointIdentityMismatch
	}
	return result
}

func advertisedStatusEndpointWarningAction(reason advertisedStatusEndpointCheckReason) string {
	switch reason {
	case advertisedStatusEndpointInputSetupFailed:
		return "check the effective status listener address and advertised endpoint settings"
	case advertisedStatusEndpointClientSetupFailed:
		return "check the internal HTTP client transport setup"
	case advertisedStatusEndpointRequestFailed:
		return "check DNS, network, TLS, and whether this TiDB instance can complete a request to the advertised status endpoint"
	case advertisedStatusEndpointUnexpectedStatus,
		advertisedStatusEndpointInvalidResponse,
		advertisedStatusEndpointMissingIdentity:
		return "check that advertise-address and status-port serve a valid TiDB /info response"
	case advertisedStatusEndpointIdentityMismatch:
		return "check that advertise-address and status-port route directly to this TiDB instance and that no TiDB exists outside the intended topology"
	default:
		return "inspect the error and advertised status endpoint"
	}
}

func logAdvertisedStatusEndpointCheckWarning(
	input advertisedStatusEndpointCheckInput,
	result advertisedStatusEndpointCheckResult,
) {
	fields := make([]zap.Field, 0, 8)
	if input.endpoint != "" {
		fields = append(fields, zap.String("advertised-status-endpoint", input.endpoint))
	} else if input.advertiseAddress != "" {
		fields = append(fields, zap.String("advertise-address", input.advertiseAddress))
	}
	fields = append(fields,
		zap.String("local-tidb-id", input.localID),
		zap.String("reason", string(result.reason)),
		zap.String("action", advertisedStatusEndpointWarningAction(result.reason)),
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
	logutil.BgLogger().Warn(advertisedStatusEndpointWarningMessage, fields...)
}

func scheduleAdvertisedStatusEndpointCheck(
	ctx context.Context,
	input advertisedStatusEndpointCheckInput,
	client *http.Client,
	checker advertisedStatusEndpointCheckFunc,
	reporter advertisedStatusEndpointWarningReporter,
) {
	go util.WithRecovery(func() {
		defer client.CloseIdleConnections()
		result := checker(ctx, client, input.endpoint, input.localID)
		// Cancellation means this Server.Run invocation is ending, not that the endpoint failed verification.
		if ctx.Err() != nil || result.reason == "" {
			return
		}
		reporter(input, result)
	}, nil)
}
