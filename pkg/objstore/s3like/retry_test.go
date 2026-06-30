// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package s3like

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/aws/smithy-go"
	smithyhttp "github.com/aws/smithy-go/transport/http"
	"github.com/pingcap/log"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

const retryableCheckLogMessage = "failed to request s3, checking whether we can retry"

func TestRetryerSuppressesBucketRegionRedirectWarningOnlyWhenConfigured(t *testing.T) {
	err := newRetryerTestHTTPAPIError("MovedPermanently", http.StatusMovedPermanently)

	core, recorded := observer.New(zap.WarnLevel)
	restore := log.ReplaceGlobals(
		zap.New(core),
		&log.ZapProperties{
			Core:  core,
			Level: zap.NewAtomicLevelAt(zap.WarnLevel),
		},
	)
	defer restore()

	retryer := NewRetryerWithLogSuppressor(
		&testStandardRetryer{retryable: true},
		IsBucketRegionRedirectError,
	)
	require.True(t, retryer.IsErrorRetryable(err))
	require.Empty(t, recorded.FilterMessage(retryableCheckLogMessage).All())

	normalRetryer := NewRetryer(&testStandardRetryer{retryable: true})
	require.True(t, normalRetryer.IsErrorRetryable(err))
	require.Len(t, recorded.FilterMessage(retryableCheckLogMessage).All(), 1)
}

func TestIsBucketRegionRedirectErrorIsConservative(t *testing.T) {
	require.True(t, IsBucketRegionRedirectError(newRetryerTestHTTPAPIError("MovedPermanently", http.StatusMovedPermanently)))
	require.True(t, IsBucketRegionRedirectError(newRetryerTestHTTPAPIError("PermanentRedirect", http.StatusMovedPermanently)))

	require.False(t, IsBucketRegionRedirectError(newRetryerTestHTTPAPIError("MovedPermanently", http.StatusForbidden)))
	require.False(t, IsBucketRegionRedirectError(newRetryerTestHTTPAPIError("AccessDenied", http.StatusMovedPermanently)))
	require.False(t, IsBucketRegionRedirectError(&smithy.GenericAPIError{Code: "MovedPermanently", Message: "missing response"}))
}

func newRetryerTestHTTPAPIError(code string, statusCode int) error {
	return &smithyhttp.ResponseError{
		Response: &smithyhttp.Response{
			Response: &http.Response{StatusCode: statusCode},
		},
		Err: &smithy.GenericAPIError{Code: code, Message: "test error", Fault: smithy.FaultUnknown},
	}
}

type testStandardRetryer struct {
	retryable bool
}

func (r *testStandardRetryer) IsErrorRetryable(error) bool {
	return r.retryable
}

func (r *testStandardRetryer) MaxAttempts() int {
	return 1
}

func (r *testStandardRetryer) RetryDelay(int, error) (time.Duration, error) {
	return 0, nil
}

func (r *testStandardRetryer) GetRetryToken(context.Context, error) (func(error) error, error) {
	return func(error) error { return nil }, nil
}

func (r *testStandardRetryer) GetInitialToken() func(error) error {
	return func(error) error { return nil }
}

func (r *testStandardRetryer) IsInstanceMetadataError(error) bool {
	return false
}
