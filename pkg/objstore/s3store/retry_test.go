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

package s3store

import (
	"context"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/aws/smithy-go"
	smithyhttp "github.com/aws/smithy-go/transport/http"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

func TestS3TidbRetryerNeverExhaustTokens(t *testing.T) {
	retry := newRetryer()
	ctx := context.Background()
	// default retry.NewStandard only have 500 tokens
	opErr := &net.DNSError{IsTimeout: true}
	for range 10000 {
		_, err := retry.GetRetryToken(ctx, opErr)
		require.NoError(t, err)
	}
}

func TestS3TiDBRetryer(t *testing.T) {
	retry := newRetryer()
	// S3 will run for retryer.MaxAttempts() attempts, so will have MaxAttempts - 1
	// retries and delay between retries
	var totalDelay time.Duration
	for i := 1; i < retry.MaxAttempts(); i++ {
		delay, _ := retry.RetryDelay(i, nil)
		totalDelay += delay
	}
	require.Greater(t, totalDelay, 7*time.Minute)
	require.Less(t, totalDelay, 9*time.Minute)
	t.Log(totalDelay)
}

func TestRetryerIsInstanceMetadataError(t *testing.T) {
	retry := newRetryer()
	require.False(t, retry.IsErrorRetryable(errors.Annotate(context.DeadlineExceeded, "169.254.169.254")))
	require.True(t, retry.IsErrorRetryable(errors.Annotate(context.DeadlineExceeded, "normal err")))
}

func TestIsBucketRegionRedirectError(t *testing.T) {
	newHTTPAPIError := func(code string, statusCode int) error {
		return &smithyhttp.ResponseError{
			Response: &smithyhttp.Response{
				Response: &http.Response{StatusCode: statusCode},
			},
			Err: &smithy.GenericAPIError{Code: code, Message: "test error", Fault: smithy.FaultUnknown},
		}
	}

	require.True(t, isBucketRegionRedirectError(newHTTPAPIError("MovedPermanently", http.StatusMovedPermanently)))
	require.True(t, isBucketRegionRedirectError(newHTTPAPIError("PermanentRedirect", http.StatusMovedPermanently)))

	require.False(t, isBucketRegionRedirectError(newHTTPAPIError("MovedPermanently", http.StatusForbidden)))
	require.False(t, isBucketRegionRedirectError(newHTTPAPIError("AccessDenied", http.StatusMovedPermanently)))
	require.False(t, isBucketRegionRedirectError(&smithy.GenericAPIError{Code: "MovedPermanently", Message: "missing response"}))
}

func TestBucketRegionDetectionRetryerSuppressesOnlyExpectedRedirectWarning(t *testing.T) {
	err := &smithyhttp.ResponseError{
		Response: &smithyhttp.Response{
			Response: &http.Response{StatusCode: http.StatusMovedPermanently},
		},
		Err: &smithy.GenericAPIError{Code: "MovedPermanently", Message: "test error", Fault: smithy.FaultUnknown},
	}

	core, recorded := observer.New(zap.WarnLevel)
	restore := log.ReplaceGlobals(
		zap.New(core),
		&log.ZapProperties{
			Core:  core,
			Level: zap.NewAtomicLevelAt(zap.WarnLevel),
		},
	)
	defer restore()

	_ = newBucketRegionDetectionRetryer().IsErrorRetryable(err)
	require.Empty(t, recorded.FilterMessage("failed to request s3, checking whether we can retry").All())

	_ = newRetryer().IsErrorRetryable(err)
	require.Len(t, recorded.FilterMessage("failed to request s3, checking whether we can retry").All(), 1)
}
