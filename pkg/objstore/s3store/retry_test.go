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
	"testing"
	"time"

	"github.com/pingcap/errors"
	"github.com/stretchr/testify/require"
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
