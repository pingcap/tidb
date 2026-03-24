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
	"strings"
	"time"

	ossretry "github.com/aliyun/alibabacloud-oss-go-sdk-v2/oss/retry"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/br/pkg/logutil"
	"github.com/pingcap/tidb/pkg/metrics"
	"go.uber.org/zap"
)

// StandardRetryer is the interface for standard retryer.
// we use this to abstract the underlying Standard retryer implementation,
// include s3 and oss, and with TiDB specific extension.
// since aws.Retryer is a super set of ossretry.Retryer, we won't wrap
// ossretry.Retryer directly.
type StandardRetryer interface {
	aws.Retryer
	// IsInstanceMetadataError checks whether the error is related to instance
	// metadata service. such as for errors of EC2 metadata service, the error
	// might contain 169.254.169.254.
	IsInstanceMetadataError(err error) bool
}

var (
	_ aws.Retryer      = (StandardRetryer)(nil)
	_ ossretry.Retryer = (StandardRetryer)(nil)
)

// Retryer implements aws.Retryer for TiDB-specific retry logic
type Retryer struct {
	standardRetryer StandardRetryer
}

var (
	_ aws.Retryer      = (*Retryer)(nil)
	_ ossretry.Retryer = (*Retryer)(nil)
)

// NewRetryer creates a new Retryer wrapping the given inner StandardRetryer.
func NewRetryer(inner StandardRetryer) *Retryer {
	return &Retryer{
		standardRetryer: inner,
	}
}

// IsErrorRetryable implements the aws.Retryer interface.
func (tr *Retryer) IsErrorRetryable(err error) bool {
	var isRetryable bool
	defer func() {
		log.Warn("failed to request s3, checking whether we can retry", zap.Error(err), zap.Bool("retry", isRetryable))
		if isRetryable {
			metrics.RetryableErrorCount.WithLabelValues(err.Error()).Inc()
		}
	}()

	// for unit test
	failpoint.Inject("replace-error-to-connection-reset-by-peer", func(_ failpoint.Value) {
		log.Info("original error", zap.Error(err))
		if err != nil {
			err = errors.New("read tcp *.*.*.*:*->*.*.*.*:*: read: connection reset by peer")
		}
	})

	// Fast fail for unreachable EC2 metadata in containers
	if tr.standardRetryer.IsInstanceMetadataError(err) && (IsDeadlineExceedError(err) || isConnectionResetError(err)) {
		log.Warn("failed to get EC2 metadata. skipping.", logutil.ShortError(err))
		isRetryable = false
		return isRetryable
	}

	// Custom connection error handling
	if isConnectionResetError(err) {
		isRetryable = true
		return isRetryable
	}
	if isConnectionRefusedError(err) {
		isRetryable = false
		return isRetryable
	}
	if IsHTTP2ConnAborted(err) {
		isRetryable = true
		return isRetryable
	}

	// Fall back to standard retry logic
	isRetryable = tr.standardRetryer.IsErrorRetryable(err)
	return isRetryable
}

// MaxAttempts implements the aws.Retryer interface.
func (tr *Retryer) MaxAttempts() int {
	return tr.standardRetryer.MaxAttempts()
}

// RetryDelay implements the aws.Retryer interface.
func (tr *Retryer) RetryDelay(attempt int, err error) (time.Duration, error) {
	delay, retryErr := tr.standardRetryer.RetryDelay(attempt, err)
	if retryErr != nil {
		return 0, retryErr
	}

	// Apply minimum delays similar to v1 configuration
	minDelay := 1 * time.Second
	if delay < minDelay {
		delay = minDelay
	}

	log.Warn("failed to request s3, retrying", zap.Int("attempt", attempt),
		zap.Duration("backoff", delay), zap.Error(err))
	return delay, nil
}

// GetRetryToken implements the aws.Retryer interface.
func (tr *Retryer) GetRetryToken(ctx context.Context, opErr error) (releaseToken func(error) error, err error) {
	return tr.standardRetryer.GetRetryToken(ctx, opErr)
}

// GetInitialToken implements the aws.Retryer interface.
func (tr *Retryer) GetInitialToken() (releaseToken func(error) error) {
	return tr.standardRetryer.GetInitialToken()
}

// IsDeadlineExceedError checks whether the error is a context deadline exceeded error.
func IsDeadlineExceedError(err error) bool {
	// TODO find a better way.
	// Known challenges:
	//
	// If we want to unwrap the r.Error:
	// 1. the err should be a smithy.APIError (let it be apiErr)
	// 2. We'd need to check the underlying error chain for *url.Error.
	// 3. urlErr.Err should be a http.httpError (which is private).
	//
	// If we want to reterive the error from the request context:
	// The error of context in the HTTPRequest (i.e. r.HTTPRequest.Context().Err() ) is nil.
	return strings.Contains(err.Error(), "context deadline exceeded")
}

func isConnectionResetError(err error) bool {
	return strings.Contains(err.Error(), "read: connection reset")
}

func isConnectionRefusedError(err error) bool {
	return strings.Contains(err.Error(), "connection refused")
}

// IsHTTP2ConnAborted checks whether the error is caused by HTTP/2 connection aborted.
func IsHTTP2ConnAborted(err error) bool {
	patterns := []string{
		"http2: client connection force closed via ClientConn.Close",
		"http2: server sent GOAWAY and closed the connection",
		"unexpected EOF",
	}
	errMsg := err.Error()

	for _, p := range patterns {
		if strings.Contains(errMsg, p) {
			return true
		}
	}
	return false
}
