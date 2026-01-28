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
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/ratelimit"
	"github.com/aws/aws-sdk-go-v2/aws/retry"
	"github.com/pingcap/tidb/pkg/objstore/s3like"
)

const (
	// number of attempts to make of operations, i.e. maxAttempts - 1 retries
	maxAttempts    = 20
	ec2MetaAddress = "169.254.169.254"
)

func newRetryer() *s3like.Retryer {
	return s3like.NewRetryer(&retryer{
		Retryer: retry.NewStandard(func(so *retry.StandardOptions) {
			so.MaxAttempts = maxAttempts
			// Standard uses exponential backoff with jitter by default, it will
			// calculate maxBackoffAttempts by log2, so we set it a power of 2.
			so.MaxBackoff = 32 * time.Second
			// this rate limiter is shared by all requests on the same S3 store
			// instance, if there are network issues, we might easily exhaust the
			// token bucket which doesn't add tokens back on error. such as for
			// global-sort, we have many concurrent requests, a short period of
			// network issue might exhaust the bucket.
			so.RateLimiter = ratelimit.None
		}),
	})
}

type retryer struct {
	aws.Retryer
}

func (r *retryer) IsInstanceMetadataError(err error) bool {
	if err == nil {
		return false
	}
	return strings.Contains(err.Error(), ec2MetaAddress)
}
