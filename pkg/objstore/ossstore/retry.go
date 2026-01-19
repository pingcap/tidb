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

package ossstore

import (
	"context"
	"strings"
	"time"

	ossretry "github.com/aliyun/alibabacloud-oss-go-sdk-v2/oss/retry"
	"github.com/pingcap/tidb/pkg/objstore/s3like"
)

const (
	// number of attempts to make of operations, i.e. maxAttempts - 1 retries
	maxAttempts    = 20
	ecsMetaAddress = "100.100.100.200"
)

func newRetryer() *s3like.Retryer {
	return s3like.NewRetryer(&retryer{
		Retryer: ossretry.NewStandard(func(so *ossretry.RetryOptions) {
			so.MaxAttempts = maxAttempts
			so.BaseDelay = time.Second
			// Standard uses exponential backoff with jitter by default, it will
			// calculate maxBackoffAttempts by log2, so we set it a power of 2.
			so.MaxBackoff = 32 * time.Second
		}),
	})
}

type retryer struct {
	ossretry.Retryer
}

func (*retryer) GetRetryToken(context.Context, error) (releaseToken func(error) error, err error) {
	return func(err error) error {
		return nil
	}, nil
}

func (*retryer) GetInitialToken() (releaseToken func(error) error) {
	return func(err error) error {
		return nil
	}
}

func (r *retryer) IsInstanceMetadataError(err error) bool {
	if err == nil {
		return false
	}
	return strings.Contains(err.Error(), ecsMetaAddress)
}
