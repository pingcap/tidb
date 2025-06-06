// Copyright 2025 PingCAP, Inc.
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

package local

import (
	"context"
	"math"

	"github.com/pingcap/errors"
	"golang.org/x/sync/semaphore"
	"golang.org/x/time/rate"
)

// ingestLimiter is used to limit the concurrency of sending ingest requests.
// It combines two strategies:
// - limit the max ingest requests in flight.
// - limit the ingest requests number per second.
type ingestLimiter struct {
	ctx     context.Context
	sem     *semaphore.Weighted
	limiter *rate.Limiter
}

func newIngestLimiter(ctx context.Context, n int) *ingestLimiter {
	if n == 0 {
		return &ingestLimiter{}
	}
	return &ingestLimiter{
		ctx:     ctx,
		sem:     semaphore.NewWeighted(int64(n)),
		limiter: rate.NewLimiter(rate.Limit(n), n),
	}
}

func (l *ingestLimiter) Acquire(n int) error {
	if l.ctx == nil {
		return nil
	}
	if err := l.limiter.WaitN(l.ctx, n); err != nil {
		return errors.Trace(err)
	}
	return l.sem.Acquire(l.ctx, int64(n))
}

func (l *ingestLimiter) Release(n int) {
	if l.ctx == nil {
		return
	}
	l.sem.Release(int64(n))
}

func (l *ingestLimiter) Burst() int {
	if l.ctx == nil {
		return math.MaxInt
	}
	return l.limiter.Burst()
}
