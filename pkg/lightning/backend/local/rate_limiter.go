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
	"sync"

	"github.com/pingcap/errors"
	"golang.org/x/sync/semaphore"
	"golang.org/x/time/rate"
)

const ratePerSecMultiplier = 1000

// ingestLimiter is used to limit the concurrency of sending ingest requests.
// It combines two strategies:
// - limit the max ingest requests in flight.
// - limit the ingest requests number per second.
type ingestLimiter struct {
	ctx            context.Context
	maxReqInFlight int
	maxReqPerSec   float64
	limiters       sync.Map // storeID(uint64) -> *ingestLimiterPerStore
}

type ingestLimiterPerStore struct {
	sem     *semaphore.Weighted
	limiter *rate.Limiter
}

func newIngestLimiter(ctx context.Context, maxReqInFlight int, maxReqPerSec float64) *ingestLimiter {
	return &ingestLimiter{
		ctx:            ctx,
		maxReqInFlight: maxReqInFlight,
		maxReqPerSec:   maxReqPerSec,
		limiters:       sync.Map{},
	}
}

func (l *ingestLimiter) Acquire(storeID uint64, n uint) error {
	if l.maxReqInFlight == 0 && l.maxReqPerSec == 0 {
		return nil
	}
	v, ok := l.limiters.Load(storeID)
	if !ok {
		eventLimit := max(1, int(l.maxReqPerSec*ratePerSecMultiplier))
		burst := getRateBurst(l.maxReqPerSec) * ratePerSecMultiplier
		v, _ = l.limiters.LoadOrStore(storeID, &ingestLimiterPerStore{
			sem:     semaphore.NewWeighted(int64(l.maxReqInFlight)),
			limiter: rate.NewLimiter(rate.Limit(eventLimit), burst),
		})
	}
	ilps := v.(*ingestLimiterPerStore)
	if l.maxReqPerSec > 0 {
		if err := ilps.limiter.WaitN(l.ctx, int(n*ratePerSecMultiplier)); err != nil {
			return errors.Trace(err)
		}
	}
	if l.maxReqInFlight > 0 {
		return ilps.sem.Acquire(l.ctx, int64(n))
	}
	return nil
}

func (l *ingestLimiter) Release(storeID uint64, n uint) {
	if l.maxReqInFlight > 0 {
		v, ok := l.limiters.Load(storeID)
		if !ok {
			return
		}
		ilps := v.(*ingestLimiterPerStore)
		ilps.sem.Release(int64(n))
	}
}

func (l *ingestLimiter) Burst() int {
	if l.maxReqInFlight == 0 && l.maxReqPerSec == 0 {
		return math.MaxInt
	}
	if l.maxReqInFlight == 0 {
		return getRateBurst(l.maxReqPerSec)
	}
	if l.maxReqPerSec == 0 {
		return l.maxReqInFlight
	}
	return min(getRateBurst(l.maxReqPerSec), l.maxReqInFlight)
}

func (l *ingestLimiter) NoLimit() bool {
	return l.maxReqInFlight == 0 && l.maxReqPerSec == 0
}

func getRateBurst(ratePerSec float64) int {
	// we allow set the rate per second be smaller than 1, such as 0.5, it means
	// 1 request every 2 seconds, but we need to ensure that the burst of rate per
	// second is at least 1 to make sure RateLimiter works.
	return max(1, int(math.Ceil(ratePerSec)))
}
