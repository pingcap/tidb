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

// ingestLimiter is used to limit the concurrency of sending ingest requests.
// It combines two strategies:
// - limit the max ingest requests in flight.
// - limit the ingest requests number per second.
type ingestLimiter struct {
	ctx            context.Context
	maxReqInFlight int
	maxReqPerSec   int
	limiters       sync.Map // storeID(uint64) -> *ingestLimiterPerStore
}

type ingestLimiterPerStore struct {
	sem     *semaphore.Weighted
	limiter *rate.Limiter
}

func newIngestLimiter(ctx context.Context, maxReqInFlight, maxReqPerSec int) *ingestLimiter {
	if maxReqInFlight == 0 || maxReqPerSec == 0 {
		return &ingestLimiter{}
	}
	return &ingestLimiter{
		ctx:            ctx,
		maxReqInFlight: maxReqInFlight,
		maxReqPerSec:   maxReqPerSec,
		limiters:       sync.Map{},
	}
}

func (l *ingestLimiter) Acquire(storeID uint64, n uint) error {
	if l.ctx == nil {
		return nil
	}
	v, _ := l.limiters.LoadOrStore(storeID, &ingestLimiterPerStore{
		sem:     semaphore.NewWeighted(int64(l.maxReqInFlight)),
		limiter: rate.NewLimiter(rate.Limit(l.maxReqPerSec), l.maxReqPerSec),
	})
	ilps := v.(*ingestLimiterPerStore)
	if err := ilps.limiter.WaitN(l.ctx, int(n)); err != nil {
		return errors.Trace(err)
	}
	return ilps.sem.Acquire(l.ctx, int64(n))
}

func (l *ingestLimiter) Release(storeID uint64, n uint) {
	if l.ctx == nil {
		return
	}
	v, ok := l.limiters.Load(storeID)
	if !ok {
		return
	}
	ilps := v.(*ingestLimiterPerStore)
	ilps.sem.Release(int64(n))
}

func (l *ingestLimiter) Burst() int {
	if l.ctx == nil {
		return math.MaxInt
	}
	return min(l.maxReqPerSec, l.maxReqInFlight)
}
