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
	"fmt"

	"golang.org/x/time/rate"
)

const (
	maxIngestRequestWorkers = 5
	maxIngestReqPerSec      = 5
)

// ingestLimiter is used to limit the concurrency of sending ingest requests.
// It combines two strategies:
// - limit the max ingest requests in flight.
// - limit the ingest requests number per second.
type ingestLimiter struct {
	ctx     context.Context
	slots   chan struct{}
	limiter *rate.Limiter
}

func newIngestLimiter(ctx context.Context, maxWorkers, maxReqPerSec int) *ingestLimiter {
	return &ingestLimiter{
		ctx:     ctx,
		slots:   make(chan struct{}, maxWorkers),
		limiter: rate.NewLimiter(rate.Limit(maxReqPerSec), maxReqPerSec),
	}
}

func (l *ingestLimiter) Acquire() error {
	if err := l.limiter.Wait(l.ctx); err != nil {
		return fmt.Errorf("rate limiter failed: %w", err)
	}
	select {
	case l.slots <- struct{}{}:
		return nil
	case <-l.ctx.Done():
		return l.ctx.Err()
	}
}

func (l *ingestLimiter) Release() {
	select {
	case <-l.slots:
	default:
	}
}
