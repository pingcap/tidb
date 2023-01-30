// Copyright 2023 PingCAP, Inc.
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

package topklimiter

import (
	"sync"

	"github.com/go-kratos/aegis/ratelimit"
	"github.com/go-kratos/aegis/ratelimit/bbr"
	"github.com/pingcap/tidb/util/topk"
)

// GlobalTopKLimiter is a global top-k limiter
var GlobalTopKLimiter = NewTopKLimiter(10, 2048, 100, 0.9)

// TopKLimiter is a top-k limiter
type TopKLimiter struct {
	hk       *topk.HeavyKeeper
	mu       sync.Mutex
	limiters map[string]*bbr.BBR
	exitCh   chan struct{}
}

// NewTopKLimiter creates a new top-k limiter
func NewTopKLimiter(k, width, depth uint32, decay float64) *TopKLimiter {
	return &TopKLimiter{
		hk:       topk.NewHeavyKeeper(k, width, depth, decay),
		limiters: make(map[string]*bbr.BBR),
		exitCh:   make(chan struct{}),
	}
}

// Allow allows the key to pass
func (l *TopKLimiter) Allow(key string) (ratelimit.DoneFunc, error) {
	l.hk.Add(key, 1)
	if l.hk.Contains(key) {
		if limiter, ok := l.limiters[key]; ok {
			return limiter.Allow()
		}
		limiter := bbr.NewLimiter()
		l.limiters[key] = limiter
		return limiter.Allow()
	}
	return nil, nil
}

// Start starts the top-k limiter
func (l *TopKLimiter) Start() {
	for {
		select {
		case <-l.exitCh:
		case item := <-l.hk.Expelled():
			l.mu.Lock()
			delete(l.limiters, item)
			l.mu.Unlock()
		}
	}
}

// Stop stops the top-k limiter
func (l *TopKLimiter) Stop() {
	close(l.exitCh)
}
