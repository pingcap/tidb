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
	"time"

	"github.com/go-kratos/aegis/ratelimit"
	"github.com/go-kratos/aegis/ratelimit/bbr"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/topk"
	"github.com/twmb/murmur3"
)

// GlobalTopKLimiter is a global top-k limiter
var GlobalTopKLimiter = NewTopKLimiter(10, 2048, 100, 0.9)

type limiters struct {
	mu      sync.RWMutex
	limiter map[string]*bbr.BBR
}

func newLimiters() *limiters {
	return &limiters{
		limiter: make(map[string]*bbr.BBR),
	}
}

func (l *limiters) Allow(key string) (ratelimit.DoneFunc, error) {
	l.mu.RLock()

	if limiter, ok := l.limiter[key]; ok {
		defer l.mu.RUnlock()
		return limiter.Allow()
	}
	l.mu.RUnlock()
	return l.addLimiterAndAllow(key)
}

func (l *limiters) addLimiterAndAllow(key string) (ratelimit.DoneFunc, error) {
	limiter := bbr.NewLimiter()
	l.mu.Lock()
	l.limiter[key] = limiter
	l.mu.Unlock()
	limiter.Allow()
}

func (l *limiters) RemoveLimiter(key string) {
	l.mu.Lock()
	delete(l.limiter, key)
	l.mu.Unlock()
}

// TopKLimiter is a top-k limiter
type TopKLimiter struct {
	mu sync.RWMutex
	hk *topk.HeavyKeeper

	limiters []limiters
	exitCh   chan struct{}
	writeCh  chan string
	wg       util.WaitGroupWrapper
}

const shard = 8

// NewTopKLimiter creates a new top-k limiter
func NewTopKLimiter(k, width, depth uint32, decay float64) *TopKLimiter {
	t := &TopKLimiter{
		hk:       topk.NewHeavyKeeper(k, width, depth, decay),
		limiters: make([]limiters, shard),
		writeCh:  make(chan string, 128),
	}
	for i := 0; i < shard; i++ {
		t.limiters[i] = *newLimiters()
	}
	return t
}

// Allow allows the key to pass
func (l *TopKLimiter) Allow(key string) (ratelimit.DoneFunc, error) {
	l.writeCh <- key
	idx := murmur3.StringSum32(key) % shard
	return l.limiters[idx].Allow(key)
}

// Start starts the top-k limiter
func (l *TopKLimiter) Start() {
	l.exitCh = make(chan struct{})
	l.wg.Run(func() {
		for {
			select {
			case <-l.exitCh:
				return
			case item := <-l.writeCh:
				l.mu.Lock()
				l.hk.Add(item, 1)
				l.mu.Unlock()
			}
		}
	})
	l.wg.Run(func() {
		for {
			select {
			case <-l.exitCh:
				return
			case item := <-l.hk.Expelled():
				l.mu.Lock()
				idx := murmur3.StringSum32(item) % shard
				l.limiters[idx].RemoveLimiter(item)
				l.mu.Unlock()
			}
		}
	})
}

// Stop stops the top-k limiter
func (l *TopKLimiter) Stop() {
	select {
	case <-l.exitCh:
	case <-time.After(time.Second):
		close(l.exitCh)
	}
	l.wg.Wait()
}
