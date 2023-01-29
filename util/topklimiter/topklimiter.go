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
	"github.com/go-kratos/aegis/ratelimit"
	"github.com/go-kratos/aegis/ratelimit/bbr"
	"github.com/pingcap/tidb/util/topk"
)

type TopKLimiter struct {
	hk       *topk.HeavyKeeper
	limiters map[string]*bbr.BBR
}

func NewTopKLimiter(k, width, depth uint32, decay float64) *TopKLimiter {
	return &TopKLimiter{
		hk:       topk.NewHeavyKeeper(k, width, depth, decay),
		limiters: make(map[string]*bbr.BBR),
	}
}

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
