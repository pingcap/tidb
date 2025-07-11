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

package membuf

import (
	"sync"

	"github.com/pingcap/log"
	"go.uber.org/zap"
)

// Limiter will block on Acquire if the number it has acquired and not released
// exceeds the limit.
type Limiter struct {
	initLimit int
	limit     int
	mu        sync.Mutex
	waitNums  []int
	waitChs   []chan struct{}
}

// NewLimiter creates a new Limiter with the given limit.
func NewLimiter(limit int) *Limiter {
	return &Limiter{limit: limit, initLimit: limit}
}

// Acquire acquires n tokens from the limiter. If the number of tokens acquired
// and not released exceeds the limit, it will block until enough tokens are
// released.
func (l *Limiter) Acquire(n int) {
	l.mu.Lock()

	if l.limit >= n {
		l.limit -= n
		l.mu.Unlock()
		return
	}

	waitCh := make(chan struct{})
	l.waitNums = append(l.waitNums, n)
	l.waitChs = append(l.waitChs, waitCh)
	l.mu.Unlock()

	<-waitCh
}

// Release releases tokens to the limiter. If there are goroutines waiting for
// tokens, it will wake them up.
func (l *Limiter) Release(n int) {
	l.mu.Lock()

	l.limit += n
	if l.limit > l.initLimit {
		log.Error(
			"limit overflow",
			zap.Int("limit", l.limit),
			zap.Int("initLimit", l.initLimit),
			zap.Stack("stack"),
		)
	}

	for len(l.waitNums) > 0 && l.limit >= l.waitNums[0] {
		l.limit -= l.waitNums[0]
		close(l.waitChs[0])
		l.waitNums = l.waitNums[1:]
		l.waitChs = l.waitChs[1:]
	}

	l.mu.Unlock()
}
