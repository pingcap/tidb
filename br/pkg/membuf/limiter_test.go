// Copyright 2023 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package membuf

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"
)

func TestLimiter(t *testing.T) {
	limit := 20
	cur := atomic.NewInt64(0)
	l := NewLimiter(limit)
	wg := sync.WaitGroup{}

	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			l.Acquire(1)
			val := cur.Inc()
			require.LessOrEqual(t, val, int64(limit))
			cur.Dec()
			l.Release(1)
		}()
	}

	wg.Wait()
	require.Equal(t, limit, l.limit)
}

func TestWaitUpMultipleCaller(t *testing.T) {
	limit := 20
	l := NewLimiter(limit)
	l.Acquire(18)

	start := make(chan struct{}, 3)
	finish := make(chan struct{}, 3)
	for i := 0; i < 3; i++ {
		go func() {
			start <- struct{}{}
			l.Acquire(3)
			finish <- struct{}{}
		}()
	}

	for i := 0; i < 3; i++ {
		<-start
	}
	require.Len(t, finish, 0)
	l.Release(18)
	for i := 0; i < 3; i++ {
		<-finish
	}
	require.Equal(t, limit-3*3, l.limit)
}
