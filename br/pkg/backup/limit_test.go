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

package backup_test

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pingcap/tidb/br/pkg/backup"
	"github.com/stretchr/testify/require"
)

func TestResourceConcurrentLimiter(t *testing.T) {
	limiter := backup.NewResourceMemoryLimiter(100)

	var tag int64 = 0
	limiter.Acquire(50)
	limiter.Acquire(50)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		limiter.Acquire(200)
		require.Equal(t, int64(1), atomic.LoadInt64(&tag))
		require.Equal(t, int64(2), atomic.AddInt64(&tag, 1))
		limiter.Release(200)
	}()

	// acquire 200 is blocken
	require.Equal(t, int64(1), atomic.AddInt64(&tag, 1))
	limiter.Release(50)
	limiter.Release(50)

	wg.Wait()
	require.Equal(t, int64(2), atomic.LoadInt64(&tag))
	limiter.Acquire(99)
	limiter.Acquire(1)
	limiter.Release(100)
}

func TestResourceConcurrentLimiter2(t *testing.T) {
	limiter := backup.NewResourceMemoryLimiter(100)
	var wg sync.WaitGroup
	for range 20 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			start := time.Now()
			current := limiter.Acquire(30)
			t.Log(current, time.Since(start))
			time.Sleep(time.Millisecond * 10)
			limiter.Release(30)
			require.LessOrEqual(t, current, 120)
		}()
	}
	wg.Wait()
}
