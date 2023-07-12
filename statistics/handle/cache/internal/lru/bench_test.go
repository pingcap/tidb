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

package lru

import (
	"sync"
	"testing"

	"github.com/pingcap/tidb/statistics/handle/cache/internal/testutil"
)

const defaultSize int64 = 1000

func BenchmarkLruPut(b *testing.B) {
	var (
		wg sync.WaitGroup
		c  = NewStatsLruCache(defaultSize)
	)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			t1 := testutil.NewMockStatisticsTable(1, 1, true, false, false)
			c.Put(int64(i), t1, true)
		}(i)
	}

	wg.Wait()

	b.StopTimer()
}

func BenchmarkLruPutGet(b *testing.B) {
	var (
		wg sync.WaitGroup
		c  = NewStatsLruCache(defaultSize)
	)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			t1 := testutil.NewMockStatisticsTable(1, 1, true, false, false)
			c.Put(int64(i), t1, true)
		}(i)
	}
	for i := 0; i < b.N; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			c.Get(int64(i), true)
		}(i)
	}
	wg.Wait()

	b.StopTimer()
}
