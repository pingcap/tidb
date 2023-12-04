// Copyright 2023 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a Copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package cache

import (
	"math/rand"
	"sync"
	"testing"

	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/statistics"
	"github.com/pingcap/tidb/pkg/statistics/handle/cache/internal/testutil"
	"github.com/pingcap/tidb/pkg/statistics/handle/types"
	"github.com/pingcap/tidb/pkg/util/benchdaily"
)

func benchCopyAndUpdate(b *testing.B, c types.StatsCache) {
	var wg sync.WaitGroup
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			t1 := testutil.NewMockStatisticsTable(1, 1, true, false, false)
			t1.PhysicalID = rand.Int63()
			c.UpdateStatsCache([]*statistics.Table{t1}, nil)
		}()
	}
	wg.Wait()
	b.StopTimer()
}

func benchPutGet(b *testing.B, c types.StatsCache) {
	var wg sync.WaitGroup
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			t1 := testutil.NewMockStatisticsTable(1, 1, true, false, false)
			t1.PhysicalID = rand.Int63()
			c.UpdateStatsCache([]*statistics.Table{t1}, nil)
		}(i)
	}
	for i := 0; i < b.N; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			c.Get(int64(i))
		}(i)
	}
	wg.Wait()
	b.StopTimer()
}

func benchGet(b *testing.B, c types.StatsCache) {
	var w sync.WaitGroup
	for i := 0; i < b.N; i++ {
		w.Add(1)
		go func(i int) {
			defer w.Done()
			t1 := testutil.NewMockStatisticsTable(1, 1, true, false, false)
			t1.PhysicalID = rand.Int63()
			c.UpdateStatsCache([]*statistics.Table{t1}, nil)
		}(i)
	}
	w.Wait()
	b.ResetTimer()
	var wg sync.WaitGroup
	for i := 0; i < b.N; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			c.Get(int64(i))
		}(i)
	}
	wg.Wait()
	b.StopTimer()
}

func BenchmarkStatsCacheLFUCopyAndUpdate(b *testing.B) {
	restore := config.RestoreFunc()
	defer restore()
	config.UpdateGlobal(func(conf *config.Config) {
		conf.Performance.EnableStatsCacheMemQuota = true
	})
	cache, err := NewStatsCacheImplForTest()
	if err != nil {
		b.Fail()
	}
	benchCopyAndUpdate(b, cache)
}

func BenchmarkStatsCacheMapCacheCopyAndUpdate(b *testing.B) {
	restore := config.RestoreFunc()
	defer restore()
	config.UpdateGlobal(func(conf *config.Config) {
		conf.Performance.EnableStatsCacheMemQuota = false
	})
	cache, err := NewStatsCacheImplForTest()
	if err != nil {
		b.Fail()
	}
	benchCopyAndUpdate(b, cache)
}

func BenchmarkLFUCachePutGet(b *testing.B) {
	restore := config.RestoreFunc()
	defer restore()
	config.UpdateGlobal(func(conf *config.Config) {
		conf.Performance.EnableStatsCacheMemQuota = true
	})
	cache, err := NewStatsCacheImplForTest()
	if err != nil {
		b.Fail()
	}
	benchPutGet(b, cache)
}

func BenchmarkMapCachePutGet(b *testing.B) {
	restore := config.RestoreFunc()
	defer restore()
	config.UpdateGlobal(func(conf *config.Config) {
		conf.Performance.EnableStatsCacheMemQuota = false
	})
	cache, err := NewStatsCacheImplForTest()
	if err != nil {
		b.Fail()
	}
	benchPutGet(b, cache)
}

func BenchmarkLFUCacheGet(b *testing.B) {
	restore := config.RestoreFunc()
	defer restore()
	config.UpdateGlobal(func(conf *config.Config) {
		conf.Performance.EnableStatsCacheMemQuota = true
	})
	cache, err := NewStatsCacheImplForTest()
	if err != nil {
		b.Fail()
	}
	benchGet(b, cache)
}

func BenchmarkMapCacheGet(b *testing.B) {
	restore := config.RestoreFunc()
	defer restore()
	config.UpdateGlobal(func(conf *config.Config) {
		conf.Performance.EnableStatsCacheMemQuota = false
	})
	cache, err := NewStatsCacheImplForTest()
	if err != nil {
		b.Fail()
	}
	benchGet(b, cache)
}

func TestBenchDaily(*testing.T) {
	benchdaily.Run(
		BenchmarkStatsCacheLFUCopyAndUpdate,
		BenchmarkStatsCacheMapCacheCopyAndUpdate,
		BenchmarkLFUCachePutGet,
		BenchmarkMapCachePutGet,
		BenchmarkLFUCacheGet,
		BenchmarkMapCacheGet,
	)
}
