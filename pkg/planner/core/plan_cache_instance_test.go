// Package core Copyright 2024 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package core

import (
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/domain"
	"github.com/stretchr/testify/require"
)

func TestInstancePlanCacheBasic(t *testing.T) {
	sctx := MockContext()
	defer func() {
		domain.GetDomain(sctx).StatsHandle().Close()
	}()

	var pc InstancePlanCache
	put := func(testKey, memUsage int64) {
		v := &PlanCacheValue{testKey: testKey, memoryUsage: memUsage}
		pc.Put(sctx, fmt.Sprintf("%v", testKey), v, nil)
	}
	hit := func(testKey int64) {
		v, ok := pc.Get(sctx, fmt.Sprintf("%v", testKey), nil)
		require.True(t, ok)
		require.Equal(t, v.(*PlanCacheValue).testKey, testKey)
	}
	miss := func(testKey int) {
		_, ok := pc.Get(sctx, fmt.Sprintf("%v", testKey), nil)
		require.False(t, ok)
	}

	pc = NewInstancePlanCache(1000, 1000)
	put(1, 100)
	put(2, 100)
	put(3, 100)
	require.Equal(t, pc.MemUsage(sctx), int64(300))
	hit(1)
	hit(2)
	hit(3)

	// exceed the hard limit during Put
	pc = NewInstancePlanCache(250, 250)
	put(1, 100)
	put(2, 100)
	put(3, 100)
	require.Equal(t, pc.MemUsage(sctx), int64(200))
	hit(1)
	hit(2)
	miss(3)

	// can't Put 2 same values
	pc = NewInstancePlanCache(250, 250)
	put(1, 100)
	put(1, 101)
	require.Equal(t, pc.MemUsage(sctx), int64(100)) // the second one will be ignored

	// eviction
	pc = NewInstancePlanCache(320, 500)
	put(1, 100)
	put(2, 100)
	put(3, 100)
	put(4, 100)
	put(5, 100)
	hit(1) // access 1-3 to refresh their last_used
	hit(2)
	hit(3)
	require.Equal(t, pc.Evict(sctx), true)
	require.Equal(t, pc.MemUsage(sctx), int64(300))
	hit(1) // access 1-3 to refresh their last_used
	hit(2)
	hit(3)
	miss(4) // 4-5 have been evicted
	miss(5)

	// no need to eviction if mem < softLimit
	pc = NewInstancePlanCache(320, 500)
	put(1, 100)
	put(2, 100)
	put(3, 100)
	require.Equal(t, pc.Evict(sctx), false)
	require.Equal(t, pc.MemUsage(sctx), int64(300))
	hit(1)
	hit(2)
	hit(3)
}

func TestInstancePlanCacheWithMatchOpts(t *testing.T) {
	sctx := MockContext()
	defer func() {
		domain.GetDomain(sctx).StatsHandle().Close()
	}()
	sctx.GetSessionVars().PlanCacheInvalidationOnFreshStats = true

	var pc InstancePlanCache
	put := func(testKey, memUsage, statsHash int64) {
		v := &PlanCacheValue{testKey: testKey, memoryUsage: memUsage, matchOpts: &PlanCacheMatchOpts{StatsVersionHash: uint64(statsHash)}}
		pc.Put(sctx, fmt.Sprintf("%v", testKey), v, &PlanCacheMatchOpts{StatsVersionHash: uint64(statsHash)})
	}
	hit := func(testKey, statsHash int64) {
		v, ok := pc.Get(sctx, fmt.Sprintf("%v", testKey), &PlanCacheMatchOpts{StatsVersionHash: uint64(statsHash)})
		require.True(t, ok)
		require.Equal(t, v.(*PlanCacheValue).testKey, testKey)
	}
	miss := func(testKey, statsHash int) {
		_, ok := pc.Get(sctx, fmt.Sprintf("%v", testKey), &PlanCacheMatchOpts{StatsVersionHash: uint64(statsHash)})
		require.False(t, ok)
	}

	// same key with different statsHash
	pc = NewInstancePlanCache(1000, 1000)
	put(1, 100, 1)
	put(1, 100, 2)
	put(1, 100, 3)
	hit(1, 1)
	hit(1, 2)
	hit(1, 3)
	miss(1, 4)
	miss(2, 1)

	// multiple keys with same statsHash
	pc = NewInstancePlanCache(1000, 1000)
	put(1, 100, 1)
	put(1, 100, 2)
	put(2, 100, 1)
	put(2, 100, 2)
	hit(1, 1)
	hit(1, 2)
	miss(1, 3)
	hit(2, 1)
	hit(2, 2)
	miss(2, 3)
	miss(3, 1)
	miss(3, 2)
	miss(3, 3)

	// hard limit can take effect in this case
	pc = NewInstancePlanCache(200, 200)
	put(1, 100, 1)
	put(1, 100, 2)
	put(1, 100, 3) // the third one will be ignored
	require.Equal(t, pc.MemUsage(sctx), int64(200))
	hit(1, 1)
	hit(1, 2)
	miss(1, 3)

	// eviction this case
	pc = NewInstancePlanCache(300, 500)
	put(1, 100, 1)
	put(1, 100, 2)
	put(1, 100, 3)
	put(1, 100, 4)
	put(1, 100, 5)
	hit(1, 1) // refresh 1-3's last_used
	hit(1, 2)
	hit(1, 3)
	require.True(t, pc.Evict(sctx))
	require.Equal(t, pc.MemUsage(sctx), int64(300))
	hit(1, 1)
	hit(1, 2)
	hit(1, 3)
	miss(1, 4)
	miss(1, 5)
}

func TestInstancePlanCacheConcurrentRead(t *testing.T) {
	sctx := MockContext()
	defer func() {
		domain.GetDomain(sctx).StatsHandle().Close()
	}()
	sctx.GetSessionVars().PlanCacheInvalidationOnFreshStats = true

	var pc InstancePlanCache
	put := func(testKey, memUsage, statsHash int64) {
		v := &PlanCacheValue{testKey: testKey, memoryUsage: memUsage, matchOpts: &PlanCacheMatchOpts{StatsVersionHash: uint64(statsHash)}}
		pc.Put(sctx, fmt.Sprintf("%v", testKey), v, &PlanCacheMatchOpts{StatsVersionHash: uint64(statsHash)})
	}
	hit := func(testKey, statsHash int) {
		v, ok := pc.Get(sctx, fmt.Sprintf("%v", testKey), &PlanCacheMatchOpts{StatsVersionHash: uint64(statsHash)})
		require.True(t, ok)
		require.Equal(t, v.(*PlanCacheValue).testKey, int64(testKey))
	}
	miss := func(testKey, statsHash int) {
		_, ok := pc.Get(sctx, fmt.Sprintf("%v", testKey), &PlanCacheMatchOpts{StatsVersionHash: uint64(statsHash)})
		require.False(t, ok)
	}

	pc = NewInstancePlanCache(300, 100000)
	var flag [100][100]bool
	for k := 0; k < 100; k++ {
		for statsHash := 0; statsHash < 100; statsHash++ {
			if rand.Intn(10) < 7 {
				put(int64(k), 1, int64(statsHash))
				flag[k][statsHash] = true
			}
		}
	}

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 10000; i++ {
				k, statsHash := rand.Intn(100), rand.Intn(100)
				if flag[k][statsHash] {
					hit(k, statsHash)
				} else {
					miss(k, statsHash)
				}
				time.Sleep(time.Nanosecond * 10)
			}
		}()
	}
	wg.Wait()
}

func TestInstancePlanCacheConcurrentWriteRead(t *testing.T) {
	sctx := MockContext()
	defer func() {
		domain.GetDomain(sctx).StatsHandle().Close()
	}()
	sctx.GetSessionVars().PlanCacheInvalidationOnFreshStats = true

	var pc InstancePlanCache
	put := func(testKey, memUsage, statsHash int64) (succ bool) {
		v := &PlanCacheValue{testKey: testKey, memoryUsage: memUsage, matchOpts: &PlanCacheMatchOpts{StatsVersionHash: uint64(statsHash)}}
		return pc.Put(sctx, fmt.Sprintf("%v", testKey), v, &PlanCacheMatchOpts{StatsVersionHash: uint64(statsHash)})
	}
	hit := func(testKey, statsHash int64) {
		v, ok := pc.Get(sctx, fmt.Sprintf("%v", testKey), &PlanCacheMatchOpts{StatsVersionHash: uint64(statsHash)})
		require.True(t, ok)
		require.Equal(t, v.(*PlanCacheValue).testKey, testKey)
	}

	var flag [100][100]atomic.Bool
	pc = NewInstancePlanCache(300, 100000)
	var wg sync.WaitGroup
	for i := 0; i < 5; i++ { // writers
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 1000; i++ {
				k, statsHash := rand.Intn(100), rand.Intn(100)
				if put(int64(k), 1, int64(statsHash)) {
					flag[k][statsHash].Store(true)
				}
				time.Sleep(time.Nanosecond * 10)
			}
		}()
	}
	for i := 0; i < 5; i++ { // readers
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 2000; i++ {
				k, statsHash := rand.Intn(100), rand.Intn(100)
				if flag[k][statsHash].Load() {
					hit(int64(k), int64(statsHash))
				}
				time.Sleep(time.Nanosecond * 5)
			}
		}()
	}
	wg.Wait()
}
