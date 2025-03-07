// Copyright 2024 PingCAP, Inc.
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
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/stretchr/testify/require"
)

func _put(pc sessionctx.InstancePlanCache, testKey, memUsage, statsHash int64) (succ bool) {
	v := &PlanCacheValue{testKey: testKey, Memory: memUsage}
	return pc.Put(fmt.Sprintf("%v-%v", testKey, statsHash), v, nil)
}

func _hit(t *testing.T, pc sessionctx.InstancePlanCache, testKey, statsHash int) {
	v, ok := pc.Get(fmt.Sprintf("%v-%v", testKey, statsHash), nil)
	require.True(t, ok)
	require.Equal(t, v.(*PlanCacheValue).testKey, int64(testKey))
}

func _miss(t *testing.T, pc sessionctx.InstancePlanCache, testKey, statsHash int) {
	_, ok := pc.Get(fmt.Sprintf("%v-%v", testKey, statsHash), nil)
	require.False(t, ok)
}

func TestInstancePlanCacheBasic(t *testing.T) {
	sctx := MockContext()
	defer func() {
		domain.GetDomain(sctx).StatsHandle().Close()
	}()

	pc := NewInstancePlanCache(1000, 1000)
	_put(pc, 1, 100, 0)
	_put(pc, 2, 100, 0)
	_put(pc, 3, 100, 0)
	require.Equal(t, pc.MemUsage(), int64(300))
	_hit(t, pc, 1, 0)
	_hit(t, pc, 2, 0)
	_hit(t, pc, 3, 0)

	// exceed the hard limit during Put
	pc = NewInstancePlanCache(250, 250)
	_put(pc, 1, 100, 0)
	_put(pc, 2, 100, 0)
	_put(pc, 3, 100, 0)
	require.Equal(t, pc.MemUsage(), int64(200))
	_hit(t, pc, 1, 0)
	_hit(t, pc, 2, 0)
	_miss(t, pc, 3, 0)

	// can't Put 2 same values
	pc = NewInstancePlanCache(250, 250)
	_put(pc, 1, 100, 0)
	_put(pc, 1, 101, 0)
	require.Equal(t, pc.MemUsage(), int64(100)) // the second one will be ignored

	// eviction
	pc = NewInstancePlanCache(320, 500)
	_put(pc, 1, 100, 0)
	_put(pc, 2, 100, 0)
	_put(pc, 3, 100, 0)
	_put(pc, 4, 100, 0)
	_put(pc, 5, 100, 0)
	_hit(t, pc, 1, 0) // access 1-3 to refresh their last_used
	_hit(t, pc, 2, 0)
	_hit(t, pc, 3, 0)
	_, numEvicted := pc.Evict(false)
	require.Equal(t, numEvicted > 0, true)
	require.Equal(t, pc.MemUsage(), int64(300))
	_hit(t, pc, 1, 0) // access 1-3 to refresh their last_used
	_hit(t, pc, 2, 0)
	_hit(t, pc, 3, 0)
	_miss(t, pc, 4, 0) // 4-5 have been evicted
	_miss(t, pc, 5, 0)

	// no need to eviction if mem < softLimit
	pc = NewInstancePlanCache(320, 500)
	_put(pc, 1, 100, 0)
	_put(pc, 2, 100, 0)
	_put(pc, 3, 100, 0)
	_, numEvicted = pc.Evict(false)
	require.Equal(t, numEvicted > 0, false)
	require.Equal(t, pc.MemUsage(), int64(300))
	_hit(t, pc, 1, 0)
	_hit(t, pc, 2, 0)
	_hit(t, pc, 3, 0)

	// empty head should be dropped after eviction
	pc = NewInstancePlanCache(1, 500)
	_put(pc, 1, 100, 0)
	_put(pc, 2, 100, 0)
	_put(pc, 3, 100, 0)
	require.Equal(t, pc.MemUsage(), int64(300))
	pcImpl := pc.(*instancePlanCache)
	numHeads := 0
	pcImpl.heads.Range(func(k, v any) bool { numHeads++; return true })
	require.Equal(t, numHeads, 3)
	_, numEvicted = pc.Evict(false)
	require.Equal(t, numEvicted > 0, true)
	require.Equal(t, pc.MemUsage(), int64(0))
	numHeads = 0
	pcImpl.heads.Range(func(k, v any) bool { numHeads++; return true })
	require.Equal(t, numHeads, 0)
}

func TestInstancePlanCacheWithMatchOpts(t *testing.T) {
	sctx := MockContext()
	defer func() {
		domain.GetDomain(sctx).StatsHandle().Close()
	}()
	sctx.GetSessionVars().PlanCacheInvalidationOnFreshStats = true

	// same key with different statsHash
	pc := NewInstancePlanCache(1000, 1000)
	_put(pc, 1, 100, 1)
	_put(pc, 1, 100, 2)
	_put(pc, 1, 100, 3)
	_hit(t, pc, 1, 1)
	_hit(t, pc, 1, 2)
	_hit(t, pc, 1, 3)
	_miss(t, pc, 1, 4)
	_miss(t, pc, 2, 1)

	// multiple keys with same statsHash
	pc = NewInstancePlanCache(1000, 1000)
	_put(pc, 1, 100, 1)
	_put(pc, 1, 100, 2)
	_put(pc, 2, 100, 1)
	_put(pc, 2, 100, 2)
	_hit(t, pc, 1, 1)
	_hit(t, pc, 1, 2)
	_miss(t, pc, 1, 3)
	_hit(t, pc, 2, 1)
	_hit(t, pc, 2, 2)
	_miss(t, pc, 2, 3)
	_miss(t, pc, 3, 1)
	_miss(t, pc, 3, 2)
	_miss(t, pc, 3, 3)

	// hard limit can take effect in this case
	pc = NewInstancePlanCache(200, 200)
	_put(pc, 1, 100, 1)
	_put(pc, 1, 100, 2)
	_put(pc, 1, 100, 3) // the third one will be ignored
	require.Equal(t, pc.MemUsage(), int64(200))
	_hit(t, pc, 1, 1)
	_hit(t, pc, 1, 2)
	_miss(t, pc, 1, 3)

	// eviction this case
	pc = NewInstancePlanCache(300, 500)
	_put(pc, 1, 100, 1)
	_put(pc, 1, 100, 2)
	_put(pc, 1, 100, 3)
	_put(pc, 1, 100, 4)
	_put(pc, 1, 100, 5)
	_hit(t, pc, 1, 1) // refresh 1-3's last_used
	_hit(t, pc, 1, 2)
	_hit(t, pc, 1, 3)
	_, numEvicted := pc.Evict(false)
	require.True(t, numEvicted > 0)
	require.Equal(t, pc.MemUsage(), int64(300))
	_hit(t, pc, 1, 1)
	_hit(t, pc, 1, 2)
	_hit(t, pc, 1, 3)
	_miss(t, pc, 1, 4)
	_miss(t, pc, 1, 5)
}

func TestInstancePlanCacheEvictAll(t *testing.T) {
	sctx := MockContext()
	defer func() {
		domain.GetDomain(sctx).StatsHandle().Close()
	}()
	sctx.GetSessionVars().PlanCacheInvalidationOnFreshStats = true

	// same key with different statsHash
	pc := NewInstancePlanCache(1000, 1000)
	_put(pc, 1, 100, 1)
	_put(pc, 1, 100, 2)
	_put(pc, 1, 100, 3)
	_, numEvicted := pc.Evict(true)
	require.Equal(t, 3, numEvicted)
	_miss(t, pc, 1, 1)
	_miss(t, pc, 1, 2)
	_miss(t, pc, 1, 3)
	require.Equal(t, pc.MemUsage(), int64(0))
	require.Equal(t, pc.Size(), int64(0))
}

func TestInstancePlanCacheConcurrentRead(t *testing.T) {
	sctx := MockContext()
	defer func() {
		domain.GetDomain(sctx).StatsHandle().Close()
	}()
	sctx.GetSessionVars().PlanCacheInvalidationOnFreshStats = true

	pc := NewInstancePlanCache(300, 100000)
	var flag [100][100]bool
	for k := 0; k < 100; k++ {
		for statsHash := 0; statsHash < 100; statsHash++ {
			if rand.Intn(10) < 7 {
				_put(pc, int64(k), 1, int64(statsHash))
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
					_hit(t, pc, k, statsHash)
				} else {
					_miss(t, pc, k, statsHash)
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
	var flag [100][100]atomic.Bool
	pc := NewInstancePlanCache(300, 100000)
	var wg sync.WaitGroup
	for i := 0; i < 5; i++ { // writers
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 1000; i++ {
				k, statsHash := rand.Intn(100), rand.Intn(100)
				if _put(pc, int64(k), 1, int64(statsHash)) {
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
					_hit(t, pc, k, statsHash)
				}
				time.Sleep(time.Nanosecond * 5)
			}
		}()
	}
	wg.Wait()
}
