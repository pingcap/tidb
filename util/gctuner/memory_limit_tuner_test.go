// Copyright 2022 PingCAP, Inc.
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

package gctuner

import (
	"math"
	"runtime"
	"runtime/debug"
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/util/memory"
	"github.com/stretchr/testify/require"
)

type mockAllocator struct {
	m [][]byte
}

func (a *mockAllocator) alloc(bytes int) (handle int) {
	sli := make([]byte, bytes)
	a.m = append(a.m, sli)
	return len(a.m) - 1
}

func (a *mockAllocator) free(handle int) {
	a.m[handle] = nil
}

func (a *mockAllocator) freeAll() {
	a.m = nil
	runtime.GC()
}

func TestGlobalMemoryTuner(t *testing.T) {
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/util/gctuner/testMemoryLimitTuner", "return(true)"))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/util/gctuner/testMemoryLimitTuner"))
	}()
	// Close GOGCTuner
	gogcTuner := EnableGOGCTuner.Load()
	EnableGOGCTuner.Store(false)
	defer EnableGOGCTuner.Store(gogcTuner)

	memory.ServerMemoryLimit.Store(1 << 30)   // 1GB
	GlobalMemoryLimitTuner.SetPercentage(0.8) // 1GB * 80% = 800MB
	GlobalMemoryLimitTuner.UpdateMemoryLimit()
	require.True(t, GlobalMemoryLimitTuner.isTuning.Load())
	defer func() {
		// If test.count > 1, wait tuning finished.
		require.Eventually(t, func() bool {
			//nolint: all_revive
			return GlobalMemoryLimitTuner.isTuning.Load()
		}, 5*time.Second, 100*time.Millisecond)
		require.Eventually(t, func() bool {
			//nolint: all_revive
			return !GlobalMemoryLimitTuner.waitingReset.Load()
		}, 5*time.Second, 100*time.Millisecond)
		require.Eventually(t, func() bool {
			//nolint: all_revive
			return !GlobalMemoryLimitTuner.nextGCTriggeredByMemoryLimit.Load()
		}, 5*time.Second, 100*time.Millisecond)
	}()

	allocator := &mockAllocator{}
	defer allocator.freeAll()
	r := &runtime.MemStats{}
	getNowGCNum := func() uint32 {
		runtime.ReadMemStats(r)
		return r.NumGC
	}
	checkNextGCEqualMemoryLimit := func() {
		runtime.ReadMemStats(r)
		nextGC := r.NextGC
		memoryLimit := GlobalMemoryLimitTuner.calcMemoryLimit()
		// In golang source, nextGC = memoryLimit - three parts memory. So check 90%~100% here.
		require.True(t, nextGC < uint64(memoryLimit))
		require.True(t, nextGC > uint64(memoryLimit)/10*9)
	}

	memory600mb := allocator.alloc(600 << 20)
	gcNum := getNowGCNum()

	memory210mb := allocator.alloc(210 << 20)
	time.Sleep(100 * time.Millisecond)
	require.True(t, GlobalMemoryLimitTuner.waitingReset.Load())
	require.True(t, gcNum < getNowGCNum())
	// Test waiting for reset
	time.Sleep(500 * time.Millisecond)
	require.Equal(t, int64(math.MaxInt64), debug.SetMemoryLimit(-1))
	gcNum = getNowGCNum()
	memory100mb := allocator.alloc(100 << 20)
	time.Sleep(100 * time.Millisecond)
	require.Equal(t, gcNum, getNowGCNum()) // No GC

	allocator.free(memory210mb)
	allocator.free(memory100mb)
	runtime.GC()
	// Trigger GC in 80% again
	time.Sleep(500 * time.Millisecond)
	require.Equal(t, GlobalMemoryLimitTuner.calcMemoryLimit(), debug.SetMemoryLimit(-1))
	time.Sleep(100 * time.Millisecond)
	gcNum = getNowGCNum()
	checkNextGCEqualMemoryLimit()
	memory210mb = allocator.alloc(210 << 20)
	time.Sleep(100 * time.Millisecond)
	require.True(t, gcNum < getNowGCNum())
	allocator.free(memory210mb)
	allocator.free(memory600mb)
}
