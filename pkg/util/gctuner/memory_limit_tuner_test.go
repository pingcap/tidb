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
	"runtime"
	"runtime/debug"
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/util/memory"
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
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/util/gctuner/testMemoryLimitTuner", "return(true)"))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/util/gctuner/testMemoryLimitTuner"))
	}()
	// Close GOGCTuner
	gogcTuner := EnableGOGCTuner.Load()
	EnableGOGCTuner.Store(false)
	defer EnableGOGCTuner.Store(gogcTuner)

	memory.ServerMemoryLimit.Store(1 << 30)   // 1GB
	GlobalMemoryLimitTuner.SetPercentage(0.8) // 1GB * 80% = 800MB
	GlobalMemoryLimitTuner.UpdateMemoryLimit()
	require.True(t, GlobalMemoryLimitTuner.isValidValueSet.Load())
	defer func() {
		// If test.count > 1, wait tuning finished.
		require.Eventually(t, func() bool {
			//nolint: all_revive
			return GlobalMemoryLimitTuner.isValidValueSet.Load()
		}, 5*time.Second, 100*time.Millisecond)
		require.Eventually(t, func() bool {
			//nolint: all_revive
			return !GlobalMemoryLimitTuner.adjustPercentageInProgress.Load()
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
		memoryLimit := GlobalMemoryLimitTuner.calcMemoryLimit(GlobalMemoryLimitTuner.GetPercentage())
		// Refer to golang source code, nextGC = memoryLimit - nonHeapMemory - overageMemory - headroom
		require.True(t, nextGC < uint64(memoryLimit))
	}

	memory600mb := allocator.alloc(600 << 20)
	gcNum := getNowGCNum()

	memory210mb := allocator.alloc(210 << 20)
	require.Eventually(t, func() bool {
		return GlobalMemoryLimitTuner.adjustPercentageInProgress.Load() && gcNum < getNowGCNum()
	}, 5*time.Second, 100*time.Millisecond)
	// Test waiting for reset
	require.Eventually(t, func() bool {
		return GlobalMemoryLimitTuner.calcMemoryLimit(fallbackPercentage) == debug.SetMemoryLimit(-1)
	}, 5*time.Second, 100*time.Millisecond)
	gcNum = getNowGCNum()
	memory100mb := allocator.alloc(100 << 20)
	require.Eventually(t, func() bool {
		return gcNum == getNowGCNum()
	}, 5*time.Second, 100*time.Millisecond) // No GC

	allocator.free(memory210mb)
	allocator.free(memory100mb)
	runtime.GC()
	// Trigger GC in 80% again
	require.Eventually(t, func() bool {
		return GlobalMemoryLimitTuner.calcMemoryLimit(GlobalMemoryLimitTuner.GetPercentage()) == debug.SetMemoryLimit(-1)
	}, 5*time.Second, 100*time.Millisecond)
	time.Sleep(100 * time.Millisecond)
	gcNum = getNowGCNum()
	checkNextGCEqualMemoryLimit()
	memory210mb = allocator.alloc(210 << 20)
	require.Eventually(t, func() bool {
		return gcNum < getNowGCNum()
	}, 5*time.Second, 100*time.Millisecond)
	allocator.free(memory210mb)
	allocator.free(memory600mb)
}

func TestIssue48741(t *testing.T) {
	// Close GOGCTuner
	gogcTuner := EnableGOGCTuner.Load()
	EnableGOGCTuner.Store(false)
	defer EnableGOGCTuner.Store(gogcTuner)

	getMemoryLimitGCTotal := func() int64 {
		return memory.MemoryLimitGCTotal.Load()
	}

	waitingTunningFinishFn := func() {
		for GlobalMemoryLimitTuner.adjustPercentageInProgress.Load() {
			time.Sleep(10 * time.Millisecond)
		}
	}

	allocator := &mockAllocator{}
	defer allocator.freeAll()

	checkIfMemoryLimitIsModified := func() {
		// Try to trigger GC by 1GB * 80% = 800MB (tidb_server_memory_limit * tidb_server_memory_limit_gc_trigger)
		gcNum := getMemoryLimitGCTotal()
		memory810mb := allocator.alloc(810 << 20)
		require.Eventually(t,
			// Wait for the GC triggered by memory810mb
			func() bool {
				return GlobalMemoryLimitTuner.adjustPercentageInProgress.Load() && gcNum < getMemoryLimitGCTotal()
			},
			500*time.Millisecond, 100*time.Millisecond)

		// update memoryLimit, and sleep 500ms, let t.UpdateMemoryLimit() be called.
		memory.ServerMemoryLimit.Store(1500 << 20) // 1.5 GB
		time.Sleep(500 * time.Millisecond)
		// UpdateMemoryLimit success during tunning.
		require.True(t, GlobalMemoryLimitTuner.adjustPercentageInProgress.Load())
		require.Equal(t, debug.SetMemoryLimit(-1), int64(1500<<20*80/100))
		waitingTunningFinishFn()
		// After the GC triggered by memory810mb.
		gcNumAfterMemory810mb := getMemoryLimitGCTotal()

		memory200mb := allocator.alloc(200 << 20)
		time.Sleep(2 * time.Second)
		// The heapInUse is less than 1.5GB * 80% = 1.2GB, so the gc will not be triggered.
		require.Equal(t, gcNumAfterMemory810mb, getMemoryLimitGCTotal())

		memory300mb := allocator.alloc(300 << 20)
		require.Eventually(t,
			// Wait for the GC triggered by memory300mb
			func() bool {
				return GlobalMemoryLimitTuner.adjustPercentageInProgress.Load() && gcNumAfterMemory810mb < getMemoryLimitGCTotal()
			},
			5*time.Second, 100*time.Millisecond)

		// Sleep 500ms, let t.UpdateMemoryLimit() be called.
		time.Sleep(500 * time.Millisecond)
		// The memory limit will be 1.5GB * 110% during tunning.
		require.Equal(t, debug.SetMemoryLimit(-1), int64(1500<<20*110/100))
		require.True(t, GlobalMemoryLimitTuner.adjustPercentageInProgress.Load())

		allocator.free(memory810mb)
		allocator.free(memory200mb)
		allocator.free(memory300mb)
	}

	checkIfMemoryLimitNotModified := func() {
		// Try to trigger GC by 1GB * 80% = 800MB (tidb_server_memory_limit * tidb_server_memory_limit_gc_trigger)
		gcNum := getMemoryLimitGCTotal()
		memory810mb := allocator.alloc(810 << 20)
		require.Eventually(t,
			// Wait for the GC triggered by memory810mb
			func() bool {
				return GlobalMemoryLimitTuner.adjustPercentageInProgress.Load() && gcNum < getMemoryLimitGCTotal()
			},
			500*time.Millisecond, 100*time.Millisecond)

		// During the process of adjusting the percentage, the memory limit will be set to 1GB * 110% = 1.1GB.
		require.Equal(t, debug.SetMemoryLimit(-1), int64(1<<30*110/100))

		gcNumAfterMemory810mb := getMemoryLimitGCTotal()
		// After the GC triggered by memory810mb.
		waitingTunningFinishFn()

		require.Eventually(t,
			// The GC will be trigged immediately after memoryLimit is set back to 1GB * 80% = 800MB.
			func() bool {
				return GlobalMemoryLimitTuner.adjustPercentageInProgress.Load() && gcNumAfterMemory810mb < getMemoryLimitGCTotal()
			},
			2*time.Second, 100*time.Millisecond)

		allocator.free(memory810mb)
	}

	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/util/gctuner/mockUpdateGlobalVarDuringAdjustPercentage", "return(true)"))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/util/gctuner/mockUpdateGlobalVarDuringAdjustPercentage"))
	}()

	memory.ServerMemoryLimit.Store(1 << 30)   // 1GB
	GlobalMemoryLimitTuner.SetPercentage(0.8) // 1GB * 80% = 800MB
	GlobalMemoryLimitTuner.UpdateMemoryLimit()
	require.Equal(t, debug.SetMemoryLimit(-1), int64(1<<30*80/100))

	checkIfMemoryLimitNotModified()
	waitingTunningFinishFn()
	checkIfMemoryLimitIsModified()
}

func TestSetMemoryLimit(t *testing.T) {
	GlobalMemoryLimitTuner.DisableAdjustMemoryLimit()
	memory.ServerMemoryLimit.Store(1 << 30)   // 1GB
	GlobalMemoryLimitTuner.SetPercentage(0.8) // 1GB * 80% = 800MB
	GlobalMemoryLimitTuner.UpdateMemoryLimit()
	require.Equal(t, initGOMemoryLimitValue, debug.SetMemoryLimit(-1))
	GlobalMemoryLimitTuner.EnableAdjustMemoryLimit()
	GlobalMemoryLimitTuner.UpdateMemoryLimit()
	require.Equal(t, int64(1<<30*80/100), debug.SetMemoryLimit(-1))
}
