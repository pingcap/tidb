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
	"github.com/pingcap/tidb/pkg/util/intest"
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
	require.True(t, intest.InTest)
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
	require.True(t, intest.InTest)
	// Close GOGCTuner
	gogcTuner := EnableGOGCTuner.Load()
	EnableGOGCTuner.Store(false)
	defer EnableGOGCTuner.Store(gogcTuner)

	allocator := &mockAllocator{}
	defer allocator.freeAll()

	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/util/gctuner/testMemoryLimitTuner", "return(200)"))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/util/gctuner/testMemoryLimitTuner"))
	}()

	oldServerMemoryLimit := memory.ServerMemoryLimit.Load()
	oldPercentage := GlobalMemoryLimitTuner.GetPercentage()
	defer func() {
		memory.ServerMemoryLimit.Store(oldServerMemoryLimit)
		GlobalMemoryLimitTuner.SetPercentage(oldPercentage)
		GlobalMemoryLimitTuner.UpdateMemoryLimit()
	}()

	getMemoryLimitGCTotal := func() int64 {
		return memory.MemoryLimitGCTotal.Load()
	}

	waitingTunningFinishFn := func() {
		require.Eventually(t, func() bool {
			return !GlobalMemoryLimitTuner.adjustPercentageInProgress.Load()
		}, 3*time.Second, 10*time.Millisecond)
	}

	baseMemoryLimit := uint64(256 << 20)
	newMemoryLimit := uint64(384 << 20) // 1.5x base

	memory.ServerMemoryLimit.Store(baseMemoryLimit)
	GlobalMemoryLimitTuner.SetPercentage(0.8)
	GlobalMemoryLimitTuner.UpdateMemoryLimit()
	require.Equal(t, int64(baseMemoryLimit)*80/100, debug.SetMemoryLimit(-1))

	checkIfMemoryLimitNotModified := func() {
		gcNum := getMemoryLimitGCTotal()
		memoryLimitGCTriggerBytes := allocator.alloc(int(int64(baseMemoryLimit) * 85 / 100))
		require.Eventually(t, func() bool {
			return GlobalMemoryLimitTuner.adjustPercentageInProgress.Load() && gcNum < getMemoryLimitGCTotal()
		}, 3*time.Second, 20*time.Millisecond)

		require.Eventually(t, func() bool {
			return debug.SetMemoryLimit(-1) == int64(baseMemoryLimit)*110/100
		}, 3*time.Second, 20*time.Millisecond)

		gcNumAfterFirstGC := getMemoryLimitGCTotal()
		waitingTunningFinishFn()

		// Trigger an allocation to make the memory limit take effect.
		memoryLimitGCTriggerBytes2 := allocator.alloc(16 << 20)
		defer allocator.free(memoryLimitGCTriggerBytes2)
		require.Eventually(t, func() bool {
			return GlobalMemoryLimitTuner.adjustPercentageInProgress.Load() && gcNumAfterFirstGC < getMemoryLimitGCTotal()
		}, 3*time.Second, 20*time.Millisecond)
		waitingTunningFinishFn()

		allocator.free(memoryLimitGCTriggerBytes)
	}

	checkIfMemoryLimitIsModified := func() {
		gcNum := getMemoryLimitGCTotal()
		memoryLimitGCTriggerBytes := allocator.alloc(int(int64(baseMemoryLimit) * 85 / 100))
		require.Eventually(t, func() bool {
			return GlobalMemoryLimitTuner.adjustPercentageInProgress.Load() && gcNum < getMemoryLimitGCTotal()
		}, 3*time.Second, 20*time.Millisecond)

		memory.ServerMemoryLimit.Store(newMemoryLimit)
		GlobalMemoryLimitTuner.UpdateMemoryLimit()
		require.Equal(t, int64(newMemoryLimit)*80/100, debug.SetMemoryLimit(-1))
		waitingTunningFinishFn()

		gcNumAfterUpdate := getMemoryLimitGCTotal()
		memoryBelowLimit := allocator.alloc(32 << 20)
		require.Never(t, func() bool {
			return getMemoryLimitGCTotal() != gcNumAfterUpdate
		}, 200*time.Millisecond, 20*time.Millisecond)

		memoryExceedLimit := allocator.alloc(int(int64(newMemoryLimit) * 30 / 100))
		require.Eventually(t, func() bool {
			return GlobalMemoryLimitTuner.adjustPercentageInProgress.Load() && gcNumAfterUpdate < getMemoryLimitGCTotal()
		}, 3*time.Second, 20*time.Millisecond)

		require.Eventually(t, func() bool {
			return debug.SetMemoryLimit(-1) == int64(newMemoryLimit)*110/100
		}, 3*time.Second, 20*time.Millisecond)
		waitingTunningFinishFn()

		allocator.free(memoryLimitGCTriggerBytes)
		allocator.free(memoryBelowLimit)
		allocator.free(memoryExceedLimit)
	}

	checkIfMemoryLimitNotModified()
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
