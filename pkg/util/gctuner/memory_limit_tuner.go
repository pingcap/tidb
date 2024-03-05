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
	"runtime/debug"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/intest"
	"github.com/pingcap/tidb/pkg/util/memory"
	atomicutil "go.uber.org/atomic"
)

// GlobalMemoryLimitTuner only allow one memory limit tuner in one process
var GlobalMemoryLimitTuner = &memoryLimitTuner{}

// Go runtime trigger GC when hit memory limit which managed via runtime/debug.SetMemoryLimit.
// So we can change memory limit dynamically to avoid frequent GC when memory usage is greater than the limit.
type memoryLimitTuner struct {
	finalizer                    *finalizer
	isValidValueSet              atomicutil.Bool
	percentage                   atomicutil.Float64
	adjustPercentageInProgress   atomicutil.Bool
	serverMemLimitBeforeAdjust   atomicutil.Uint64
	percentageBeforeAdjust       atomicutil.Float64
	nextGCTriggeredByMemoryLimit atomicutil.Bool

	// The flag to disable memory limit adjust. There might be many tasks need to activate it in future,
	// so it is integer type.
	adjustDisabled atomicutil.Int64
}

// fallbackPercentage indicates the fallback memory limit percentage when turning.
const fallbackPercentage float64 = 1.1

var memoryGoroutineCntInTest = *atomicutil.NewInt64(0)

// WaitMemoryLimitTunerExitInTest is used to wait memory limit tuner exit in test.
func WaitMemoryLimitTunerExitInTest() {
	if intest.InTest {
		for memoryGoroutineCntInTest.Load() > 0 {
			time.Sleep(100 * time.Millisecond)
		}
	}
}

// DisableAdjustMemoryLimit makes memoryLimitTuner directly return `initGOMemoryLimitValue` when function `calcMemoryLimit` is called.
func (t *memoryLimitTuner) DisableAdjustMemoryLimit() {
	t.adjustDisabled.Add(1)
	debug.SetMemoryLimit(initGOMemoryLimitValue)
}

// EnableAdjustMemoryLimit makes memoryLimitTuner return an adjusted memory limit when function `calcMemoryLimit` is called.
func (t *memoryLimitTuner) EnableAdjustMemoryLimit() {
	t.adjustDisabled.Add(-1)
	t.UpdateMemoryLimit()
}

// tuning check the memory nextGC and judge whether this GC is trigger by memory limit.
// Go runtime ensure that it will be called serially.
func (t *memoryLimitTuner) tuning() {
	if !t.isValidValueSet.Load() {
		return
	}
	r := memory.ForceReadMemStats()
	gogc := util.GetGOGC()
	ratio := float64(100+gogc) / 100
	// This `if` checks whether the **last** GC was triggered by MemoryLimit as far as possible.
	// If the **last** GC was triggered by MemoryLimit, we'll set MemoryLimit to MAXVALUE to return control back to GOGC
	// to avoid frequent GC when memory usage fluctuates above and below MemoryLimit.
	// The logic we judge whether the **last** GC was triggered by MemoryLimit is as follows:
	// suppose `NextGC` = `HeapInUse * (100 + GOGC) / 100)`,
	// - If NextGC < MemoryLimit, the **next** GC will **not** be triggered by MemoryLimit thus we do not care about
	//   why the **last** GC is triggered. And MemoryLimit will not be reset this time.
	// - Only if NextGC >= MemoryLimit , the **next** GC will be triggered by MemoryLimit. Thus, we need to reset
	//   MemoryLimit after the **next** GC happens if needed.
	if float64(r.HeapInuse)*ratio > float64(debug.SetMemoryLimit(-1)) {
		if t.nextGCTriggeredByMemoryLimit.Load() && t.adjustPercentageInProgress.CompareAndSwap(false, true) {
			// It's ok to update `adjustPercentageInProgress`, `serverMemLimitBeforeAdjust` and `percentageBeforeAdjust` not in a transaction.
			// The update of memory limit is eventually consistent.
			t.serverMemLimitBeforeAdjust.Store(memory.ServerMemoryLimit.Load())
			t.percentageBeforeAdjust.Store(t.GetPercentage())
			go func() {
				if intest.InTest {
					memoryGoroutineCntInTest.Inc()
					defer memoryGoroutineCntInTest.Dec()
				}
				memory.MemoryLimitGCLast.Store(time.Now())
				memory.MemoryLimitGCTotal.Add(1)
				debug.SetMemoryLimit(t.calcMemoryLimit(fallbackPercentage))
				resetInterval := 1 * time.Minute // Wait 1 minute and set back, to avoid frequent GC
				if intest.InTest {
					resetInterval = 3 * time.Second
				}
				failpoint.Inject("mockUpdateGlobalVarDuringAdjustPercentage", func(val failpoint.Value) {
					if val, ok := val.(bool); val && ok {
						time.Sleep(300 * time.Millisecond)
						t.UpdateMemoryLimit()
					}
				})
				failpoint.Inject("testMemoryLimitTuner", func(val failpoint.Value) {
					if val, ok := val.(bool); val && ok {
						resetInterval = 1 * time.Second
					}
				})
				time.Sleep(resetInterval)
				debug.SetMemoryLimit(t.calcMemoryLimit(t.GetPercentage()))
				for !t.adjustPercentageInProgress.CompareAndSwap(true, false) {
					continue
				}
			}()
			memory.TriggerMemoryLimitGC.Store(true)
		}
		t.nextGCTriggeredByMemoryLimit.Store(true)
	} else {
		t.nextGCTriggeredByMemoryLimit.Store(false)
		memory.TriggerMemoryLimitGC.Store(false)
	}
}

// Start starts the memory limit tuner.
func (t *memoryLimitTuner) Start() {
	t.finalizer = newFinalizer(t.tuning) // Start tuning
}

// Stop stops the memory limit tuner.
func (t *memoryLimitTuner) Stop() {
	t.finalizer.stop()
}

// SetPercentage set the percentage for memory limit tuner.
func (t *memoryLimitTuner) SetPercentage(percentage float64) {
	t.percentage.Store(percentage)
}

// GetPercentage get the percentage from memory limit tuner.
func (t *memoryLimitTuner) GetPercentage() float64 {
	return t.percentage.Load()
}

// UpdateMemoryLimit updates the memory limit.
// This function should be called when `tidb_server_memory_limit` or `tidb_server_memory_limit_gc_trigger` is modified.
func (t *memoryLimitTuner) UpdateMemoryLimit() {
	if t.adjustPercentageInProgress.Load() {
		if t.serverMemLimitBeforeAdjust.Load() == memory.ServerMemoryLimit.Load() && t.percentageBeforeAdjust.Load() == t.GetPercentage() {
			return
		}
	}
	var memoryLimit = t.calcMemoryLimit(t.GetPercentage())
	if memoryLimit == math.MaxInt64 {
		t.isValidValueSet.Store(false)
		memoryLimit = initGOMemoryLimitValue
	} else {
		t.isValidValueSet.Store(true)
	}
	debug.SetMemoryLimit(memoryLimit)
}

func (t *memoryLimitTuner) calcMemoryLimit(percentage float64) int64 {
	if t.adjustDisabled.Load() > 0 {
		return initGOMemoryLimitValue
	}
	memoryLimit := int64(float64(memory.ServerMemoryLimit.Load()) * percentage) // `tidb_server_memory_limit` * `tidb_server_memory_limit_gc_trigger`
	if memoryLimit == 0 {
		memoryLimit = math.MaxInt64
	}
	return memoryLimit
}

var initGOMemoryLimitValue int64

func init() {
	initGOMemoryLimitValue = debug.SetMemoryLimit(-1)
	GlobalMemoryLimitTuner.Start()
}
