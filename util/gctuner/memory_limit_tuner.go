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
	"os"
	"runtime"
	"runtime/debug"
	"strings"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/memory"
	atomicutil "go.uber.org/atomic"
)

// GlobalMemoryLimitTuner only allow one memory limit tuner in one process
var GlobalMemoryLimitTuner = &memoryLimitTuner{}

// Go runtime trigger GC when hit memory limit which managed via runtime/debug.SetMemoryLimit.
// So we can change memory limit dynamically to avoid frequent GC when memory usage is greater than the limit.
type memoryLimitTuner struct {
	finalizer                    *finalizer
	isTuning                     atomicutil.Bool
	percentage                   atomicutil.Float64
	waitingReset                 atomicutil.Bool
	nextGCTriggeredByMemoryLimit atomicutil.Bool
}

// tuning check the memory nextGC and judge whether this GC is trigger by memory limit.
// Go runtime ensure that it will be called serially.
func (t *memoryLimitTuner) tuning() {
	if !t.isTuning.Load() {
		return
	}
	r := &runtime.MemStats{}
	runtime.ReadMemStats(r)
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
		if t.nextGCTriggeredByMemoryLimit.Load() && t.waitingReset.CompareAndSwap(false, true) {
			go func() {
				debug.SetMemoryLimit(math.MaxInt64)
				resetInterval := 1 * time.Minute // Wait 1 minute and set back, to avoid frequent GC
				failpoint.Inject("testMemoryLimitTuner", func(val failpoint.Value) {
					if val, ok := val.(bool); val && ok {
						resetInterval = 1 * time.Second
					}
				})
				time.Sleep(resetInterval)
				debug.SetMemoryLimit(t.calcMemoryLimit())
				for !t.waitingReset.CompareAndSwap(true, false) {
					continue
				}
			}()
		}
		t.nextGCTriggeredByMemoryLimit.Store(true)
	} else {
		t.nextGCTriggeredByMemoryLimit.Store(false)
	}
}

func (t *memoryLimitTuner) start() {
	t.finalizer = newFinalizer(t.tuning) // start tuning
}

func (t *memoryLimitTuner) stop() {
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
	var memoryLimit int64 = math.MaxInt64
	if !EnableGOGCTuner.Load() {
		memoryLimit = t.calcMemoryLimit()
	}
	if memoryLimit == math.MaxInt64 {
		t.isTuning.Store(false)
	} else {
		t.isTuning.Store(true)
	}
	debug.SetMemoryLimit(memoryLimit)
}

func (t *memoryLimitTuner) calcMemoryLimit() int64 {
	memoryLimit := int64(float64(memory.ServerMemoryLimit.Load()) * t.percentage.Load()) // `tidb_server_memory_limit` * `tidb_server_memory_limit_gc_trigger`
	if memoryLimit == 0 {
		memoryLimit = math.MaxInt64
	}
	return memoryLimit
}

func init() {
	// In test, we should enable global_memory_limit_tuner by manual
	if !strings.HasSuffix(os.Args[0], ".test") {
		GlobalMemoryLimitTuner.start()
	}
}
