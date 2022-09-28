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
	"sync/atomic"
	"time"

	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/memory"
	atomicutil "go.uber.org/atomic"
)

// GlobalMemoryLimitTuner only allow one memory limit tuner in one process
var GlobalMemoryLimitTuner = &memoryLimitTuner{}

// Go runtime trigger GC when hit memory limit which managed via runtime/debug.SetMemoryLimit.
// So we can change memory limit dynamically to avoid frequent GC when memory usage is greater than the soft limit.
type memoryLimitTuner struct {
	finalizer  *finalizer
	isTuning   atomic.Bool
	percentage atomicutil.Float64
	coolDown   atomic.Bool
	times      int
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
	// If theoretical NextGC(Equivalent to HeapInUse * (100 + GOGC) / 100) is bigger than MemoryLimit twice in a row,
	// it means that the second GC is caused by the memory limit
	if r.HeapInuse > uint64(float64(debug.SetMemoryLimit(-1))/ratio) {
		t.times++
		if t.times >= 2 && t.coolDown.CompareAndSwap(false, true) {
			t.times = 0
			go func() {
				debug.SetMemoryLimit(math.MaxInt)
				time.Sleep(1 * time.Minute) // 1 minute to cool down, to avoid frequent GC
				debug.SetMemoryLimit(t.calcSoftMemoryLimit())
				for !t.coolDown.CompareAndSwap(true, false) {
					continue
				}
			}()
		}
	} else {
		t.times = 0
	}
}

func (t *memoryLimitTuner) init() {
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
	softLimit := t.calcSoftMemoryLimit()
	if softLimit == math.MaxInt64 {
		t.isTuning.Store(false)
	} else {
		t.isTuning.Store(true)
	}
	debug.SetMemoryLimit(softLimit)
}

func (t *memoryLimitTuner) calcSoftMemoryLimit() int64 {
	softLimit := int64(float64(memory.ServerMemoryLimit.Load()) * t.percentage.Load()) // `tidb_server_memory_limit` * `tidb_server_memory_limit_gc_trigger`
	if softLimit == 0 {
		softLimit = math.MaxInt64
	}
	return softLimit
}

func init() {
	GlobalMemoryLimitTuner.init()
}
