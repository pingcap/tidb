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
)

// GlobalTuner only allow one memory limit tuner in one process
var GlobalTuner = &memoryLimitTuner{}

// Go runtime trigger GC when hit memory limit which managed via runtime/debug.SetMemoryLimit.
// So we can change memory limit dynamically to avoid frequent GC when memory usage is greater than the soft limit.
type memoryLimitTuner struct {
	finalizer *finalizer
	softLimit atomic.Uint64
	running   atomic.Bool
}

// tuning check the memory nextGC and judge whether this GC is trigger by memory limit.
// Go runtime ensure that it will be called serially.
func (t *memoryLimitTuner) tuning() {
	r := &runtime.MemStats{}
	runtime.ReadMemStats(r)
	if r.NextGC > t.softLimit.Load()/10*9 {
		if t.running.CompareAndSwap(false, true) {
			go func() {
				debug.SetMemoryLimit(math.MaxInt)
				time.Sleep(60 * time.Second)
				debug.SetMemoryLimit(int64(t.softLimit.Load()))
				for !t.running.CompareAndSwap(true, false) {
				}
			}()
		}
	}
}

func (t *memoryLimitTuner) Stop() {
	if t.finalizer != nil {
		t.finalizer.stop()
	}
}

func (t *memoryLimitTuner) SetSoftLimit(softLimit uint64) {
	t.softLimit.Store(softLimit)
}

func (t *memoryLimitTuner) GetSoftLimit() uint64 {
	return t.softLimit.Load()
}

func (t *memoryLimitTuner) Start() {
	t.Stop()
	t.finalizer = newFinalizer(t.tuning) // start tuning
}
