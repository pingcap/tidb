// Copyright 2016 PingCAP, Inc.
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

package sync

import (
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/tidb/util/fastrand"
)

const (
	// number of reader slots; must be a power of two
	rslots = 4096
	// slow-down guard
	nslowdown = 9
)

// A RBMutex is a reader biased reader/writer mutual exclusion lock.
// The lock can be held by an many readers or a single writer.
// The zero value for a RBMutex is an unlocked mutex.
//
// A RBMutex must not be copied after first use.
//
// RBMutex is based on the BRAVO (Biased Locking for Reader-Writer
// Locks) algorithm: https://arxiv.org/pdf/1810.01553.pdf
//
// RBMutex is a specialized mutex for scenarios, such as caches,
// where the vast majority of locks are acquired by readers and write
// lock acquire attempts are infrequent. In such scenarios, RBMutex
// performs better than the sync.RWMutex on large multicore machines.
//
// RBMutex extends sync.RWMutex internally and uses it as the "reader
// bias disabled" fallback, so the same semantics apply. The only
// noticeable difference is in reader tokens returned from the
// RLock/RUnlock methods.
type RBMutex struct {
	readers      [rslots]int32
	rbias        int32
	inhibitUntil time.Time
	rw           sync.RWMutex
}

// RLock locks m for reading and returns a reader token. The
// token must be used in the later RUnlock call.
//
// Should not be used for recursive read locking; a blocked Lock
// call excludes new readers from acquiring the lock.
func (m *RBMutex) RLock() *uint32 {
	if atomic.LoadInt32(&m.rbias) == 1 {
		// Since rslots is a power of two, we can use & instead of %.
		t := fastrand.Uint32() & (rslots - 1)

		if atomic.CompareAndSwapInt32(&m.readers[t], 0, 1) {
			if atomic.LoadInt32(&m.rbias) == 1 {
				return &t
			}
			atomic.StoreInt32(&m.readers[t], 0)
		}
	}
	m.rw.RLock()
	if atomic.LoadInt32(&m.rbias) == 0 && time.Now().After(m.inhibitUntil) {
		atomic.StoreInt32(&m.rbias, 1)
	}
	return nil
}

// RUnlock undoes a single RLock call. A reader token obtained from
// the RLock call must be provided. RUnlock does not affect other
// simultaneous readers. A panic is raised if m is not locked for
// reading on entry to RUnlock.
func (m *RBMutex) RUnlock(t *uint32) {
	if t == nil {
		m.rw.RUnlock()
		return
	}
	if !atomic.CompareAndSwapInt32(&m.readers[*t], 1, 0) {
		panic("invalid reader state detected")
	}
}

// Lock locks m for writing. If the lock is already locked for
// reading or writing, Lock blocks until the lock is available.
func (m *RBMutex) Lock() {
	m.rw.Lock()
	if atomic.LoadInt32(&m.rbias) == 1 {
		atomic.StoreInt32(&m.rbias, 0)
		start := time.Now()
		for i := 0; i < rslots; i++ {
			for atomic.LoadInt32(&m.readers[i]) == 1 {
				runtime.Gosched()
			}
		}
		m.inhibitUntil = time.Now().Add(time.Since(start) * nslowdown)
	}
}

// Unlock unlocks m for writing. A panic is raised if m is not locked
// for writing on entry to Unlock.
//
// As with RWMutex, a locked RBMutex is not associated with a
// particular goroutine. One goroutine may RLock (Lock) a RBMutex and
// then arrange for another goroutine to RUnlock (Unlock) it.
func (m *RBMutex) Unlock() {
	m.rw.Unlock()
}
