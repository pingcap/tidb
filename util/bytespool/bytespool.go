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

package bytespool

import (
	"fmt"
	"reflect"
	"runtime"
	"sync"
	"unsafe"

	"github.com/pingcap/log"
)

// Bpool is a pool of sync.Pools of different sizes.
type Bpool struct {
	pools   []sync.Pool
	roundTo int  // round-up requested size when looking for the pool
	minSz   int  // minimum size, rounded
	maxSz   int  // maximum size, not rounded
	alloc   bool // if true, will alloc missing or oversize
}

// Init initialises the []byte pool.
// The parameters are: the minimum alloc size, the maximum size and
// the round-up factor. The created pool array will have (max-min)/roundto
// elements.
// Returns true on success.
func (bp *Bpool) Init(minSz, maxSz, roundTo int) bool {
	if roundTo <= 0 || maxSz <= 0 || maxSz < minSz {
		// invalid parameters
		return false
	}
	if len(bp.pools) != 0 {
		// already init...
		return false
	}
	bp.minSz = minSz
	bp.maxSz = maxSz
	bp.roundTo = roundTo
	poolsNo := szPoolIdx(maxSz, roundTo, minSz) + 1
	bp.pools = make([]sync.Pool, poolsNo)
	return true
}

// Get tries to allocate a byte slice from the pool.
// It returns the byte slice with len == sz on success and
// true if it was found in the pool or false if not and alloc == true
// (was allocated).
// On error it returns nil, false (not found in pool or size too big and
// alloc == false).
// For 0 size it returns always nil, true.
func (bp *Bpool) Get(sz int, alloc bool) ([]byte, bool) {
	if sz == 0 {
		// 0 length always succeeds
		return nil, true
	}
	if sz > bp.maxSz {
		if alloc {
			return make([]byte, sz), true
		}
		return nil, false
	}
	i := szPoolIdx(sz, bp.roundTo, bp.minSz)
	if i >= len(bp.pools) || i < 0 {
		log.Error(fmt.Sprintf("pool idx exceeds pool no: %d >= %d,"+
			" sz = %d  roundTo %d minSz %d\n",
			i, len(bp.pools), sz, bp.roundTo, bp.minSz))
		return nil, false
	}
	p, _ := bp.pools[i].Get().(unsafe.Pointer)
	s, max := idxSzRange(i, bp.roundTo, bp.minSz)
	if (sz > bp.minSz && sz < s) || sz > max {
		log.Error(fmt.Sprintf("size does not match: sz %d, pool idx %d"+
			" => range [%d, %d), min %d max %d round %d\n",
			sz, i, s, max, bp.minSz, bp.maxSz, bp.roundTo))
		return nil, false
	}
	if p == nil {
		if alloc {
			return make([]byte, sz, max), true
		}
		return nil, false
	}
	var buf []byte
	slice := (*reflect.SliceHeader)(unsafe.Pointer(&buf))
	slice.Data = uintptr(p)
	slice.Len = sz
	slice.Cap = max
	runtime.KeepAlive(p)
	return buf, true
}

// Put puts a bytes slice in the pool.
// It returns true if it succeeded or false if it failed
// (0 length slice or size too big)
func (bp *Bpool) Put(buf []byte) bool {
	if cap(buf) == 0 || len(buf) > bp.maxSz {
		return false
	}
	i := szPoolIdx(cap(buf), bp.roundTo, bp.minSz)
	if i >= len(bp.pools) || i < 0 {
		log.Error(fmt.Sprintf("bad pool idx : %d  pool entries %d,"+
			" sz = %d (len %d)  roundTo %d minSz %d\n",
			i, len(bp.pools), cap(buf), len(buf), bp.roundTo, bp.minSz))
		return false
	}
	bp.pools[i].Put(unsafe.Pointer(&buf[0]))
	return true
}

// szRoundUp will return sz rounded up to a multiple of roundTo.
//
//	0 will be rounded-up to roundTo
func szRoundUp(sz, roundTo int) int {
	return ((sz-1)/roundTo + 1) * roundTo
}

// szPoolIdx returns the pool index for sz, rounded up to a multiple of
// roundTo and with a minimal size of minSz.
// For 0 sizes it returns -1 (there's no pool for 0).
func szPoolIdx(sz, roundTo, minSz int) int {
	if sz <= minSz || sz <= 0 {
		if sz <= 0 {
			// 0  alloc
			return -1
		}
		// <= minSz
		return 0
	}
	diff := sz - minSz
	// return round down diff / roundTo
	// ( idx 0 == minSz+1, minSz + roundTo,
	// 1 == minSz +1 + roundTo, minsz + 2* roundTo  ... )
	return (diff - 1) / roundTo
}

func idxSzRange(idx, roundTo, minSz int) (size int, max int) {
	if idx < 0 {
		return 0, 0
	}
	// pool[0] = minSz+1 - minsSz+roundTo
	max = (idx+1)*roundTo + minSz
	return max - roundTo + 1, max
}
