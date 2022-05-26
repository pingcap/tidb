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

package set

import (
	"math"
	"unsafe"

	"github.com/pingcap/tidb/util/hack"
)

// MemAwareMap is a map which is aware of its memory usage. It's adapted from SetWithMemoryUsage.
// It doesn't support delete.
// The estimate usage of memory is usually smaller than the real usage.
// According to experiments with SetWithMemoryUsage, in worst case the maximum bias is 50%, i.e. real usage <= 1.5 * estimated usage.
type MemAwareMap[K comparable, V any] struct {
	m                 map[K]V
	bInMap            int64
	bucketMemoryUsage uint64
}

// EstimateBucketMemoryUsage returns the estimated memory usage of a bucket in a map.
func EstimateBucketMemoryUsage(sizeofKeyAndValue uint64) uint64 {
	return 8*(1+sizeofKeyAndValue) + 16
}

// EstimateMapSize returns the estimated size of the map. It doesn't include the dynamic part, e.g. objects pointed to by pointers in the map.
// len(map) <= load_factor * 2^bInMap. bInMap = ceil(log2(len(map)/load_factor)).
// memory = bucketSize * 2^bInMap
func EstimateMapSize(len int, bucketSize uint64) uint64 {
	if len == 0 {
		return 0
	}
	bInMap := uint64(math.Ceil(math.Log2(float64(len) * hack.LoadFactorDen / hack.LoadFactorNum)))
	return bucketSize * uint64(1<<bInMap)
}

// NewMemAwareMap creates a new MemAwareMap.
func NewMemAwareMap[K comparable, V any]() MemAwareMap[K, V] {
	return MemAwareMap[K, V]{
		m:                 make(map[K]V),
		bInMap:            0,
		bucketMemoryUsage: EstimateBucketMemoryUsage(uint64(unsafe.Sizeof(*new(K)) + unsafe.Sizeof(*new(V)))),
	}
}

// Get the value of the key.
func (m *MemAwareMap[K, V]) Get(k K) (v V, ok bool) {
	v, ok = m.m[k]
	return
}

// Set the value of the key.
func (m *MemAwareMap[K, V]) Set(k K, v V) (memDelta int64) {
	m.m[k] = v
	if len(m.m) > (1<<m.bInMap)*hack.LoadFactorNum/hack.LoadFactorDen {
		memDelta = int64(m.bucketMemoryUsage * (1 << m.bInMap))
		m.bInMap++
	}
	return memDelta
}

// Len returns the number of elements in the map.
func (m *MemAwareMap[K, V]) Len() int {
	return len(m.m)
}
