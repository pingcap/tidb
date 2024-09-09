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

	"github.com/pingcap/tidb/pkg/util/hack"
)

// MemAwareMap is a map which is aware of its memory usage. It's adapted from SetWithMemoryUsage.
// It doesn't support delete.
// The estimate usage of memory is usually smaller than the real usage.
// According to experiments with SetWithMemoryUsage, 2/3 * estimated usage <= real usage <= estimated usage.
type MemAwareMap[K comparable, V any] struct {
	M                 map[K]V // it's public, when callers want to directly access it, e.g. use in a for-range-loop
	bInMap            int64
	bucketMemoryUsage uint64
}

// EstimateMapSize returns the estimated size of the map. It doesn't include the dynamic part, e.g. objects pointed to by pointers in the map.
// len(map) <= load_factor * 2^bInMap. bInMap = ceil(log2(len(map)/load_factor)).
// memory = bucketSize * 2^bInMap
func EstimateMapSize(length int, bucketSize uint64) uint64 {
	if length == 0 {
		return 0
	}
	bInMap := uint64(math.Ceil(math.Log2(float64(length) * hack.LoadFactorDen / float64(hack.LoadFactorNum))))
	return bucketSize * uint64(1<<bInMap)
}

// NewMemAwareMap creates a new MemAwareMap.
func NewMemAwareMap[K comparable, V any]() MemAwareMap[K, V] {
	return MemAwareMap[K, V]{
		M:                 make(map[K]V),
		bInMap:            0,
		bucketMemoryUsage: hack.EstimateBucketMemoryUsage[K, V](),
	}
}

// Get the value of the key.
func (m *MemAwareMap[K, V]) Get(k K) (v V, ok bool) {
	v, ok = m.M[k]
	return
}

// Set the value of the key.
func (m *MemAwareMap[K, V]) Set(k K, v V) (memDelta int64) {
	m.M[k] = v
	if len(m.M) > (1<<m.bInMap)*hack.LoadFactorNum/hack.LoadFactorDen {
		memDelta = int64(m.bucketMemoryUsage * (1 << m.bInMap))
		m.bInMap++
	}
	return memDelta
}

// Len returns the number of elements in the map.
func (m *MemAwareMap[K, V]) Len() int {
	return len(m.M)
}
