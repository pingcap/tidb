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

// NewMemAwareMap creates a new MemAwareMap.
func NewMemAwareMap[K comparable, V any](sizeOfKeyAndValue uint64) MemAwareMap[K, V] {
	return MemAwareMap[K, V]{
		m:                 make(map[K]V),
		bInMap:            0,
		bucketMemoryUsage: 8*(1+sizeOfKeyAndValue) + 16,
	}
}

// Get the value of the key.
func (m *MemAwareMap[K, V]) Get(k K) (v V, ok bool) {
	v, ok = m.m[k]
	return
}

// Set the value of the key.
// `extraSizeOfKeyAndValue` indicates any extra memory consumed by the key and the value, besides what's included
// in `sizeOfKeyAndValue` specified when creating the map.
func (m *MemAwareMap[K, V]) Set(k K, v V, extraSizeOfKeyAndValue uint64) (memDelta int64) {
	m.m[k] = v
	if len(m.m) > (1<<m.bInMap)*hack.LoadFactorNum/hack.LoadFactorDen {
		memDelta = int64(m.bucketMemoryUsage * (1 << m.bInMap))
		m.bInMap++
	}
	return memDelta + int64(extraSizeOfKeyAndValue)
}

// Len returns the number of elements in the map.
func (m *MemAwareMap[K, V]) Len() int {
	return len(m.m)
}
