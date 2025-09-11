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
	"github.com/pingcap/tidb/pkg/util/hack"
)

// MemAwareMap is a map which is aware of its memory usage. It's adapted from SetWithMemoryUsage.
// It doesn't support delete.
// The estimate usage of memory is usually smaller than the real usage.
// According to experiments with SetWithMemoryUsage, 2/3 * estimated usage <= real usage <= estimated usage.
type MemAwareMap[K comparable, V any] = hack.MemAwareMap[K, V]

// NewMemAwareMap creates a new MemAwareMap.
func NewMemAwareMap[K comparable, V any]() (res MemAwareMap[K, V]) {
	res.Init(make(map[K]V))
	return res
}

// NewMemAwareMapWithCap creates a new MemAwareMap with the given capacity.
func NewMemAwareMapWithCap[K comparable, V any](capacity int) (res MemAwareMap[K, V]) {
	res.Init(make(map[K]V, capacity))
	return res
}
