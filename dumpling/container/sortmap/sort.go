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
// See the License for the specific language governing permissions and
// limitations under the License.

package sortmap

import (
	"sort"

	"golang.org/x/exp/constraints"
)

// Pair represents the KV pairs in the input map of Sort.
type Pair[K constraints.Ordered, V any] struct {
	Key   K
	Value V
}

// Sort converts an unordered golang map to a slice sorted by map key.
func Sort[K constraints.Ordered, V any](m map[K]V) []Pair[K, V] {
	s := make([]Pair[K, V], 0, len(m))
	for k, v := range m {
		s = append(s, Pair[K, V]{k, v})
	}
	sort.Slice(s, func(i, j int) bool {
		return s[i].Key < s[j].Key
	})
	return s
}
