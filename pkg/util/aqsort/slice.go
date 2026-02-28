// Copyright 2026 PingCAP, Inc.
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

package aqsort

import "sort"

// SliceConfig configures AQSort integration for generic slice sorting.
type SliceConfig[T any] struct {
	// Enabled returns whether AQSort should be attempted.
	Enabled func() bool
	// EncodeKey returns the byte-orderable key for an item. The index is provided
	// so callers can implement periodic checkpoints.
	EncodeKey func(int, T) ([]byte, error)
	// Disable is called when AQSort fails and fallback is required.
	Disable func(error)
	// CheckpointEvery triggers CheckpointFn every N comparisons in AQSort.
	CheckpointEvery uint
	// CheckpointFn is invoked periodically during AQSort.
	CheckpointFn func()
}

// Slice sorts items in-place. It attempts AQSort when enabled; otherwise it
// falls back to sort.Slice using the provided less function.
func Slice[T any](items []T, less func(i, j int) bool, cfg SliceConfig[T]) {
	if len(items) <= 1 {
		return
	}
	if cfg.Enabled != nil && !cfg.Enabled() {
		sort.Slice(items, less)
		return
	}
	if cfg.EncodeKey == nil {
		sort.Slice(items, less)
		return
	}

	pairs := make([]Pair[T], len(items))
	for i := range items {
		key, err := cfg.EncodeKey(i, items[i])
		if err != nil {
			if cfg.Disable != nil {
				cfg.Disable(err)
			}
			sort.Slice(items, less)
			return
		}
		pairs[i] = Pair[T]{Key: key, Val: items[i]}
	}

	var sorter PairSorter[T]
	if cfg.CheckpointEvery > 0 && cfg.CheckpointFn != nil {
		sorter.SortWithCheckpoint(pairs, cfg.CheckpointEvery, cfg.CheckpointFn)
	} else {
		sorter.Sort(pairs)
	}
	for i := range pairs {
		items[i] = pairs[i].Val
	}
}
